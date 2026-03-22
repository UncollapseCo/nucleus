#![feature(try_trait_v2)]
#![feature(associated_type_defaults)]
#![feature(async_fn_track_caller)]
#![allow(clippy::collapsible_if)] // this is a stupid lint tbh, encourages more unreadable code

use std::{
    any::Any,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use net::{
    registry::{RemoteRegistry, SystemMessage},
    remote::HandlerRegistryBuilder,
};
use pubsub::PubsubHandler;
use std::fmt::Debug;
use tokio_util::sync::CancellationToken;

// import the __private.rs file
#[cfg(feature = "static_registry")]
#[path = "private.rs"]
pub mod __private;

pub mod actor;
#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "debug")]
pub mod debug;
pub mod errors;
pub mod net;
pub mod pubsub;
pub mod spawner;
pub mod utils;

pub use nucleus_macros::handle_delayed;

pub use nucleus_macros::async_trait_nobox;
pub use nucleus_macros::handler;
#[cfg(feature = "static_registry")]
pub use nucleus_macros::{define_default_registry, define_registry};
pub use smallbox::SmallBox;
pub use utils::{ArcAwaiter, AwaitableArc, TaskAwaiter, TaskHolder, ctrl_c};

use crate::actor::message::METRICS_NODE_ID;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate nucleus_macros;

#[macro_use]
extern crate tracing;

#[macro_use]
extern crate snowflake;

#[cfg(feature = "metrics")]
mod metrics;

pub const NUCLEUS_PROTOCOL_VERSION: u8 = 1;

pub type NodeId = u16;

#[derive(Clone, Debug)]
pub struct NucleusNode {
    inner: Arc<NucleusNodeInner>,
}

impl NucleusNode {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_root<S: Any + Sync + Send + 'static>(
        id: NodeId,
        cluster: &'static str,
        state: S,
        bind_address: SocketAddr,
        public_address: Option<String>, // if none then same as bind_address
        upstream_addr: Option<String>,
        registry: HandlerRegistryBuilder,
        node_flags: impl Into<u64>,
        #[cfg(feature = "metrics")] heartbeat_interval: Option<Duration>,
    ) -> Result<Self, ()> {
        let registry = RemoteRegistry::new(registry);
        let pubsub_handler = PubsubHandler::new();

        let snowflake = Arc::new(snowflake::SnowflakeGenerator::new(id));

        let listener_cancel = CancellationToken::new();
        let listener_cancel_2 = listener_cancel.clone();

        let metrics_id = METRICS_NODE_ID.get_or_init(|| id.to_string());
        if metrics_id != &id.to_string() {
            warn!(
                "<hazard> Multiple nucleus runtimes detected within the same program execution. Metrics will only be emitted as node_id={} !",
                metrics_id
            )
        }

        let node = Self {
            inner: Arc::new(NucleusNodeInner {
                id,
                cluster,
                state: Box::new(state),
                instance_id: OnceLock::new(),
                bind_addr: bind_address,
                public_addr: public_address.unwrap_or(bind_address.to_string()),
                shutdown_token: listener_cancel,
                shutdown_finished: CancellationToken::new(),
                upstream_addr,
                registry,
                pubsub: pubsub_handler,
                snowflake,
                flags: node_flags.into(),
            }),
        };

        let node2 = node.clone();
        let raw = Arc::as_ptr(&node.inner) as *mut NucleusNodeInner;
        unsafe {
            // SAFETY: idk i dont think theres any unsoundness here, we just want a circular reference without mutexes or such inside the registry, for ergonomics
            let rawed: &mut NucleusNodeInner = raw.as_mut().unwrap();
            rawed
                .registry
                .setup(
                    node2.clone(),
                    #[cfg(feature = "metrics")]
                    heartbeat_interval,
                )
                .await;
            rawed.pubsub.setup(node2);
        }

        node.start_listener(listener_cancel_2).await.unwrap();

        node.registry().ensure_connect_upstream();

        Ok(node)
    }

    #[cfg(feature = "metrics")]
    pub fn init_telemetry() {
        metrics::init_telemetry();
    }

    pub async fn default(cluster: &'static str) -> Self {
        Self::new_root(
            0,
            cluster,
            (),
            SocketAddr::from(([127, 0, 0, 1], 8080)),
            None,
            None,
            HandlerRegistryBuilder::default(),
            0u64,
            #[cfg(feature = "metrics")]
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap()
    }

    pub fn builder(node_id: NodeId, cluster: &'static str) -> NucleusNodeBuilder<()> {
        NucleusNodeBuilder::new(node_id, cluster)
    }

    pub fn registry(&self) -> &RemoteRegistry {
        &self.inner.registry
    }
    pub(crate) fn pubsub(&self) -> &PubsubHandler {
        &self.inner.pubsub
    }
    pub fn public_addr(&self) -> &str {
        &self.inner.public_addr
    }
    pub fn node_id(&self) -> NodeId {
        self.inner.id
    }
    pub fn cluster(&self) -> &'static str {
        self.inner.cluster
    }
    pub fn instance_id(&self) -> u64 {
        *self
            .inner
            .instance_id
            .get_or_init(|| self.inner.snowflake.generate::<ActorMarker>().id())
    }
    pub fn node_flags<T: From<u64>>(&self) -> T {
        T::from(self.inner.flags)
    }
    pub fn upstream_addr(&self) -> Option<&str> {
        self.inner.upstream_addr.as_deref()
    }
    pub fn state<S: Any>(&self) -> &S {
        self.inner.state.downcast_ref().unwrap()
    }
    pub fn snowflake(&self) -> &Arc<snowflake::SnowflakeGenerator> {
        &self.inner.snowflake
    }

    pub async fn shutdown(&self) {
        if self.inner.shutdown_token.is_cancelled() {
            self.inner.shutdown_finished.cancelled().await;
            return;
        }
        info!("Shutting down node {}", self.inner.id);
        self.inner.shutdown_token.cancel();

        let shutdown_arc = AwaitableArc::new();

        self.registry()
            .publish_system_event(SystemMessage::SystemShutdown(shutdown_arc.clone()));

        shutdown_arc.wait_zero().await;

        self.inner.registry.shutdown().await;
        self.inner.pubsub.shutdown().await;

        info!("Node {} shutdown complete", self.inner.id);
        self.inner.shutdown_finished.cancel();
    }
    pub fn is_closing(&self) -> bool {
        self.inner.shutdown_token.is_cancelled()
    }

    pub async fn wait_until_shutdown(&self) {
        self.inner.shutdown_token.cancelled().await;
        self.inner.shutdown_finished.cancelled().await;
    }

    pub fn weak(&self) -> WeakNucleusNode {
        WeakNucleusNode {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

struct NucleusNodeInner {
    id: NodeId,
    cluster: &'static str,
    state: Box<dyn Any + Sync + Send + 'static>,
    instance_id: OnceLock<u64>,
    bind_addr: SocketAddr,
    public_addr: String,
    shutdown_token: CancellationToken,
    shutdown_finished: CancellationToken,
    upstream_addr: Option<String>,
    registry: RemoteRegistry,
    pubsub: PubsubHandler,
    snowflake: Arc<snowflake::SnowflakeGenerator>,
    flags: u64,
}

pub struct WeakNucleusNode {
    inner: std::sync::Weak<NucleusNodeInner>,
}

impl WeakNucleusNode {
    pub fn upgrade(&self) -> Option<NucleusNode> {
        self.inner.upgrade().map(|inner| NucleusNode { inner })
    }
}

impl Debug for NucleusNodeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NucleusNodeInner")
            .field("id", &self.id)
            .field("upstream_addr", &self.upstream_addr)
            .finish()
    }
}

pub struct NucleusNodeBuilder<S: Any + Sync + Send + 'static> {
    id: NodeId,
    cluster: &'static str,
    state: S,
    bind_address: Option<SocketAddr>,
    public_address: Option<String>, // if none then same as bind_address
    upstream_addr: Option<String>,
    registry: HandlerRegistryBuilder,
    node_flags: Option<u64>,
    #[cfg(feature = "metrics")]
    heartbeat_interval: Option<Option<Duration>>,
}

impl NucleusNodeBuilder<()> {
    pub fn new(node_id: NodeId, cluster: &'static str) -> Self {
        Self {
            id: node_id,
            cluster,
            state: (),
            bind_address: None,
            public_address: None,
            upstream_addr: None,
            registry: HandlerRegistryBuilder::default(),
            node_flags: None,
            #[cfg(feature = "metrics")]
            heartbeat_interval: None,
        }
    }
}

impl<S: Any + Sync + Send + 'static> NucleusNodeBuilder<S> {
    pub fn with_state<NS: Any + Sync + Send + 'static>(self, state: NS) -> NucleusNodeBuilder<NS> {
        NucleusNodeBuilder {
            id: self.id,
            cluster: self.cluster,
            state,
            bind_address: self.bind_address,
            public_address: self.public_address,
            upstream_addr: self.upstream_addr,
            registry: self.registry,
            node_flags: self.node_flags,
            #[cfg(feature = "metrics")]
            heartbeat_interval: self.heartbeat_interval,
        }
    }

    pub fn with_bind_address(mut self, bind_address: SocketAddr) -> Self {
        self.bind_address = Some(bind_address);
        self
    }

    pub fn with_public_address(mut self, public_address: String) -> Self {
        self.public_address = Some(public_address);
        self
    }

    pub fn with_upstream_address(mut self, upstream_addr: String) -> Self {
        self.upstream_addr = Some(upstream_addr);
        self
    }

    pub fn with_registry(mut self, registry: HandlerRegistryBuilder) -> Self {
        self.registry = registry;
        self
    }

    pub fn registry(
        mut self,
        f: impl FnOnce(HandlerRegistryBuilder) -> HandlerRegistryBuilder,
    ) -> Self {
        self.registry = f(self.registry);
        self
    }

    pub fn with_node_flags(mut self, flags: u64) -> Self {
        self.node_flags = Some(flags);
        self
    }

    #[cfg(feature = "metrics")]
    pub fn with_heartbeat_interval(mut self, interval: Option<Duration>) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    pub async fn build(self) -> Result<NucleusNode, ()> {
        NucleusNode::new_root(
            self.id,
            self.cluster,
            self.state,
            self.bind_address
                .unwrap_or(SocketAddr::from(([127, 0, 0, 1], 8080))),
            self.public_address,
            self.upstream_addr,
            self.registry,
            self.node_flags.unwrap_or(0),
            #[cfg(feature = "metrics")]
            self.heartbeat_interval
                .unwrap_or(Some(Duration::from_secs(60))),
        )
        .await
    }
}

//snowflake markers
marker!(ActorMarker);
marker!(RequestMarker);
