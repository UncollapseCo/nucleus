use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actor::RemoteRegistryActor;
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use fxhash::FxBuildHasher;
use rand::seq::SliceRandom as _;
use snowflake::BetterString;
use std::sync::Mutex;
use tokio::net::{TcpStream, lookup_host};
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use crate::actor::message::system::ActorStopMessage;
use crate::actor::message::{DeserializationError, Handler, Message, Serializable};
use crate::actor::moving::MovableActor;
use crate::actor::refs::{ActorRef, BoxedActorRef, LocalActorRef, NamedActorRef};
use crate::actor::{ActorContext, ActorRuntime};
use crate::errors::SpawnErr;
use crate::pubsub::{AnyTopic, PubsubMessage, PubsubRef, Topic};
use crate::spawner::BoxedSpawner;
use crate::utils::{ArcAwaiter, AwaitableArc, TaskHolder, join_all_unordered};
use crate::{ActorMarker, NucleusNode};
use crate::{
    NodeId,
    actor::{Actor, RemoteActor},
    snowflake::Snowflake,
};

use super::errors::{RemoteError, RemoteInitError};
use super::mover::handlers::RegisterMovable;
use super::mover::{MovableSubRef, Mover};
use super::proto::{self};
use super::remote::client_handler::ClientHandler;
use super::remote::{
    ActorMessagePairTrait, GetStatus, HandlerRegistry, HandlerRegistryBuilder, RemoteNode,
    RemoteNodeStatus, ResumeStream, ShutdownIfNotConnected,
};

pub(crate) mod actor;

type ActorType = Option<&'static str>;

#[derive(Clone)]
pub struct RemoteNodeRecord {
    inner: Arc<RemoteNodeRecordInner>,
}

struct RemoteNodeRecordInner {
    pub(crate) actor_ref: LocalActorRef<RemoteNode>,
    pub handshake: proto::core::RemoteHandshake,
    pub shutdown_arc: ArcAwaiter,
}

impl RemoteNodeRecord {
    pub fn addr(&self) -> &str {
        &self.inner.handshake.listen_address
    }
    pub fn node_flags(&self) -> u64 {
        self.inner.handshake.node_flags
    }
    pub(crate) fn actor_ref(&self) -> &LocalActorRef<RemoteNode> {
        &self.inner.actor_ref
    }
    pub fn shutdown_arc(&self) -> &ArcAwaiter {
        &self.inner.shutdown_arc
    }
    pub fn handshake(&self) -> &proto::core::RemoteHandshake {
        &self.inner.handshake
    }
}

pub struct RemoteRegistry {
    nucleus: Option<NucleusNode>,
    actor_ref: Option<(LocalActorRef<RemoteRegistryActor>, JoinHandle<()>)>,
    mover_ref: Option<LocalActorRef<Mover>>,
    latency_task: Option<JoinHandle<()>>,
    handler_registry: HandlerRegistry,
    remote_nodes: DashMap<NodeId, RemoteNodeRecord, FxBuildHasher>,
    actor_refs: DashMap<Snowflake<ActorMarker>, BoxedActorRef, FxBuildHasher>,
    named_ref_cache: DashMap<Arc<str>, BoxedActorRef, FxBuildHasher>,
    pub(crate) spawners: DashMap<String, BoxedSpawner, FxBuildHasher>,

    actor_join_handles: Mutex<TaskHolder<()>>,
}

impl RemoteRegistry {
    pub fn new(mut handler_registry: HandlerRegistryBuilder) -> Self {
        handler_registry = handler_registry
            .with_handler::<RemoteRegistryActor, proto::remote::NamedActorDiscoveryRequest>()
            .with_handler::<RemoteRegistryActor, proto::remote::RemoteNodeRegister>()
            .with_handler::<RemoteRegistryActor, proto::remote::RemoteNodeUnregister>()
            .with_handler::<RemoteRegistryActor, proto::remote::RemoteNodeHeartbeat>()
            .with_handler::<RemoteRegistryActor, proto::pubsub::PubsubSubscribe>()
            .with_handler::<RemoteRegistryActor, proto::pubsub::PubsubUnsubscribe>()
            .with_handler::<RemoteRegistryActor, proto::pubsub::PubsubMessage>()
            .with_handler::<RemoteRegistryActor, proto::pubsub::PublisherMoved>()
            .with_handler::<Mover, proto::mover::ActorMoveInIntent>()
            .with_handler::<Mover, proto::mover::ActorMoveInPayload>()
            .with_handler::<Mover, proto::mover::ActorMoveOutFinished>()
            .with_topic(SystemTopic);

        #[cfg(feature = "debug")]
        {
            handler_registry = handler_registry
                .with_handler::<RemoteRegistryActor, proto::debug::ListActors>()
                .with_debuggable_actor::<RemoteNode>()
                .with_debuggable_actor::<Mover>();
        }

        Self {
            nucleus: None,
            actor_ref: None,
            mover_ref: None,
            latency_task: None,
            handler_registry: handler_registry.build(),
            remote_nodes: DashMap::default(),
            actor_refs: DashMap::default(),
            named_ref_cache: DashMap::default(),
            spawners: DashMap::default(),

            actor_join_handles: Mutex::new(TaskHolder::new()),
        }
    }

    pub(crate) async fn setup(
        &mut self,
        nucleus: NucleusNode,
        #[cfg(feature = "metrics")] heartbeat_interval: Option<Duration>,
    ) {
        self.nucleus = Some(nucleus.clone());
        let name: Arc<str> = "System.Registry".into();
        let (r, h) = ActorRuntime::spawn(
            RemoteRegistryActor::default(),
            name.clone(),
            self.nucleus().clone(),
            "RemoteRegistry",
        )
        .await
        .unwrap();

        // this should be close to spawn_local, which however depends on self.actor_ref being set so we do it manually instead
        let actor_id = r.actor_id();
        self.actor_ref = Some((r.clone(), h));

        self.actor_refs
            .insert(actor_id, BoxedActorRef(Arc::new(r.clone())));
        self.set_local_named_actor(name, r);

        self.mover_ref = Some(
            nucleus
                .spawn_actor(
                    format!("System.Mover-{}", nucleus.node_id()),
                    Mover::new(nucleus.clone()),
                )
                .await
                .unwrap(),
        );

        #[cfg(feature = "metrics")]
        self.periodic_latency_check(&nucleus, heartbeat_interval);
    }

    #[cfg(feature = "metrics")]
    fn periodic_latency_check(
        &mut self,
        nucleus: &NucleusNode,
        heartbeat_interval: Option<Duration>,
    ) {
        use tracing::Instrument as _;

        if heartbeat_interval.is_none() {
            warn!("heartbeat interval is not set, latency checks will not run");
            return;
        }

        let heartbeat_interval = heartbeat_interval.unwrap();

        let weak = nucleus.weak();

        self.latency_task = Some(tokio::spawn(
            async move {
                loop {
                    tokio::time::sleep(heartbeat_interval).await;

                    if let Some(nucleus) = weak.upgrade() {
                        if nucleus.is_closing() {
                            break;
                        }

                        nucleus
                            .registry()
                            .actor_refs
                            .iter()
                            .for_each(|r| r.0.notify_latency_msg());
                    } else {
                        break;
                    }
                }
            }
            .in_current_span(),
        ));
    }

    fn nucleus(&self) -> &NucleusNode {
        self.nucleus
            .as_ref()
            .expect("RemoteRegistry .nucleus() was called before setup")
    }
    pub(crate) fn aref(&self) -> &LocalActorRef<RemoteRegistryActor> {
        &self
            .actor_ref
            .as_ref()
            .expect("RemoteRegistry .aref() was called before setup")
            .0
    }
    pub(crate) fn mover(&self) -> &LocalActorRef<Mover> {
        self.mover_ref
            .as_ref()
            .expect("RemoteRegistry .mover() was called before setup")
    }

    #[instrument(skip(self, stream), fields(node_id = self.nucleus().node_id()))]
    pub(crate) async fn new_remote_received(&self, stream: TcpStream) {
        // let peer_addr = stream.peer_addr().unwrap();

        let remote = RemoteNode::new_received(self.nucleus().clone(), stream).await;
        if let Err(e) = remote {
            if !matches!(e, RemoteError::Resumed) {
                error!("error handshaking received remote node - {:?}", e);
            }
            return;
        }
        let remote = remote.unwrap();
        if let Err(e) = self.ensure_nonduped_remote(remote.handshake()).await {
            error!(
                "remote node id {} already connected, dropping stream - {e:?}",
                remote.node_id()
            );
            return;
        }

        if let Err(e) = self
            .nucleus()
            .spawn_actor_local(format!("System.Remote-{}", remote.node_id()), remote)
            .await
        {
            error!("spawning remote node errored (recv) - {:?}", e);
        }
    }

    #[instrument(skip(self, addr), fields(node_id = self.nucleus().node_id()))]
    pub(crate) async fn connect_remote(&self, addr: String) -> Result<(), RemoteInitError> {
        if addr == self.nucleus().public_addr() {
            // dont connect to self
            return Err(RemoteInitError::IsSelf);
        }

        if let Some(remote) = self.remote_nodes.iter().find(|r| r.value().addr() == addr) {
            if let Ok(s) = remote.actor_ref().send_eager(GetStatus {}) {
                // if we are already connected to this address, but the remote node is reconnecting or shutting down, wait for it to finish but then connect normally
                let aref = remote.actor_ref().clone();
                let waiter = remote.shutdown_arc().clone();
                drop(remote);
                if matches!(s.await, RemoteNodeStatus::Connected) {
                    return Err(RemoteInitError::AddrAlreadyConnected);
                }

                let _ = aref.notify(ShutdownIfNotConnected {});

                waiter.await;
            }
        }

        let remote_ips: Vec<SocketAddr> = lookup_host(addr)
            .await
            .map_err(|_| RemoteInitError::LookupFailed)?
            .collect();

        // subtract connected hosts from remote_ips
        let available_hosts: Vec<SocketAddr> = remote_ips
            .into_iter()
            .filter(|ip| !self.remote_nodes.iter().any(|r| r.addr() == ip.to_string()))
            .collect();

        // choose one randomly
        let addr = match available_hosts.len() {
            0 => {
                return Err(RemoteInitError::AddrAlreadyConnected);
            }
            1 => available_hosts[0],
            _ => *available_hosts.choose(&mut rand::thread_rng()).unwrap(),
        };

        let remote = RemoteNode::new_connect(self.nucleus().clone(), addr).await;
        if let Err(e) = remote {
            error!(
                "error handshaking while connecting to remote node - {:?}",
                e
            );
            return Err(RemoteInitError::ErrorHandshaking(e));
        }
        let remote = remote.unwrap();

        self.ensure_nonduped_remote(remote.handshake()).await?;

        if let Err(e) = self
            .nucleus()
            .spawn_actor_local(format!("System.Remote-{}", remote.node_id()), remote)
            .await
        {
            error!("spawning remote node errored (connect) - {:?}", e);
            return Err(RemoteInitError::ActorSpawnError);
        }

        Ok(())
    }

    async fn ensure_nonduped_remote(
        &self,
        handshake: &proto::core::RemoteHandshake,
    ) -> Result<(), RemoteInitError> {
        let node_id = handshake.node_id as NodeId;
        if node_id == self.nucleus().node_id() {
            error!("connected and handshaked with self!!");
            return Err(RemoteInitError::IsSelf);
        }

        if let Some(remote) = self.get_remote_node(node_id) {
            match remote.actor_ref().send(ShutdownIfNotConnected {}).await {
                Ok((status, awaiter)) => {
                    if !matches!(status, RemoteNodeStatus::Connected) {
                        // its shutting down, wait for it to finish
                        drop(remote); // this has a clone of the ArcAwaiter, need to drop before awaiting for zero otherwise deadlocks
                        awaiter.await;
                        Ok(())
                    } else {
                        error!("remote node id {} already connected", node_id);

                        if remote.handshake().instance_id != handshake.instance_id {
                            error!(
                                "<hazard> remote node id {} already connected with different instance id, possible duplicated node id - current: {}, new: {}",
                                node_id,
                                remote.handshake().instance_id,
                                handshake.instance_id
                            );
                        }

                        Err(RemoteInitError::NodeIdAlreadyConnected)
                    }
                }
                Err(_) => {
                    // remote mode channel is closed, which means its in the process of stopping
                    // wait for the arcawaiter to finish
                    remote.shutdown_arc().clone().await;
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self, stream), fields(node_id = self.nucleus().node_id()))]
    pub(crate) async fn resume_remote(&self, resume: proto::core::RemoteResume, stream: TcpStream) {
        // get node id from resume message
        let remote_ref = self.get_remote_node(resume.node_id as NodeId);
        if let Some(remote_ref) = remote_ref {
            debug!("resuming stream for remote node {}", resume.node_id);
            let resume_node_id = resume.node_id;
            let sres = remote_ref
                .actor_ref()
                .send(ResumeStream { stream, resume })
                .await;

            if let Err(e) = sres {
                warn!(
                    "error resuming stream for remote node {} - {:?}",
                    resume_node_id, e.err
                );
                return;
            }

            if let Err(e) = sres.unwrap() {
                warn!(
                    "error resuming stream for remote node {} - {:?}",
                    resume_node_id, e
                );
            }
        } else {
            warn!(
                "remote node {} not found for resuming, dropping stream",
                resume.node_id
            );
        }
    }

    pub(crate) async fn receive_client(
        &self,
        handshake: proto::core::RemoteClientHandshake,
        stream: TcpStream,
    ) {
        let client =
            ClientHandler::receive_handshake(self.nucleus().clone(), handshake, stream).await;

        if let Ok(client) = client {
            if let Err(e) = self
                .nucleus()
                .spawn_actor_local(format!("client-{}", client.client_id().id()), client)
                .await
            {
                error!("spawning client errored - {:?}", e);
            }
        } else {
            error!(
                "error receiving client handshake - {:?}",
                client.err().unwrap()
            );
        }
    }

    pub(crate) fn ensure_connect_upstream(&self) {
        if let Some(upstream_addr) = self.nucleus().upstream_addr() {
            // check if connected

            trace!("ensuring upstream connection to {}", upstream_addr);

            let nucleus = self.nucleus().clone();
            if self
                .remote_nodes
                .iter()
                .any(|r| r.value().addr() == upstream_addr)
            {
                // sleep 120 and retry
                tokio::spawn(
                    async move {
                        #[cfg(not(debug_assertions))]
                        let sleep_time = std::time::Duration::from_secs(120);
                        // if testing, set sleep time to 5
                        #[cfg(debug_assertions)]
                        let sleep_time = std::time::Duration::from_secs(3);

                        tokio::time::sleep(sleep_time).await;
                        nucleus.registry().ensure_connect_upstream();
                    }
                    .in_current_span(),
                );
            } else {
                let upstream_addr = upstream_addr.to_owned();
                tokio::spawn(
                    async move {
                        #[cfg(not(debug_assertions))]
                        let mut sleep_time = std::time::Duration::from_secs(120);
                        // if testing, set sleep time to 5
                        #[cfg(debug_assertions)]
                        let mut sleep_time = std::time::Duration::from_secs(3);

                        if let Err(e) = nucleus.registry().connect_remote(upstream_addr).await {
                            if !e.is_duplicate() {
                                error!(
                                    "error connecting upstream - {:?}, backing off and retrying..",
                                    e
                                );
                                sleep_time = std::time::Duration::from_secs(5);
                            } else {
                                debug!("acceptable error connecting upstream - {:?}", e);
                            }
                        }

                        tokio::time::sleep(sleep_time).await;
                        nucleus.registry().ensure_connect_upstream();
                    }
                    .in_current_span(),
                );
            }
        }
    }

    pub(crate) fn get_actor<A: Actor>(
        &self,
        id: Snowflake<ActorMarker>,
    ) -> Option<LocalActorRef<A>> {
        match self.actor_refs.get(&id) {
            Some(actor_ref) => actor_ref
                .0
                .as_any()
                .downcast_ref::<LocalActorRef<A>>()
                .cloned()
                .or_else(|| {
                    error!(
                        "<hazard> actor type mismatch (actor_id={}, expected={}, actual_typeid={:?})",
                        id.id(),
                        std::any::type_name::<A>(),
                        actor_ref.0.as_any().type_id()
                    );
                    None
                }),
            None => None,
        }
    }

    pub(crate) fn local_actor_exists(&self, id: Snowflake<ActorMarker>) -> bool {
        self.actor_refs.contains_key(&id)
    }

    pub(crate) async fn get_named_actor_local(
        &self,
        name: Arc<str>,
    ) -> Option<(Snowflake<ActorMarker>, Option<&'static str>)> {
        self.aref()
            .send(GetNamedActor { actor_name: name })
            .await
            .unwrap()
    }

    #[expect(dead_code)]
    pub(crate) async fn get_local_named_actor_ref<A: Actor>(
        &self,
        name: Arc<str>,
    ) -> Option<LocalActorRef<A>> {
        match self.get_named_actor_local(name).await {
            Some((id, _)) => self.get_actor(id),
            None => None,
        }
    }

    pub(crate) fn set_local_named_actor<A: Actor>(
        &self,
        name: Arc<str>,
        actor_ref: LocalActorRef<A>,
    ) {
        self.aref()
            .notify(SetNamedActor {
                name,
                id: actor_ref.actor_id(),
                actor_type: None,
            })
            .unwrap();
    }

    pub(crate) fn set_remote_named_actor<A: RemoteActor>(
        &self,
        name: Arc<str>,
        actor_ref: LocalActorRef<A>,
    ) {
        self.aref()
            .notify(SetNamedActor {
                name,
                id: actor_ref.actor_id(),
                actor_type: Some(A::info().actor_type),
            })
            .unwrap();
    }

    fn new_actor_inner<A: Actor>(
        &self,
        actor_ref: LocalActorRef<A>,
        join_handle: tokio::task::JoinHandle<()>,
        is_system_actor: bool,
    ) {
        if self.nucleus().is_closing() {
            warn!("ignoring new_local_actor, nucleus is closing");
            return;
        }

        self.actor_refs.insert(
            actor_ref.actor_id(),
            BoxedActorRef(Arc::new(actor_ref.clone())),
        );

        if !is_system_actor {
            self.actor_join_handles
                .lock()
                .unwrap()
                .add_task(join_handle);
        }
    }

    pub(crate) fn new_local_actor<A: Actor>(
        &self,
        name: Arc<str>,
        actor_ref: LocalActorRef<A>,
        join_handle: tokio::task::JoinHandle<()>,
    ) {
        self.new_actor_inner(actor_ref.clone(), join_handle, name.starts_with("System."));
        self.set_local_named_actor(name, actor_ref);
    }

    pub(crate) async fn new_remotable_actor<A: RemoteActor>(
        &self,
        name: Arc<str>,
        actor_ref: LocalActorRef<A>,
        join_handle: tokio::task::JoinHandle<()>,
    ) {
        self.new_actor_inner(actor_ref.clone(), join_handle, name.starts_with("System."));
        self.set_remote_named_actor(name, actor_ref);
    }

    pub(crate) async fn new_resumed_actor_prepare<A: MovableActor>(
        &self,
        actor_ref: LocalActorRef<A>,
        name: Arc<str>,
    ) {
        self.actor_refs.insert(
            actor_ref.actor_id(),
            BoxedActorRef(Arc::new(actor_ref.clone())),
        );
        self.set_remote_named_actor(name, actor_ref);
    }

    pub(crate) fn unset_named_actor(&self, name: Arc<str>) {
        if let Err(e) = self.aref().notify(UnsetNamedActor { name }) {
            if self.nucleus().is_closing() {
                debug!(
                    "ignoring unset_named_actor error, nucleus is closing - {:?}",
                    e
                );
            } else {
                error!("error unsetting named actor - {:?}", e);
            }
        }
    }

    pub(crate) fn unset_remote_ref(&self, name: Arc<str>, actor_id: &Snowflake<ActorMarker>) {
        self.unset_named_actor(name);
        self.actor_refs.remove(actor_id);
    }

    pub(crate) fn get_remote_node(&self, node_id: NodeId) -> Option<RemoteNodeRecord> {
        self.remote_nodes.get(&node_id).as_deref().cloned()
    }

    pub(crate) fn add_remote_node(
        &self,
        node_id: NodeId,
        actor_ref: LocalActorRef<RemoteNode>,
        handshake: proto::core::RemoteHandshake,
        shutdown_arc: ArcAwaiter,
    ) {
        debug!("adding remote node {} to registry", node_id);
        self.remote_nodes.insert(
            node_id,
            RemoteNodeRecord {
                inner: Arc::new(RemoteNodeRecordInner {
                    actor_ref,
                    handshake,
                    shutdown_arc,
                }),
            },
        );
    }

    pub(crate) fn remove_remote_node(&self, node_id: NodeId) {
        debug!("removing remote node {} from registry", node_id);
        self.remote_nodes.remove(&node_id);
    }

    pub fn get_remote_node_flags<T: From<u64>>(&self, node_id: NodeId) -> Option<T> {
        self.remote_nodes
            .get(&node_id)
            .map(|r| r.node_flags())
            .map(|f| T::from(f))
    }

    pub fn get_remote_nodes(&self) -> &DashMap<NodeId, RemoteNodeRecord, FxBuildHasher> {
        &self.remote_nodes
    }

    pub(crate) fn get_remote_addresses(&self) -> Vec<proto::remote::RemoteNodeRegister> {
        self.remote_nodes
            .iter()
            .map(|record| proto::remote::RemoteNodeRegister {
                node_id: *record.key() as u32,
                address: record.value().addr().to_string(),
                flags: record.value().node_flags(),
            })
            .collect()
    }

    pub(crate) fn get_handler<'a, 'b>(
        &'b self,
        ident: &'a (&'a str, &'a str),
    ) -> Option<&'b dyn ActorMessagePairTrait> {
        self.handler_registry.get_handler(ident)
    }

    pub(crate) fn get_movable_sub_ref<'a, A: MovableActor>(
        &'a self,
        topic: &'a str,
    ) -> Option<&'a dyn MovableSubRef> {
        self.handler_registry.get_movable_sub_ref::<A>(topic)
    }

    pub(crate) fn get_topic_handler(&self, topic: &str) -> Option<&dyn AnyTopic> {
        self.handler_registry
            .topics
            .get(topic)
            .map(|h| h.as_ref())
            .or_else(|| {
                error!("<hazard> topic handler not found for topic {}", topic);
                None
            })
    }

    pub(crate) async fn discover_named_actor(
        &self,
        name: Arc<str>,
        actor_type: Option<BetterString>,
        discovery_nodes: Option<&[NodeId]>,
    ) -> Option<(Snowflake<ActorMarker>, Option<String>)> {
        // check local
        if let Some(actor_id) = self.get_named_actor_local(name.clone()).await {
            return Some((actor_id.0, actor_id.1.map(|s| s.to_string())));
        }

        // send a discovery request to all remote nodes
        let mut futures = FuturesUnordered::new();

        let remote_nodes: Vec<NodeId> = match discovery_nodes {
            Some(nodes) => nodes.to_vec(),
            None => self.remote_nodes.iter().map(|r| *r.key()).collect(),
        };

        let remote_nodes_len = remote_nodes.len();

        for remote in remote_nodes.into_iter() {
            // let aref = remote.value().clone().0;
            let n = name.clone();

            if let Ok(f) = self.nucleus().send_remote_node(
                remote,
                proto::remote::NamedActorDiscoveryRequest {
                    actor_name: n.into(),
                    actor_type: actor_type.clone(),
                },
            ) {
                futures.push(async move { f.await.ok().map(|r| (r.actor_id, r.actor_type)) });
            }
        }

        trace!(
            "discovering named actor (name={}, node_count={})",
            name, remote_nodes_len
        );

        while let Some(i) = futures.next().await {
            if let Some(r) = i {
                if r.0.is_some() {
                    return Some((r.0.unwrap(), r.1.map(|v| v.to_string())));
                }
            }
        }

        None
    }

    pub(crate) fn get_named_actor_ref_cache_only<A: RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> Option<NamedActorRef<A>> {
        match self.named_ref_cache.get(&*name) {
            Some(actor_ref) => actor_ref
                .value()
                .0
                .as_any()
                .downcast_ref::<NamedActorRef<A>>()
                .cloned()
                .or_else(|| {
                    error!(
                        "<hazard> actor type mismatch (actor_name={}, expected={}, actual_typeid={:?})",
                        name,
                        std::any::type_name::<A>(),
                        actor_ref.value().0.as_any().type_id()
                    );
                    None
                }),
            None => None,
        }
    }

    pub(crate) fn get_named_actor_ref_cache<A: RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> NamedActorRef<A> {
        let res = self.get_named_actor_ref_cache_only(name.clone());

        if let Some(res) = res {
            res
        } else {
            trace!("named actor ref cache miss (name={})", name);
            let r = NamedActorRef::new(self.nucleus().clone(), name.clone());
            self.named_ref_cache
                .insert(name, BoxedActorRef(Arc::new(r.clone())));
            r
        }
    }

    /// When an actor moves, it sends a PublisherMoved to its subcribers. If a node receives this, it is likely
    /// that it also holds a namedref to this actor. The move message contains actor name and new publisher id,
    /// so we can optimistically update the namedref cache with the new id. Inside, this is done with a try_lock
    /// and if it fails to lock instantly then so be it
    pub(crate) fn update_named_actor_id(&self, name: &str, id: Snowflake<ActorMarker>) {
        if let Some(actor_ref) = self.named_ref_cache.get(name) {
            actor_ref.value().0.named_update_actor_id(id);
        }
    }

    pub async fn subscribe_system_events<A: Actor + Handler<PubsubMessage<SystemTopic>>>(
        &self,
        ctx: &mut ActorContext<A>,
    ) -> PubsubRef<SystemTopic> {
        ctx.pubsub()
            .subscribe::<SystemTopic>("System.Registry")
            .await
            .unwrap()
    }

    pub(crate) fn publish_system_event(&self, message: SystemMessage) {
        self.aref().notify(PublishSystemEvent { message }).unwrap();
    }

    pub fn has_spawner<A: RemoteActor>(&self) -> bool {
        self.spawners.contains_key(A::info().actor_type)
    }

    pub(crate) async fn get_or_spawn<A: RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> Result<ActorRef<A>, SpawnErr> {
        if let Some(spawner) = self.spawners.get(A::info().actor_type) {
            let spawner_ref = spawner.value();
            let fut = spawner_ref.get_or_create_actor_box(name.clone());
            drop(spawner);

            let actor_ref = fut.await;

            actor_ref.map(|r| r.0.as_any().downcast_ref::<ActorRef<A>>().cloned().unwrap())
        } else {
            warn!("spawner not found for actor type {}", A::info().actor_type);
            Err(SpawnErr::SpawnerNotFound(name))
        }
    }

    pub(crate) async fn get_from_spawner<A: RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> Option<ActorRef<A>> {
        if let Some(spawner) = self.spawners.get(A::info().actor_type) {
            let spawner_ref = spawner.value();
            let fut = spawner_ref.get_actor_box(name);
            drop(spawner);

            let actor_ref = fut.await;
            actor_ref.map(|r| r.0.as_any().downcast_ref::<ActorRef<A>>().cloned().unwrap())
        } else {
            warn!("spawner not found for actor type {}", A::info().actor_type);
            None
        }
    }

    pub fn register_movable<A: MovableActor>(&self) {
        self.mover()
            .notify(RegisterMovable::new::<A>(self.nucleus().clone()))
            .unwrap();
    }

    pub(crate) async fn shutdown(&self) {
        // get actor ids of all remote node handlers
        let mut system_actor_ids: Vec<Snowflake<ActorMarker>> = self
            .remote_nodes
            .iter()
            .map(|r| r.value().actor_ref().actor_id())
            .collect::<Vec<_>>();

        // mover also belongs there
        system_actor_ids.push(self.mover().actor_id());

        self.actor_refs
            .iter()
            // we dont notify stop for remote node actors since theoretically actors want to still send something
            .filter(|v| !system_actor_ids.contains(v.key()))
            .for_each(|a| {
                // skip self, we stop it manually later
                if *a.key() != self.aref().actor_id() {
                    a.value().notify_stop();
                }
            });

        let awaiter = self.actor_join_handles.lock().unwrap().awaiter();
        awaiter.await_all().await;

        // wait for mover to finish moving out all actors
        match self.mover().send(InternalSystemShutdown {}).await {
            Ok(awaiter) => {
                awaiter.await;
            }
            Err(e) => {
                warn!("error sending InternalSystemShutdown to Mover - {:?}", e);
            }
        }

        // send RemoteNodeSystemShutdown to all remote nodes
        let mut futs = vec![];
        for remote in self.remote_nodes.iter() {
            let r = remote
                .value()
                .actor_ref()
                .send_eager(InternalSystemShutdown {});
            match r {
                Ok(awaiter) => {
                    futs.push(awaiter);
                }
                Err(e) => {
                    warn!(
                        "error sending RemoteNodeSystemShutdown to remote node {} - {:?}",
                        remote.key(),
                        e
                    );
                }
            }
        }
        let mut futs2 = vec![];
        for f in futs {
            futs2.push(async move {
                f.await.await;
            });
        }
        join_all_unordered(futs2).await;

        self.aref().send(ActorStopMessage).await.unwrap();
        let join_handle = &self.actor_ref.as_ref().unwrap().1;
        // since we cant move it out, we can sleep until it finishes
        loop {
            if join_handle.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        info!("RemoteRegistry shutdown complete");
    }
}

pub enum SystemMessage {
    NodeRegister(proto::remote::RemoteNodeRegister),
    NodeUnregister(proto::remote::RemoteNodeUnregister),
    SystemShutdown(AwaitableArc),
}
impl Serializable for SystemMessage {
    fn serialize(&self) -> Bytes {
        panic!("this is a local-only pubsub message, cannot serialize");
    }

    fn deserialize<B>(
        _data: B,
        _nucleus: Option<&NucleusNode>,
    ) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        panic!("this is a local-only pubsub message, cannot deserialize");
    }
}

pub struct SystemTopic;
impl Topic for SystemTopic {
    type M = SystemMessage;
    type Publisher = RemoteRegistryActor;

    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System"
    }
}

struct GetNamedActor {
    actor_name: Arc<str>,
}
impl Message for GetNamedActor {
    type Result = Option<(Snowflake<ActorMarker>, Option<&'static str>)>;
    fn name() -> &'static str {
        "System.Registry.GetNamedActor"
    }
}

struct SetNamedActor {
    name: Arc<str>,
    id: Snowflake<ActorMarker>,
    actor_type: ActorType,
}
impl Message for SetNamedActor {
    type Result = ();
    fn name() -> &'static str {
        "System.Registry.SetNamedActor"
    }
}

struct UnsetNamedActor {
    name: Arc<str>,
}
impl Message for UnsetNamedActor {
    type Result = ();
    fn name() -> &'static str {
        "System.Registry.UnsetNamedActor"
    }
}

struct PublishSystemEvent {
    message: SystemMessage,
}
impl Message for PublishSystemEvent {
    type Result = ();
    fn name() -> &'static str {
        "System.Registry.PublishSystemEvent"
    }
}

pub(crate) struct InternalSystemShutdown {}
impl Message for InternalSystemShutdown {
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.RemoteNodeSystemShutdown"
    }
    type Result = ArcAwaiter;
}
