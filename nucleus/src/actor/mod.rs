use crate::actor::mailbox::Mailbox;
use crate::actor::refs::LocalActorRef;
use crate::errors::{ActorProcessingErr, SpawnErr};
use crate::net::mover::handlers::{MoveActor, StartMoving, WaitResumeMessages};
use crate::net::proto;
use crate::pubsub::{PubsubMessage, Topic};
use crate::pubsub::{PubsubPayload, PubsubRef};
use crate::{ActorMarker, NodeId, NucleusNode};
use message::{Handler, system};
use metrics::{counter, gauge, histogram};
use moving::{ActorRx, MovableActor, MovingVtable};
use refs::ActorRefErr;
use snowflake::Snowflake;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{Instrument, Span};

pub mod delay;
pub mod guards;
pub(crate) mod mailbox;
pub mod message;
pub mod moving;
pub mod node;
pub mod refs;
#[cfg(test)]
mod tests;

#[async_trait_nobox]
pub trait Actor: 'static + Send + Sync + Sized {
    #[cfg(feature = "smallbox")]
    type HandlerStorage = [usize; 64];

    const MAX_MAILBOX_SIZE: usize = 1 << 18; // 256k messages - empty msg throughput is ~150k msg/s 

    async fn init(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext<Self>) {}

    /// This is called when actor panics during message processing - on_stop does NOT run in this case
    /// This hook is also **NOT** protected against panics. If your code panics here, it will crash the entire node
    async fn on_panic(&mut self, ctx: &mut ActorContext<Self>) {
        let backtrace = std::backtrace::Backtrace::capture();
        error!(?backtrace, "Actor {} panicked", ctx.name());
    }

    #[cfg(feature = "metrics")]
    async fn on_heartbeat(&mut self, _latency: Duration, _ctx: &mut ActorContext<Self>) {}

    /// If set, messages that take longer than this duration will cause a panic
    /// This should be used as a safeguard against deadlocks bringing down the whole cluster
    fn max_message_processing_time(&self) -> Option<Duration> {
        None
    }

    fn hook_before_handle(&mut self, _ctx: &mut ActorContext<Self>, _message_type: &'static str) {}

    fn on_mailbox_full_message_drop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message_type: &'static str,
    ) {
    }
}

pub struct RemoteActorInfo {
    pub actor_type: &'static str,
}

pub struct RemoteActorSettings {
    pub discover_nospawn: bool,
}

#[async_trait_nobox]
pub trait RemoteActor: Actor {
    fn info() -> &'static RemoteActorInfo;
    fn settings() -> &'static RemoteActorSettings {
        &RemoteActorSettings {
            discover_nospawn: false,
        }
    }
}
pub struct ActorContext<A: Actor> {
    nucleus: NucleusNode,
    name: Arc<str>,
    message_rx: Mailbox<A>,
    self_ref: LocalActorRef<A>,
    actor_span: Option<Span>,
    actor_type: &'static str,

    pub_topic_seq_ids: HashMap<&'static str, Arc<Mutex<u64>>>,
    pub(super) sub_seq_ids: HashMap<(Arc<str>, &'static str), u64>,
}

impl<A: Actor> ActorContext<A> {
    pub fn get_ref(&self) -> LocalActorRef<A> {
        self.self_ref.clone()
    }

    #[tracing::instrument(skip(self))]
    pub fn stop(&mut self) {
        self.nucleus
            .registry()
            .unset_remote_ref(self.name.clone(), &self.self_ref.actor_id());

        self.message_rx.close();
        debug!("Actor {} closing", self.name);
    }

    pub fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.self_ref
            .notify(system::ActorStopMessage)
            .map_err(|e| e.err)
    }

    pub fn nucleus(&self) -> &NucleusNode {
        &self.nucleus
    }

    pub fn pubsub(&'_ mut self) -> PubsubContext<'_, A> {
        PubsubContext { context: self }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn actor_id(&self) -> Snowflake<ActorMarker> {
        self.self_ref.actor_id()
    }
}

impl<A: MovableActor> ActorContext<A> {
    /// Starts the process of moving this actor to another node
    pub fn move_to(&mut self, node_id: NodeId) -> MoveBuilder<'_, A> {
        MoveBuilder {
            ctx: self,
            node_id,
            actor_subs: Vec::new(),
        }
    }
}

pub struct MoveBuilder<'a, A: MovableActor> {
    ctx: &'a mut ActorContext<A>,
    node_id: NodeId,
    actor_subs: Vec<proto::mover::PubsubRef>,
}

impl<A: MovableActor> MoveBuilder<'_, A> {
    pub async fn commit(self) -> Result<(), ActorRefErr> {
        if self.ctx.self_ref.is_moving_out() {
            return Err(ActorRefErr::MoveError);
        }

        if self.ctx.nucleus.node_id() == self.node_id {
            warn!(
                "<hazard> Actor {} tried to move to the node it is already on",
                self.ctx.name()
            );
            return Err(ActorRefErr::MoveError);
        }

        self.ctx
            .nucleus
            .registry()
            .mover()
            .send(StartMoving {
                actor_id: self.ctx.self_ref.actor_id(),
                destination_node_id: self.node_id,
                actor_type: A::info().actor_type,
                actor_name: self.ctx.name.clone(),
                actor_subs: self.actor_subs,
            })
            .await
            .map_err(|e| e.err)
            .unwrap()
            .map_err(|e| {
                error!("Error moving out actor {} - {e}", self.ctx.name());
                ActorRefErr::MoveError
            })?;

        self.ctx.self_ref.set_moving_out(MovingVtable::new());

        self.ctx.message_rx.close();

        Ok(())
    }

    pub fn with_pubsub_ref<T: Topic>(mut self, pref: &PubsubRef<T>) -> Self
    where
        A: Handler<PubsubMessage<T>>,
    {
        self.actor_subs.push(proto::mover::PubsubRef {
            topic: T::name().into(),
            actor_name: pref.actor_name.clone().into(),
            message_id: None,
            publisher_actor_id: self
                .ctx
                .nucleus
                .pubsub()
                .get_publisher_actor_id(&pref.actor_name),
        });

        self
    }
}

pub struct PubsubContext<'a, A: Actor> {
    context: &'a mut ActorContext<A>,
}

impl<A: Actor> PubsubContext<'_, A> {
    pub async fn subscribe<T: Topic>(
        &self,
        actor_name: impl Into<Arc<str>>,
    ) -> Result<PubsubRef<T>, ActorRefErr>
    where
        A: Handler<PubsubMessage<T>>,
    {
        SubscriberContext::new(self.context.nucleus.clone(), self.context.get_ref())
            .subscribe::<T>(actor_name)
            .await
    }

    pub fn detached_subscriber(&self) -> SubscriberContext<A> {
        SubscriberContext::new(self.context.nucleus.clone(), self.context.get_ref())
    }

    pub fn publish_local<T: Topic>(&mut self, message: T::M) {
        self.publisher::<T>().publish_local(message);
    }

    pub fn publish<T: Topic>(&mut self, message: T::M) {
        self.publisher::<T>().publish(message);
    }

    pub fn subscriber_count<T: Topic>(&self) -> (u64, u64) {
        self.context
            .nucleus
            .pubsub()
            .subscriber_count::<T>(&self.context.name)
    }

    #[inline]
    pub fn publisher<T: Topic>(&mut self) -> PubsubPublisherContext<T> {
        PubsubPublisherContext::new(
            self.context.nucleus.clone(),
            self.context.name.clone(),
            self.get_msg_id::<T>(),
        )
    }

    fn get_msg_id<T: Topic>(&mut self) -> Arc<Mutex<u64>> {
        self.context
            .pub_topic_seq_ids
            .entry(T::name())
            .or_insert_with(|| Arc::new(Mutex::new(0u64)))
            .clone()
    }

    /// Returns a wrapper that enables viewing the seq id for messages published on this topic
    /// Useful for synchronizing state across remote nodes
    /// Use publisher_seq_view().view(|id| { magic() }) to access the id
    /// The id is internall wrapped in a mutex, and the id os only valid while you hold it, since
    /// delayed publishers may increment it.
    /// If you are sending this ID in a response, you should use a delayed handler, and then
    /// call sender.send(YourResponseWithSeqId { seq_id: id, response }) from within the view() closure
    pub fn publisher_seq_view<T: Topic>(&mut self) -> PublisherSeqView {
        PublisherSeqView(self.get_msg_id::<T>())
    }
}

pub struct PublisherSeqView(Arc<Mutex<u64>>);

impl PublisherSeqView {
    pub fn view<R, F: FnOnce(u64) -> R>(&self, f: F) -> R {
        let _lock = self.0.lock().unwrap();
        f(*_lock)
    }
}

#[derive(Clone)]
pub struct PubsubPublisherContext<T: Topic> {
    nucleus: NucleusNode,
    name: Arc<str>,
    msg_id: Arc<Mutex<u64>>,
    _marker: std::marker::PhantomData<T>,
}
impl<T: Topic> PubsubPublisherContext<T> {
    pub fn new(nucleus: NucleusNode, name: Arc<str>, msg_id: Arc<Mutex<u64>>) -> Self {
        Self {
            nucleus,
            name,
            msg_id,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn publish_local(&self, message: T::M) {
        let msg_id = self.msg_id.clone();
        let nucleus = self.nucleus.clone();
        let name = self.name.clone();

        tokio::spawn(
            async move {
                let mut id_locked = msg_id.lock().unwrap();
                *id_locked = id_locked.wrapping_add(1);

                nucleus.pubsub().publish_local::<T>(
                    &name,
                    PubsubPayload::new_payload(message),
                    *id_locked,
                );

                drop(id_locked);
            }
            .in_current_span(),
        );
    }

    pub fn publish(&self, message: T::M) {
        let msg_id = self.msg_id.clone();
        let nucleus = self.nucleus.clone();
        let name = self.name.clone();

        tokio::spawn(
            async move {
                let mut id_locked = msg_id.lock().unwrap();
                *id_locked = id_locked.wrapping_add(1);

                nucleus.pubsub().publish_global::<T>(
                    &name,
                    PubsubPayload::new_payload(message),
                    *id_locked,
                );

                drop(id_locked);
            }
            .in_current_span(),
        );
    }

    pub fn subscriber_count(&self) -> (u64, u64) {
        self.nucleus.pubsub().subscriber_count::<T>(&self.name)
    }
}

pub struct SubscriberContext<A: Actor> {
    nucleus: NucleusNode,
    self_ref: LocalActorRef<A>,
}

impl<A: Actor> SubscriberContext<A> {
    fn new(nucleus: NucleusNode, self_ref: LocalActorRef<A>) -> Self {
        Self { nucleus, self_ref }
    }

    pub async fn subscribe<T: Topic>(
        &self,
        actor_name: impl Into<Arc<str>>,
    ) -> Result<PubsubRef<T>, ActorRefErr>
    where
        A: Handler<PubsubMessage<T>>,
    {
        self.nucleus
            .pubsub()
            .add_subscriber::<A, T>(actor_name.into(), self.self_ref.clone(), None)
            .await
    }
}

pub(crate) struct ActorRuntime<T>
where
    T: Actor,
{
    actor: T,
    pub(crate) ctx: ActorContext<T>,
}

impl<T> ActorRuntime<T>
where
    T: Actor + 'static,
{
    fn new(actor: T, name: Arc<str>, node: NucleusNode, actor_type: &'static str) -> Self {
        let (tx, rx) = Mailbox::new();
        let actor_ref = LocalActorRef::new(node.inner.snowflake.generate(), tx, None);
        let actor_span =
            info_span!(parent: None, "Actor", actor_name = name.deref(), node_id = node.node_id());

        Self {
            actor,
            ctx: ActorContext {
                nucleus: node,
                message_rx: rx,
                self_ref: actor_ref.clone(),
                name,
                actor_span: Some(actor_span),
                actor_type,
                pub_topic_seq_ids: HashMap::new(),
                sub_seq_ids: HashMap::new(),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new_resumed(
        actor: T,
        name: Arc<str>,
        node: NucleusNode,
        actor_type: &'static str,
        actor_ref: LocalActorRef<T>,
        rx: Mailbox<T>,
        sub_seq_ids: HashMap<(Arc<str>, &'static str), u64>,
        outgoing_seqs: HashMap<&'static str, u64>,
    ) -> Self {
        let actor_span =
            info_span!(parent: None, "Actor", actor_name = name.deref(), node_id = node.node_id());

        Self {
            actor,
            ctx: ActorContext {
                nucleus: node,
                message_rx: rx,
                self_ref: actor_ref.clone(),
                name,
                actor_span: Some(actor_span),
                actor_type,
                pub_topic_seq_ids: outgoing_seqs
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
                    .collect(),
                sub_seq_ids,
            },
        }
    }

    pub async fn spawn(
        handler: T,
        name: Arc<str>,
        node: NucleusNode,
        actor_type: &'static str,
    ) -> Result<(LocalActorRef<T>, JoinHandle<()>), SpawnErr> {
        let runtime = Self::new(handler, name, node, actor_type);
        runtime.start().await
    }

    // #[tracing::instrument(parent=&self.ctx.actor_span, name = "Actor Init", skip(self), fields(actor_name = self.ctx.name, actor_id = self.ctx.self_ref.actor_id().id(), node = self.ctx.nucleus.inner.id))]
    async fn do_init(&mut self) -> Result<Result<(), ActorProcessingErr>, SpawnErr> {
        // let span: Span = trace_span!("Actor Init");
        trace!("Actor {} initializing", self.ctx.name);
        let future = self.actor.init(&mut self.ctx);
        let res = tokio::time::timeout(
            Duration::from_secs(60 * 2),
            futures::FutureExt::catch_unwind(AssertUnwindSafe(future)),
        )
        // .instrument(span)
        .await;

        match res {
            Err(_) => {
                warn!(
                    "<hazard> Actor {} timed out after 2 minutes on startup, killing it",
                    self.ctx.name
                );
                Err(SpawnErr::StartupFailed(
                    "Actor initialization timed out".into(),
                ))
            }
            Ok(Err(e)) => {
                let err = get_panic_string(e);
                warn!(
                    "<hazard> Actor {} panicked during initialization: {}",
                    self.ctx.name, err
                );
                Err(SpawnErr::StartupFailed(err))
            }
            Ok(Ok(Err(e))) => {
                warn!(
                    "<hazard> Actor {} failed to initialize: {}",
                    self.ctx.name, e
                );
                Ok(Err(e))
            }
            Ok(Ok(Ok(()))) => {
                // initialized successfully
                trace!("Actor {} initialized", self.ctx.name);
                Ok(Ok(()))
            }
        }
    }

    // #[tracing::instrument(parent = &self.ctx.actor_span, name = "Actor Lifetime", skip(self), fields(actor_name = self.ctx.name, actor_id = self.ctx.self_ref.actor_id().id(), node = self.ctx.nucleus.inner.id))]
    async fn start(mut self) -> Result<(LocalActorRef<T>, JoinHandle<()>), SpawnErr> {
        Self::do_init(&mut self).await?.map_err(|e| {
            warn!("<hazard> Actor {} failed to start", self.ctx.name);
            SpawnErr::StartupFailed(e)
        })?;

        let actor_ref = self.ctx.self_ref.clone();
        let actor_id = actor_ref.actor_id().id().to_string();
        let aspan = self.ctx.actor_span.take().unwrap();

        let handle = tokio::task::spawn(
            async move {
                let span = info_span!(
                    "Actor runtime",
                    actor_name = self.ctx.name.deref(),
                    actor_id = actor_id,
                    node = self.ctx.nucleus.inner.id
                );
                let _evt = Self::processing_loop(self).instrument(span).await;
            }
            .instrument(aspan),
        );

        Ok((actor_ref, handle))
    }

    async fn processing_loop(mut self) {
        let node_id_str: Cow<'_, str> = Cow::Owned(self.ctx.nucleus.node_id().to_string());

        gauge!("actor_count", "actor_type" => self.ctx.actor_type, "node_id" => node_id_str.clone())
            .increment(1);
        counter!("actor_spawned", "actor_type" => self.ctx.actor_type, "node_id" => node_id_str.clone())
            .increment(1);

        let start = Instant::now();
        let name = self.ctx.name.clone();

        let future = async {
            while let Some(mut message) = self.ctx.message_rx.recv().await {
                let handle_fut = message.handle(&mut self.actor, &mut self.ctx);
                handle_fut.await;
            }

            let span = trace_span!("Actor Stop hook");
            let vtable = self.ctx.self_ref.get_moving_out();
            if let Some(vt) = vtable {
                (vt.on_pause)(&mut self.actor, &mut self.ctx)
                    .instrument(span)
                    .await;
            } else {
                self.actor.on_stop(&mut self.ctx).instrument(span).await;
            }
        };

        debug!("Actor {} now running", name);

        let loop_done = futures::FutureExt::catch_unwind(AssertUnwindSafe(future)).await;

        let elapsed = start.elapsed();

        histogram!("actor_lifetime", "actor" => self.ctx.actor_type, "node_id" => node_id_str.clone())
            .record(elapsed.as_nanos() as f64);
        gauge!("actor_count", "actor_type" => self.ctx.actor_type, "node_id" => node_id_str)
            .decrement(1);

        if let Err(e) = loop_done {
            let err = get_panic_string(e);
            counter!("actor_panic", "actor" => self.ctx.actor_type, "node_id" => self.ctx.nucleus.node_id().to_string()).increment(1);

            let backtrace = std::backtrace::Backtrace::capture();
            error!(
                ?backtrace,
                "<hazard> Actor {} - {} panicked: {}", self.ctx.actor_type, name, err
            );

            self.actor.on_panic(&mut self.ctx).await;
            debug!("Actor {} - {} ran panic hook", self.ctx.actor_type, name);
        } else if let Some(vt) = self.ctx.self_ref.get_moving_out() {
            let serialized_actor = (vt.serialize)(&self.actor);
            let nucleus = self.ctx.nucleus.clone();
            nucleus
                .registry()
                .mover()
                .send(MoveActor {
                    actor_id: self.ctx.self_ref.actor_id(),
                    actor_serialized: serialized_actor,
                    name: self.ctx.name.clone(),
                    actor_type: self.ctx.actor_type,
                    actor_subs: self.ctx.sub_seq_ids.into_iter().collect(),
                    outgoing_pubsub_seqs: self
                        .ctx
                        .pub_topic_seq_ids
                        .iter()
                        .map(|(k, v)| (*k, *v.lock().unwrap()))
                        .collect(),
                })
                .await
                .unwrap();

            debug!("Actor {} done moving", self.ctx.name);
        } else {
            debug!("Actor {} stopped", name);
        }

        if !self.ctx.self_ref.is_moving_out() {
            self.ctx.nucleus.pubsub().publisher_stopped(&self.ctx.name);
        } else {
            self.ctx.nucleus.pubsub().publisher_moved(&self.ctx.name);
        };
    }
}

impl<A: MovableActor> ActorRuntime<A> {
    pub async fn spawn_resumed(
        actor_rx: ActorRx,
        ref_tx: oneshot::Sender<Box<dyn Any + Send>>,
        name: Arc<str>,
        node: NucleusNode,
    ) -> Result<(LocalActorRef<A>, JoinHandle<()>), SpawnErr> {
        let (tx, rx) = Mailbox::new();
        let (resume_tx, mut resume_rx) = mpsc::unbounded_channel();
        let actor_ref = LocalActorRef::new(node.inner.snowflake.generate(), tx, Some(resume_tx));
        let actor_type = A::info().actor_type;

        node.registry()
            .new_resumed_actor_prepare(actor_ref.clone(), name.clone())
            .await;

        let _ = ref_tx.send(Box::new(actor_ref.clone()));
        let (actor_bytes, mut sub_ref_map) = actor_rx.await.unwrap();

        let sub_seq_map = sub_ref_map.get_seqs(&node);
        let outgoing_seqs = sub_ref_map.get_outgoing_seqs();
        let actor = A::deserialize(actor_bytes, &node, sub_ref_map);
        let mut runtime = Self::new_resumed(
            actor,
            name.clone(),
            node.clone(),
            actor_type,
            actor_ref,
            rx,
            sub_seq_map,
            outgoing_seqs,
        );

        // catch panic on calling on_resume
        futures::FutureExt::catch_unwind(AssertUnwindSafe(
            runtime.actor.before_resume(&mut runtime.ctx),
        ))
        .await
        .map_err(|e| {
            error!(
                "<hazard> Actor {} - {} panicked during before resume hook",
                actor_type, name
            );
            SpawnErr::StartupFailed(get_panic_string(e))
        })?;

        let (tx, mut rx) = oneshot::channel();
        // send tx to mover
        node.registry()
            .mover()
            .send_with_sender(
                WaitResumeMessages {
                    name: runtime.ctx.name.clone(),
                    actor_id: runtime.ctx.actor_id(),
                },
                tx.into(),
            )
            .unwrap();

        let future = async {
            'l: loop {
                select! {
                    _ = &mut rx => {
                        break 'l;
                    },
                    msg = resume_rx.recv() => {
                        if msg.is_none() {
                            break 'l;
                        }
                        let mut msg = msg.unwrap();
                        let handle_fut = msg.handle(&mut runtime.actor, &mut runtime.ctx);
                        handle_fut.await;
                    }
                }
            }
        };

        debug!(
            "Actor {} - {} processing resuming messages",
            actor_type, name
        );
        let loop_done = futures::FutureExt::catch_unwind(AssertUnwindSafe(future)).await;

        if let Err(e) = loop_done {
            let err = get_panic_string(e);
            counter!("actor_panic_during_resume", "actor" => runtime.ctx.actor_type, "node_id" => runtime.ctx.nucleus.node_id().to_string()).increment(1);
            let backtrace = std::backtrace::Backtrace::capture();
            error!(
                ?backtrace,
                "<hazard> Actor {} - {} panicked during resume: {}",
                runtime.ctx.actor_type,
                runtime.ctx.name,
                err
            );

            runtime.actor.on_panic(&mut runtime.ctx).await;
            return Err(SpawnErr::StartupFailed(err));
        }

        debug!(
            "Actor {} - {} done processing resuming messages",
            actor_type, name
        );

        resume_rx.close();

        // call post_resume hook
        if let Err(e) = futures::FutureExt::catch_unwind(AssertUnwindSafe(
            runtime.actor.post_resume(&mut runtime.ctx),
        ))
        .await
        {
            let err = get_panic_string(e);
            counter!("actor_panic_during_resume", "actor" => runtime.ctx.actor_type, "node_id" => runtime.ctx.nucleus.node_id().to_string()).increment(1);
            let backtrace = std::backtrace::Backtrace::capture();
            error!(
                ?backtrace,
                "<hazard> Actor {} - {} panicked during post resume hook: {}",
                runtime.ctx.actor_type,
                runtime.ctx.name,
                err
            );

            runtime.actor.on_panic(&mut runtime.ctx).await;
            return Err(SpawnErr::StartupFailed(err));
        }

        // resuming is done, move on to the processing loop

        let actor_id = runtime.ctx.self_ref.actor_id().id().to_string();
        let aspan = runtime.ctx.actor_span.take().unwrap();
        let lref = runtime.ctx.self_ref.clone();
        let handle = tokio::task::spawn(
            async move {
                let span = info_span!(
                    "Actor runtime",
                    actor_name = runtime.ctx.name.deref(),
                    actor_id = actor_id,
                    node = runtime.ctx.nucleus.inner.id
                );
                let _evt = Self::processing_loop(runtime).instrument(span).await;
            }
            .instrument(aspan),
        );

        Ok((lref, handle))
    }
}

pub(crate) fn get_panic_string(e: Box<dyn std::any::Any + Send>) -> ActorProcessingErr {
    match e.downcast::<String>() {
        Ok(v) => From::from(*v),
        Err(e) => match e.downcast::<&str>() {
            Ok(v) => From::from(*v),
            _ => From::from("Unknown panic occurred which couldn't be coerced to a string"),
        },
    }
}

pub type ByteSender = Sender<Result<Option<Vec<u8>>, ActorRefErr>>;

#[must_use]
pub struct Sender<T> {
    tx: Option<tokio::sync::oneshot::Sender<T>>,
}
impl<T> From<tokio::sync::oneshot::Sender<T>> for Sender<T> {
    fn from(tx: tokio::sync::oneshot::Sender<T>) -> Self {
        Sender { tx: Some(tx) }
    }
}
impl<T> Sender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        match self.tx.take() {
            Some(tx) => tx.send(value),
            None => Err(value),
        }
    }

    pub(crate) fn new_empty() -> Self {
        Sender { tx: None }
    }
}
