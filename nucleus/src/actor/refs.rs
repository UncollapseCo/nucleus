use crate::actor::Actor;
use crate::actor::mailbox::{MailboxSendError, MailboxSender};
use crate::actor::message::system::MessageDroppedDueToFullMailbox;
use crate::actor::message::{ActorMessageHandlerVtable, HandlerMarker};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::refs::weak::{WeakActorRef, WeakActorRefLocal};
use crate::errors::SpawnErr;
use crate::net::mover::handlers::WaitForMoveFinish;
use crate::net::registry::actor::RemoteRegistryActor;
use crate::net::remote::ResponseMode;
use crate::net::{convert_response, proto};
use crate::utils::{PeriodSendHandler, join_all_unordered};
use crate::{ActorMarker, NodeId, NucleusNode, RequestMarker};
use bytes::{Buf, Bytes};
use fxhash::FxHashSet;
use metrics::counter;
use prost::Message as _;
use snowflake::Snowflake;
use std::any::{Any, type_name};
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::{Mutex, MutexGuard, OnceCell, oneshot};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tracing::{Instrument, Span, trace};

#[cfg(feature = "metrics")]
use super::message::system::LatencyMsg;
use super::message::system::{ActorStopMessage, CallbackMessage};
use super::message::{DeserializationError, Serializable};
use super::moving::MovingVtable;
use super::{ActorContext, RemoteActor, Sender};

pub enum ActorRef<A: Actor> {
    Local(LocalActorRef<A>),
    Remote(RemoteActorRef<A>),
    Named(NamedActorRef<A>),
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        match self {
            ActorRef::Local(r) => ActorRef::Local(r.clone()),
            ActorRef::Remote(r) => ActorRef::Remote(r.clone()),
            ActorRef::Named(r) => ActorRef::Named(r.clone()),
        }
    }
}

impl<A: Actor> std::fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorRef::Local(r) => f
                .debug_struct("ActorRef::Local")
                .field("actor_id", &r.actor_id())
                .field("actor_type", &std::any::type_name::<A>())
                .finish(),
            ActorRef::Remote(r) => f
                .debug_struct("ActorRef::Remote")
                .field("actor_id", &r.actor_id())
                .field("actor_type", &std::any::type_name::<A>())
                .finish(),
            ActorRef::Named(r) => f
                .debug_struct("ActorRef::Named")
                .field("actor_name", &r.inner.name)
                .field("actor_type", &std::any::type_name::<A>())
                .finish(),
        }
    }
}

impl<A: RemoteActor> ActorRef<A> {
    #[tracing::instrument(name = "ActorRef Send", skip(self, msg), fields(message_type = M::name()))]
    pub async fn send<M: Message + Serializable>(&self, msg: M) -> Result<M::Result, SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        match self {
            ActorRef::Local(r) => r.send(msg).await,
            ActorRef::Remote(r) => r.send(msg).await,
            ActorRef::Named(r) => r.send(msg).await,
        }
    }

    pub fn send_with_timeout<M: Message + Serializable>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = Result<Result<M::Result, SendError<M>>, Elapsed>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let fut = self.send(msg);
        tokio::time::timeout(timeout, fut).in_current_span()
    }

    #[tracing::instrument(name = "ActorRef Notify", skip(self, msg), fields(message_type = M::name()))]
    pub async fn notify<M: Message + Serializable>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        match self {
            ActorRef::Local(r) => r.notify(msg),
            ActorRef::Remote(r) => r.notify(msg).await,
            ActorRef::Named(r) => r.notify(msg).await,
        }
    }

    #[tracing::instrument(name = "ActorRef NotifyForget", skip(self, msg), fields(message_type = M::name()))]
    pub fn notify_forget<M: Message + Serializable>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        match self {
            ActorRef::Local(r) => r.notify(msg),
            ActorRef::Remote(r) => r.notify_forget(msg),
            ActorRef::Named(r) => r.notify_forget(msg),
        }
    }

    pub fn notify_delayed<M: Message + Serializable>(
        &self,
        msg: M,
        delay: Duration,
        on_failure: Option<impl FnOnce(SendError<M>) + Send + 'static>,
    ) where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        match self {
            ActorRef::Local(r) => r.notify_delayed(msg, delay, on_failure),
            ActorRef::Remote(r) => r.notify_delayed(msg, delay, on_failure),
            ActorRef::Named(r) => r.notify_delayed(msg, delay, on_failure),
        }
    }

    pub fn unwrap_local(&self) -> LocalActorRef<A> {
        match self {
            ActorRef::Local(r) => r.clone(),
            _ => panic!("expected local actor ref"),
        }
    }

    pub fn unwrap_remote(&self) -> RemoteActorRef<A> {
        match self {
            ActorRef::Remote(r) => r.clone(),
            _ => panic!("expected remote actor ref"),
        }
    }

    pub fn unwrap_named(&self) -> NamedActorRef<A> {
        match self {
            ActorRef::Named(r) => r.clone(),
            _ => panic!("expected named actor ref"),
        }
    }
}

pub struct LocalActorRef<A: Actor> {
    inner: Arc<LocalActorRefInner<A>>,
}

impl<A: Actor> std::fmt::Debug for LocalActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalActorRef")
            .field("actor_id", &self.actor_id())
            .field("actor_type", &std::any::type_name::<A>())
            .finish()
    }
}

impl<A: Actor> LocalActorRef<A> {
    pub(crate) fn new(
        id: Snowflake<ActorMarker>,
        sender: MailboxSender<A>,
        resume_sender: Option<UnboundedSender<MessageHandler<A>>>,
    ) -> Self {
        Self {
            inner: Arc::new(LocalActorRefInner {
                actor_id: id,
                sender,
                moving_out: OnceCell::new(),
                moving_in: resume_sender,
            }),
        }
    }

    pub fn actor_id(&self) -> Snowflake<ActorMarker> {
        self.inner.actor_id
    }

    pub fn is_moving_out(&self) -> bool {
        self.inner.moving_out.initialized()
    }

    pub fn as_weak(&self, nucleus: NucleusNode) -> WeakActorRefLocal
    where
        A: RemoteActor,
    {
        WeakActorRef::new_unchecked(nucleus, self.actor_id(), A::info().actor_type).into_local()
    }

    pub(crate) fn set_moving_out(&self, vtable: MovingVtable<A>) {
        if let Err(e) = self.inner.moving_out.set(vtable) {
            panic!("failed to set moving out vtable: {}", e);
        }
    }
    pub(crate) fn get_moving_out(&self) -> Option<&MovingVtable<A>> {
        self.inner.moving_out.get()
    }

    fn send_inner_common<const RESUME: bool>(
        &self,
        to_send: MessageHandler<A>,
        message_type: &'static str,
        respect_backpressure: bool,
    ) -> Result<(), MailboxSendError<A>> {
        let result = if RESUME {
            self.inner
                .moving_in
                .as_ref()
                .unwrap()
                .send(to_send)
                .map_err(|e| MailboxSendError::new(e.0, ActorRefErr::InvalidRef))
        } else if respect_backpressure {
            self.inner.sender.send_limit_len(to_send)
        } else {
            self.inner.sender.send(to_send)
        };

        if let Err(e) = &result {
            let actor_type = type_name::<A>();
            warn!(
                "failed to send message (msg_type={msg_type} actor_type={actor_type} moving={moving}): {error}",
                msg_type = message_type,
                actor_type = actor_type,
                moving = self.is_moving_out(),
                error = e
            );

            if matches!(e.err(), ActorRefErr::ActorMailboxFull) {
                self.inner
                    .sender
                    .send(ActorMessageHandlerVtable::new(
                        MessageDroppedDueToFullMailbox(message_type),
                        Sender::new_empty(),
                        None,
                        Span::current(),
                    ))
                    .ok();
            }
        }

        result
    }

    fn send_inner<M: Message, const RESUME: bool>(
        &self,
        msg: M,
        tx: Sender<M::Result>,
        drop_tx: Option<Sender<M>>,
    ) -> Result<(), SendError<M>>
    where
        A: Handler<M>,
    {
        let message_type = M::name();

        let sender_span = Span::current();

        let to_send: MessageHandler<A> =
            ActorMessageHandlerVtable::new(msg, tx, drop_tx, sender_span);

        let channel_res =
            self.send_inner_common::<RESUME>(to_send, message_type, M::RESPECT_BACKPRESSURE);

        if let Err(e) = channel_res {
            // could be faster by skipping conversion into box, but this is a cold path and idk how to do it properly
            let err = e.err();
            let mut msg = e.into_inner().extract_msg();
            let _ = msg.sender.take();
            return Err(SendError::new(err, msg.msg.take().unwrap()));
        }

        Ok(())
    }

    /// This function is similar to send(), but will panic if the recv channel is closed unexpectedly.
    /// This is useful in cases where you might want to get the early send error in sync code, before awaiting the result
    pub fn send_eager<M: Message>(
        &self,
        msg: M,
    ) -> Result<impl Future<Output = M::Result> + use<M, A>, SendError<M>>
    where
        A: Handler<M>,
    {
        // its own function to prevent monomorphization bloat
        fn log_trace(message_type: &'static str, actor_type: &'static str, actor_id: u64) {
            trace!(
                "local ref result received (msg_type={msg_type} actor_type={actor_type} actor_id={actor_id})",
                msg_type = message_type,
                actor_type = actor_type,
                actor_id = actor_id
            );
        }

        fn do_panic(message_type: &str) -> ! {
            panic!(
                "recv channel was closed unexpectedly (msg_type={})",
                message_type
            );
        }
        fn make_span(message_type: &'static str, actor_id: u64, node_id: NodeId) -> Span {
            // #[tracing::instrument(name = "LocalRef Send", skip(self, msg), fields(message_type = M::name(), actor_id = self.inner.actor_id.id(), node_id = self.inner.actor_id.worker_id()))]
            info_span!(
                "LocalRef Send",
                message_type = message_type,
                actor_id = actor_id,
                node_id = node_id
            )
        }

        let message_type = M::name();
        let actor_type = type_name::<A>();
        let actor_id = self.inner.actor_id.id();

        let _span = make_span(message_type, actor_id, self.inner.actor_id.worker_id());
        let _span_enter = _span.enter();

        let (tx, rx) = oneshot::channel();

        match self.send_inner::<_, false>(msg, tx.into(), None) {
            Ok(_) => Ok(async move {
                match rx.await {
                    Ok(res) => {
                        log_trace(message_type, actor_type, actor_id);
                        res
                    }
                    Err(_e) => {
                        do_panic(message_type);
                    }
                }
            }
            .in_current_span()),
            Err(e) => Err(e),
        }
    }

    pub fn send<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>> + use<A, M>
    where
        A: Handler<M>,
    {
        // its own function to prevent monomorphization bloat
        fn log_trace(message_type: &'static str, actor_type: &'static str, actor_id: u64) {
            trace!(
                "local ref result received (msg_type={msg_type} actor_type={actor_type} actor_id={actor_id})",
                msg_type = message_type,
                actor_type = actor_type,
                actor_id = actor_id
            );
        }

        fn make_span(message_type: &'static str, actor_id: u64, node_id: NodeId) -> Span {
            // #[tracing::instrument(name = "LocalRef Send", skip(self, msg), fields(message_type = M::name(), actor_id = self.inner.actor_id.id(), node_id = self.inner.actor_id.worker_id()))]
            info_span!(
                "LocalRef TrySend",
                message_type = message_type,
                actor_id = actor_id,
                node_id = node_id
            )
        }

        let message_type = M::name();
        let actor_type = type_name::<A>();
        let actor_id = self.inner.actor_id.id();

        let _span = make_span(message_type, actor_id, self.inner.actor_id.worker_id());
        let _span_enter = _span.enter();

        let (tx, rx) = oneshot::channel();
        let (drop_tx, drop_rx) = oneshot::channel();

        let sent = self.send_inner::<_, false>(msg, tx.into(), Some(drop_tx.into()));

        async move {
            match sent {
                Ok(_) => match rx.await {
                    Ok(res) => {
                        log_trace(message_type, actor_type, actor_id);
                        Ok(res)
                    }
                    Err(_e) => {
                        let msg = drop_rx
                            .await
                            .expect("try_send drop_rx was closed unexpectedly");

                        Err(SendError::new(ActorRefErr::ResultChannelClosed, msg))
                    }
                },
                Err(e) => Err(e),
            }
        }
    }

    #[tracing::instrument(name = "LocalRef SendWithTimeout", skip(self, msg), fields(message_type = M::name(), actor_id = self.inner.actor_id.id(), node_id = self.inner.actor_id.worker_id()))]
    pub async fn send_with_timeout<M: Message + Serializable>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> Result<Result<M::Result, SendError<M>>, Elapsed>
    where
        A: Handler<M>,
        M::Result: Serializable,
    {
        let f = self.send(msg);
        tokio::time::timeout(timeout, f).await
    }

    #[tracing::instrument(name = "LocalRef Notify", skip(self, msg), fields(message_type = M::name(), actor_id = self.inner.actor_id.id()))]
    pub fn notify<M: Message>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: Handler<M>,
    {
        let message_type = M::name();
        let actor_type = type_name::<A>();

        fn log_trace(message_type: &'static str, actor_type: &'static str) {
            trace!(
                "local ref notification sent (msg_type={msg_type} actor_type={actor_type})",
                msg_type = message_type,
                actor_type = actor_type
            );
        }

        match self.send_inner::<_, false>(msg, Sender::new_empty(), None) {
            Ok(_) => {
                log_trace(message_type, actor_type);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn notify_delayed<M: Message>(
        &self,
        msg: M,
        delay: Duration,
        on_failure: Option<impl FnOnce(SendError<M>) + Send + 'static>,
    ) where
        A: Handler<M>,
    {
        let r = self.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(delay).await;

                if let Err(e) = r.notify(msg) {
                    if let Some(f) = on_failure {
                        f(e);
                    }
                }
            }
            .in_current_span(),
        );
    }

    pub fn send_with_sender<M: Message>(
        &self,
        msg: M,
        tx: Sender<M::Result>,
    ) -> Result<(), SendError<M>>
    where
        A: Handler<M>,
    {
        self.send_inner::<_, false>(msg, tx, None)
    }

    pub fn is_closed(&self) -> bool {
        self.inner.sender.is_closed()
    }

    pub(crate) fn notify_stop(&self) {
        let _ = self.send_inner::<_, false>(ActorStopMessage, Sender::new_empty(), None);
    }

    pub(crate) fn send_resume<M: Message + Serializable>(
        &self,
        msg: M,
        sender: Option<Sender<Bytes>>,
    ) -> Result<(), SendError<M>>
    where
        A: Handler<M>,
        M::Result: Serializable,
    {
        if let Some(sender) = sender {
            let (tx, rx) = oneshot::channel();
            self.send_inner::<_, true>(msg, tx.into(), None)?;
            tokio::spawn(
                async move {
                    let res = rx
                        .await
                        .expect("resume recv channel was closed unexpectedly while resuming");

                    let _ = sender.send(res.serialize());
                }
                .in_current_span(),
            );

            Ok(())
        } else {
            self.send_inner::<_, true>(msg, Sender::new_empty(), None)
        }
    }

    pub fn send_periodic<M: Message + Clone>(&self, msg: M, period: Duration) -> PeriodSendHandler
    where
        A: Handler<M>,
    {
        let r = self.clone();

        let handle = tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(period);

                loop {
                    interval.tick().await;

                    if let Err(e) = r.notify(msg.clone()) {
                        warn!("failed to send periodic message, stopping - {:?}", e);
                        break;
                    } else {
                        trace!(
                            "sent periodic message {} to actor {}",
                            M::name(),
                            r.inner.actor_id
                        );
                    }
                }
            }
            .in_current_span(),
        );

        PeriodSendHandler::new(handle)
    }

    pub fn send_callback<F: FnOnce(&mut A, &mut ActorContext<A>) + Send + Sync + 'static>(
        &self,
        cb: F,
    ) -> Result<(), ActorRefErr> {
        self.send_inner::<CallbackMessage<A, F>, false>(
            CallbackMessage::new(cb),
            Sender::new_empty(),
            None,
        )
        .map_err(|e| e.err)
    }
}

pub struct RemoteActorRef<A: Actor> {
    inner: Arc<RemoteActorRefInner>,
    _a: std::marker::PhantomData<A>,
}
impl<A: Actor> RemoteActorRef<A> {
    pub fn actor_id(&self) -> Snowflake<ActorMarker> {
        self.inner.actor_id
    }
}

impl<A: Actor> std::fmt::Debug for RemoteActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteActorRef")
            .field("actor_id", &self.actor_id())
            .field("actor_type", &std::any::type_name::<A>())
            .finish()
    }
}

impl<A: RemoteActor> RemoteActorRef<A> {
    pub(crate) fn new(id: Snowflake<ActorMarker>, nucleus: NucleusNode) -> Self {
        Self {
            inner: Arc::new(RemoteActorRefInner {
                actor_id: id,
                nucleus,
            }),
            _a: std::marker::PhantomData,
        }
    }

    pub fn send<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        self.inner
            .send_unchecked(msg, A::info().actor_type.into())
            .in_current_span()
    }

    pub fn send_with_timeout<M: Message + Serializable>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = Result<Result<M::Result, SendError<M>>, Elapsed>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let fut = self.send(msg);
        tokio::time::timeout(timeout, fut).in_current_span()
    }

    #[must_use = "You must use the returned future to handle possible errors. If you do not handle errors, consider using notify_forget instead."]
    pub fn notify<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<(), SendError<M>>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        self.inner
            .notify_unchecked(msg, A::info().actor_type.into())
            .in_current_span()
    }

    /// Sends a message to the remote actor without waiting for a notify ack
    /// This is useful for messages that don't do not handle remote errors at all
    /// Confusingly, it still returns an error type. This error is returned if the
    /// destination node is unknown (or ref is invalid), or is closing down.
    #[tracing::instrument(name = "RemoteRef NotifyForget", skip(self, msg), fields(message_type = M::name(), otel.kind = "producer"))]
    pub fn notify_forget_unchecked<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> Result<(), SendError<M>> {
        let res = self
            .inner
            .send_inner_unchecked(&msg, A::info().actor_type.into(), None, true);

        if let Err(e) = res {
            return Err(SendError::new(e, msg));
        }

        Ok(())
    }

    pub fn notify_forget<M: Message + Serializable>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
    {
        self.notify_forget_unchecked(msg)
    }

    pub fn notify_delayed<M: Message + Serializable>(
        &self,
        msg: M,
        delay: Duration,
        on_failure: Option<impl FnOnce(SendError<M>) + Send + 'static>,
    ) where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let r = self.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(delay).await;

                if let Err(e) = r.notify(msg).await {
                    if let Some(f) = on_failure {
                        f(e);
                    }
                }
            }
            .in_current_span(),
        );
    }

    pub fn as_weak(&self) -> WeakActorRef {
        WeakActorRef::new_unchecked(
            self.inner.nucleus.clone(),
            self.actor_id(),
            A::info().actor_type,
        )
    }
}

impl RemoteActorRefInner {
    #[tracing::instrument(name = "RemoteRef Send", skip(self, msg), fields(message_type = M::name(), otel.kind = "client", otel.status_code, otel.status_message))]
    fn send_unchecked<M: Message + Serializable>(
        &self,
        msg: M,
        actor_type: Bytes,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>>
    where
        M::Result: Serializable,
    {
        let (tx, rx) = oneshot::channel();

        let sent = self.send_inner_unchecked(&msg, actor_type, Some(tx.into()), false);

        async move {
            if let Err(e) = sent {
                Span::current().record("otel.status_code", "Error");
                Span::current().record("otel.status_message", format!("{:?}", e));
                return Err(SendError::new(e, msg));
            }

            let res = rx
                .await
                .expect("remote send recv channel was closed unexpectedly");

            match convert_response(res) {
                Ok(Some(r)) => M::Result::deserialize(r, Some(&self.nucleus))
                    .map_err(|e| {
                        error!("failed to deserialize remote response: {:?}", e);
                        Span::current().record("otel.status_code", "Error");
                        Span::current().record("otel.status_message", format!("{:?}", e));
                        SendError::new(ActorRefErr::DeserializationError, msg)
                    })
                    .inspect(|_| {
                        Span::current().record("otel.status_code", "Ok");
                    }),
                Ok(None) => {
                    Span::current().record("otel.status_code", "Error");
                    Span::current().record(
                        "otel.status_message",
                        "Received notifyack on a message that expected a response",
                    );
                    panic!("received notifyack on a message that expected a response");
                }
                Err(err) => {
                    Span::current().record("otel.status_code", "Error");
                    Span::current().record("otel.status_message", format!("{:?}", err));
                    Err(SendError::new(err, msg))
                }
            }
        }
        .in_current_span()
    }

    fn send_inner_unchecked<M: Message + Serializable>(
        &self,
        msg: &M,
        actor_type: Bytes,
        tx: Option<Sender<proto::core::remote_response::Response>>,
        notify: bool,
    ) -> Result<(), ActorRefErr> {
        if self.actor_id.worker_id() == self.nucleus.node_id() {
            warn!("<hazard> attempted to send a remote message to self");
            return Err(ActorRefErr::InvalidRef);
        }

        remote_send(
            &self.nucleus,
            tx,
            notify,
            M::name().into(),
            msg.serialize(),
            actor_type,
            self.actor_id,
            false,
        )
    }

    #[tracing::instrument(name = "RemoteRef Notify", skip(self, msg, actor_type), fields(message_type = M::name(), otel.kind = "client", otel.status_code, otel.status_message))]
    #[must_use = "You must use the returned future to handle possible errors. If you do not handle errors, consider using notify_forget instead."]
    fn notify_unchecked<M: Message + Serializable>(
        &self,
        msg: M,
        actor_type: Bytes,
    ) -> impl Future<Output = Result<(), SendError<M>>>
    where
        M::Result: Serializable,
    {
        let (tx, rx) = oneshot::channel();
        let sent = self.send_inner_unchecked(&msg, actor_type, Some(tx.into()), true);

        async move {
            if let Err(e) = sent {
                Span::current().record("otel.status_code", "Error");
                Span::current().record("otel.status_message", format!("{:?}", e));
                return Err(SendError::new(e, msg));
            }

            let res = rx
                .await
                .expect("remote send recv channel was closed unexpectedly");

            match convert_response(res) {
                Ok(Some(_r)) => {
                    error!("<hazard> received non-ack response on a notify message?");
                    Span::current().record("otel.status_code", "Ok");
                    Span::current().record(
                        "otel.status_message",
                        "Received non-ack response on a notify message",
                    );
                    Ok(())
                }
                Ok(None) => {
                    Span::current().record("otel.status_code", "Ok");
                    Ok(())
                }
                Err(err) => {
                    Span::current().record("otel.status_code", "Error");
                    Span::current().record("otel.status_message", format!("{:?}", err));
                    Err(SendError::new(err, msg))
                }
            }
        }
        .in_current_span()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn remote_send(
    nucleus: &NucleusNode,
    tx: Option<Sender<proto::core::remote_response::Response>>,
    notify: bool,
    msg_type: Bytes,
    msg_bytes: Bytes,
    actor_type: Bytes,
    actor_id: Snowflake<ActorMarker>,
    message_resume: bool,
) -> Result<(), ActorRefErr> {
    let node_id = actor_id.worker_id();
    let remote = nucleus.registry().get_remote_node(node_id);
    if remote.is_none() {
        warn!("unknown node: {}", node_id);
        return Err(ActorRefErr::UnknownNode);
    }

    trace!(
        "sending message (msg_type={msg_type} actor_type={actor_type}) to node {node_id}",
        msg_type = str::from_utf8(msg_type.as_ref()).unwrap(),
        actor_type = str::from_utf8(actor_type.as_ref()).unwrap()
    );

    let remote = remote.unwrap();
    let remote = remote.actor_ref();

    let msg_wrap = proto::core::RemoteMessage {
        actor_id: Some(actor_id),
        request_id: nucleus.inner.snowflake.generate::<RequestMarker>(),
        actor_type,
        message_type: msg_type,
        response_mode: if message_resume {
            ResponseMode::PayloadMovedin as u32
        } else if tx.is_none() {
            ResponseMode::Nothing as u32
        } else if notify {
            ResponseMode::AckOnly as u32
        } else {
            ResponseMode::Payload as u32
        },
        payload: msg_bytes,
    };

    let res = if let Some(tx) = tx {
        remote.send_with_sender(msg_wrap, tx)
    } else {
        remote.notify(msg_wrap)
    };

    if let Err(e) = res {
        return Err(e.err);
    }

    Ok(())
}

pub struct NamedActorRef<A: Actor> {
    inner: Arc<NamedActorRefInner<A>>,
}

impl<A: Actor> std::fmt::Debug for NamedActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamedActorRef")
            .field("actor_name", &self.inner.name)
            .field("actor_type", &std::any::type_name::<A>())
            .finish()
    }
}

impl<A: RemoteActor> NamedActorRef<A> {
    pub(crate) fn new(nucleus: NucleusNode, name: Arc<str>) -> Self {
        let is_spawnable = nucleus.registry().has_spawner::<A>();
        Self {
            inner: Arc::new(NamedActorRefInner {
                id: Mutex::new(None),
                nucleus,
                name,
                is_spawnable,
                _a: std::marker::PhantomData,
            }),
        }
    }

    async fn lock(&self) -> Result<MutexGuard<'_, Option<UnnamedActorRef<A>>>, Elapsed> {
        tokio::time::timeout(Duration::from_secs(5), self.inner.id.lock()).await
    }

    #[tracing::instrument(name = "NamedRef Rediscover", skip(self), fields(actor_name=self.inner.name.deref()))]
    pub async fn rediscover(&self) -> Result<(), ActorRefErr> {
        let id_guard = match self.lock().await {
            Ok(guard) => guard,
            Err(_) => return Err(ActorRefErr::DiscoveryTimeout),
        };

        self.discovery(id_guard).await?;

        Ok(())
    }

    #[cold]
    #[tracing::instrument(name = "NamedRef Discovery Wrapper", skip(self, guard), fields(actor_name=self.inner.name.deref()))]
    async fn discovery(
        &self,
        mut guard: MutexGuard<'_, Option<UnnamedActorRef<A>>>,
    ) -> Result<UnnamedActorRef<A>, ActorRefErr> {
        match self.discovery_inner(&mut guard).await {
            Ok(id) => Ok(id),
            Err(ActorRefErr::MoveError) => self.discovery_inner(&mut guard).await,
            Err(e) => Err(e),
        }
    }
    #[cold]
    #[tracing::instrument(name = "NamedRef Discovery", skip(self, guard), fields(actor_name=self.inner.name.deref()))]
    async fn discovery_inner(
        &self,
        guard: &mut MutexGuard<'_, Option<UnnamedActorRef<A>>>,
    ) -> Result<UnnamedActorRef<A>, ActorRefErr> {
        let f = async move {
            if self.inner.is_spawnable {
                let res = if A::settings().discover_nospawn {
                    self.inner
                        .nucleus
                        .registry()
                        .get_from_spawner(self.inner.name.clone())
                        .await
                } else {
                    fn log_warn(e: &SpawnErr) {
                        // monomorphization bloat prevention
                        warn!(
                            "failed to spawn named actor transparently from named ref: {:?}",
                            e
                        );
                    }

                    self.inner
                        .nucleus
                        .registry()
                        .get_or_spawn(self.inner.name.clone())
                        .await
                        .map_err(|e| {
                            log_warn(&e);
                        })
                        .ok()
                };

                match res {
                    Some(ref_) => {
                        let ref_: UnnamedActorRef<A> = ref_.try_into().unwrap();
                        **guard = Some(ref_.clone());
                        Ok(ref_)
                    }
                    None => Err(if A::settings().discover_nospawn {
                        ActorRefErr::InvalidRef
                    } else {
                        ActorRefErr::SpawnerError
                    }),
                }
            } else {
                // we try to discover the current actor id for the named actor
                let res = self
                    .inner
                    .nucleus
                    .registry()
                    .discover_named_actor(
                        self.inner.name.clone(),
                        Some(A::info().actor_type.into()),
                        None,
                    )
                    .await;

                match res {
                    Some((id, _)) => {
                        let ref_: UnnamedActorRef<A> = self
                            .inner
                            .nucleus
                            .get_ref_by_id(id)
                            .unwrap() // this always resolves to a valid actor due to the discover_named_actor call
                            .try_into()
                            .unwrap(); // this errors only if actor is NamedActor, which it never is

                        match ref_ {
                            UnnamedActorRef::Local(ref r) => {
                                if r.is_moving_out() {
                                    fn log_warn(actor_name: &str) {
                                        // monomorphization bloat prevention
                                        warn!(
                                            "Actor {} moving - rediscovering after moving finishes",
                                            actor_name
                                        );
                                    }
                                    log_warn(self.inner.name.deref());

                                    self.inner
                                        .nucleus
                                        .registry()
                                        .mover()
                                        .send(WaitForMoveFinish {
                                            source_actor_id: r.actor_id(),
                                        })
                                        .await
                                        .unwrap();

                                    return Err(ActorRefErr::MoveError);
                                }
                            }
                            UnnamedActorRef::Remote(_) => (),
                        }

                        **guard = Some(ref_.clone());
                        Ok(ref_)
                    }
                    None => Err(ActorRefErr::UnknownNode),
                }
            }
        };

        let timetout = timeout(Duration::from_secs(10), f).await;
        match timetout {
            Ok(res) => res,
            Err(_) => Err(ActorRefErr::DiscoveryTimeout),
        }
    }

    #[tracing::instrument(name = "NamedRef Send", skip(self, msg), fields(message_type = M::name(), actor_name=self.inner.name.deref()))]
    pub async fn send<M: Message + Serializable>(&self, msg: M) -> Result<M::Result, SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let id_guard = match self.lock().await {
            Ok(guard) => guard,
            Err(_) => return Err(SendError::new(ActorRefErr::DiscoveryTimeout, msg)),
        };

        if id_guard.is_none() {
            return match self.discovery(id_guard).await {
                Ok(id) => id.send(msg).await,
                Err(e) => Err(SendError::new(e, msg)),
            };
        }

        let r = id_guard.clone().unwrap();
        drop(id_guard); // this is the most common route, so we drop the lock early
        let res = r.send(msg).await;

        if let Err(e) = res {
            let id_guard = match self.lock().await {
                Ok(guard) => guard,
                Err(_) => return Err(SendError::new(ActorRefErr::DiscoveryTimeout, e.msg)),
            };
            return match self.discovery(id_guard).await {
                Ok(id) => id.send(e.msg).await,
                Err(err) => Err(SendError::new(err, e.msg)),
            };
        }

        res
    }

    pub fn send_with_timeout<M: Message + Serializable>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = Result<Result<M::Result, SendError<M>>, Elapsed>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let fut = self.send(msg);
        tokio::time::timeout(timeout, fut).in_current_span()
    }

    #[tracing::instrument(name = "NamedRef Notify", skip(self, msg), fields(message_type = M::name(), actor_name=self.inner.name.deref()))]
    pub async fn notify<M: Message + Serializable>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let id_guard = match self.lock().await {
            Ok(guard) => guard,
            Err(_) => return Err(SendError::new(ActorRefErr::DiscoveryTimeout, msg)),
        };

        if id_guard.is_none() {
            return match self.discovery(id_guard).await {
                Ok(id) => id.notify(msg).await,
                Err(e) => Err(SendError::new(e, msg)),
            };
        }

        let r = id_guard.clone().unwrap();
        drop(id_guard); // this is the most common route, so we drop the lock early

        let res = r.notify(msg).await;
        if let Err(e) = res {
            let id_guard = match self.lock().await {
                Ok(guard) => guard,
                Err(_) => return Err(SendError::new(ActorRefErr::DiscoveryTimeout, e.msg)),
            };

            return match self.discovery(id_guard).await {
                Ok(id) => id.notify(e.msg).await,
                Err(err) => Err(SendError::new(err, e.msg)),
            };
        }
        res
    }

    #[tracing::instrument(name = "NamedRef NotifyForget", skip(self, msg), fields(message_type = M::name(), actor_name=self.inner.name.deref()))]
    pub fn notify_forget<M: Message + Serializable>(&self, mut msg: M) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        // TODO: this is a trylock, which means it will fail if the lock is held
        // this is potentially unacceptable and might happen too often, even though
        // the critical path is relatively short (only clones inner value)

        if let Ok(guard) = self.inner.id.try_lock() {
            if let Some(aref) = guard.as_ref() {
                let r = aref.clone();
                drop(guard); // this is the most common route, so we drop the lock early

                if let Err(e) = r.notify_forget(msg) {
                    msg = e.msg;
                } else {
                    return Ok(());
                }
            }
        };

        // simple try_lock failed, spawn a task to do proper locking/discovery, then notify(_forget)
        let self_ = self.clone();
        tokio::spawn(
            async move {
                counter!("named_ref_notify_forget_cold_path", "node_id" => self_.inner.nucleus.node_id().to_string(), "actor_type" => A::info().actor_type).increment(1);

                let id_guard = match self_.lock().await {
                    Ok(guard) => guard,
                    Err(_) => return,
                };

                if id_guard.is_none() {
                    if let Ok(id) = self_.discovery(id_guard).await {
                        let _ = id.notify_forget(msg);
                    };
                } else {
                    let r = id_guard.clone().unwrap();
                    drop(id_guard); // this is the most common route, so we drop the lock early
                    let _ = r.notify_forget(msg);
                }
            }
            .in_current_span(),
        );

        Ok(())
    }

    pub fn notify_delayed<M: Message + Serializable>(
        &self,
        msg: M,
        delay: Duration,
        on_failure: Option<impl FnOnce(SendError<M>) + Send + 'static>,
    ) where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        let r = self.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(delay).await;

                if let Err(e) = r.notify(msg).await {
                    if let Some(f) = on_failure {
                        f(e);
                    }
                }
            }
            .in_current_span(),
        );
    }

    pub async fn actor_id(&self) -> Option<Snowflake<ActorMarker>> {
        let id_guard = self.inner.id.lock().await;
        if id_guard.is_none() {
            return match self.discovery(id_guard).await {
                Ok(id) => Some(id.actor_id()),
                Err(_) => None,
            };
        }
        Some(id_guard.as_ref().unwrap().actor_id())
    }

    pub fn actor_id_sync(&self) -> Option<Snowflake<ActorMarker>> {
        let id_guard = self.inner.id.try_lock().ok()?;

        Some(id_guard.as_ref()?.actor_id())
    }

    pub(crate) async fn set_inner_ref(
        &self,
        ref_: UnnamedActorRef<A>,
    ) -> Option<UnnamedActorRef<A>> {
        let mut id_guard = self.inner.id.lock().await;
        id_guard.replace(ref_)
    }

    pub async fn get_inner_local(&self) -> Option<LocalActorRef<A>> {
        let id_guard = self.inner.id.lock().await;

        if id_guard.is_none() {
            return match self.discovery(id_guard).await {
                Ok(id) => match id {
                    UnnamedActorRef::Local(r) => Some(r),
                    _ => None,
                },
                Err(_) => None,
            };
        }

        match id_guard.as_ref().unwrap() {
            UnnamedActorRef::Local(r) => Some(r.clone()),
            _ => None,
        }
    }
}

type MovingInField<A> = Option<mpsc::UnboundedSender<MessageHandler<A>>>;

pub(crate) struct LocalActorRefInner<A: Actor> {
    pub(crate) actor_id: Snowflake<ActorMarker>,
    //path: ActorPath,
    sender: MailboxSender<A>,
    moving_out: OnceCell<MovingVtable<A>>,
    moving_in: MovingInField<A>,
}

#[derive(Clone)]
struct RemoteActorRefInner {
    actor_id: Snowflake<ActorMarker>,
    nucleus: NucleusNode,
}

pub(crate) struct NamedActorRefInner<A: Actor> {
    name: Arc<str>,
    id: Mutex<Option<UnnamedActorRef<A>>>,
    nucleus: NucleusNode,
    is_spawnable: bool,
    _a: std::marker::PhantomData<A>,
}

impl<A: Actor> Clone for NamedActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub(crate) enum UnnamedActorRef<A: Actor> {
    Local(LocalActorRef<A>),
    Remote(RemoteActorRef<A>),
}

impl<A: Actor> std::fmt::Debug for UnnamedActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnnamedActorRef::Local(r) => f
                .debug_struct("UnnamedActorRef::Local")
                .field("actor_id", &r.actor_id())
                .field("actor_type", &std::any::type_name::<A>())
                .finish(),
            UnnamedActorRef::Remote(r) => f
                .debug_struct("UnnamedActorRef::Remote")
                .field("actor_id", &r.actor_id())
                .field("actor_type", &std::any::type_name::<A>())
                .finish(),
        }
    }
}

impl<A: Actor> UnnamedActorRef<A> {
    pub fn actor_id(&self) -> Snowflake<ActorMarker> {
        match self {
            UnnamedActorRef::Local(r) => r.actor_id(),
            UnnamedActorRef::Remote(r) => r.actor_id(),
        }
    }
}
impl<A: RemoteActor> UnnamedActorRef<A> {
    pub(crate) fn send<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        async move {
            match self {
                UnnamedActorRef::Local(r) => r.send(msg).await,
                UnnamedActorRef::Remote(r) => r.send(msg).await,
            }
        }
        .in_current_span()
    }
    pub(crate) fn notify<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<(), SendError<M>>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        async move {
            match self {
                UnnamedActorRef::Local(r) => r.notify(msg),
                UnnamedActorRef::Remote(r) => r.notify(msg).await,
            }
        }
        .in_current_span()
    }

    pub(crate) fn notify_forget<M: Message + Serializable>(
        &self,
        msg: M,
    ) -> Result<(), SendError<M>>
    where
        A: HandlerMarker<M>,
        M::Result: Serializable,
    {
        match self {
            UnnamedActorRef::Local(r) => r.notify(msg),
            UnnamedActorRef::Remote(r) => r.notify_forget(msg),
        }
    }
}

impl<A: Actor> Clone for LocalActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<A: Actor> Clone for UnnamedActorRef<A> {
    fn clone(&self) -> Self {
        match self {
            UnnamedActorRef::Local(r) => UnnamedActorRef::Local(r.clone()),
            UnnamedActorRef::Remote(r) => UnnamedActorRef::Remote(r.clone()),
        }
    }
}
impl<A: Actor> From<LocalActorRef<A>> for ActorRef<A> {
    fn from(val: LocalActorRef<A>) -> Self {
        ActorRef::Local(val)
    }
}

impl<A: Actor> Clone for RemoteActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _a: std::marker::PhantomData,
        }
    }
}

impl<A: Actor> From<RemoteActorRef<A>> for ActorRef<A> {
    fn from(val: RemoteActorRef<A>) -> Self {
        ActorRef::Remote(val)
    }
}
impl<A: Actor> From<UnnamedActorRef<A>> for ActorRef<A> {
    fn from(val: UnnamedActorRef<A>) -> Self {
        match val {
            UnnamedActorRef::Local(r) => ActorRef::Local(r),
            UnnamedActorRef::Remote(r) => ActorRef::Remote(r),
        }
    }
}
impl<A: Actor> TryFrom<ActorRef<A>> for UnnamedActorRef<A> {
    type Error = ActorRef<A>;

    fn try_from(value: ActorRef<A>) -> Result<Self, Self::Error> {
        match value {
            ActorRef::Local(r) => Ok(UnnamedActorRef::Local(r)),
            ActorRef::Remote(r) => Ok(UnnamedActorRef::Remote(r)),
            ActorRef::Named(_) => Err(value),
        }
    }
}

pub(crate) trait ActorRefTrait: Any {
    fn as_any(&self) -> &dyn std::any::Any;
    fn notify_stop(&self);
    fn named_update_actor_id(&self, _id: Snowflake<ActorMarker>) {}
    #[cfg(feature = "metrics")]
    fn notify_latency_msg(&self) {}
}

impl<A: Actor> ActorRefTrait for LocalActorRef<A> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn notify_stop(&self) {
        if let Err(e) = self.notify(ActorStopMessage) {
            warn!("failed to notify stop: {:?}", e);
        }
    }

    #[cfg(feature = "metrics")]
    fn notify_latency_msg(&self) {
        let _ = self.notify(LatencyMsg::new());
    }
}

impl<A: RemoteActor> ActorRefTrait for RemoteActorRef<A> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn notify_stop(&self) {
        let cloned = self.clone();
        tokio::spawn(
            async move {
                if let Err(e) = cloned.notify(ActorStopMessage).await {
                    warn!("failed to notify stop: {:?}", e);
                }
            }
            .in_current_span(),
        );
    }
}

impl<A: RemoteActor> ActorRefTrait for NamedActorRef<A> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn notify_stop(&self) {
        let cloned = self.clone();
        tokio::spawn(
            async move {
                if let Err(e) = cloned.notify(ActorStopMessage).await {
                    warn!("failed to notify stop: {:?}", e);
                }
            }
            .in_current_span(),
        );
    }

    fn named_update_actor_id(&self, id: Snowflake<ActorMarker>) {
        if let Ok(mut guard) = self.inner.id.try_lock() {
            *guard = self
                .inner
                .nucleus
                .get_ref_by_id(id)
                .map(|r| r.try_into().unwrap());
        }
    }
}

impl<A: RemoteActor> ActorRefTrait for ActorRef<A> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn notify_stop(&self) {
        let cloned = self.clone();
        tokio::spawn(
            async move {
                if let Err(e) = cloned.notify(ActorStopMessage).await {
                    warn!("failed to notify stop: {:?}", e);
                }
            }
            .in_current_span(),
        );
    }
}

/// A type-omitted reference to an [`Actor`][Actor]
#[derive(Clone)]
pub(crate) struct BoxedActorRef(pub Arc<dyn ActorRefTrait + Send + Sync>);

impl BoxedActorRef {
    pub(crate) fn notify_stop(&self) {
        self.0.notify_stop();
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub enum ActorRefErr {
    InvalidRef = 0,
    ResultChannelClosed = 1,
    UnknownNode = 2,
    ShuttingDown = 3,
    ActorPanicked = 4,
    SpawnerError = 5,
    DiscoveryTimeout = 6,
    NamedActorInnerRefError = 7,
    MoveError = 8,
    DeserializationError = 9,
    HandlerNotRegistered = 10,
    ActorMailboxFull = 11,
}

impl Serializable for ActorRefErr {
    fn serialize(&self) -> Bytes {
        vec![*self as u8].into()
    }

    fn deserialize<B>(
        mut data: B,
        _nucleus: Option<&NucleusNode>,
    ) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(match data.get_u8() {
            0 => ActorRefErr::InvalidRef,
            1 => ActorRefErr::ResultChannelClosed,
            2 => ActorRefErr::UnknownNode,
            3 => ActorRefErr::ShuttingDown,
            4 => ActorRefErr::ActorPanicked,
            5 => ActorRefErr::SpawnerError,
            6 => ActorRefErr::DiscoveryTimeout,
            7 => ActorRefErr::NamedActorInnerRefError,
            8 => ActorRefErr::MoveError,
            9 => ActorRefErr::DeserializationError,
            10 => ActorRefErr::HandlerNotRegistered,
            11 => ActorRefErr::ActorMailboxFull,
            _ => panic!("invalid ActorRefErr variant"),
        })
    }
}

impl From<DeserializationError> for ActorRefErr {
    fn from(_e: DeserializationError) -> Self {
        ActorRefErr::DeserializationError
    }
}

impl NucleusNode {
    pub fn get_local_actor<A: Actor>(
        &self,
        id: Snowflake<ActorMarker>,
    ) -> Option<LocalActorRef<A>> {
        self.registry().get_actor(id)
    }

    pub fn get_weak_actor_ref(
        &self,
        id: Snowflake<ActorMarker>,
        actor_type: &'static str,
    ) -> Option<WeakActorRef> {
        if id.worker_id() == self.inner.id {
            if !self.registry().local_actor_exists(id) {
                return None;
            }
        }

        Some(WeakActorRef::new_unchecked(self.clone(), id, actor_type))
    }

    pub async fn discover_named_weak_actor_ref(
        &self,
        name: impl Into<Arc<str>>,
        actor_type: &'static str,
        search_nodes: Option<&[NodeId]>,
    ) -> Option<WeakActorRef> {
        if let Some((actor_id, _ret_type)) = self
            .registry()
            .discover_named_actor(name.into(), Some(actor_type.into()), search_nodes)
            .await
        {
            Some(WeakActorRef::new_unchecked(
                self.clone(),
                actor_id,
                actor_type,
            ))
        } else {
            None
        }
    }

    pub fn get_ref_by_id<A: RemoteActor>(&self, id: Snowflake<ActorMarker>) -> Option<ActorRef<A>> {
        if id.worker_id() == self.inner.id {
            self.get_local_actor(id).map(ActorRef::from)
        } else {
            Some(ActorRef::Remote(RemoteActorRef::<A>::new(id, self.clone())))
        }
    }

    pub fn get_raw_named_ref<A: RemoteActor>(&self, name: impl Into<Arc<str>>) -> NamedActorRef<A> {
        self.registry().get_named_actor_ref_cache(name.into())
    }

    pub async fn get_named_ref<A: RemoteActor>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> Result<NamedActorRef<A>, ActorRefErr> {
        let ref_ = self.get_raw_named_ref(name.into());
        let guard = ref_.inner.id.lock().await;
        if guard.is_none() {
            ref_.discovery(guard).await?;
        } else {
            drop(guard);
        }
        Ok(ref_)
    }

    pub async fn get_named_ref_with_hint<A: RemoteActor>(
        &self,
        name: impl Into<Arc<str>>,
        hint: Snowflake<ActorMarker>,
    ) -> NamedActorRef<A> {
        let named = self.get_raw_named_ref(name);
        let mut guard = named.inner.id.lock().await;
        if guard.is_none() {
            let hint_ref = self.get_ref_by_id(hint).map(|r| r.try_into().unwrap());
            *guard = hint_ref;
        }

        drop(guard);

        named
    }

    pub async fn get_named_ref_with_node_hint<A: RemoteActor>(
        &self,
        name: impl Into<Arc<str>>,
        hint: Vec<NodeId>,
    ) -> Result<NamedActorRef<A>, ActorRefErr> {
        let name: Arc<str> = name.into();
        let aref = self.get_raw_named_ref(name.clone());

        let mut guard = aref.inner.id.lock().await;
        if guard.is_some() {
            drop(guard);
            return Ok(aref);
        }

        let res = self
            .registry()
            .discover_named_actor(name.clone(), Some(A::info().actor_type.into()), Some(&hint))
            .await;

        if let Some((id, _)) = res {
            // Ok(self.get_named_ref_with_hint(name, id).await)
            guard.replace(self.get_ref_by_id(id).unwrap().try_into().unwrap());
            drop(guard);
            Ok(aref)
        } else {
            let hint: FxHashSet<u16> = FxHashSet::from_iter(hint.into_iter());
            let remaining_nodes: Vec<NodeId> = self
                .registry()
                .get_remote_nodes()
                .iter()
                .filter_map(|n| {
                    let id = n.key();
                    if hint.contains(id) { None } else { Some(*id) }
                })
                .collect();

            let res = self
                .registry()
                .discover_named_actor(
                    name.clone(),
                    Some(A::info().actor_type.into()),
                    Some(&remaining_nodes),
                )
                .await;

            if let Some((id, _)) = res {
                guard.replace(self.get_ref_by_id(id).unwrap().try_into().unwrap());
                drop(guard);
                Ok(aref)
            } else {
                Err(ActorRefErr::InvalidRef)
            }
        }
    }

    pub(crate) fn send_remote_node<M: Message + Serializable>(
        &self,
        node_id: NodeId,
        message: M,
    ) -> Result<impl Future<Output = Result<M::Result, SendError<M>>> + use<M>, SendError<M>>
    where
        RemoteRegistryActor: Handler<M>,
        M::Result: Serializable,
    {
        let node_ref = self.registry().get_remote_node(node_id);
        if node_ref.is_none() {
            warn!(
                "tried to send message to remote node registry of an unknown node: {}",
                node_id
            );
            return Err(SendError::new(ActorRefErr::UnknownNode, message));
        }
        let node_ref = node_ref.unwrap();
        let node_ref = node_ref.actor_ref();

        let msg_wrap = proto::core::RemoteMessage {
            actor_id: None,
            request_id: self.inner.snowflake.generate(),
            actor_type: RemoteRegistryActor::info().actor_type.into(),
            message_type: M::name().into(),
            response_mode: ResponseMode::Payload as u32,
            payload: message.serialize(),
        };

        let (tx, rx) = oneshot::channel();
        let send_res = node_ref.send_with_sender(msg_wrap, tx.into());
        if let Err(e) = send_res {
            warn!(
                "failed to send message {} to remote node {}: {:?}",
                M::name(),
                node_id,
                e
            );
            return Err(SendError::new(e.err, message));
        }

        let nucleus = self.clone();

        Ok(async move {
            let res = rx.await;

            match res {
                Ok(r) => match convert_response(r) {
                    Ok(Some(r)) => M::Result::deserialize(r, Some(&nucleus)).map_err(|e| {
                        error!("<hazard> failed to deserialize remote response: {:?}", e);
                        SendError::new(ActorRefErr::DeserializationError, message)
                    }),
                    Ok(None) => {
                        panic!("received notify ack on a send message!");
                    }
                    Err(err) => Err(SendError::new(err, message)),
                },
                Err(_err) => Err(SendError::new(ActorRefErr::InvalidRef, message)),
            }
        })
    }

    pub(crate) async fn publish_remote_nodes<M: Message + Serializable + Clone>(&self, message: M)
    where
        RemoteRegistryActor: Handler<M>,
        M::Result: Serializable,
    {
        debug!("publishing message {} to all remote nodes", M::name());
        let nodes = self.registry().get_remote_addresses();

        let mut futs = Vec::with_capacity(nodes.len());

        for node in nodes {
            match self.send_remote_node(node.node_id as u16, message.clone()) {
                Ok(fut) => {
                    futs.push(async move {
                        let res = fut.await;
                        if let Err(e) = res {
                            warn!(
                                "<hazard> failed to publish message {} to remote node: {:?}",
                                M::name(),
                                e
                            );
                        }
                    });
                }
                Err(e) => {
                    warn!(
                        "<hazard> failed to publish message {} to remote node: {:?}",
                        M::name(),
                        e
                    );
                }
            }
        }

        join_all_unordered(futs).await;
    }
}

pub struct SendError<M> {
    pub err: ActorRefErr,
    pub msg: M,
}
impl<M> SendError<M> {
    pub fn new(err: ActorRefErr, msg: M) -> Self {
        Self { err, msg }
    }
    // pub fn new_empty(err: ActorRefErr) -> Self {
    //     Self { err, msg: None }
    // }
    // pub fn new_option(err: ActorRefErr, msg: Option<M>) -> Self {
    //     Self { err, msg }
    // }
}
impl<T> Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.err, f)
    }
}

impl<A: RemoteActor> Serializable for ActorRef<A> {
    fn serialize(&self) -> Bytes {
        let actor_id = match self {
            ActorRef::Local(r) => Some(r.inner.actor_id),
            ActorRef::Remote(r) => Some(r.inner.actor_id),
            ActorRef::Named(r) => r.actor_id_sync(),
        };

        let name = match self {
            ActorRef::Named(r) => Some(r.inner.name.to_string()),
            _ => None,
        };

        proto::mover::SerializedActorRef {
            actor_id,
            name: name.map(|n| n.into()),
            actor_type: A::info().actor_type.into(),
        }
        .encode_to_vec()
        .into()
    }

    fn deserialize<B>(data: B, nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        if nucleus.is_none() {
            return Err(DeserializationError::Other(
                "nucleus needs to be provided to deserialize ActoRef".into(),
            ));
        }
        let nucleus = nucleus.unwrap();

        let decoded = proto::mover::SerializedActorRef::decode(data).map_err(|e| {
            error!("failed to decode serialized actor ref: {:?}", e);
            DeserializationError::Prost(e)
        })?;

        if decoded.actor_type.as_str() != A::info().actor_type {
            return Err(DeserializationError::Other(
                format!(
                    "actor type mismatch, received {} expected {}",
                    decoded.actor_type,
                    A::info().actor_type
                )
                .into(),
            ));
        }

        let mut id_ref: Option<UnnamedActorRef<A>> = None;
        if let Some(actor_id) = decoded.actor_id {
            id_ref = nucleus
                .get_ref_by_id(actor_id)
                .map(|r| r.try_into().unwrap());
        }

        if let Some(name) = decoded.name {
            let ref_ = nucleus.get_raw_named_ref(name.get_counted());
            if let Ok(mut guard) = ref_.inner.id.try_lock() {
                *guard = id_ref;
            }

            Ok(ActorRef::Named(ref_))
        } else {
            match id_ref {
                Some(r) => Ok(r.into()),
                None => Err(DeserializationError::Other("invalid actor ref".into())),
            }
        }
    }
}

pub mod weak {
    use snowflake::Snowflake;
    use tokio::sync::oneshot;

    use crate::{
        ActorMarker, NucleusNode,
        actor::{
            Actor, RemoteActor,
            message::{Message, Serializable},
            refs::{ActorRef, LocalActorRef, RemoteActorRef, RemoteActorRefInner, SendError},
        },
    };

    #[derive(Clone)]
    pub struct WeakActorRef {
        inner: RemoteActorRefInner,
        actor_type: &'static str,
        actor_id: Snowflake<ActorMarker>,
    }

    impl WeakActorRef {
        pub fn new_unchecked(
            nucleus: NucleusNode,
            actor_id: Snowflake<ActorMarker>,
            actor_type: &'static str,
        ) -> Self {
            Self {
                inner: RemoteActorRefInner { actor_id, nucleus },
                actor_type,
                actor_id,
            }
        }

        pub fn is_local(&self) -> bool {
            self.actor_id.worker_id() == self.inner.nucleus.node_id()
        }

        pub fn upgrade<A: RemoteActor>(&self) -> Option<ActorRef<A>> {
            if A::info().actor_type != self.actor_type {
                error!(
                    "<hazard> attempted to upgrade WeakActorRef to incompatible actor type: {} != {}",
                    A::info().actor_type,
                    self.actor_type
                );
                return None;
            }

            if self.is_local() {
                self.inner
                    .nucleus
                    .get_local_actor::<A>(self.actor_id)
                    .map(ActorRef::from)
            } else {
                Some(ActorRef::Remote(RemoteActorRef::<A>::new(
                    self.actor_id,
                    self.inner.nucleus.clone(),
                )))
            }
        }

        pub async fn send<M: Message + Serializable>(
            &self,
            msg: M,
        ) -> Result<M::Result, crate::actor::refs::SendError<M>>
        where
            M::Result: Serializable,
        {
            if self.is_local() {
                self.clone_as_local().send(msg).await
            } else {
                // remote send
                self.inner.send_unchecked(msg, self.actor_type.into()).await
            }
        }

        pub async fn notify<M: Message + Serializable>(
            &self,
            msg: M,
        ) -> Result<(), crate::actor::refs::SendError<M>>
        where
            M::Result: Serializable,
        {
            if self.is_local() {
                self.clone_as_local().notify(msg).await
            } else {
                // remote notify
                self.inner
                    .notify_unchecked(msg, self.actor_type.into())
                    .await
            }
        }

        pub fn into_local(self) -> WeakActorRefLocal {
            assert!(
                self.is_local(),
                "attempted to create WeakActorRefLocal from remote WeakActorRef"
            );

            WeakActorRefLocal { inner: self }
        }

        fn clone_as_local(&self) -> WeakActorRefLocal {
            assert!(
                self.is_local(),
                "attempted to create WeakActorRefLocal from remote WeakActorRef"
            );

            WeakActorRefLocal {
                inner: self.clone(),
            }
        }

        pub fn actor_id(&self) -> Snowflake<ActorMarker> {
            self.actor_id
        }
    }

    #[derive(Clone)]
    pub struct WeakActorRefLocal {
        inner: WeakActorRef,
    }

    impl WeakActorRefLocal {
        pub async fn send<M: Message>(
            &self,
            msg: M,
        ) -> Result<M::Result, crate::actor::refs::SendError<M>> {
            let local_handler = self
                .inner
                .inner
                .nucleus
                .registry()
                .get_handler(&(self.inner.actor_type, M::name()));

            if let Some(handler) = &local_handler {
                match handler.handle_local(
                    &self.inner.inner.nucleus,
                    self.inner.actor_id,
                    Box::new(msg),
                ) {
                    Ok(receiver) => Ok({
                        let receiver = receiver.downcast::<oneshot::Receiver<M::Result>>().unwrap();

                        receiver.await.expect("sender dropped")
                    }),
                    Err(err) => {
                        error!(
                            "<hazard> failed to handle local message in WeakActorRef: {:?}",
                            err
                        );
                        let msg = err.msg.downcast::<M>().unwrap();
                        Err(SendError::new(err.err, *msg))
                    }
                }
            } else {
                error!(
                    "<hazard> no local handler registered for message {} in WeakActorRef for actor type {}",
                    M::name(),
                    self.inner.actor_type
                );

                Err(SendError::new(
                    crate::actor::refs::ActorRefErr::HandlerNotRegistered,
                    msg,
                ))
            }
        }

        pub async fn notify<M: Message>(
            &self,
            msg: M,
        ) -> Result<(), crate::actor::refs::SendError<M>> {
            let local_handler = self
                .inner
                .inner
                .nucleus
                .registry()
                .get_handler(&(self.inner.actor_type, M::name()));

            if let Some(handler) = &local_handler {
                match handler.handle_local(
                    &self.inner.inner.nucleus,
                    self.inner.actor_id,
                    Box::new(msg),
                ) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        let msg = err.msg.downcast::<M>().unwrap();
                        Err(SendError::new(err.err, *msg))
                    }
                }
            } else {
                error!(
                    "<hazard> no local handler registered for message {} in WeakActorRef for actor type {}",
                    M::name(),
                    self.inner.actor_type
                );
                Err(SendError::new(
                    crate::actor::refs::ActorRefErr::HandlerNotRegistered,
                    msg,
                ))
            }
        }

        pub fn into_weak(self) -> WeakActorRef {
            self.inner
        }

        pub fn actor_id(&self) -> Snowflake<ActorMarker> {
            self.inner.actor_id
        }

        pub fn upgrade<A: Actor>(&self) -> Option<LocalActorRef<A>> {
            self.inner
                .inner
                .nucleus
                .get_local_actor::<A>(self.inner.actor_id)
        }
    }
}

#[cfg(test)]
mod tests {
    fn check_sized<T: Sized>() {}

    struct RefTestActor;
    impl crate::actor::Actor for RefTestActor {}

    #[test]
    fn check_local_actor_ref_size() {
        check_sized::<crate::actor::refs::LocalActorRef<RefTestActor>>();
    }
}
