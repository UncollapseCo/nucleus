use bytes::{Buf, Bytes};

#[cfg(feature = "static_registry")]
use crate::actor::message::HandlerMarker;
use crate::{
    NucleusNode,
    actor::{Actor, ActorContext},
};

use super::{DeserializationError, Handler, Message, Serializable};

#[derive(Debug, Clone)]
pub struct PingMsg;

impl Message for PingMsg {
    type Result = ();
    fn name() -> &'static str {
        "System.PingMsg"
    }
}

impl Serializable for PingMsg {
    fn serialize(&self) -> Bytes {
        Bytes::new()
    }

    fn deserialize<B>(
        _data: B,
        _nucleus: Option<&NucleusNode>,
    ) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(Self {})
    }
}

#[async_trait_nobox]
impl<T: Actor> Handler<PingMsg> for T {
    async fn handle(&mut self, _message: PingMsg, ctx: &mut ActorContext<Self>) -> () {
        debug!("Ping received on {}", ctx.name());
    }
}

#[cfg(feature = "static_registry")]
#[diagnostic::do_not_recommend]
impl<T: Actor> HandlerMarker<PingMsg> for T {}

pub struct ActorStopMessage;

impl Message for ActorStopMessage {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.ActorStopMessage"
    }
}

impl Serializable for ActorStopMessage {
    fn serialize(&self) -> Bytes {
        Bytes::new()
    }

    fn deserialize<B>(
        _data: B,
        _nucleus: Option<&NucleusNode>,
    ) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(Self {})
    }
}

#[async_trait_nobox]
#[diagnostic::do_not_recommend]
impl<T: Actor> Handler<ActorStopMessage> for T {
    async fn handle(&mut self, _message: ActorStopMessage, ctx: &mut ActorContext<Self>) -> () {
        debug!("Stopping actor {}", ctx.name());
        ctx.stop();
    }
}

#[cfg(feature = "static_registry")]
#[diagnostic::do_not_recommend]
impl<T: Actor> HandlerMarker<ActorStopMessage> for T {}

pub(crate) struct CallbackMessage<
    A: Actor,
    F: FnOnce(&mut A, &mut ActorContext<A>) + Send + Sync + 'static,
> {
    cb: F,
    marker: std::marker::PhantomData<A>,
}
impl<A: Actor, F: FnOnce(&mut A, &mut ActorContext<A>) + Send + Sync + 'static>
    CallbackMessage<A, F>
{
    pub fn new(cb: F) -> Self {
        Self {
            cb,
            marker: std::marker::PhantomData,
        }
    }
}

impl<A: Actor, F: FnOnce(&mut A, &mut ActorContext<A>) + Send + Sync + 'static> Message
    for CallbackMessage<A, F>
{
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.CallbackMessage"
    }
}

#[async_trait_nobox]
impl<A: Actor, F: FnOnce(&mut A, &mut ActorContext<A>) + Send + Sync + 'static>
    Handler<CallbackMessage<A, F>> for A
{
    async fn handle(&mut self, message: CallbackMessage<A, F>, ctx: &mut ActorContext<Self>) {
        debug!("Callback message received from {}", ctx.name());
        (message.cb)(self, ctx);
    }
}

#[cfg(feature = "metrics")]
// LatencyMsg - contains Instant, all actors implement it, log latency since the passed instant and log it as a metric as well
#[derive(Debug, Clone)]
pub struct LatencyMsg {
    pub instant: std::time::Instant,
}
#[cfg(feature = "metrics")]
impl LatencyMsg {
    pub fn new() -> Self {
        Self {
            instant: std::time::Instant::now(),
        }
    }
}
#[cfg(feature = "metrics")]
impl Default for LatencyMsg {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(feature = "metrics")]
impl Message for LatencyMsg {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.LatencyMsg"
    }
}
#[cfg(feature = "metrics")]
#[async_trait_nobox]
impl<A: Actor> Handler<LatencyMsg> for A {
    async fn handle(&mut self, message: LatencyMsg, ctx: &mut ActorContext<Self>) {
        use metrics::counter;
        use metrics::histogram;

        let latency = message.instant.elapsed();
        const LATENCY_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(3);
        if latency > LATENCY_THRESHOLD {
            warn!(
                "Latency on actor {} exceeded threshold of {}ms: {}ms",
                ctx.name(),
                LATENCY_THRESHOLD.as_millis(),
                latency.as_millis()
            );
        }

        histogram!("actor_latency", "actor_type" => ctx.actor_type, "actor_name" => ctx.name().to_string(), "node_id" => ctx.nucleus().node_id().to_string()).record(latency.as_millis() as f64);

        let queue_size = ctx.message_rx.len();
        counter!("actor_mailbox_size", "actor_type" => ctx.actor_type, "node_id" => ctx.nucleus().node_id().to_string()).absolute(queue_size as u64);

        self.on_heartbeat(latency, ctx).await;
    }
}

pub(crate) struct MessageDroppedDueToFullMailbox(pub(crate) &'static str);
impl Message for MessageDroppedDueToFullMailbox {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.MessageDroppedDueToFullMailbox"
    }
}
#[async_trait_nobox]
impl<A: Actor> Handler<MessageDroppedDueToFullMailbox> for A {
    async fn handle(&mut self, msg: MessageDroppedDueToFullMailbox, ctx: &mut ActorContext<Self>) {
        self.on_mailbox_full_message_drop(ctx, msg.0);
    }
}
