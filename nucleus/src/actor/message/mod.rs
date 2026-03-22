use crate::actor::Actor;
use crate::utils::timed_closure;
use crate::{ActorMarker, NodeId, NucleusNode};
use bytes::{Buf, Bytes};
use metrics::Histogram;
use snowflake::Snowflake;
use std::any::Any;
use std::error::Error;
use std::mem;
use std::sync::OnceLock;
use std::time::Duration;
use std::{any::type_name, marker::PhantomData};
use tokio::time::Instant;
use tracing::{Instrument, Span};

#[cfg(feature = "smallbox")]
use smallbox::SmallBox;

use super::{ActorContext, Sender};
mod impls;
pub(crate) mod system;

pub(crate) static METRICS_NODE_ID: OnceLock<String> = OnceLock::new();

pub trait Message: 'static + Sync + Send + Sized {
    type Result: 'static + Sync + Send;
    const RESPECT_BACKPRESSURE: bool = true;

    fn name() -> &'static str;
}

#[derive(Debug)]
pub enum DeserializationError {
    Prost(prost::DecodeError),
    Other(Box<dyn Error + Send + Sync>),
}

impl From<prost::DecodeError> for DeserializationError {
    fn from(e: prost::DecodeError) -> Self {
        DeserializationError::Prost(e)
    }
}
impl<T> From<Box<T>> for DeserializationError
where
    T: Error + Send + Sync + 'static,
{
    fn from(e: Box<T>) -> Self {
        DeserializationError::Other(e)
    }
}

pub trait Serializable: 'static + Sync + Send + Sized {
    fn serialize(&self) -> Bytes;

    fn serialize_into<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let data = self.serialize();
        buf.put_slice(&data);
    }

    fn deserialize<B>(data: B, nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf;
}

pub trait IntoAnyBoxed: Send + Sync {
    fn into_any(self) -> Box<dyn Any>;
}

#[macro_export]
macro_rules! actor_message {
    ($name:ident -> $result:ty) => {
        impl ActorMessage for $name {
            type Result = $result;

            fn name() -> &'static str {
                stringify!($name)
            }
        }
    };
}

#[cfg(feature = "smallbox")]
type BoxStorage = [usize; 28];

enum DynResponse<'a> {
    Message(Box<dyn Any + Send>),
    Future(
        std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
        Span,
        #[cfg(feature = "metrics")] Histogram,
        #[cfg(not(feature = "metrics"))] (),
    ),
}

pub(crate) struct ActorMessageHandlerVtable<A: Actor> {
    #[cfg(feature = "smallbox")]
    handle_fn: SmallBox<
        dyn for<'a> FnMut(Option<(&'a mut A, &'a mut ActorContext<A>)>) -> DynResponse<'a> + Send,
        BoxStorage,
    >,

    #[allow(clippy::type_complexity)]
    #[cfg(not(feature = "smallbox"))]
    handle_fn: Box<
        dyn for<'a> FnMut(Option<(&'a mut A, &'a mut ActorContext<A>)>) -> DynResponse<'a> + Send,
    >,

    message_name: &'static str,
}

macro_rules! do_box {
    ($e:expr) => {{
        #[cfg(feature = "smallbox")]
        {
            SmallBox::new($e)
        }

        #[cfg(not(feature = "smallbox"))]
        {
            Box::new($e)
        }
    }};
}

impl<A: Actor> ActorMessageHandlerVtable<A> {
    pub fn new<M: Message>(
        msg: M,
        sender: Sender<M::Result>,
        drop_tx: Option<Sender<M>>,
        span: Span,
    ) -> Self
    where
        A: Handler<M>,
    {
        let mut msg_outer = Some(ActorMessage::<A, M> {
            msg: Some(msg),
            sender: Some(sender),
            drop_tx,
            _a: PhantomData,
            sender_span: span,
        });

        // this is its own function to limit monomorphization bloat
        fn make_span(
            sender_span: &mut Span,
            actor_name: &str,
            actor_id: Snowflake<ActorMarker>,
            msg_name: &'static str,
            nucleus_node_id: NodeId,
        ) -> Span {
            if msg_name == "System.RemoteMessage" {
                mem::replace(sender_span, Span::none())
            } else {
                info_span!(parent: &*sender_span, "Actor Handler", actor_name = actor_name, actor_id = actor_id.id(), message_type = msg_name, node_id = nucleus_node_id)
            }
        }

        let r = Self {
            handle_fn: do_box!(move |params| {
                if let Some(mut msg) = msg_outer.take() {
                    if let Some((actor, ctx)) = params {
                        #[cfg(feature = "metrics")]
                        let histogram = ActorMessage::<A, M>::histogram();
                        #[cfg(not(feature = "metrics"))]
                        let histogram = ();

                        let sender = msg.sender.take().unwrap();

                        let msg_span = make_span(
                            &mut msg.sender_span,
                            &ctx.name,
                            ctx.actor_id(),
                            M::name(),
                            ctx.nucleus().node_id(),
                        );

                        let msg_inner = msg.msg.take().unwrap();

                        msg_span.in_scope(|| {
                            actor.hook_before_handle(ctx, M::name());
                        });

                        let block = actor.handle_delayed(msg_inner, ctx, sender);

                        let fut = DynResponse::Future(Box::pin(block), msg_span, histogram);

                        drop(msg);

                        fut
                    } else {
                        DynResponse::Message(Box::new(msg))
                    }
                } else {
                    panic!("ActorMessageHandlerVtable called after being consumed");
                }
            }),
            message_name: M::name(),
        };

        #[cfg(feature = "smallbox")]
        {
            let handle_fn_size = std::mem::size_of_val(&r.handle_fn);
            if r.handle_fn.is_heap() {
                panic!(
                    "Warning: ActorMessageHandlerVtable for message {} on actor {} is heap allocated (size = {} bytes)",
                    M::name(),
                    type_name::<A>(),
                    handle_fn_size,
                );
            }
        }

        r
    }

    // pub fn handle calls the inner handle function
    pub async fn handle(&mut self, actor: &mut A, ctx: &mut ActorContext<A>) {
        let actor_timeout_setting = actor.max_message_processing_time();

        let msg_type_name: &'static str = self.message_name;
        let handle_fut = (self.handle_fn)(Some((actor, ctx)));

        match handle_fut {
            #[allow(unused_variables)]
            DynResponse::Future(fut, msg_span, histogram) => {
                let now = Instant::now();
                let handle_fut = async {
                    if let Some(t) = actor_timeout_setting {
                        let (res, _) = timed_closure(fut, Some(now), t, move || {
                            panic!(
                                "Actor {} took too long to process message {} (timeout set to {:?})",
                                type_name::<A>(),
                                msg_type_name,
                                t
                            )
                        })
                        .await;

                        res
                    } else {
                        fut.await
                    }
                };

                let (_res, _started) =
                    timed_closure(handle_fut, Some(now), Duration::from_secs(5), move || {
                        warn!(
                            "<hazard> Message {} on actor {} taking more than 5 seconds to process!",
                            msg_type_name,
                            type_name::<A>()
                        );
                    })
                    .instrument(msg_span)
                    .await;

                #[cfg(feature = "metrics")]
                let message_processing_took = now.elapsed();

                #[cfg(feature = "metrics")]
                histogram.record(message_processing_took.as_nanos() as f64);
            }
            DynResponse::Message(_) => panic!("Expected a future, got a message"),
        }
    }

    pub fn extract_msg<M: Message>(&mut self) -> ActorMessage<A, M>
    where
        A: Handler<M>,
    {
        let any_box = (self.handle_fn)(None);
        let any_box = match any_box {
            DynResponse::Future(_, _, _) => panic!("Expected a message, got a future"),
            DynResponse::Message(b) => b,
        };

        let down = any_box
            .downcast::<ActorMessage<A, M>>()
            .expect("Failed to downcast ActorMessage");

        *down
    }
}

impl<A: Actor, M: Message> IntoAnyBoxed for Box<ActorMessage<A, M>>
where
    A: Handler<M>,
{
    fn into_any(self) -> Box<dyn Any> {
        Box::new(self)
    }
}

impl<A: Actor, M: Message> ActorMessage<A, M>
where
    A: Handler<M>,
{
    fn histogram() -> Histogram {
        metrics::with_recorder(|r| {
            let key =
                metrics::Key::from_static_name("actor_message_handle").with_extra_labels(vec![
                    metrics::Label::from_static_parts("actor_type", type_name::<A>()),
                    metrics::Label::from_static_parts("message_type", M::name()),
                    metrics::Label::from_static_parts(
                        "node_id",
                        METRICS_NODE_ID.get().expect(
                            "ActorMessage is somehow being handled before nucleus runtime started?",
                        ),
                    ),
                ]);

            let metadata =
                metrics::Metadata::new(module_path!(), metrics::Level::INFO, Some(module_path!()));

            r.register_histogram(&key, &metadata)
        })
    }
}

pub(crate) type MessageHandler<A> = ActorMessageHandlerVtable<A>;

#[nucleus_macros::async_trait_nobox]
pub trait Handler<M: Message>: Actor {
    async fn handle(&mut self, message: M, _ctx: &mut ActorContext<Self>) -> M::Result;

    // #[tracing::instrument(name = "Actor", skip(self, message, _ctx, sender), fields(actor_name = _ctx.name))]
    async fn handle_delayed(
        &mut self,
        message: M,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<M::Result>,
    ) {
        // info!("inside handle delayed");
        let result = self.handle(message, _ctx).await;
        if sender.send(result).is_err() {
            // trace!(
            //     "message result ignored (message={}, actor={})",
            //     M::name(),
            //     type_name::<Self>()
            // );
        }
    }
}

/// Marker trait for static registry generation
/// This serves as a way to throw a compiler error in case the user uses static registries
/// and forgets to register a handler with it
#[cfg(feature = "static_registry")]
pub trait HandlerMarker<M: Message>
where
    Self: Actor,
    Self: Handler<M>,
{
}

#[cfg(not(feature = "static_registry"))]
pub use Handler as HandlerMarker;

// takes in a message, and outputs
/*
{
let (tx, rx) = oneshot::channel();
self.handle_delayed(message, ctx, tx.into()).await;
rx.await.unwrap()
}
*/
#[macro_export]
macro_rules! handle_self {
    ($self:ident, $ctx:expr, $message:expr) => {{
        use tokio::sync::oneshot;
        let (tx, rx) = oneshot::channel();
        $self.handle_delayed($message, $ctx, tx.into()).await;
        rx.await.unwrap()
    }};
}

pub(crate) struct ActorMessage<A: Actor, M: Message>
where
    A: Handler<M>,
{
    pub(crate) msg: Option<M>,
    pub(crate) sender: Option<Sender<M::Result>>,
    pub(crate) drop_tx: Option<Sender<M>>,
    // created_at: Instant,
    _a: PhantomData<A>,
    sender_span: Span,
}

impl<A: Actor, M: Message> Drop for ActorMessage<A, M>
where
    A: Handler<M>,
{
    fn drop(&mut self) {
        fn log_error(message_type: &'static str, actor_type: &'static str) {
            error!(
                "message dropped before being processed (message={}, actor={})",
                message_type, actor_type
            );
        }

        if let Some(msg) = self.msg.take() {
            if let Some(drop_tx) = self.drop_tx.take() {
                if drop_tx.send(msg).is_err() {
                    log_error(M::name(), type_name::<A>());
                }
            } else {
                log_error(M::name(), type_name::<A>());
            }
        }
    }
}
