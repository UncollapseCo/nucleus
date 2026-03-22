use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::io::IoSlice;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHashMap};
use metrics::histogram;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::actor::ActorContext;
use crate::actor::RemoteActorInfo;
use crate::actor::Sender;
use crate::actor::delay::delayed;
use crate::actor::message::system::ActorStopMessage;
use crate::actor::message::{Handler, Message};
use crate::actor::moving::MovableActor;
use crate::actor::refs::LocalActorRef;
use crate::actor::refs::{ActorRefErr, SendError};
use crate::net::registry::{InternalSystemShutdown, SystemMessage};
use crate::pubsub::{AnyTopic, PubsubMessage, PubsubPayload};
use crate::pubsub::{PubsubRef, Topic};
use crate::utils::{ArcAwaiter, AsyncErrorSquash, AwaitableArc, TaskHolder};
use crate::{
    ActorMarker, NUCLEUS_PROTOCOL_VERSION, NodeId, NucleusNode, RequestMarker, handle_self,
};
use crate::{
    actor::{Actor, RemoteActor, message::Serializable},
    snowflake::Snowflake,
};
use prost::Message as ProtoMessage;

use super::errors::RemoteError;
use super::mover::handlers::ForwardRemoteMessage;
use super::mover::{MovableSubRef, MovableSubRefStruct};
use super::proto::{self};
use super::registry::{RemoteNodeRecord, SystemTopic};

pub(crate) mod client_handler;

#[cfg(feature = "client")]
pub(crate) mod client;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_INTERVAL_GRACE: Duration = Duration::from_secs(30);

struct ActorMessagePair<A: RemoteActor, M: Message> {
    _a: PhantomData<A>,
    _m: PhantomData<M>,
}

pub(crate) trait ActorMessagePairTrait: Send + Sync {
    fn ident(&self) -> (&'static str, &'static str);
    fn handle(
        &self,
        nucleus: &NucleusNode,
        data: Bytes,
        actor_id: Snowflake<ActorMarker>,
        response: Option<Sender<Bytes>>,
        is_movein: bool,
    ) -> Result<(), ActorRefErr>;

    fn handle_local(
        &self,
        nucleus: &NucleusNode,
        actor_id: Snowflake<ActorMarker>,
        message: Box<dyn Any + Send>,
    ) -> Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>>>; // returns oneshot::Receiver<M::Result>
}

impl<A: RemoteActor, M: Message + Serializable + Any + Send> ActorMessagePairTrait
    for ActorMessagePair<A, M>
where
    A: Handler<M>,
    M::Result: Serializable,
{
    fn ident(&self) -> (&'static str, &'static str) {
        let actor_name: &'static str = A::info().actor_type;
        let message_name: &'static str = M::name();
        (actor_name, message_name)
    }

    fn handle(
        &self,
        nucleus: &NucleusNode,
        data: Bytes,
        actor_id: Snowflake<ActorMarker>,
        response: Option<Sender<Bytes>>,
        is_movein: bool,
    ) -> Result<(), ActorRefErr> {
        let actor = nucleus.get_local_actor(actor_id);
        if actor.is_none() {
            error!("failed to get local actor {}", actor_id);
            return Err(ActorRefErr::InvalidRef);
        }
        let actor: LocalActorRef<A> = actor.unwrap();
        if actor.is_moving_out() {
            Self::handle_moving(nucleus, data, actor_id, response);
            return Ok(());
        } else if is_movein {
            return Self::handle_moving_in(nucleus, data, actor, response);
        }

        Self::handle_normal(nucleus, data, &actor, response)
    }

    fn handle_local(
        &self,
        nucleus: &NucleusNode,
        actor_id: Snowflake<ActorMarker>,
        message: Box<dyn Any + Send>,
    ) -> Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>>> {
        ActorLocalMessagePair::<A, M> {
            _a: PhantomData,
            _m: PhantomData,
        }
        .handle_local(nucleus, actor_id, message)
    }
}

impl<A: RemoteActor, M: Message + Serializable> ActorMessagePair<A, M>
where
    A: Handler<M>,
    M::Result: Serializable,
{
    #[cold]
    fn handle_moving(
        nucleus: &NucleusNode,
        data: Bytes,
        actor_id: Snowflake<ActorMarker>,
        response: Option<Sender<Bytes>>,
    ) {
        let msg = ForwardRemoteMessage {
            actor_id,
            message_type: M::name(),
            payload: data,
        };

        if let Some(response) = response {
            nucleus
                .registry()
                .mover()
                .send_with_sender(msg, response)
                .unwrap();
        } else {
            nucleus.registry().mover().notify(msg).unwrap();
        }
    }

    #[cold]
    fn handle_moving_in(
        nucleus: &NucleusNode,
        data: Bytes,
        actor: LocalActorRef<A>,
        response: Option<Sender<Bytes>>,
    ) -> Result<(), ActorRefErr> {
        if actor.is_closed() {
            return Err(ActorRefErr::InvalidRef);
        }
        let message = M::deserialize(data, Some(nucleus))?;

        actor.send_resume(message, response).map_err(|e| e.err)?;

        Ok(())
    }

    fn handle_normal(
        nucleus: &NucleusNode,
        data: Bytes,
        actor: &LocalActorRef<A>,
        response: Option<Sender<Bytes>>,
    ) -> Result<(), ActorRefErr> {
        let message = M::deserialize(data, Some(nucleus))?;

        if let Some(response) = response {
            let result = actor.send_eager(message);
            match result {
                Ok(result) => {
                    tokio::spawn(
                        async move {
                            let result = result.await;

                            let data = result.serialize();

                            if response.send(data).is_err() {
                                fn log_warn() {
                                    // monomorphization bloat prevention
                                    warn!("failed to send response, receiver was closed");
                                }
                                log_warn();
                            }
                        }
                        .instrument({
                            fn make_span(
                                actor_id: Snowflake<ActorMarker>,
                                actor_type: &'static str,
                                message_type: &'static str,
                            ) -> Span {
                                // monomorphization bloat prevention
                                trace_span!(
                                    "RemoteActorMessageHandle",
                                    actor_id = actor_id.id(),
                                    actor_type = actor_type,
                                    message_type = message_type,
                                )
                            }
                            make_span(actor.actor_id(), A::info().actor_type, M::name())
                        }),
                    );
                }
                Err(e) => {
                    return Err(e.err);
                }
            }
        } else {
            actor.notify(message).map_err(|e| e.err)?;
        }

        Ok(())
    }
}

struct ActorLocalMessagePair<A: RemoteActor, M: Message> {
    _a: PhantomData<A>,
    _m: PhantomData<M>,
}

impl<A: RemoteActor, M: Message> ActorMessagePairTrait for ActorLocalMessagePair<A, M>
where
    A: Handler<M>,
{
    fn ident(&self) -> (&'static str, &'static str) {
        let actor_name: &'static str = A::info().actor_type;
        let message_name: &'static str = M::name();
        (actor_name, message_name)
    }

    fn handle(
        &self,
        _nucleus: &NucleusNode,
        _data: Bytes,
        _actor_id: Snowflake<ActorMarker>,
        _response: Option<Sender<Bytes>>,
        _is_movein: bool,
    ) -> Result<(), ActorRefErr> {
        panic!("ActorLocalMessagePair should not handle remote messages");
    }

    fn handle_local(
        &self,
        nucleus: &NucleusNode,
        actor_id: Snowflake<ActorMarker>,
        message: Box<dyn Any + Send>,
    ) -> Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>>> {
        let message_downcasted = message.downcast::<M>();
        if let Err(e) = message_downcasted {
            fn log_error(actor_id: Snowflake<ActorMarker>, msg_name: &'static str) {
                // monomorphization bloat prevention
                error!(
                    "message type mismatch for actor {}, expected {}, got different type",
                    actor_id, msg_name
                );
            }
            log_error(actor_id, M::name());
            return Err(SendError::new(ActorRefErr::HandlerNotRegistered, e));
        }
        let message_downcasted = message_downcasted.unwrap();

        let actor = nucleus.get_local_actor(actor_id);
        if actor.is_none() {
            fn log_error(actor_id: Snowflake<ActorMarker>) {
                // monomorphization bloat prevention
                error!("failed to get local actor {}", actor_id);
            }
            log_error(actor_id);
            return Err(SendError::new(ActorRefErr::InvalidRef, message_downcasted));
        }

        let actor: LocalActorRef<A> = actor.unwrap();

        let (tx, rx) = oneshot::channel();

        if let Err(e) = actor.send_with_sender(*message_downcasted, tx.into()) {
            return Err(SendError::new(e.err, Box::new(e.msg)));
        }

        let rx = Box::new(rx);

        Ok(rx)
    }
}

#[derive(Default)]
pub struct HandlerRegistryBuilder {
    handlers: HashMap<(&'static str, &'static str), Box<dyn ActorMessagePairTrait>>,
    topics: Vec<(&'static str, Box<dyn AnyTopic>)>,
    #[expect(clippy::type_complexity)] // 🤓
    movable_sub_refs: Vec<((&'static str, &'static str), Box<dyn MovableSubRef>)>,
}

impl HandlerRegistryBuilder {
    pub fn with_handler<A: RemoteActor + Handler<M>, M: Message + Serializable>(
        mut self,
    ) -> HandlerRegistryBuilder
    where
        M::Result: Serializable,
    {
        let h = Box::new(ActorMessagePair::<A, M> {
            _a: PhantomData,
            _m: PhantomData,
        });

        let ident = h.ident();
        match self.handlers.entry(ident) {
            Entry::Occupied(mut o) => {
                o.insert(h);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(h);
                self = self.with_actor::<A>();
            }
        }

        self
    }

    pub fn with_local_handler<
        A: RemoteActor + Handler<M>,
        M: Message + 'static + Sync + Send + Sized,
    >(
        mut self,
    ) -> HandlerRegistryBuilder {
        let h = Box::new(ActorLocalMessagePair::<A, M> {
            _a: PhantomData,
            _m: PhantomData,
        });

        let ident = h.ident();
        match self.handlers.entry(ident) {
            Entry::Occupied(_o) => {}
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(h);
                self = self.with_actor::<A>();
            }
        }

        self
    }

    pub fn with_actor<A: RemoteActor>(self) -> Self {
        let mut a = self;
        // registers all default messages like Stop
        a = a.with_handler::<A, ActorStopMessage>();
        #[cfg(feature = "debug")]
        {
            a = a.with_handler::<A, super::proto::debug::PingMsg>();
        }
        a
    }

    pub fn with_topic<T: Topic>(mut self, t: T) -> Self {
        self.topics.push((T::name(), Box::new(t)));
        self
    }

    pub fn with_movable_sub_ref<A: MovableActor + Handler<PubsubMessage<T>>, T: Topic>(
        mut self,
    ) -> Self {
        let sub_ref: MovableSubRefStruct<T, A> = MovableSubRefStruct::new();
        self.movable_sub_refs
            .push(((A::info().actor_type, T::name()), Box::new(sub_ref)));

        self
    }

    #[cfg(feature = "debug")]
    pub fn with_debuggable_actor<A: RemoteActor + Handler<proto::debug::GetSerialized>>(
        self,
    ) -> Self {
        let mut a = self;
        a = a.with_actor::<A>();
        a = a.with_handler::<A, proto::debug::GetSerialized>();
        a
    }

    pub fn with_static_registry(
        self,
        registry: &'static [fn(HandlerRegistryBuilder) -> HandlerRegistryBuilder],
    ) -> Self {
        let mut a = self;
        for reg_fn in registry.iter() {
            // generated by macro, calls self.with_handler inside
            a = reg_fn(a);
        }
        a
    }

    pub fn build(self) -> HandlerRegistry {
        HandlerRegistry::new(self.handlers, self.topics, self.movable_sub_refs)
    }
}

pub struct HandlerRegistry {
    handler_function: ph::fmph::Function,
    handlers: Vec<Box<dyn ActorMessagePairTrait>>,
    pub(crate) topics: FxHashMap<&'static str, Box<dyn AnyTopic>>,
    movable_sub_refs: FxHashMap<(&'static str, &'static str), Box<dyn MovableSubRef>>,
}

impl HandlerRegistry {
    #[expect(clippy::type_complexity)] // 🤓
    fn new(
        handlers: HashMap<(&'static str, &'static str), Box<dyn ActorMessagePairTrait>>,
        topics: Vec<(&'static str, Box<dyn AnyTopic>)>,
        movable_sub_refs: Vec<((&'static str, &'static str), Box<dyn MovableSubRef>)>,
    ) -> Self {
        let handlers = handlers.into_iter().map(|f| f.1).collect::<Vec<_>>();
        let keys = handlers.iter().map(|h| h.ident()).collect::<Vec<_>>();
        let function = ph::fmph::Function::new(keys);

        let mut ordered_handlers: Vec<Option<Box<dyn ActorMessagePairTrait>>> = Vec::new();
        for _ in 0..handlers.len() {
            ordered_handlers.push(None);
        }

        for handler in handlers.into_iter() {
            let key = handler.ident();
            let idx = function.get(&key).unwrap() as usize;
            ordered_handlers[idx] = Some(handler);
        }

        let handlers = ordered_handlers.into_iter().map(|h| h.unwrap()).collect();

        HandlerRegistry {
            handler_function: function,
            handlers,
            topics: topics.into_iter().collect(),
            movable_sub_refs: movable_sub_refs.into_iter().collect(),
        }
    }

    pub fn check_handler(&self, key: &(&str, &str)) -> bool {
        let idx = self.handler_function.get(key);
        if idx.is_none() {
            return false;
        }

        let idx = idx.unwrap() as usize;
        let to_ret = self.handlers[idx].as_ref();

        &to_ret.ident() == key
    }

    pub(crate) fn get_handler(&self, key: &(&str, &str)) -> Option<&dyn ActorMessagePairTrait> {
        let idx = self.handler_function.get(key);
        if idx.is_none() {
            warn!(
                "<hazard> handler not found for Actor {} Message {}",
                key.0, key.1
            );
            return None;
        }

        let idx = idx.unwrap() as usize;
        let to_ret = self.handlers[idx].as_ref();

        if &to_ret.ident() != key {
            return None;
        }

        Some(to_ret)
    }

    pub(crate) fn get_movable_sub_ref<'a, A: MovableActor>(
        &'a self,
        topic: &'a str,
    ) -> Option<&'a dyn MovableSubRef> {
        let key = (A::info().actor_type, topic);
        self.movable_sub_refs.get(&key).map(move |r| r.deref())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RemoteNodeStatus {
    Connected,
    Reconnecting,
    GracefulShutdown,
    ErrorStopping,
}

impl RemoteNodeStatus {
    fn is_stopping(&self) -> bool {
        matches!(
            self,
            RemoteNodeStatus::ErrorStopping | RemoteNodeStatus::GracefulShutdown
        )
    }
}

pub(crate) struct RemoteNode {
    nucleus: NucleusNode,
    status: RemoteNodeStatus,
    received: bool,
    writer: Option<OwnedWriteHalf>,
    reader: Option<OwnedReadHalf>,
    read_task: Option<(tokio::task::JoinHandle<OwnedReadHalf>, oneshot::Sender<()>)>,
    self_ref: Option<LocalActorRef<Self>>,
    response_waiters: Arc<
        DashMap<
            Snowflake<RequestMarker>,
            Sender<proto::core::remote_response::Response>,
            FxBuildHasher,
        >,
    >,

    send_buffer: VecDeque<proto::core::WireMessage>,
    handshake: proto::core::RemoteHandshake,
    seq: u64,
    seq_ack: u64,

    awaitable_arc: AwaitableArc,
    system_sub: Option<PubsubRef<SystemTopic>>,

    batch_send_buffer: Vec<proto::core::BatchMessageInner>,

    last_heartbeat: Instant,
}

async fn raw_handshake_receive(rx: &mut OwnedReadHalf) -> Result<Vec<u8>, RemoteError> {
    // read handshake
    let mut header = [0u8; 5];
    rx.read_exact(&mut header).await?;
    let version = header[0];
    let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;

    if version != NUCLEUS_PROTOCOL_VERSION {
        return Err(RemoteError::ProtocolError(format!(
            "expected protocol version {}, got {}",
            NUCLEUS_PROTOCOL_VERSION, version
        )));
    }

    let mut buf = vec![0u8; len];
    rx.read_exact(&mut buf).await?;

    trace!("received raw handshake message of length {}", len);

    Ok(buf)
}

async fn raw_handshake_write(tx: &mut OwnedWriteHalf, buf: Vec<u8>) -> Result<(), std::io::Error> {
    let mut header = [0u8; 5];
    let len = buf.len() as u32;
    header[0] = NUCLEUS_PROTOCOL_VERSION;
    header[1..5].copy_from_slice(&len.to_be_bytes());

    tx.write_all(&header).await?;
    tx.write_all(&buf).await?;
    tx.flush().await?;

    trace!("wrote raw handshake message of length {}", len);

    Ok(())
}

async fn read_raw_message(
    rx: &mut OwnedReadHalf,
    msg_buffer: &mut BytesMut,
    cb_on_blocking: impl FnOnce(),
) -> Result<(), io::Error> {
    // read 8 bytes, len of message
    let mut len_buf = [0u8; 8];
    let mut len_buf_ref = &mut len_buf[..];

    // check if rx is immediately readable
    match rx.try_read(len_buf_ref) {
        Ok(amount) => {
            trace!("read {} bytes instantly", amount);
            len_buf_ref = &mut len_buf_ref[amount..];
        }
        Err(_e) => {
            // would block, we send an internal ack
            cb_on_blocking();
        }
    }

    rx.read_exact(len_buf_ref)
        .instrument(trace_span!("RemoteTaskTCPHeaderRead"))
        .await?;

    let _flags: u16 = u16::from_be_bytes([len_buf[0], len_buf[1]]);
    let _unused = u16::from_be_bytes([len_buf[2], len_buf[3]]);
    let msg_len = u32::from_be_bytes(len_buf[4..8].try_into().unwrap()) as usize;
    msg_buffer.resize(msg_len, 0);

    rx.read_exact(&mut msg_buffer[..msg_len])
        .instrument(trace_span!("RemoteTaskTCPMessageRead"))
        .await
        .inspect_err(|_| {
            warn!("failed to read the whole message");
        })?;

    Ok(())
}

async fn write_raw_message(writer: &mut OwnedWriteHalf, buf: Bytes) -> Result<(), std::io::Error> {
    let mut header = [0u8; 8];
    let len = buf.len() as u32;
    header[4..8].copy_from_slice(&len.to_be_bytes());

    let header_io = IoSlice::new(&header);
    let buf_io = IoSlice::new(&buf);

    let mut written = writer.write_vectored(&[header_io, buf_io]).await?;
    written -= 8; // sub header length to get how much of buf was written
    if written < buf.len() {
        // finish writing rest of buf if not all written
        writer.write_all(&buf[written..]).await?;
    }

    // flush
    // writer.flush().await?;
    // trace!("wire written {} bytes", len);
    Ok(())
}

impl RemoteNode {
    #[instrument(skip(nucleus, stream), fields(node_id = nucleus.node_id()))]
    pub async fn new_received(
        nucleus: NucleusNode,
        stream: TcpStream,
    ) -> Result<Self, RemoteError> {
        let peer_addr = stream.peer_addr().unwrap();
        debug!("received connection from remote node at {}", peer_addr);
        RemoteNode::do_handshake(nucleus, stream, true)
            .await
            .inspect_err(|e| {
                warn!("failed to handshake with remote node (received) - {:?}", e);
            })
    }

    #[instrument(skip(nucleus, addr), fields(node_id = nucleus.node_id()))]
    pub async fn new_connect(nucleus: NucleusNode, addr: SocketAddr) -> Result<Self, RemoteError> {
        debug!("connecting to remote node at {}", addr);
        let stream = timeout(Duration::from_secs(5), TcpStream::connect(addr))
            .await
            .map_err(|_| RemoteError::TimedOut)??;

        RemoteNode::do_handshake(nucleus, stream, false)
            .await
            .inspect_err(|e| {
                warn!("failed to handshake with remote node (outgoing) - {:?}", e);
            })
    }

    #[instrument(skip(nucleus, stream), fields(node_id = nucleus.node_id()))]
    async fn do_handshake(
        nucleus: NucleusNode,
        stream: TcpStream,
        received: bool,
    ) -> Result<Self, RemoteError> {
        if nucleus.is_closing() {
            return Err(RemoteError::ClusterShuttingDown);
        }

        // connect
        let seq = rand::thread_rng().r#gen::<u64>() & 0x000FFFFFFFFFFFFF;

        let (mut rx, mut tx) = stream.into_split();

        let handshake: proto::core::RemoteHandshake = if !received {
            let addresses = nucleus.registry().get_remote_addresses();

            // we send a handshake
            let handshake = proto::core::RemoteHandshake {
                node_id: nucleus.node_id() as u32,
                instance_id: nucleus.instance_id(),
                nodes: addresses,
                seq,
                listen_address: nucleus.public_addr().to_string(),
                node_flags: nucleus.node_flags(),
                cluster: nucleus.cluster().into(),
            };
            let msg = proto::core::handshake_message::Message::Handshake(handshake);
            let msg = proto::core::HandshakeMessage { message: Some(msg) };
            let vec = msg.encode_to_vec();

            raw_handshake_write(&mut tx, vec).await?;

            let buf = raw_handshake_receive(&mut rx).await?;
            let hresponse: proto::core::RemoteHandshakeResponse =
                proto::core::RemoteHandshakeResponse::decode(buf.as_slice())
                    .map_err(|e| RemoteError::ProtocolError(e.to_string()))?;

            hresponse.into()
        } else {
            let buf = raw_handshake_receive(&mut rx).await?;
            let msg = proto::core::HandshakeMessage::decode(buf.as_slice())
                .map_err(|e| RemoteError::ProtocolError(e.to_string()))?;

            if msg.message.is_none() {
                return Err(RemoteError::ProtocolError("empty message".into()));
            }

            let handshake = match msg.message.unwrap() {
                proto::core::handshake_message::Message::Handshake(h) => h,
                proto::core::handshake_message::Message::Resume(resume) => {
                    let joined = rx.reunite(tx).unwrap();
                    info!("resuming remote node connection");
                    nucleus.registry().resume_remote(resume, joined).await;
                    return Err(RemoteError::Resumed);
                }
                proto::core::handshake_message::Message::ClientHandshake(c) => {
                    let joined = rx.reunite(tx).unwrap();
                    info!("client handshake received");
                    nucleus.registry().receive_client(c, joined).await;
                    return Err(RemoteError::ClientHandshake);
                }
            };

            let response = proto::core::RemoteHandshakeResponse {
                node_id: nucleus.node_id() as u32,
                instance_id: nucleus.instance_id(),
                nodes: nucleus.registry().get_remote_addresses(),
                seq,
                listen_address: nucleus.public_addr().to_string(),
                node_flags: nucleus.node_flags(),
                cluster: nucleus.cluster().into(),
            };

            let buf = proto::core::RemoteHandshakeResponse::encode_to_vec(&response);
            raw_handshake_write(&mut tx, buf).await?;

            handshake
        };

        if handshake.cluster.as_str() != nucleus.cluster() {
            warn!(
                "<hazard> remote node cluster mismatch, expected {}, got {}",
                nucleus.cluster(),
                handshake.cluster
            );
            return Err(RemoteError::ClusterMismatch);
        }

        let remote_node = RemoteNode {
            nucleus,
            status: RemoteNodeStatus::Connected,
            received,
            writer: Some(tx),
            reader: Some(rx),
            read_task: None,
            self_ref: None,
            response_waiters: Arc::new(DashMap::with_hasher_and_shard_amount(
                Default::default(),
                256,
            )),
            send_buffer: VecDeque::new(),
            handshake,
            seq,
            seq_ack: 0,
            awaitable_arc: AwaitableArc::new(),
            system_sub: None,
            last_heartbeat: Instant::now(),
            batch_send_buffer: Vec::new(),
        };

        debug!(
            "remote node handshake successful, id={}, instance_id={}, addr={}",
            remote_node.handshake.node_id,
            remote_node.handshake.instance_id,
            remote_node.handshake.listen_address
        );

        Ok(remote_node)
    }

    fn write_wire_inner(
        &mut self,
        message: proto::core::wire_message::Message,
    ) -> Result<impl Future<Output = Result<(), std::io::Error>> + '_, std::io::Error> {
        let wire_msg = proto::core::make_wire_msg(self.seq, self.seq_ack, Some(message));

        self.seq += 1;

        let buf: Bytes = wire_msg.encode_to_vec().into();

        self.send_buffer.push_back(wire_msg);

        histogram!("remote_node_write", "source_node_id" => self.nucleus.node_id().to_string(), "dest_node_id" => self.handshake.node_id.to_string()).record((buf.len() as u64 + 8) as f64);

        if !matches!(
            self.status,
            RemoteNodeStatus::Connected | RemoteNodeStatus::GracefulShutdown
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "not connected",
            ));
        }

        let writer = self.writer.as_mut().ok_or(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "not connected",
        ))?;
        Ok(write_raw_message(writer, buf))
    }

    fn error_stop(&mut self) {
        error!("<hazard> remote node error stopping");
        self.status = RemoteNodeStatus::ErrorStopping;
        let r = self.self_ref.as_ref();
        if let Some(r) = r {
            let _ = r.notify(ActorStopMessage {});
        }
    }

    #[instrument(skip(self))]
    async fn reconnect(&mut self) {
        #[cfg(test)]
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        if matches!(
            self.status,
            RemoteNodeStatus::ErrorStopping
                | RemoteNodeStatus::GracefulShutdown
                | RemoteNodeStatus::Reconnecting
        ) || self.nucleus.is_closing()
        {
            debug!("ignoring reconnect attempt, status={:?}", self.status);
            return;
        }

        self.writer.as_mut().map(|w| w.shutdown());

        self.status = RemoteNodeStatus::Reconnecting;
        if !self.received {
            let selfref = self.self_ref.as_ref().unwrap().clone();
            selfref.notify_delayed(
                ShutdownIfNotConnected {},
                Duration::from_secs(15),
                Some(|_| {}),
            );

            let addr = self.handshake.listen_address.clone();
            let last_acked_seq = self.seq_ack;
            let nucleus = self.nucleus.clone();
            let handshake = self.handshake.clone();

            tokio::spawn(async move {
                let res = async move {
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: u32 = 3;
                    let tcp = loop {
                        debug!("recreating TCP conn to remote node at {}", &addr);
                        let tcp = TcpStream::connect(addr.clone()).await;
                        if let Err(e) = tcp {
                            warn!("failed to reconnect to remote node - {}", e);
                            // exponential backoff
                            attempts += 1;
                            if attempts == MAX_ATTEMPTS {
                                warn!("failed to reconnect to remote node, giving up");
                                break None;
                            }
                            tokio::time::sleep(Duration::from_secs(2u64.pow(attempts))).await;
                            continue;
                        } else {
                            break Some(tcp.unwrap());
                        }
                    };
                    if tcp.is_none() {
                        warn!("failed to reconnect to remote node");
                        return Err(());
                    }

                    debug!("stream reconnecting to remote node");
                    let tcp = tcp.unwrap();
                    let (mut rx, mut tx) = tcp.into_split();

                    let resume_msg = proto::core::RemoteResume {
                        node_id: nucleus.node_id() as u32,
                        last_acked_seq,
                        instance_id: nucleus.instance_id(),
                    };
                    let resume_msg = proto::core::HandshakeMessage {
                        message: Some(proto::core::handshake_message::Message::Resume(resume_msg)),
                    };

                    let buf = resume_msg.encode_to_vec();
                    if raw_handshake_write(&mut tx, buf).await.is_err() {
                        warn!("failed to send resume message");
                        return Err(());
                    }

                    let read = raw_handshake_receive(&mut rx).await;
                    if let Err(read) = read {
                        warn!("failed to read resume response - {:?}", read);
                        return Err(());
                    }

                    let resume_response = {
                        let s = proto::core::HandshakeMessage::decode(read.unwrap().as_slice())
                            .map_err(|e| RemoteError::ProtocolError(e.to_string()));
                        if let Err(e) = s {
                            warn!("<hazard> failed to decode resume response - {:?}", e);
                            return Err(());
                        }

                        let s = s.unwrap();
                        match s.message {
                            Some(proto::core::handshake_message::Message::Resume(resume)) => resume,
                            _ => {
                                warn!("<hazard> expected resume response, got something else");
                                return Err(());
                            }
                        }
                    };

                    if resume_response.instance_id != handshake.instance_id {
                        warn!(
                            "<hazard> remote node instance id mismatch on resume, expected {}, got {} (sender)",
                            handshake.instance_id, resume_response.instance_id
                        );
                        return Err(());
                    }

                    let tcp = rx.reunite(tx).unwrap();

                    Ok(ResumeStream {
                        stream: tcp,
                        resume: resume_response,
                    })
                }
                .await;

                match res {
                    Ok(r) => {
                        let _ = selfref.notify(r);
                    }
                    Err(_) => {
                        let _ = selfref.notify(ShutdownIfNotConnected {});
                    }
                }
            }.in_current_span());
        } else {
            debug!("waiting for remote node to reconnect");

            let selfref = self.self_ref.as_ref().unwrap().clone();
            tokio::spawn(
                async move {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    let _ = selfref.notify(ShutdownIfNotConnected {});
                }
                .in_current_span(),
            );
        }
    }

    async fn successful_resume(
        &mut self,
        resume: proto::core::RemoteResume,
        rx: OwnedReadHalf,
    ) -> Result<(), RemoteError> {
        info!("remote node resumed {}", resume.node_id);
        self.spawn_read_task(
            rx,
            self.self_ref.as_ref().unwrap().clone(),
            resume.node_id as NodeId,
        );
        self.status = RemoteNodeStatus::Connected;

        // replay messages that were not acked
        let mut replay = Vec::new();
        for msg in self.send_buffer.iter() {
            if msg.seq > resume.last_acked_seq {
                replay.push(msg.clone());
            }
        }

        let writer = self.writer.as_mut().unwrap();
        for msg in replay.iter() {
            if msg.seq > resume.last_acked_seq {
                let buf = msg.encode_to_vec().into();
                if let Err(e) = write_raw_message(writer, buf).await {
                    error!("failed to replay message on resume");
                    self.error_stop();
                    return Err(e.into());
                }
            }
        }

        self.last_heartbeat = Instant::now();

        info!(
            "remote node resumed {}, replayed {} messages",
            resume.node_id,
            replay.len()
        );

        Ok(())
    }

    // #[instrument(skip(self, message), fields(node_id = self.nucleus.inner.id))]
    async fn write_wire(&mut self, message: proto::core::wire_message::Message) {
        let err = {
            let res = self.write_wire_inner(message);
            match res {
                Ok(f) => (f.await).err(),
                Err(e) => Some(e),
            }
        };

        if err.is_none() {
            return;
        }
        let err = err.unwrap();

        match &self.status {
            RemoteNodeStatus::Connected => {
                warn!("failed to write message to remote node, reconnecting");
                self.reconnect().await;
            }
            RemoteNodeStatus::Reconnecting => {
                // still in process of reconnecting, ignore
            }
            RemoteNodeStatus::GracefulShutdown => {
                // we are shutting down, ignore
                warn!("failed write attempt while shutting down  - {:?}", err);
                // self.error_stop();
            }
            RemoteNodeStatus::ErrorStopping => {}
        }
    }

    #[instrument(parent = None, "RemoteReadTask", skip(rx, nucleus, self_ref, cancel_rx, response_waiters), fields(node_id = nucleus.node_id()))]
    async fn read_task_inner(
        mut rx: OwnedReadHalf,
        nucleus: NucleusNode,
        self_ref: LocalActorRef<Self>,
        mut cancel_rx: oneshot::Receiver<()>,
        remote_node_id: NodeId,
        response_waiters: Arc<
            DashMap<
                Snowflake<RequestMarker>,
                Sender<proto::core::remote_response::Response>,
                FxBuildHasher,
            >,
        >,
    ) -> OwnedReadHalf {
        let mut shutting_down = false;
        let mut last_seqs = None;

        let mut response_senders: TaskHolder<()> = TaskHolder::new();

        // let span = info_span!("RemoteReadTask", node_id = nucleus.inner.id);

        loop {
            let mut msg_buffer: BytesMut = BytesMut::new();

            match cancel_rx.try_recv() {
                Ok(_) => {
                    break;
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    break;
                }
                _ => {}
            };

            if read_raw_message(&mut rx, &mut msg_buffer, || {
                if let Some((seq_ack, seq)) = last_seqs {
                    let _ = self_ref.notify(AckMsg {
                        remote_acked: seq_ack,
                        own_acked: seq,
                    });
                }
            })
            .await
            .is_err()
            {
                break;
            }

            // decode message
            match proto::core::WireMessage::decode(msg_buffer.freeze()) {
                #[allow(unused_mut)]
                Ok(mut msg) => {
                    if msg.seq_ack != 0 {
                        last_seqs = Some((msg.seq_ack, msg.seq));
                    }

                    let span = info_span!(parent: None, "WireMessageRecv", node_id = nucleus.node_id(), "otel.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty );
                    span.follows_from(Span::current());

                    #[cfg(feature = "metrics")]
                    {
                        use opentelemetry::global;

                        use crate::metrics::TraceContextInjector;
                        let parent_context = global::get_text_map_propagator(|propagator| {
                            propagator.extract(&TraceContextInjector::new(&mut msg.trace_parent))
                        });
                        span.set_parent(parent_context);
                    }

                    let mut handle_remote_message = |remote_msg: proto::core::RemoteMessage| {
                        fn send_response(
                            response: proto::core::remote_response::Response,
                            request_id: Snowflake<RequestMarker>,
                            local_self_ref: &LocalActorRef<RemoteNode>,
                        ) {
                            if matches!(&response, proto::core::remote_response::Response::Error(_))
                            {
                                Span::current().record("otel.status_code", "Error");
                            } else {
                                Span::current().record("otel.status_code", "Ok");
                            }

                            let resp = proto::core::RemoteResponse {
                                request_id,
                                response: Some(response),
                            };

                            debug_span!("RemoteResponseSend").in_scope(|| {
                                if local_self_ref.notify(resp).is_err() {
                                    warn!("failed to send response to remote");
                                }
                            });
                        }

                        let request_id: Snowflake<RequestMarker> = remote_msg.request_id;

                        fn bytes_as_str(bytes: &Bytes) -> &str {
                            match std::str::from_utf8(bytes) {
                                Ok(s) => s,
                                Err(_) => {
                                    panic!("failed to parse bytes as str");
                                }
                            }
                        }

                        let actor_type_str = bytes_as_str(&remote_msg.actor_type);
                        let message_type_str = bytes_as_str(&remote_msg.message_type);

                        let handler = nucleus
                            .registry()
                            .get_handler(&(actor_type_str, message_type_str));

                        if handler.is_none() {
                            error!(
                                "<hazard> handler not found for Actor {} Message {}",
                                actor_type_str, message_type_str
                            );

                            send_response(
                                proto::core::remote_response::Response::Error(
                                    ActorRefErr::HandlerNotRegistered.serialize(),
                                ),
                                request_id,
                                &self_ref,
                            );

                            return;
                        }
                        let handler = handler.unwrap();

                        trace!(
                            "remote node received message for actor {} message {}",
                            actor_type_str, message_type_str
                        );

                        let channels = if ResponseMode::from(remote_msg.response_mode).is_payload()
                        {
                            Some(oneshot::channel())
                        } else {
                            None
                        };

                        let (tx, rx) = if let Some((tx, rx)) = channels {
                            (Some(tx), Some(rx))
                        } else {
                            (None, None)
                        };

                        let actor_id = if remote_msg.actor_id.is_none() {
                            if message_type_str == "System.PubsubMessage" {
                                span.record("otel.kind", "consumer");
                            } else {
                                span.record("otel.kind", "server");
                            }
                            nucleus.registry().aref().actor_id()
                        } else {
                            // if response mode is Nothing, set span otel.kind to consumer, otherwise server
                            // we also only set this for non-registry messages, since those can be special
                            if ResponseMode::from(remote_msg.response_mode).is_nothing() {
                                span.record("otel.kind", "consumer");
                            } else {
                                span.record("otel.kind", "server");
                            }

                            remote_msg.actor_id.unwrap()
                        };

                        let response = if let Err(e) = handler.handle(
                            &nucleus,
                            remote_msg.payload,
                            actor_id,
                            tx.map(|tx| tx.into()),
                            ResponseMode::from(remote_msg.response_mode).is_movein(),
                        ) {
                            Some(proto::core::remote_response::Response::Error(e.serialize()))
                        } else if rx.is_none()
                            && ResponseMode::from(remote_msg.response_mode).is_ack_only()
                        {
                            // rx.is_none() is same as is_notify
                            Some(proto::core::remote_response::Response::NotifyAck(true))
                        } else {
                            None
                        };

                        if let Some(response) = response {
                            send_response(response, request_id, &self_ref);
                        } else if ResponseMode::from(remote_msg.response_mode).is_payload() {
                            let self_ref = self_ref.clone();

                            let response_task = tokio::spawn(
                                async move {
                                    let response = match rx.unwrap().await {
                                        Ok(res) => {
                                            proto::core::remote_response::Response::Payload(res)
                                        }
                                        Err(_) => {
                                            error!("failed to receive response, remote responding with panicked error");
                                            proto::core::remote_response::Response::Error(
                                                ActorRefErr::ActorPanicked.serialize(),
                                            )
                                        }
                                    };

                                    send_response(response, request_id, &self_ref);
                                }
                                .in_current_span(),
                            );

                            response_senders.add_task(response_task);
                        }
                    };

                    let handle_remote_response = |remote_resp: proto::core::RemoteResponse| {
                        let request_id = remote_resp.request_id;
                        trace!("received response for request {}", request_id.id());

                        if let Some((_id, sender)) = response_waiters.remove(&request_id) {
                            if sender.send(remote_resp.response.unwrap()).is_err() {
                                warn!(
                                    "waiter for request {} was dropped, use notify_forget instead?",
                                    request_id.id()
                                );
                            }
                        } else {
                            warn!("response waiter for request {} not found", request_id.id());
                        }
                    };

                    match msg.message {
                        Some(proto::core::wire_message::Message::RemoteMessage(remote_msg)) => {
                            span.in_scope(|| {
                                handle_remote_message(remote_msg);
                            });
                        }
                        Some(proto::core::wire_message::Message::RemoteResponse(remote_resp)) => {
                            span.in_scope(|| handle_remote_response(remote_resp));
                        }
                        Some(proto::core::wire_message::Message::RemoteShutdown(s)) => {
                            debug!("remote node requested shutdown");
                            shutting_down = true;
                            let is_ready_to_close = s.ready_to_close;
                            let _ = self_ref.notify(s);

                            if is_ready_to_close {
                                break;
                            }
                        }
                        Some(proto::core::wire_message::Message::BatchMessage(b)) => {
                            for mut msg in b.messages.into_iter() {
                                #[cfg(feature = "metrics")]
                                {
                                    use opentelemetry::global;

                                    use crate::metrics::TraceContextInjector;
                                    let parent_context =
                                        global::get_text_map_propagator(|propagator| {
                                            propagator.extract(&TraceContextInjector::new(
                                                &mut msg.trace_parent,
                                            ))
                                        });
                                    span.set_parent(parent_context);
                                }

                                span.in_scope(|| match msg.message {
                                    Some(
                                        proto::core::batch_message_inner::Message::RemoteMessage(
                                            remote_msg,
                                        ),
                                    ) => {
                                        handle_remote_message(remote_msg);
                                    }
                                    Some(
                                        proto::core::batch_message_inner::Message::RemoteResponse(
                                            remote_resp,
                                        ),
                                    ) => {
                                        handle_remote_response(remote_resp);
                                    }
                                    None => {
                                        warn!("<hazard> received empty batch message??");
                                    }
                                })
                            }
                        }
                        None => {
                            warn!("received empty message");
                        }
                    };
                }
                Err(e) => {
                    warn!("<hazard> failed to decode message: {:?}", e);
                }
            }

            if shutting_down {
                let all_done = !response_senders.await_all().await;

                if all_done {
                    // send shutdown message
                    let shutdown = proto::core::RemoteShutdown {
                        ready_to_close: true,
                    };
                    let _ = self_ref.notify(WireMsgSend {
                        message: proto::core::wire_message::Message::RemoteShutdown(shutdown),
                    });
                }
            }
        }

        if let Some((seq_ack, seq)) = last_seqs {
            let _ = self_ref.notify(AckMsg {
                remote_acked: seq_ack,
                own_acked: seq,
            });
        }

        info!("remote node {} disconnected", remote_node_id);

        if shutting_down {
            response_senders
                .await_all()
                .instrument(trace_span!("RemoteResponseTaskAwait"))
                .await;
        }

        let notif = ReadTaskStopped {};
        let _ = self_ref.notify(notif);

        rx
    }

    fn spawn_read_task(
        &mut self,
        rx: OwnedReadHalf,
        self_ref: LocalActorRef<Self>,
        remote_node_id: NodeId,
    ) {
        let nucleus = self.nucleus.clone();

        let (cancel_tx, cancel_rx) = oneshot::channel();

        trace!("starting read task, status={:?}", self.status);
        // spawn reader
        let task = tokio::spawn(
            Self::read_task_inner(
                rx,
                nucleus,
                self_ref,
                cancel_rx,
                remote_node_id,
                self.response_waiters.clone(),
            )
            .in_current_span(),
        );

        self.read_task = Some((task, cancel_tx));
    }

    pub(super) fn node_id(&self) -> NodeId {
        self.handshake.node_id as NodeId
    }

    pub(crate) fn handshake(&self) -> &proto::core::RemoteHandshake {
        &self.handshake
    }

    async fn shutdown(&mut self) {
        if self.status.is_stopping() {
            return;
        }

        debug!(
            "shutting down remote node for node {}",
            self.handshake.node_id
        );
        self.status = RemoteNodeStatus::GracefulShutdown;

        self.nucleus
            .pubsub()
            .remote_node_shutdown(self.handshake.node_id as u16);

        if let Some((task, cancel_tx)) = self.read_task.take() {
            let _ = cancel_tx.send(());

            let self_ref = self.self_ref.as_ref().unwrap().clone();
            let _ = self_ref.notify(WireMsgSend {
                message: proto::core::wire_message::Message::RemoteShutdown(
                    proto::core::RemoteShutdown {
                        ready_to_close: false,
                    },
                ),
            });

            tokio::spawn(
                async move {
                    let r = timeout(Duration::from_secs(5), task).await;
                    if let Err(e) = &r {
                        warn!("waiting for read task timed out on shutdown: {:?}", e);
                    }
                    if let Ok(Err(e)) = &r {
                        warn!("read task errored on shutdown: {:?}", e);
                    }

                    // reading is done, send shutdown message
                    let _ = self_ref.notify(WireMsgSend {
                        message: proto::core::wire_message::Message::RemoteShutdown(
                            proto::core::RemoteShutdown {
                                ready_to_close: true,
                            },
                        ),
                    });

                    debug!("remote node shutdown process complete, stopping actor");
                    let _ = self_ref.notify(ActorStopMessage {});
                }
                .in_current_span(),
            );
        } else {
            let self_ref = self.self_ref.as_ref().unwrap().clone();
            let _ = self_ref.notify(WireMsgSend {
                message: proto::core::wire_message::Message::RemoteShutdown(
                    proto::core::RemoteShutdown {
                        ready_to_close: false,
                    },
                ),
            });
            let _ = self_ref.notify(WireMsgSend {
                message: proto::core::wire_message::Message::RemoteShutdown(
                    proto::core::RemoteShutdown {
                        ready_to_close: true,
                    },
                ),
            });
            let _ = self_ref.notify(ActorStopMessage {});
        }
    }
}

#[async_trait_nobox]
impl Actor for RemoteNode {
    #[cfg(not(feature = "unbounded_remote_node"))]
    const MAX_MAILBOX_SIZE: usize = 1 << 20; // 1M messages 
    #[cfg(feature = "unbounded_remote_node")]
    const MAX_MAILBOX_SIZE: usize = usize::MAX;

    async fn init(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), crate::errors::ActorProcessingErr> {
        self.system_sub = Some(self.nucleus.registry().subscribe_system_events(ctx).await);
        self.self_ref = Some(ctx.get_ref().clone());

        let rx = self.reader.take().unwrap();
        self.spawn_read_task(rx, ctx.get_ref(), self.handshake().node_id as NodeId);

        self.nucleus.registry().add_remote_node(
            self.handshake.node_id as u16,
            ctx.get_ref(),
            self.handshake.clone(),
            self.awaitable_arc.clone().wait_zero(),
        );
        info!("remote node connected: {}", self.handshake.node_id);
        self.seq_ack = self.handshake.seq;

        for to_discover in self.handshake.nodes.drain(0..) {
            let _ = ctx.nucleus().registry().aref().notify(to_discover);
        }

        ctx.get_ref()
            .send_periodic(SendHeartbeat, HEARTBEAT_INTERVAL);

        ctx.nucleus()
            .registry()
            .publish_system_event(SystemMessage::NodeRegister(
                proto::remote::RemoteNodeRegister {
                    node_id: self.handshake.node_id,
                    address: self.handshake.listen_address.clone(),
                    flags: self.handshake.node_flags,
                },
            ));

        self.last_heartbeat = Instant::now();

        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext<Self>) {
        self.nucleus
            .pubsub()
            .remote_node_shutdown(self.handshake.node_id as u16);

        self.response_waiters.retain(|_id, sender| {
            let mut temp = Sender::new_empty();
            mem::swap(sender, &mut temp);

            let _ = temp.send(proto::core::remote_response::Response::Error(
                ActorRefErr::ShuttingDown.serialize(),
            ));

            false
        });

        let mut death_messages = Vec::new();

        for pubsub_handler in self.nucleus.pubsub().handlers.iter().filter(|r| {
            r.value()
                .publisher_id()
                .map(|v| v.worker_id() == self.handshake.node_id as NodeId)
                .unwrap_or(false)
        }) {
            let subbed_actor_name = pubsub_handler.key();
            let topics = &pubsub_handler.value().topics;
            for topic in topics {
                let t = *topic.key();
                let msg = proto::pubsub::PubsubMessage {
                    topic: t.into(),
                    actor_name: subbed_actor_name.into(),
                    payload: Some(proto::pubsub::pubsub_message::Payload::RemoteNodeDied(
                        self.handshake.node_id,
                    )),
                    message_id: u64::MAX,
                };

                death_messages.push(msg);
            }
        }

        for death_msg in death_messages {
            self.nucleus.pubsub().publish_incoming(death_msg);
        }

        self.nucleus
            .registry()
            .publish_system_event(SystemMessage::NodeUnregister(
                proto::remote::RemoteNodeUnregister {
                    node_id: self.handshake.node_id,
                },
            ));

        if !matches!(self.status, RemoteNodeStatus::GracefulShutdown) {
            let nucleus = self.nucleus.clone();
            let addr = self.handshake.listen_address.clone();
            tokio::spawn(
                async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    if let Err(e) = nucleus.registry().connect_remote(addr).await {
                        warn!(
                            "failed to reconnect remote node after error shutdown: {:?}",
                            e
                        );
                    }
                }
                .in_current_span(),
            );
        }

        self.nucleus
            .registry()
            .remove_remote_node(self.handshake.node_id as u16);
    }
}

impl RemoteActor for RemoteNode {
    fn info() -> &'static RemoteActorInfo {
        &RemoteActorInfo {
            actor_type: "System.RemoteNode",
        }
    }
}

const BATCH_SIZE: usize = 1000;

#[handler(manual)]
impl RemoteNode {
    #[handler(delayed)]
    async fn handle_proto_core_remote_message(
        &mut self,
        message: proto::core::RemoteMessage,
        ctx: &mut ActorContext<Self>,
        sender: Sender<proto::core::remote_response::Response>,
    ) {
        // if !matches!(self.status, RemoteNodeStatus::Connected) {
        //     warn!("remote node not connected");
        //     return;
        // }

        if self.status.is_stopping() {
            warn!("remote node is stopping");
            let _ = sender.send(proto::core::remote_response::Response::Error(
                ActorRefErr::ShuttingDown.serialize(),
            ));
            return;
        }

        let expect_response = !(ResponseMode::from(message.response_mode).is_nothing());

        let request_id = message.request_id;

        if expect_response {
            self.response_waiters.insert(request_id, sender);
        }

        if self.batch_send_buffer.is_empty() {
            debug_span!(parent: None, "FlushBatchMessages").in_scope(|| {
                let _ = ctx.get_ref().notify(FlushBatchMessages {});
            });
        }

        let mut inner_msg = proto::core::BatchMessageInner {
            message: Some(proto::core::batch_message_inner::Message::RemoteMessage(
                message,
            )),
            trace_parent: None,
        };

        #[cfg(feature = "metrics")]
        {
            let mut injector =
                crate::metrics::TraceContextInjector::new(&mut inner_msg.trace_parent);

            opentelemetry::global::get_text_map_propagator(|propagator| {
                let context = Span::current().context();
                propagator.inject_context(&context, &mut injector)
            });
        }

        self.batch_send_buffer.push(inner_msg);

        if self.batch_send_buffer.len() >= BATCH_SIZE {
            handle_self!(self, ctx, FlushBatchMessages {});
        }
    }

    #[handler(delayed)]
    async fn handle_proto_core_remote_response(
        &mut self,
        message: proto::core::RemoteResponse,
        ctx: &mut ActorContext<Self>,
        sender: Sender<()>,
    ) {
        let _ = sender.send(());

        if self.batch_send_buffer.is_empty() {
            let _ = ctx.get_ref().notify(FlushBatchMessages {});
        }

        let mut inner_msg = proto::core::BatchMessageInner {
            message: Some(proto::core::batch_message_inner::Message::RemoteResponse(
                message,
            )),
            trace_parent: None,
        };

        #[cfg(feature = "metrics")]
        {
            use tracing::Span;
            use tracing_opentelemetry::OpenTelemetrySpanExt;

            use crate::metrics::TraceContextInjector;

            let mut injector = TraceContextInjector::new(&mut inner_msg.trace_parent);
            opentelemetry::global::get_text_map_propagator(|propagator| {
                let context = Span::current().context();
                propagator.inject_context(&context, &mut injector)
            });
        }

        self.batch_send_buffer.push(inner_msg);

        if self.batch_send_buffer.len() >= BATCH_SIZE {
            handle_self!(self, ctx, FlushBatchMessages {});
        }
    }

    async fn handle_flush_batch_messages(
        &mut self,
        _message: FlushBatchMessages,
        _ctx: &mut ActorContext<Self>,
    ) {
        if self.batch_send_buffer.is_empty() {
            return;
        }

        let mut new_buf = Vec::with_capacity(BATCH_SIZE);
        mem::swap(&mut self.batch_send_buffer, &mut new_buf);

        let msg = proto::core::wire_message::Message::BatchMessage(proto::core::BatchMessage {
            messages: new_buf,
        });

        self.write_wire(msg).await;
    }

    async fn handle_wire_msg_send(&mut self, message: WireMsgSend, _ctx: &mut ActorContext<Self>) {
        if !self.batch_send_buffer.is_empty() {
            // flush first
            handle_self!(self, _ctx, FlushBatchMessages {});
        }

        self.write_wire(message.message).await;
    }

    #[handler(delayed)]
    async fn handle_proto_pubsub_pubsub_subscribe(
        &mut self,
        message: proto::pubsub::PubsubSubscribe,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<bool>,
    ) {
        let nucleus = self.nucleus.clone();
        let dest_node_id = self.handshake.node_id as u16;

        delayed(sender, async move {
            let r = nucleus
                .send_remote_node(dest_node_id, message)
                .squash()
                .await;
            r.unwrap_or(false)
        });
    }

    #[handler(delayed)]
    async fn handle_proto_pubsub_pubsub_unsubscribe(
        &mut self,
        message: proto::pubsub::PubsubUnsubscribe,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<bool>,
    ) {
        let nucleus = self.nucleus.clone();
        let dest_node_id = self.handshake.node_id as u16;

        delayed(sender, async move {
            let r = nucleus
                .send_remote_node(dest_node_id, message)
                .squash()
                .await;
            r.unwrap_or(false)
        });
    }

    async fn handle_ack_msg(&mut self, message: AckMsg, _ctx: &mut ActorContext<Self>) {
        self.seq_ack = message.own_acked;
        // pop all messages that are acked
        let mut popped = 0;
        while !self.send_buffer.is_empty() {
            let msg = self.send_buffer.pop_front().unwrap();

            if msg.seq > message.remote_acked {
                self.send_buffer.push_front(msg);
                break;
            }
            popped += 1;
        }

        trace!(
            "remote acked {} own acked {}, send buffer size {}, just popped {}",
            message.remote_acked,
            message.own_acked,
            self.send_buffer.len(),
            popped
        );
    }

    async fn handle_resume_stream(
        &mut self,
        message: ResumeStream,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), RemoteError> {
        match &self.status {
            RemoteNodeStatus::Reconnecting => {
                debug!("resuming remote node connection");
                // let addr = message.stream.peer_addr().unwrap();
                // self.addr = addr;
                let stream = message.stream;
                let (rx, tx) = stream.into_split();
                self.writer = Some(tx);

                // send a resume message back
                debug!(
                    "remote node resume attempt received for {}",
                    message.resume.node_id
                );

                if message.resume.instance_id != self.handshake.instance_id {
                    error!(
                        "<hazard> remote node instance id mismatch, expected {}, got {} (receiving)",
                        self.handshake.instance_id, message.resume.instance_id
                    );
                    self.error_stop();
                    return Err(RemoteError::ProtocolError("instance id mismatch".into()));
                }

                if self.received {
                    let resume_msg = proto::core::RemoteResume {
                        last_acked_seq: self.seq_ack,
                        node_id: self.nucleus.inner.id as u32,
                        instance_id: self.nucleus.instance_id(),
                    };
                    let resume_msg = proto::core::HandshakeMessage {
                        message: Some(proto::core::handshake_message::Message::Resume(resume_msg)),
                    };
                    let buf = resume_msg.encode_to_vec();
                    if raw_handshake_write(self.writer.as_mut().unwrap(), buf)
                        .await
                        .is_err()
                    {
                        error!("failed to send response resume message");
                        self.error_stop();
                        return Err(RemoteError::ProtocolError(
                            "failed to send resume message".into(),
                        ));
                    }
                }

                self.successful_resume(message.resume, rx).await?;
                Ok(())
            }
            s => {
                warn!("ignoring unexpected resume message (status={:?})", s);
                Err(RemoteError::ProtocolError("unexpected resume".into()))
            }
        }
    }

    async fn handle_read_task_stopped(
        &mut self,
        _message: ReadTaskStopped,
        _ctx: &mut ActorContext<Self>,
    ) {
        if matches!(self.status, RemoteNodeStatus::Connected) {
            if let Some((read_task, _cancel)) = self.read_task.as_ref() {
                if !read_task.is_finished() {
                    // means this was a resume and this message came from the old read task, do not try to reconnect since theres a new valid read task
                    return;
                }
            }
            warn!(
                "remote for {} disconnected unexpectedly, reconnecting",
                self.handshake.node_id
            );
            self.reconnect().await;
        }
    }

    async fn handle_proto_core_remote_shutdown(
        &mut self,
        _message: proto::core::RemoteShutdown,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.shutdown().await;
    }

    async fn handle_proto_remote_remote_node_unregister(
        &mut self,
        _message: proto::remote::RemoteNodeUnregister,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.shutdown().await;
    }

    async fn handle_proto_remote_remote_node_heartbeat(
        &mut self,
        message: proto::remote::RemoteNodeHeartbeat,
        ctx: &mut ActorContext<Self>,
    ) {
        let elapsed = self.last_heartbeat.elapsed();
        if elapsed > HEARTBEAT_INTERVAL_GRACE {
            warn!(
                heartbeat_delay = ?elapsed.as_millis(),
                "remote node heartbeat took more than {}ms",
                HEARTBEAT_INTERVAL_GRACE.as_millis(),
            );
        }
        self.last_heartbeat = Instant::now();

        let missing_nodes: Vec<(NodeId, RemoteNodeRecord)> = ctx
            .nucleus()
            .registry()
            .get_remote_nodes()
            .iter()
            .filter_map(|v| {
                // check not in message.connected_nodes
                if message.connected_nodes.contains(&(*v.key() as u32)) {
                    None
                } else {
                    Some((*v.key(), v.value().clone()))
                }
            })
            .collect();

        for missing in missing_nodes {
            let (node_id, record) = missing;
            let msg = proto::remote::RemoteNodeRegister {
                node_id: node_id as u32,
                flags: record.node_flags(),
                address: record.addr().to_owned(),
            };
            let remote_msg = proto::core::RemoteMessage {
                actor_id: None,
                request_id: ctx.nucleus().snowflake().generate(),
                actor_type: "RemoteRegistry".into(),
                message_type: proto::remote::RemoteNodeRegister::name().into(),
                response_mode: ResponseMode::Nothing as u32,
                payload: msg.serialize(),
            };

            let _ = ctx.get_ref().notify(remote_msg);
        }
    }

    async fn handle_pubsub_message_system_topic(
        &mut self,
        message: PubsubMessage<SystemTopic>,
        _ctx: &mut ActorContext<Self>,
    ) {
        #[allow(clippy::single_match)]
        match message.payload {
            PubsubPayload::Payload(p) => match p.as_ref() {
                SystemMessage::SystemShutdown(_) => {
                    // self.shutdown().await;
                }
                _ => {}
            },
            _ => {}
        }
    }

    #[handler(delayed)]
    async fn handle_shutdown_if_not_connected(
        &mut self,
        _message: ShutdownIfNotConnected,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<<ShutdownIfNotConnected as Message>::Result>,
    ) {
        let _ = sender.send((self.status, self.awaitable_arc.clone().wait_zero()));
        if !matches!(self.status, RemoteNodeStatus::Connected) {
            warn!("remote node did not initiate resume, shutting down");
            self.shutdown().await;
        }
    }

    #[handler(delayed)]
    async fn handle_internal_system_shutdown(
        &mut self,
        _message: InternalSystemShutdown,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<ArcAwaiter>,
    ) {
        let _ = sender.send(self.awaitable_arc.clone().wait_zero());

        if !self.status.is_stopping() {
            warn!("remote node system shutdown requested, shutting down");
            self.shutdown().await;
        }
    }

    async fn handle_get_status(
        &mut self,
        _message: GetStatus,
        _ctx: &mut ActorContext<Self>,
    ) -> RemoteNodeStatus {
        self.status
    }

    async fn handle_send_heartbeat(
        &mut self,
        _message: SendHeartbeat,
        ctx: &mut ActorContext<Self>,
    ) {
        debug!(
            "sending heartbeat from node {} to node {}",
            ctx.nucleus().node_id(),
            self.node_id()
        );

        if self.last_heartbeat.elapsed() > HEARTBEAT_INTERVAL_GRACE {
            warn!(
                "<hazard> remote node heartbeat took more than {}ms, reconnecting!",
                HEARTBEAT_INTERVAL_GRACE.as_millis()
            );
            self.reconnect().await;
        }
        let msg = proto::remote::RemoteNodeHeartbeat {
            node_id: ctx.nucleus().node_id() as u32,
            connected_nodes: ctx
                .nucleus()
                .registry()
                .get_remote_nodes()
                .iter()
                .map(|v| *v.key() as u32)
                .collect(),
        };

        let remote_msg = proto::core::RemoteMessage {
            actor_id: None,
            request_id: ctx.nucleus().snowflake().generate(),
            actor_type: "RemoteRegistry".into(),
            message_type: proto::remote::RemoteNodeHeartbeat::name().into(),
            response_mode: ResponseMode::Nothing as u32,
            payload: msg.serialize(),
        };

        let _ = ctx.get_ref().notify(remote_msg);
    }

    #[cfg(feature = "debug")]
    async fn handle_proto_debug_get_serialized(
        &mut self,
        _message: proto::debug::GetSerialized,
        _ctx: &mut ActorContext<Self>,
    ) -> Vec<u8> {
        proto::debug::SerializedRemoteNode {
            handshake: Some(self.handshake.clone()),
            status: format!("{:?}", self.status),
            send_buffer_length: self.send_buffer.len() as u32,
            response_waiter_count: self.response_waiters.len() as u32,
        }
        .encode_to_vec()
    }

    #[cfg(test)]
    async fn handle_debug_interrupt_stream(
        &mut self,
        _message: DebugInterruptStream,
        _ctx: &mut ActorContext<Self>,
    ) {
        if let Some(writer) = &mut self.writer {
            let _ = writer.shutdown().await;
            // sleep for 2s
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

impl Message for proto::core::RemoteResponse {
    fn name() -> &'static str {
        "System.RemoteResponse"
    }
    const RESPECT_BACKPRESSURE: bool = false;

    type Result = ();
}

struct FlushBatchMessages {}
impl Message for FlushBatchMessages {
    fn name() -> &'static str {
        "System.FlushBatchMessages"
    }
    const RESPECT_BACKPRESSURE: bool = false;

    type Result = ();
}

struct WireMsgSend {
    message: proto::core::wire_message::Message,
}
impl Message for WireMsgSend {
    fn name() -> &'static str {
        "System.WireMsgSend"
    }
    const RESPECT_BACKPRESSURE: bool = false;

    type Result = ();
}

struct AckMsg {
    remote_acked: u64,
    own_acked: u64,
}
impl Message for AckMsg {
    fn name() -> &'static str {
        "System.AckMsg"
    }
    type Result = ();
}

pub(crate) struct ResumeStream {
    pub(crate) stream: TcpStream,
    pub(crate) resume: proto::core::RemoteResume,
}

impl Message for ResumeStream {
    fn name() -> &'static str {
        "System.ResumeStream"
    }
    type Result = Result<(), RemoteError>;
}

struct ReadTaskStopped {}
impl Message for ReadTaskStopped {
    fn name() -> &'static str {
        "System.ReadTaskStopped"
    }
    const RESPECT_BACKPRESSURE: bool = false;

    type Result = ();
}

pub(super) struct ShutdownIfNotConnected {}
impl Message for ShutdownIfNotConnected {
    fn name() -> &'static str {
        "System.ShutdownIfNotConnected"
    }
    type Result = (RemoteNodeStatus, ArcAwaiter);
}

pub(super) struct GetStatus {}
impl Message for GetStatus {
    fn name() -> &'static str {
        "System.GetRemoteNodeStatus"
    }
    type Result = RemoteNodeStatus;
}

#[derive(Clone)]
struct SendHeartbeat;
impl Message for SendHeartbeat {
    fn name() -> &'static str {
        "System.SendHeartbeat"
    }
    type Result = ();
}

#[repr(u32)]
pub enum ResponseMode {
    Payload = 0,
    AckOnly = 1,
    Nothing = 2,
    PayloadMovedin = 3,
}
impl ResponseMode {
    pub fn is_payload(&self) -> bool {
        matches!(self, ResponseMode::Payload | ResponseMode::PayloadMovedin)
    }
    pub fn is_ack_only(&self) -> bool {
        matches!(self, ResponseMode::AckOnly)
    }
    pub fn is_nothing(&self) -> bool {
        matches!(self, ResponseMode::Nothing)
    }
    pub fn is_movein(&self) -> bool {
        matches!(self, ResponseMode::PayloadMovedin)
    }
}
impl From<u32> for ResponseMode {
    fn from(v: u32) -> Self {
        match v {
            0 => ResponseMode::Payload,
            1 => ResponseMode::AckOnly,
            2 => ResponseMode::Nothing,
            3 => ResponseMode::PayloadMovedin,
            _ => panic!("invalid response mode"),
        }
    }
}

#[cfg(test)]
pub(crate) struct DebugInterruptStream;

#[cfg(test)]
impl Message for DebugInterruptStream {
    fn name() -> &'static str {
        "System.DebugInterruptStream"
    }
    type Result = ();
}
