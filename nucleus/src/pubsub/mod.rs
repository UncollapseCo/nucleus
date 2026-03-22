use std::{any::Any, marker::PhantomData, ops::Deref, sync::Arc};

use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use fxhash::{FxBuildHasher, FxHashSet};
use handler::PubsubMsgWrapper;
use tokio::sync::OnceCell;
use tracing::{Instrument, Span};

use crate::{
    ActorMarker, NodeId, NucleusNode,
    actor::{
        Actor, RemoteActor,
        message::{DeserializationError, Handler, IntoAnyBoxed, Message, Serializable},
        refs::{ActorRefErr, LocalActorRef, SendError},
    },
    net::{
        proto::{self, pubsub::PubsubSubscribe},
        registry::actor::RemoteRegistryActor,
    },
    snowflake::Snowflake,
    utils::AsyncErrorSquash,
};

mod handler;

pub trait Topic: Send + Sync + 'static {
    type M: Serializable;
    type Publisher: RemoteActor;

    const RESPECT_BACKPRESSURE: bool = true;

    fn name() -> &'static str;
}

#[async_trait_nobox]
pub(crate) trait AnyTopic: Send + Sync {
    fn deserialize_message(
        &self,
        data: &proto::pubsub::pubsub_message::Payload,
        actor_name: Arc<str>,
        message_id: u64,
        nucleus: &NucleusNode,
    ) -> Result<(Box<dyn Any>, bool), DeserializationError>;

    fn name(&self) -> &'static str;
}

#[async_trait_nobox]
impl<T: Topic> AnyTopic for T {
    fn deserialize_message(
        &self,
        data: &proto::pubsub::pubsub_message::Payload,
        actor_name: Arc<str>,
        seq_id: u64,
        nucleus: &NucleusNode,
    ) -> Result<(Box<dyn Any>, bool), DeserializationError> {
        let mut is_final = false;
        let payload = match data {
            proto::pubsub::pubsub_message::Payload::PubsubPayload(p) => PubsubPayload::<T>::Payload(
                Arc::new(T::M::deserialize(p.clone(), Some(nucleus)).inspect_err(|e| {
                    error!(
                        "<hazard> Failed to deserialize pubsub message payload (topic={}, actor={}) - {:?}",
                        T::name(),
                        actor_name,
                        e
                    );
                })?),
            ),
            proto::pubsub::pubsub_message::Payload::PublisherStopped(b) => {
                is_final = true;
                PubsubPayload::<T>::PublisherStopped(*b)
            }
            proto::pubsub::pubsub_message::Payload::RemoteNodeDied(node_id) => {
                is_final = true;
                PubsubPayload::<T>::RemoteNodeDied(*node_id as NodeId)
            }
        };

        Ok((
            Box::new(PubsubMessage::<T> {
                actor_name,
                payload,
                seq_id,
            }),
            is_final,
        ))
    }

    fn name(&self) -> &'static str {
        T::name()
    }
}

pub struct PubsubMessage<T: Topic> {
    pub actor_name: Arc<str>,
    pub seq_id: u64,
    pub payload: PubsubPayload<T>,
}

pub enum PubsubPayload<T: Topic> {
    Payload(Arc<T::M>),
    PublisherStopped(bool),
    RemoteNodeDied(NodeId),
}

impl<T: Topic> PubsubPayload<T> {
    pub fn new_payload(payload: T::M) -> Self {
        PubsubPayload::Payload(Arc::new(payload))
    }

    pub fn unwrap_payload(&self) -> Arc<T::M> {
        match self {
            PubsubPayload::Payload(p) => p.clone(),
            _ => panic!("PubsubPayload is not a Payload variant"),
        }
    }

    pub fn is_stopped(&self) -> bool {
        matches!(self, PubsubPayload::PublisherStopped(_b))
    }
}

impl<T: Topic> From<PubsubPayload<T>> for Option<proto::pubsub::pubsub_message::Payload> {
    fn from(payload: PubsubPayload<T>) -> Self {
        Some(match payload {
            PubsubPayload::Payload(p) => {
                proto::pubsub::pubsub_message::Payload::PubsubPayload(p.serialize())
            }
            PubsubPayload::PublisherStopped(b) => {
                proto::pubsub::pubsub_message::Payload::PublisherStopped(b)
            }
            PubsubPayload::RemoteNodeDied(node_id) => {
                proto::pubsub::pubsub_message::Payload::RemoteNodeDied(node_id as u32)
            }
        })
    }
}

impl<T: Topic> Message for PubsubMessage<T> {
    type Result = ();

    const RESPECT_BACKPRESSURE: bool = T::RESPECT_BACKPRESSURE;

    fn name() -> &'static str {
        T::name()
    }
}

impl<T: Topic> Clone for PubsubMessage<T> {
    fn clone(&self) -> Self {
        Self {
            actor_name: self.actor_name.clone(),
            payload: self.payload.clone(),
            seq_id: self.seq_id,
        }
    }
}

impl<T: Topic> Clone for PubsubPayload<T> {
    fn clone(&self) -> Self {
        match self {
            PubsubPayload::Payload(p) => PubsubPayload::Payload(p.clone()),
            PubsubPayload::PublisherStopped(b) => PubsubPayload::PublisherStopped(*b),
            PubsubPayload::RemoteNodeDied(node_id) => PubsubPayload::RemoteNodeDied(*node_id),
        }
    }
}

impl<T> IntoAnyBoxed for PubsubMessage<T>
where
    T: Topic,
{
    fn into_any(self) -> Box<dyn Any> {
        Box::new(self)
    }
}

struct ActorPubsubHandler<A: Actor, T: Topic> {
    aref: LocalActorRef<A>,
    _t: PhantomData<T>,
}
impl<A: Actor, T: Topic> ActorPubsubHandler<A, T> {
    pub fn new(aref: LocalActorRef<A>) -> Self {
        Self {
            aref,
            _t: PhantomData,
        }
    }
}

pub(crate) trait ActorPubsubHandlerTrait: Send + Sync {
    fn handle(&self, data: &Box<dyn Any>);
}

impl<A: Actor + Handler<PubsubMsgWrapper<T>>, T: Topic> ActorPubsubHandlerTrait
    for ActorPubsubHandler<A, T>
{
    fn handle(&self, data: &Box<dyn Any>) {
        let message = data.downcast_ref::<PubsubMessage<T>>().unwrap();

        let wrapped: PubsubMsgWrapper<T> = PubsubMsgWrapper {
            msg: message.clone(),
        };

        if let Err(e) = self.aref.notify(wrapped) {
            warn!(
                "Failed to notify actor about pubsub message (topic={}) - {:?}",
                T::name(),
                e.err
            );
        }
    }
}

// impl ActorPubsubHandlerTrait for RemoteNodePubsubHandler {
//     fn handle(&self, data: &Box<dyn Any>) {
//         let topic = self.nucleus.registry().get_topic_handler(&self.topic);
//         if topic.is_none() {
//             error!("Received pubsub message for unknown topic: {}", self.topic);
//             return;
//         }
//         let topic = topic.unwrap();

//         let (serialized, actor_name) = topic.serialize_message(data);
//         let nucleus = self.nucleus.clone();
//         let node_id = self.node_id;
//         let topic_name = self.topic.clone();

//         tokio::spawn(async move {
//             nucleus
//                 .send_remote_node(
//                     node_id,
//                     proto::pubsub::PubsubMessage {
//                         topic: topic_name,
//                         actor_name,
//                         payload: serialized,
//                     },
//                 )
//                 .await;
//         });
//     }
// }

pub(crate) struct PubsubHandler {
    nucleus: Option<NucleusNode>,

    pub(crate) handlers: DashMap<
        // publisher actor name
        Arc<str>,
        PubHandler,
        FxBuildHasher,
    >,

    remote_node_handlers: DashMap<
        // actor name
        Arc<str>,
        DashMap<
            //topic
            &'static str,
            // set of remote nodes
            FxHashSet<NodeId>,
            FxBuildHasher,
        >,
        FxBuildHasher,
    >,
}

#[derive(Default)]
pub(crate) struct PubHandler {
    pub(crate) publisher_id: Arc<OnceCell<Snowflake<ActorMarker>>>,
    #[expect(clippy::type_complexity)] // 🤓
    pub(crate) topics: DashMap<
        //topic
        &'static str,
        DashMap<
            // subscriber actor id
            Snowflake<ActorMarker>,
            // subscriber recv channel
            Box<dyn ActorPubsubHandlerTrait>,
            FxBuildHasher,
        >,
        FxBuildHasher,
    >,
}
impl PubHandler {
    pub fn publisher_id(&self) -> Option<Snowflake<ActorMarker>> {
        self.publisher_id.get().copied()
    }
}

impl PubsubHandler {
    pub(crate) fn new() -> Self {
        Self {
            nucleus: None,
            handlers: DashMap::default(),
            remote_node_handlers: DashMap::default(),
        }
    }

    pub(crate) fn setup(&mut self, nucleus: NucleusNode) {
        self.nucleus = Some(nucleus);
    }

    fn nucleus(&self) -> &NucleusNode {
        self.nucleus
            .as_ref()
            .expect("NucleusNode not set, setup() not called")
    }

    pub(crate) async fn add_subscriber<A: Actor + Handler<PubsubMessage<T>>, T: Topic>(
        &self,
        actor_name: Arc<str>,
        aref: LocalActorRef<A>,
        publisher_actor_id: Option<Snowflake<ActorMarker>>,
    ) -> Result<PubsubRef<T>, ActorRefErr> {
        let actor_id = aref.actor_id();
        let handler = ActorPubsubHandler::new(aref.clone());
        let handler = Box::new(handler) as Box<dyn ActorPubsubHandlerTrait>;

        self.add_subscriber_raw::<T>(actor_name.clone(), handler, actor_id, publisher_actor_id)
            .await?;

        Ok(PubsubRef::new(
            self.nucleus().clone(),
            actor_name,
            actor_id,
            aref,
        ))
    }

    pub(crate) async fn add_subscriber_raw<T: Topic>(
        &self,
        actor_name: Arc<str>,
        handler: Box<dyn ActorPubsubHandlerTrait>,
        susbcriber_actor_id: Snowflake<ActorMarker>,
        publisher_actor_id: Option<Snowflake<ActorMarker>>,
    ) -> Result<(), ActorRefErr> {
        let pubhandler = self.handlers.entry(actor_name.clone()).or_insert_with(|| {
            debug!(
                "Node {} creating new pubsub handler for actor {} and topic {}",
                self.nucleus().node_id(),
                actor_name,
                T::name()
            );
            Default::default()
        });

        let cell = pubhandler.publisher_id.clone();

        let publisher_actor_id =
            publisher_actor_id.or_else(|| pubhandler.publisher_id.get().cloned());

        let sub_future = match pubhandler.topics.entry(T::name()) {
            dashmap::mapref::entry::Entry::Occupied(mut o) => {
                o.get_mut().insert(susbcriber_actor_id, handler);
                None
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                debug!(
                    "Node {} inserting new topic {} for {}",
                    self.nucleus().node_id(),
                    T::name(),
                    actor_name
                );

                let topics_map = DashMap::default();
                topics_map.insert(susbcriber_actor_id, handler);
                v.insert(topics_map);

                // if we have a publisher actor id, we return a sub future
                // if we do not yet (this is a first sub), `sub_remote_node` will be called by the OnceCell below, which does discovery first
                if let Some(publisher_actor_id) = publisher_actor_id {
                    if publisher_actor_id.worker_id() == self.nucleus().node_id() {
                        // local node, no need to do remote subs
                        None
                    } else {
                        let actor_name_2 = actor_name.clone();

                        Some(self.nucleus().sub_remote_node(
                            T::name(),
                            actor_name_2,
                            publisher_actor_id.worker_id(),
                        ))
                    }
                } else {
                    None
                }
            }
        };

        drop(pubhandler);

        if let Some(sub_future) = sub_future {
            if let Err(e) = sub_future.await {
                warn!(
                    "Failed to subscribe to remote node for topic {} on actor {}: {:?}",
                    T::name(),
                    actor_name,
                    e.err
                );
                self.remove_subscriber::<T>(actor_name.clone(), susbcriber_actor_id);
                return Err(e.err);
            }
        }

        cell.get_or_try_init(|| async move {
            let named_ref = if let Some(publisher_actor_id) = publisher_actor_id {
                let r = self.nucleus().get_raw_named_ref(&*actor_name);

                let inner_ref = self
                    .nucleus()
                    .get_ref_by_id(publisher_actor_id)
                    .ok_or(ActorRefErr::UnknownNode)?;

                r.set_inner_ref(inner_ref.try_into().unwrap()).await;

                Ok(r)
            } else {
                self.nucleus()
                    .get_named_ref::<T::Publisher>(&*actor_name)
                    .await
            };

            match named_ref {
                Ok(r) => {
                    let do_sub = async || {
                        let actor_id = r.actor_id().await.unwrap();

                        if actor_id.worker_id() == self.nucleus().node_id() {
                            // publisher is local, no need to subscribe to remote node
                            // trace!(
                            //     "Publisher for topic {} is local, no need to subscribe to remote node",
                            //     T::name(),
                            // );

                            Some(actor_id)
                        } else if (self
                            .nucleus()
                            .sub_remote_node(T::name(), actor_name.clone(), actor_id.worker_id())
                            .await)
                            .unwrap_or_default()
                        {
                            Some(actor_id)
                        } else {
                            None
                        }
                    };

                    let subbed = do_sub().await;

                    if let Some(subbed) = subbed {
                        Ok(subbed)
                    } else {
                        // rediscover and try again once
                        if r.rediscover().await.is_err() {
                            warn!(
                                "Failed to rediscover remote node for topic {} on actor {}",
                                T::name(),
                                actor_name
                            );
                            self.remove_subscriber::<T>(actor_name.clone(), susbcriber_actor_id);
                            return Err(ActorRefErr::UnknownNode);
                        }

                        // try again
                        let subbed = do_sub().await;
                        if let Some(subbed) = subbed {
                            Ok(subbed)
                        } else {
                            warn!(
                                "Failed to subscribe to remote node for topic {} on actor {}",
                                T::name(),
                                actor_name
                            );
                            self.remove_subscriber::<T>(actor_name.clone(), susbcriber_actor_id);
                            Err(ActorRefErr::UnknownNode)
                        }
                    }
                }

                Err(e) => {
                    // remove own handler, and possibly remove if empty
                    warn!(
                        "Failed to get actor id for remote node subscription: {:?}",
                        e
                    );
                    self.remove_subscriber::<T>(actor_name, susbcriber_actor_id);
                    Err(e)
                }
            }
        })
        .await?;

        Ok(())
    }

    pub(crate) fn remove_subscriber<T: Topic>(
        &self,
        actor_name: Arc<str>,
        actor_id: Snowflake<ActorMarker>,
    ) {
        trace!(
            "Node {} removing subscriber for topic {} on actor {} and actor id {}",
            self.nucleus().node_id(),
            T::name(),
            actor_name,
            actor_id
        );

        if let Some(mut topics_ref) = self.handlers.get_mut(&actor_name) {
            let pub_handler = topics_ref.value_mut();

            if let Some(handlers) = pub_handler.topics.get_mut(T::name()) {
                handlers.remove(&actor_id);
                if handlers.is_empty() {
                    drop(handlers); // so we dont deadlock
                    pub_handler.topics.remove(T::name());
                    let nucleus = self.nucleus().clone();

                    if let Some(publisher_id) = pub_handler.publisher_id() {
                        nucleus.unsub_remote_node(T::name(), actor_name.clone(), publisher_id);
                    }
                }
            }

            if pub_handler.topics.is_empty() {
                drop(topics_ref); // so we dont deadlock
                debug!(
                    "Node {} removing pubsub handler for actor {} since there are no more subscribers",
                    self.nucleus().node_id(),
                    actor_name,
                );
                self.handlers.remove(&actor_name);
            }
        }
    }

    pub(crate) fn get_publisher_actor_id(
        &self,
        actor_name: &str,
    ) -> Option<Snowflake<ActorMarker>> {
        if let Some(pub_handler_ref) = self.handlers.get(actor_name) {
            return pub_handler_ref.value().publisher_id();
        }
        None
    }

    pub(crate) fn publish_incoming(&self, message: proto::pubsub::PubsubMessage) -> u64 {
        if message.payload.is_none() {
            error!("Received pubsub message with no payload");
            return 0;
        }

        let handler = self
            .nucleus()
            .registry()
            .get_topic_handler(message.topic.as_str());

        debug_span!(
            "pubsub_publish_incoming",
            topic = message.topic.as_str(),
            actor = message.actor_name.as_str(),
            "otel.kind" = "consumer"
        )
        .in_scope(|| {
            let mut pub_count = 0;
            if let Some(topic) = handler {
                if let Some(pub_handler_ref) = self.handlers.get(message.actor_name.as_str()) {
                    let mut final_msg = false;
                    let pub_handler = pub_handler_ref.value();

                    if let Some(handlers) = pub_handler.topics.get(message.topic.as_str()) {
                        let deser_res = topic.deserialize_message(
                            &message.payload.unwrap(),
                            message.actor_name.get_counted(),
                            message.message_id,
                            self.nucleus(),
                        );

                        if let Err(e) = deser_res {
                            error!(
                                "<hazard> Failed to deserialize pubsub message (topic={}, actor={}): {:?}",
                                message.topic, message.actor_name, e
                            );
                            return 0;
                        }

                        let (msg, is_final) = deser_res.unwrap();
                        final_msg = is_final;

                        pub_count += handlers
                            .value()
                            .iter()
                            .map(|handler| {
                                let handler = handler.value();
                                handler.handle(&msg);
                                1
                            })
                            .sum::<u64>();
                    }

                    if final_msg {
                        pub_handler.topics.remove(message.topic.as_str());
                        if pub_handler.topics.is_empty() {
                            drop(pub_handler_ref); // so we dont deadlock
                            self.handlers.remove(message.actor_name.as_str());
                        }
                    }
                }
            } else {
                error!(
                    "<hazard> Received pubsub message for unregistered topic: {}",
                    message.topic
                );
            }

            trace!(
                "Published incoming message on topic {} to {} local recipients",
                message.topic, pub_count
            );

            pub_count
        })
    }

    #[instrument(skip_all, fields(topic = T::name(), published_count = tracing::field::Empty))]
    pub(crate) fn publish_local<T: Topic>(
        &self,
        actor_name: &Arc<str>,
        message: PubsubPayload<T>,
        msg_id: u64,
    ) {
        let msg: Box<dyn Any> = Box::new(PubsubMessage::<T> {
            actor_name: actor_name.clone(),
            payload: message,
            seq_id: msg_id,
        });
        let mut pub_count = 0u64;
        if let Some(pub_handler_ref) = self.handlers.get(actor_name) {
            if let Some(handlers) = pub_handler_ref.value().topics.get(T::name()) {
                pub_count += handlers
                    .value()
                    .iter()
                    .map(|handler| {
                        let handler = handler.value();
                        handler.handle(&msg);
                        1
                    })
                    .sum::<u64>();
            }
        }

        Span::current().record("published_count", pub_count);

        debug!(
            "Published local message from {} on topic {} to {} local recipients",
            actor_name,
            T::name(),
            pub_count
        );
    }

    #[instrument(skip_all, fields(topic = T::name()))]
    pub(crate) fn publish_global<T: Topic>(
        &self,
        actor_name: &Arc<str>,
        message: PubsubPayload<T>,
        message_id: u64,
    ) {
        self.publish_local::<T>(actor_name, message.clone(), message_id);
        let serialized_msg: Option<proto::pubsub::pubsub_message::Payload> = message.into();

        let pubsub_msg = proto::pubsub::PubsubMessage {
            topic: T::name().into(),
            actor_name: actor_name.into(),
            payload: serialized_msg.clone(),
            message_id,
        };

        self.publish_global_raw(actor_name, pubsub_msg, T::name());
    }

    #[instrument(skip_all, fields(topic = topic, published_node_count = tracing::field::Empty))]
    pub(crate) fn publish_global_raw<M>(
        &self,
        actor_name: &Arc<str>,
        message: M,
        topic: &'static str,
    ) where
        RemoteRegistryActor: Handler<M>,
        M: Message + Clone + Serializable,
        M::Result: Serializable + Into<u64>,
    {
        // publish to all subscribed remote nodes
        let mut pub_node_count = 0;
        if let Some(topics) = self.remote_node_handlers.get(actor_name) {
            if let Some(subscribers) = topics.get(topic) {
                let mut futures = FuturesUnordered::new();
                for node_id in subscribers.iter() {
                    let node_id = *node_id;
                    let nucleus = self.nucleus().clone();
                    let msg = message.clone();

                    match nucleus.send_remote_node(node_id, msg) {
                        Ok(f) => {
                            futures.push(
                                async move {
                                    let res = f.await;
                                    if let Err(e) = res {
                                        error!(
                                            "<hazard> Failed to send pubsub message to remote node: {:?} (topic {})",
                                            e, topic
                                        );
                                        0
                                    } else {
                                        let res: u64 = res.unwrap().into();
                                        res
                                    }
                                }
                                .instrument(debug_span!(
                                    "pubsub_publish_remote",
                                    dest_node_id = node_id,
                                    "otel.kind" = "producer",
                                    topic = topic,
                                    actor = actor_name.deref()
                                )),
                            );
                        }
                        Err(e) => {
                            error!(
                                "<hazard> Failed to send global pubsub message to remote node {}: {:?}",
                                node_id, e
                            );
                        }
                    };
                }

                pub_node_count += subscribers.len();
                let actor_name_2 = actor_name.to_owned();
                tokio::spawn(async move {
                    let mut pub_actor_count = 0;
                    while let Some(i) = futures.next().await {
                        pub_actor_count += i;
                    }

                    debug!(
                        "published global message from {} on topic {} to {} actors across {} remote nodes",
                        actor_name_2, topic, pub_actor_count, pub_node_count
                    );
                }.in_current_span());
            }
        }

        Span::current().record("published_node_count", pub_node_count);

        if pub_node_count == 0 {
            debug!(
                "Published global message from {} on topic {} to 0 actors across 0 remote nodes",
                actor_name, topic
            );
        }
    }

    pub(crate) fn subscriber_count<T: Topic>(&self, actor_name: &str) -> (u64, u64) {
        // returns (local_subcount, remote_subcount)
        let mut local_sub_count = 0;
        if let Some(pub_handler_ref) = self.handlers.get(actor_name) {
            if let Some(handlers) = pub_handler_ref.value().topics.get(T::name()) {
                local_sub_count = handlers.len() as u64;
            }
        }

        let mut remote_sub_count = 0;
        if let Some(topics) = self.remote_node_handlers.get(actor_name) {
            if let Some(subscribers) = topics.get(T::name()) {
                remote_sub_count = subscribers.len() as u64;
            }
        }

        (local_sub_count, remote_sub_count)
    }

    pub(crate) fn get_subscribed_nodes(&self, actor_name: &str, topic: &str) -> Vec<NodeId> {
        let mut nodes = Vec::new();

        //check if there are local subscribers, if yes add self node id
        if let Some(pub_handler) = self.handlers.get(actor_name) {
            if let Some(handlers) = pub_handler.value().topics.get(topic) {
                if !handlers.value().is_empty() {
                    nodes.push(self.nucleus().node_id());
                }
            }
        }

        // add remote subscribers
        if let Some(topics) = self.remote_node_handlers.get(actor_name) {
            if let Some(subscribers) = topics.get(topic) {
                for node_id in subscribers.iter() {
                    nodes.push(*node_id);
                }
            }
        }

        nodes
    }

    pub(crate) fn upsert_subscribed_nodes(
        &self,
        topic: &'static str,
        actor_name: Arc<str>,
        nodes: Vec<NodeId>,
    ) {
        let self_node_id = self.nucleus().node_id();
        // remove self node id from nodes
        let nodes: FxHashSet<NodeId> = nodes.into_iter().filter(|n| *n != self_node_id).collect();

        // when an actor moves, who is also a publisher, we register the subscribed nodes here
        self.remote_node_handlers
            .entry(actor_name)
            .or_default()
            .entry(topic)
            .or_default()
            .extend(nodes);
    }

    pub(crate) async fn remote_node_subscribe(
        &self,
        request: proto::pubsub::PubsubSubscribe,
    ) -> bool {
        let actor_name: Arc<str> = request.actor_name.get_counted();
        if self
            .nucleus()
            .registry()
            .get_named_actor_local(actor_name.clone())
            .await
            .is_none()
        {
            // return false if named actor is not located here
            return false;
        }

        let topic = self
            .nucleus()
            .registry()
            .get_topic_handler(request.topic.as_str());
        if topic.is_none() {
            error!(
                "Received pubsub subscribe for unregistered topic: {}",
                request.topic
            );
            return false;
        }
        let topic = topic.unwrap();

        self.remote_node_handlers
            .entry(actor_name)
            .or_default()
            .entry(topic.name())
            .or_default()
            .value_mut()
            .insert(request.subscriber_node_id as u16);

        true
    }
    pub(crate) fn remote_node_unsubscribe(
        &self,
        request: proto::pubsub::PubsubUnsubscribe,
    ) -> bool {
        let topic: Option<&dyn AnyTopic> = self
            .nucleus()
            .registry()
            .get_topic_handler(request.topic.as_str());
        if topic.is_none() {
            error!(
                "Received pubsub unsubscribe for unregistered topic: {}",
                request.topic
            );
            return false;
        }
        let topic = topic.unwrap();

        let actor_name: Arc<str> = request.actor_name.get_counted();
        if let Some(topics) = self.remote_node_handlers.get_mut(&*actor_name) {
            if let Some(mut subscribers) = topics.get_mut(topic.name()) {
                subscribers.remove(&(request.subscriber_node_id as u16));
                if subscribers.is_empty() {
                    drop(subscribers); // so we dont deadlock
                    topics.remove(topic.name());
                }
            }

            if topics.is_empty() {
                drop(topics); // so we dont deadlock
                self.remote_node_handlers.remove(&*actor_name);
            }
        }

        true
    }

    pub(crate) fn remote_publisher_moved(
        &self,
        actor_name: &str,
        new_publisher_id: Snowflake<ActorMarker>,
    ) -> bool {
        if let Some(mut pub_handler_ref) = self.handlers.get_mut(actor_name) {
            let cell = OnceCell::new_with(Some(new_publisher_id));
            pub_handler_ref.publisher_id = Arc::new(cell);

            true
        } else {
            false
        }
    }

    fn publisher_stopped_local(&self, actor_name: &Arc<str>) {
        if let Some((_, pub_handler)) = self.handlers.remove(actor_name) {
            for topic in pub_handler.topics.into_iter() {
                let topic_handler = self.nucleus().registry().get_topic_handler(topic.0);

                if topic_handler.is_none() {
                    error!("<hazard> Topic not registered: {}", topic.0);
                    continue;
                }

                let topic_handler = topic_handler.unwrap();

                let (local_msg, _) = topic_handler
                    .deserialize_message(
                        &proto::pubsub::pubsub_message::Payload::PublisherStopped(true),
                        actor_name.clone(),
                        u64::MAX,
                        self.nucleus(),
                    )
                    .unwrap();

                for handler in topic.1.into_iter() {
                    let handler = handler.1;
                    handler.handle(&local_msg);
                }
            }
        }
    }

    fn publisher_stopped_remote(&self, actor_name: Arc<str>) {
        if let Some(topics) = self.remote_node_handlers.remove(&actor_name) {
            for topic in topics.1.into_iter() {
                for node_id in topic.1.into_iter() {
                    let msg = proto::pubsub::PubsubMessage {
                        topic: topic.0.into(),
                        actor_name: actor_name.clone().into(),
                        payload: Some(proto::pubsub::pubsub_message::Payload::PublisherStopped(
                            true,
                        )),
                        message_id: u64::MAX,
                    };

                    let nucleus = self.nucleus().clone();
                    let topic_name = topic.0.to_owned();

                    tokio::spawn(async move {
                        let node_id = node_id;
                        let res = nucleus
                            .send_remote_node(node_id, msg.clone())
                            .squash()
                            .await;

                        if let Err(e) = res {
                            error!(
                                "<hazard> Failed to send closing pubsub message to remote node: {:?} (topic={})",
                                e, topic_name
                            );
                        }
                    }.in_current_span());
                }
            }
        }
    }

    pub(crate) fn publisher_stopped(&self, actor_name: &Arc<str>) {
        // publishing stop msg to local subscribers
        self.publisher_stopped_local(actor_name);

        // publish stop msg to remote subscribers
        self.publisher_stopped_remote(actor_name.clone());
    }

    pub(crate) fn publisher_moved(&self, actor_name: &Arc<str>) {
        self.remote_node_handlers.remove(actor_name);
    }

    pub(crate) fn remote_node_shutdown(&self, node_id: NodeId) {
        let mut removed = 0;
        for mut actor_names in self.remote_node_handlers.iter_mut() {
            for mut subscribers in actor_names.value_mut().iter_mut() {
                if subscribers.value_mut().remove(&node_id) {
                    removed += 1;
                }
            }
        }

        debug!(
            "Removed {} remote node subscriptions for node {}",
            removed, node_id
        );
    }

    pub(crate) async fn shutdown(&self) {
        let handler_names: Vec<Arc<str>> = self.handlers.iter().map(|h| h.key().clone()).collect();

        for handler_name in handler_names {
            self.publisher_stopped_local(&handler_name);
        }

        let handler_names_remote: Vec<Arc<str>> = self
            .remote_node_handlers
            .iter()
            .map(|h| h.key().clone())
            .collect();

        for handler_name in handler_names_remote {
            self.publisher_stopped_remote(handler_name);
        }

        info!("Pubsub handler shutdown complete");
    }
}

#[must_use]
pub struct PubsubRef<T: Topic> {
    nucleus: NucleusNode,
    pub(crate) actor_name: Arc<str>,
    sub_actor_id: Snowflake<ActorMarker>,
    drop_callback: Option<Box<dyn FnOnce() + Send + Sync>>,
    _t: PhantomData<T>,
}

impl<T: Topic> PubsubRef<T> {
    pub fn actor_name(&self) -> &Arc<str> {
        &self.actor_name
    }

    pub(crate) fn new(
        nucleus: NucleusNode,
        actor_name: Arc<str>,
        sub_actor_id: Snowflake<ActorMarker>,
        subscriber_ref: LocalActorRef<impl Actor>,
    ) -> Self {
        let actor_name_clone = actor_name.clone();

        Self {
            nucleus,
            actor_name,
            sub_actor_id,

            // remove seq id tracking for this actor+topic, so that if this actor resubscribes
            // and remote actor restarts, this actor doesnt ignore messages incorrectly
            drop_callback: Some(Box::new(move || {
                // ignore error - if this fails actor is stopping anyway and we dont care
                let _ = subscriber_ref.send_callback(move |_, ctx| {
                    ctx.sub_seq_ids.remove(&(actor_name_clone, T::name()));
                });
            })),
            _t: PhantomData,
        }
    }
}

impl<T: Topic> Drop for PubsubRef<T> {
    fn drop(&mut self) {
        debug!(
            "Removing subscriber for topic {} on actor {} - pubsub ref was dropped",
            T::name(),
            self.actor_name
        );

        if let Some(callback) = self.drop_callback.take() {
            callback();
        }

        self.nucleus
            .pubsub()
            .remove_subscriber::<T>(self.actor_name.clone(), self.sub_actor_id);
    }
}

impl NucleusNode {
    fn sub_remote_node(
        &self,
        topic: &'static str,
        actor_name: Arc<str>,
        dest_node_id: NodeId,
    ) -> impl Future<Output = Result<bool, SendError<PubsubSubscribe>>> {
        debug!(
            "Node {} subscribing to remote node {} for topic {} on actor {}",
            self.node_id(),
            dest_node_id,
            topic,
            actor_name
        );

        let msg = proto::pubsub::PubsubSubscribe {
            topic: topic.into(),
            actor_name: actor_name.clone().into(),
            subscriber_node_id: self.node_id() as u32,
        };

        let remote_node = self.registry().get_remote_node(dest_node_id);

        async move {
            if remote_node.is_none() {
                return Err(SendError::new(ActorRefErr::UnknownNode, msg));
            }

            let res = remote_node.unwrap().actor_ref().send(msg).await;

            match &res {
                Ok(true) => {
                    debug!(
                        "Node {} subscribed to remote node {} for topic {} on actor {} successfully",
                        self.node_id(),
                        dest_node_id,
                        topic,
                        actor_name
                    );
                }
                Ok(false) => {
                    warn!(
                        "Node {} failed to subscribe to remote node {} for topic {} on actor {}: remote actor not found",
                        self.node_id(),
                        dest_node_id,
                        topic,
                        actor_name
                    );
                }
                Err(e) => {
                    error!(
                        "Node {} failed to subscribe to remote node {} for topic {} on actor {}: {:?}",
                        self.node_id(),
                        dest_node_id,
                        topic,
                        actor_name,
                        e
                    );
                }
            }

            res
        }
    }

    fn unsub_remote_node(
        &self,
        topic: &'static str,
        actor_name: Arc<str>,
        publisher_id: Snowflake<ActorMarker>,
    ) {
        // TODO: make this more efficient since this notifies all remote nodes...
        debug!(
            "Unsubscribing from remote nodes for topic {} on actor {}",
            topic, actor_name
        );
        let record = self.registry().get_remote_node(publisher_id.worker_id());

        if let Some(record) = record {
            let _ = record.actor_ref().notify(proto::pubsub::PubsubUnsubscribe {
                topic: topic.into(),
                actor_name: actor_name.into(),
                subscriber_node_id: self.node_id() as u32,
            });
        }
    }
}

// impl<T: Topic> Serializable for PubsubMessage<T> {
//     fn serialize(&self) -> Bytes {
//         proto::pubsub::PubsubMessage {
//             topic: T::name().to_string(),
//             actor_name: self.actor_name.clone(),
//             payload: self.message.serialize(),
//         }
//         .serialize()
//     }

//     fn deserialize(data: &[u8]) -> Self {
//         let message = proto::pubsub::PubsubMessage::deserialize(data);
//         let payload = T::M::deserialize(&message.payload);
//         PubsubMessage {
//             actor_name: message.actor_name,
//             message: Arc::new(payload),
//             _t: PhantomData,
//         }
//     }
// }
