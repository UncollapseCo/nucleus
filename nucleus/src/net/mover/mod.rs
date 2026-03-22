use std::{any::Any, collections::HashSet, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use fxhash::FxHashMap;
use snowflake::Snowflake;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::{
    ActorMarker, NodeId, NucleusNode,
    actor::{
        Actor, RemoteActor, Sender,
        message::Handler,
        moving::{ActorRx, MovableActor},
        refs::{ActorRefErr, LocalActorRef, NamedActorRef},
    },
    pubsub::{PubsubMessage, PubsubRef, Topic},
    utils::AwaitableArc,
};

use super::proto;

pub(crate) mod handlers;
mod utils;

pub use utils::SubRefMap;

type FwdMsgQueue = Vec<((Bytes, &'static str), Sender<Bytes>)>;
struct LeavingMoverData {
    remote_msg_queue: FwdMsgQueue,
    destination_node_id: NodeId,
    actor_type: &'static str,
    actor_name: Arc<str>,
    move_finish_waiters: Vec<Sender<()>>,
    actor_subs: HashSet<String>,
    _awaitable_arc: AwaitableArc,
}

struct ArrivingMoverData {
    // movein_complete: Sender<()>,
    resume_messages_processed_tx: Option<Sender<()>>,
    arriving_actor_id: Option<Snowflake<ActorMarker>>,
    actor_id_sender: Option<Sender<Snowflake<ActorMarker>>>,
    actor_type: String,
    actor_tx: Option<oneshot::Sender<(Bytes, SubRefMap)>>,
    sub_refs: Option<SubRefMap>,
}

pub(crate) struct Mover {
    nucleus: NucleusNode,
    actors_leaving: FxHashMap<Snowflake<ActorMarker>, LeavingMoverData>,
    actors_arriving: FxHashMap<Arc<str>, ArrivingMoverData>,
    registered_movable: FxHashMap<&'static str, Box<dyn MovableActorTrait>>,
    leaving_awaitable_arc: Option<AwaitableArc>,
}
impl Mover {
    pub(crate) fn new(nucleus: NucleusNode) -> Self {
        Self {
            nucleus,
            actors_leaving: FxHashMap::default(),
            actors_arriving: FxHashMap::default(),
            registered_movable: FxHashMap::default(),
            leaving_awaitable_arc: Some(AwaitableArc::new()),
        }
    }

    pub(crate) fn mover_ref_for_node(
        nucleus: &NucleusNode,
        node_id: NodeId,
    ) -> NamedActorRef<Self> {
        nucleus.get_raw_named_ref(format!("System.Mover-{}", node_id))
    }
}

impl Actor for Mover {}
impl RemoteActor for Mover {
    fn info() -> &'static crate::actor::RemoteActorInfo {
        &crate::actor::RemoteActorInfo {
            actor_type: "System.Mover",
        }
    }
}

#[async_trait]
trait MovableActorTrait: Send + Sync + 'static {
    fn resume_moved(
        &mut self,
        actor_rx: ActorRx,
        ref_tx: oneshot::Sender<Box<dyn Any + Send>>,
        name: Arc<str>,
    );

    async fn sub_actors(
        &self,
        actor_subs: Vec<proto::mover::PubsubRef>,
        r: Box<dyn Any + Send>,
    ) -> SubRefMap;
}
pub(crate) struct Resumer<A: MovableActor> {
    nucleus: NucleusNode,
    marker: PhantomData<A>,
}

#[async_trait]
impl<A: MovableActor> MovableActorTrait for Resumer<A> {
    fn resume_moved(
        &mut self,
        actor_rx: ActorRx,
        ref_tx: oneshot::Sender<Box<dyn Any + Send>>,
        name: Arc<str>,
    ) {
        let nucleus = self.nucleus.clone();
        tokio::spawn(
            async move {
                nucleus
                    .spawn_actor_resumed::<A>(name, actor_rx, ref_tx)
                    .await
            }
            .in_current_span(),
        );
    }

    async fn sub_actors(
        &self,
        actor_subs: Vec<proto::mover::PubsubRef>,
        r: Box<dyn Any + Send>,
    ) -> SubRefMap {
        // downcast r to local actor ref of A
        let ref_tx = r.downcast::<LocalActorRef<A>>().unwrap();
        let count = actor_subs.len();

        let mut result: SubRefMap = SubRefMap::new();

        for pubsub_ref in actor_subs.into_iter() {
            let nucleus = self.nucleus.clone();
            let lref = ref_tx.clone();
            let movable_dyn = self
                .nucleus
                .registry()
                .get_movable_sub_ref::<A>(pubsub_ref.topic.as_str());

            if movable_dyn.is_none() {
                warn!(
                    "<hazard> No movable sub ref found for topic: {}, actor {}",
                    pubsub_ref.topic,
                    A::info().actor_type
                );
                continue;
            }

            let movable = movable_dyn.unwrap();
            let actor_name: Arc<str> = pubsub_ref.actor_name.get_counted();
            let f = movable
                .add_subscriber_dyn(
                    actor_name.clone(),
                    lref,
                    nucleus,
                    pubsub_ref.publisher_actor_id,
                )
                .await;

            match f {
                Ok(r) => result.insert_ref(pubsub_ref.topic.to_string(), actor_name, r),
                Err(e) => {
                    error!("<hazard> Error moving subscriber: {:?}", e);
                }
            }
        }

        debug!("Moved {} sub refs", count);

        result
    }
}

#[async_trait]
pub(crate) trait MovableSubRef: Send + Sync {
    async fn add_subscriber_dyn(
        &self,
        actor_name: Arc<str>,
        aref: Box<dyn Any + Send>,
        nucleus: NucleusNode,
        publisher_actor_id: Option<Snowflake<ActorMarker>>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActorRefErr>;
}

pub(crate) struct MovableSubRefStruct<T: Topic, A: Actor + Handler<PubsubMessage<T>>> {
    marker: PhantomData<A>,
    marker2: PhantomData<T>,
}

impl<T: Topic, A: Actor + Handler<PubsubMessage<T>>> MovableSubRefStruct<T, A> {
    pub fn new() -> Self {
        Self {
            marker: PhantomData,
            marker2: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Topic, A: Actor + Handler<PubsubMessage<T>>> MovableSubRef for MovableSubRefStruct<T, A> {
    async fn add_subscriber_dyn(
        &self,
        actor_name: Arc<str>,
        aref: Box<dyn Any + Send>,
        nucleus: NucleusNode,
        publisher_actor_id: Option<Snowflake<ActorMarker>>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActorRefErr> {
        let lref = aref.downcast::<LocalActorRef<A>>().unwrap();
        nucleus
            .pubsub()
            .add_subscriber(actor_name, *lref, publisher_actor_id)
            .await
            .map(|r: PubsubRef<T>| Box::new(r) as Box<dyn Any + Send + Sync>)
    }
}

#[macro_export]
macro_rules! test_sleep (
    () => {
        #[cfg(test)]
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
);
