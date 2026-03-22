use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use snowflake::Snowflake;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::{
    ActorMarker, NodeId, NucleusNode,
    actor::{
        ActorContext, Sender,
        delay::{MaybeDelayed, maybe_delayed},
        message::{Handler, Message},
        moving::MovableActor,
        refs::{NamedActorRef, remote_send},
    },
    net::{proto, registry::InternalSystemShutdown},
    test_sleep,
    utils::AwaitableArc,
};

use super::{ArrivingMoverData, LeavingMoverData, MovableActorTrait, Mover, Resumer, SubRefMap};

impl Mover {
    fn forward_leaving_messages(
        &mut self,
        source_actor_id: Snowflake<ActorMarker>,
        dest_actor_id: Snowflake<ActorMarker>,
    ) -> AwaitableArc {
        let entry = self
            .actors_leaving
            .get_mut(&source_actor_id)
            .expect("Actor is not being moved!!");

        let arc = AwaitableArc::new();

        let mut temp = vec![];
        std::mem::swap(&mut entry.remote_msg_queue, &mut temp);
        let actor_type = entry.actor_type;

        let nucleus = self.nucleus.clone();

        let tarc = arc.clone();
        tokio::spawn(
            async move {
                for (msg, sender) in temp {
                    let (tx, rx) = oneshot::channel();
                    let arc = tarc.clone();
                    tokio::spawn(
                        async move {
                            let res = rx.await.unwrap();
                            match res {
                                proto::core::remote_response::Response::Payload(vec) => {
                                    let _ = sender.send(vec);
                                }
                                proto::core::remote_response::Response::Error(_vec) => {
                                    todo!("idk how to handle errors for forwarded messages")
                                }
                                proto::core::remote_response::Response::NotifyAck(_) => {
                                    unreachable!()
                                }
                            };
                            drop(arc);
                        }
                        .in_current_span(),
                    );

                    remote_send(
                        &nucleus,
                        Some(tx.into()),
                        false,
                        msg.1.into(),
                        msg.0,
                        actor_type.into(),
                        dest_actor_id,
                        true,
                    )
                    .unwrap();
                }
            }
            .in_current_span(),
        );

        arc
    }
}

pub(crate) struct RegisterMovable {
    actor_type: &'static str,
    boxed: Box<dyn MovableActorTrait>,
}
impl RegisterMovable {
    pub(crate) fn new<A: MovableActor>(nucleus: NucleusNode) -> Self {
        let r: Resumer<A> = Resumer {
            nucleus,
            marker: PhantomData,
        };

        Self {
            actor_type: A::info().actor_type,
            boxed: Box::new(r),
        }
    }
}
impl Message for RegisterMovable {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.RegisterMovable"
    }
}
#[async_trait_nobox]
impl Handler<RegisterMovable> for Mover {
    async fn handle(&mut self, msg: RegisterMovable, _ctx: &mut ActorContext<Self>) {
        self.registered_movable.insert(msg.actor_type, msg.boxed);
    }
}

pub(crate) struct ForwardRemoteMessage {
    pub(crate) actor_id: Snowflake<ActorMarker>,
    pub(crate) message_type: &'static str,
    pub(crate) payload: Bytes,
}
impl Message for ForwardRemoteMessage {
    type Result = Bytes;
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.ForwardRemoteMessage"
    }
}

#[handler(delayed, manual)]
impl Handler<ForwardRemoteMessage> for Mover {
    async fn handle_delayed(
        &mut self,
        msg: ForwardRemoteMessage,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<<ForwardRemoteMessage as Message>::Result>,
    ) {
        let entry = self
            .actors_leaving
            .get_mut(&msg.actor_id)
            .expect("Actor is not being moved!!");

        entry
            .remote_msg_queue
            .push(((msg.payload, msg.message_type), sender));
    }
}

pub(crate) struct StartMoving {
    pub(crate) actor_id: Snowflake<ActorMarker>,
    pub(crate) actor_type: &'static str,
    pub(crate) actor_name: Arc<str>,
    pub(crate) destination_node_id: NodeId,
    pub(crate) actor_subs: Vec<proto::mover::PubsubRef>,
}
impl Message for StartMoving {
    type Result = Result<(), String>;

    fn name() -> &'static str {
        "System.Mover.StartMoving"
    }
}

#[handler(manual)]
impl Handler<StartMoving> for Mover {
    async fn handle(
        &mut self,
        msg: StartMoving,
        ctx: &mut ActorContext<Self>,
    ) -> <StartMoving as Message>::Result {
        let remote_mover_ref = Mover::mover_ref_for_node(ctx.nucleus(), msg.destination_node_id);

        let response = remote_mover_ref
            .send(proto::mover::ActorMoveInIntent {
                name: msg.actor_name.clone().into(),
                actor_type: msg.actor_type.into(),
                actor_subs: msg.actor_subs.clone(),
            })
            .await
            .unwrap();

        match response.response {
            Some(proto::mover::actor_move_in_response::Response::Error(e)) => {
                return Err(e);
            }
            Some(_) => {
                warn!("<hazard> wuh? bwuh?");
                Ok(())
            }
            None => {
                self.actors_leaving.insert(
                    msg.actor_id,
                    LeavingMoverData {
                        remote_msg_queue: Vec::new(),
                        destination_node_id: msg.destination_node_id,
                        actor_type: msg.actor_type,
                        actor_name: msg.actor_name.clone(),
                        move_finish_waiters: Vec::new(),
                        actor_subs: msg
                            .actor_subs
                            .into_iter()
                            .map(|x| x.actor_name.to_string())
                            .collect(),
                        _awaitable_arc: self.leaving_awaitable_arc.clone().unwrap_or_default(),
                    },
                );

                Ok(())
            }
        }
    }
}

pub(crate) struct MoveActor {
    pub(crate) actor_id: Snowflake<ActorMarker>,
    pub(crate) actor_serialized: Bytes,
    pub(crate) name: Arc<str>,
    pub(crate) actor_type: &'static str,
    pub(crate) actor_subs: Vec<((Arc<str>, &'static str), u64)>,
    pub(crate) outgoing_pubsub_seqs: HashMap<&'static str, u64>,
}
impl Message for MoveActor {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.MoveActor"
    }
}

#[async_trait_nobox]
impl Handler<MoveActor> for Mover {
    async fn handle(
        &mut self,
        msg: MoveActor,
        ctx: &mut ActorContext<Self>,
    ) -> <MoveActor as Message>::Result {
        let entry = self
            .actors_leaving
            .get_mut(&msg.actor_id)
            .expect("Actor is not being moved!!");

        let remote_mover_ref = Mover::mover_ref_for_node(ctx.nucleus(), entry.destination_node_id);

        let outgoing_topics = msg.outgoing_pubsub_seqs.keys().cloned().collect::<Vec<_>>();

        let response = remote_mover_ref
            .send(proto::mover::ActorMoveInPayload {
                actor_serialized: msg.actor_serialized,
                name: msg.name.clone().into(),
                actor_type: msg.actor_type.into(),
                actor_subs_numbered: msg
                    .actor_subs
                    .into_iter()
                    // filter by being present in entry.actor_subs
                    .filter_map(|((name, topic), seq)| {
                        if entry.actor_subs.contains(&*name) {
                            Some(proto::mover::PubsubRef {
                                actor_name: name.clone().into(),
                                topic: topic.into(),
                                message_id: Some(seq),
                                publisher_actor_id: self
                                    .nucleus
                                    .pubsub()
                                    .get_publisher_actor_id(&name),
                            })
                        } else {
                            None
                        }
                    })
                    .collect(),
                outgoing_seqs: msg
                    .outgoing_pubsub_seqs
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            proto::mover::NodeList {
                                seq: v,
                                node_ids: ctx
                                    .nucleus()
                                    .pubsub()
                                    .get_subscribed_nodes(&entry.actor_name, k)
                                    .into_iter()
                                    .map(|v| v as u32)
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            })
            .await
            .unwrap();

        // when this is done, unregister the actor from the registry, usually done in ctx.stop()
        ctx.nucleus()
            .registry()
            .unset_remote_ref(msg.name.clone(), &msg.actor_id);

        match response.response.unwrap() {
            proto::mover::actor_move_in_response::Response::Error(_) => return,
            proto::mover::actor_move_in_response::Response::ActorId(r) => {
                let new_actor_id = Snowflake::try_from(r).unwrap();

                for topic in outgoing_topics {
                    ctx.nucleus().pubsub().publish_global_raw(
                        &entry.actor_name,
                        proto::pubsub::PublisherMoved {
                            topic: topic.into(),
                            actor_name: entry.actor_name.clone().into(),
                            new_publisher_id: new_actor_id,
                        },
                        topic,
                    );

                    // also notify own registry
                    let _ = ctx
                        .nucleus()
                        .registry()
                        .aref()
                        .notify(proto::pubsub::PublisherMoved {
                            topic: topic.into(),
                            actor_name: entry.actor_name.clone().into(),
                            new_publisher_id: new_actor_id,
                        });
                }

                // send all remote messages
                self.forward_leaving_messages(msg.actor_id, new_actor_id);
                ctx.get_ref()
                    .notify(DelayedMsgForward {
                        source_actor_id: msg.actor_id,
                        dest_actor_id: new_actor_id,
                    })
                    .unwrap();
            }
        }
    }
}

struct DelayedMsgForward {
    source_actor_id: Snowflake<ActorMarker>,
    dest_actor_id: Snowflake<ActorMarker>,
}
impl Message for DelayedMsgForward {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.DelayedMsgForward"
    }
}
#[async_trait_nobox]
impl Handler<DelayedMsgForward> for Mover {
    async fn handle(
        &mut self,
        msg: DelayedMsgForward,
        _ctx: &mut ActorContext<Self>,
    ) -> <DelayedMsgForward as Message>::Result {
        let waiter = self.forward_leaving_messages(msg.source_actor_id, msg.dest_actor_id);
        let r = _ctx.get_ref();
        tokio::spawn(
            async move {
                waiter.wait_zero().await;
                // sleep 100ms TODO: move this to a notif from destination mover instead of being eepy
                // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                r.notify(FinishLeaving {
                    source_actor_id: msg.source_actor_id,
                    dest_actor_id: msg.dest_actor_id,
                })
            }
            .in_current_span(),
        );
    }
}

struct FinishLeaving {
    source_actor_id: Snowflake<ActorMarker>,
    dest_actor_id: Snowflake<ActorMarker>,
}
impl Message for FinishLeaving {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.FinishLeaving"
    }
}
#[async_trait_nobox]
impl Handler<FinishLeaving> for Mover {
    async fn handle(
        &mut self,
        msg: FinishLeaving,
        _ctx: &mut ActorContext<Self>,
    ) -> <FinishLeaving as Message>::Result {
        let mut entry = self
            .actors_leaving
            .remove(&msg.source_actor_id)
            .expect("Actor is not being moved!!");
        // send to remote mover that moving is finished
        let remote_mover_ref = format!("System.Mover-{}", entry.destination_node_id);
        let remote_mover_ref: NamedActorRef<Mover> = _ctx
            .nucleus()
            .get_named_ref_with_node_hint(remote_mover_ref, vec![entry.destination_node_id])
            .await
            .unwrap();

        entry
            .move_finish_waiters
            .drain(..)
            .for_each(|w| w.send(()).unwrap());

        remote_mover_ref
            .send(proto::mover::ActorMoveOutFinished {
                actor_id: msg.dest_actor_id,
            })
            .await
            .unwrap();
    }
}

// defined in proto
impl Message for proto::mover::ActorMoveOutFinished {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.ActorMoveOutFinished"
    }
}

#[handler(manual)]
impl Handler<proto::mover::ActorMoveOutFinished> for Mover {
    async fn handle(
        &mut self,
        message: proto::mover::ActorMoveOutFinished,
        _ctx: &mut ActorContext<Self>,
    ) -> <proto::mover::ActorMoveOutFinished as Message>::Result {
        let (name, _) = self
            .actors_arriving
            .iter()
            .find(|(_k, v)| v.arriving_actor_id == Some(message.actor_id))
            .expect("Actor is not being moved!!");

        let name = name.clone();

        let entry = self.actors_arriving.remove(&name).unwrap();

        let _ = entry.resume_messages_processed_tx.map(|s| s.send(()));
    }
}

// defined in proto
impl Message for proto::mover::ActorMoveInIntent {
    type Result = proto::mover::ActorMoveInResponse;

    fn name() -> &'static str {
        "System.Mover.ActorMoveInIntent"
    }
}

#[handler(manual)]
impl Handler<proto::mover::ActorMoveInIntent> for Mover {
    async fn handle(
        &mut self,
        msg: proto::mover::ActorMoveInIntent,
        _ctx: &mut ActorContext<Self>,
    ) -> <proto::mover::ActorMoveInIntent as Message>::Result {
        let proto::mover::ActorMoveInIntent {
            name,
            actor_type,
            actor_subs,
        } = msg;

        let name: Arc<str> = name.get_counted();

        let registered = self.registered_movable.get_mut(actor_type.as_str());
        if registered.is_none() {
            error!(
                "<hazard> Actor type not registered for moving: {}",
                actor_type
            );
            return proto::mover::ActorMoveInResponse {
                response: Some(proto::mover::actor_move_in_response::Response::Error(
                    "Actor type not registered for moving".to_string(),
                )),
            };
        }
        let registered = registered.unwrap();

        let (actor_tx, actor_rx) = oneshot::channel();
        let (ref_tx, ref_rx) = oneshot::channel();

        self.actors_arriving.insert(
            name.clone(),
            ArrivingMoverData {
                // movein_complete: sender,
                resume_messages_processed_tx: None,
                arriving_actor_id: None,
                actor_id_sender: None,
                actor_type: actor_type.to_string(),
                actor_tx: Some(actor_tx),
                sub_refs: Some(SubRefMap::new()),
            },
        );

        registered.resume_moved(actor_rx, ref_tx, name.clone());

        test_sleep!();

        if !actor_subs.is_empty() {
            let actor_ref = ref_rx.await.unwrap();

            let sub_refs: SubRefMap = registered.sub_actors(actor_subs, actor_ref).await;

            let arriving_info = self.actors_arriving.get_mut(&*name).unwrap();

            arriving_info.sub_refs = Some(sub_refs);
        }

        proto::mover::ActorMoveInResponse { response: None }
    }
}

// defined in proto
impl Message for proto::mover::ActorMoveInPayload {
    type Result = proto::mover::ActorMoveInResponse;
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.ActorMoveInPayload"
    }
}

#[handler(manual, delayed)]
impl Handler<proto::mover::ActorMoveInPayload> for Mover {
    async fn handle_delayed(
        &mut self,
        msg: proto::mover::ActorMoveInPayload,
        ctx: &mut ActorContext<Self>,
        sender: Sender<<proto::mover::ActorMoveInPayload as Message>::Result>,
    ) {
        let maybe = async {
            let proto::mover::ActorMoveInPayload {
                actor_serialized,
                name,
                actor_type,
                actor_subs_numbered,
                outgoing_seqs,
            } = msg;

            let actor_name: Arc<str> = name.get_counted();

            let registered = self.registered_movable.get(actor_type.as_str());
            if registered.is_none() {
                error!(
                    "<hazard> Actor type not registered for moving: {}",
                    actor_type
                );
                return MaybeDelayed::Instant(proto::mover::ActorMoveInResponse {
                    response: Some(proto::mover::actor_move_in_response::Response::Error(
                        "Actor type not registered for moving".to_string(),
                    )),
                });
            }

            let arriving_info = self.actors_arriving.get_mut(&*actor_name);
            if arriving_info.is_none() {
                error!("<hazard> Actor not registered for moving: {}", actor_name);
                return MaybeDelayed::Instant(proto::mover::ActorMoveInResponse {
                    response: Some(proto::mover::actor_move_in_response::Response::Error(
                        "Actor not registered for moving! Send an ActorMoveInIntent first!"
                            .to_string(),
                    )),
                });
            }

            let arriving_info = arriving_info.unwrap();
            if arriving_info.actor_type != actor_type.as_str() {
                error!("<hazard> Actor type mismatch for moving: {}", actor_name);
                return MaybeDelayed::Instant(proto::mover::ActorMoveInResponse {
                    response: Some(proto::mover::actor_move_in_response::Response::Error(
                        "Actor type mismatch for moving!".to_string(),
                    )),
                });
            }

            let (tx, rx) = oneshot::channel();
            arriving_info.actor_id_sender = Some(tx.into());

            let mut sub_ref_map = arriving_info.sub_refs.take().unwrap();

            for sub in actor_subs_numbered {
                sub_ref_map.insert_seq(
                    sub.topic.as_str(),
                    sub.actor_name.as_str(),
                    sub.message_id.unwrap(),
                );
            }

            for outgoing_seq in outgoing_seqs {
                if let Some(topic) = ctx.nucleus().registry().get_topic_handler(&outgoing_seq.0) {
                    sub_ref_map.insert_outgoing_seq(
                        &self.nucleus,
                        topic.name(),
                        outgoing_seq.1.seq,
                    );
                    ctx.nucleus().pubsub().upsert_subscribed_nodes(
                        topic.name(),
                        actor_name.clone(),
                        outgoing_seq
                            .1
                            .node_ids
                            .into_iter()
                            .map(|v| v as NodeId)
                            .collect(),
                    );
                } else {
                    error!(
                        "<hazard> No topic handler found for topic: {}, skipping importing publisher info",
                        outgoing_seq.0
                    );
                }
            }

            arriving_info
                .actor_tx
                .take()
                .unwrap()
                .send((actor_serialized, sub_ref_map))
                .unwrap();

            test_sleep!();

            MaybeDelayed::Delayed(async move {
                let result = rx.await;

                match result {
                    Ok(actor_id) => proto::mover::ActorMoveInResponse {
                        response: Some(proto::mover::actor_move_in_response::Response::ActorId(
                            actor_id.id(),
                        )),
                    },
                    Err(_err) => proto::mover::ActorMoveInResponse {
                        response: Some(proto::mover::actor_move_in_response::Response::Error(
                            "Failed to move in actor".to_owned(),
                        )),
                    },
                }
            })
        };

        maybe_delayed(sender, maybe).await;
    }
}

pub(crate) struct WaitResumeMessages {
    pub(crate) name: Arc<str>,
    pub(crate) actor_id: Snowflake<ActorMarker>,
}
impl Message for WaitResumeMessages {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.SendResumeMessages"
    }
}

#[handler(delayed, manual)]
impl Handler<WaitResumeMessages> for Mover {
    async fn handle_delayed(
        &mut self,
        msg: WaitResumeMessages,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<<WaitResumeMessages as Message>::Result>,
    ) {
        trace!("inside WaitResumeMessages handler");
        let entry = self.actors_arriving.get_mut(&msg.name).expect("wtf");

        entry.resume_messages_processed_tx = Some(sender);
        entry.arriving_actor_id = Some(msg.actor_id);
        entry
            .actor_id_sender
            .take()
            .unwrap()
            .send(msg.actor_id)
            .unwrap();
    }
}

pub(crate) struct WaitForMoveFinish {
    pub(crate) source_actor_id: Snowflake<ActorMarker>,
}
impl Message for WaitForMoveFinish {
    type Result = ();
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "System.Mover.WaitForMoveFinish"
    }
}

#[handler(delayed, manual)]
impl Handler<WaitForMoveFinish> for Mover {
    async fn handle_delayed(
        &mut self,
        message: WaitForMoveFinish,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<<WaitForMoveFinish as Message>::Result>,
    ) {
        self.actors_leaving
            .get_mut(&message.source_actor_id)
            .expect("Actor is not being moved!!")
            .move_finish_waiters
            .push(sender);
    }
}

#[async_trait_nobox]
impl Handler<InternalSystemShutdown> for Mover {
    async fn handle(
        &mut self,
        _message: InternalSystemShutdown,
        _ctx: &mut ActorContext<Self>,
    ) -> <InternalSystemShutdown as Message>::Result {
        let awaitable = self
            .leaving_awaitable_arc
            .take()
            .expect("leaving awaitable actor on Mover was already taken?");

        awaitable.wait_zero()
    }
}

#[cfg(feature = "debug")]
mod debug_handlers {
    use prost::Message;

    use crate::{
        actor::{ActorContext, message::Handler},
        net::{mover::Mover, proto},
    };

    #[async_trait_nobox]
    impl Handler<proto::debug::GetSerialized> for Mover {
        async fn handle(
            &mut self,
            _message: proto::debug::GetSerialized,
            _ctx: &mut ActorContext<Self>,
        ) -> Vec<u8> {
            proto::debug::SerializedMover {
                leaving_movers: self
                    .actors_leaving
                    .iter()
                    .map(|f| {
                        (
                            f.0.id(),
                            proto::debug::MoverLeavingMoverData {
                                actor_id: *f.0,
                                actor_name: f.1.actor_name.to_string(),
                                actor_type: f.1.actor_type.to_string(),
                                actor_subs: f.1.actor_subs.iter().map(|x| x.to_string()).collect(),
                                destination_node_id: f.1.destination_node_id as u32,
                                fwd_msg_queue_length: f.1.remote_msg_queue.len() as u32,
                            },
                        )
                    })
                    .collect(),
                arriving_movers: self
                    .actors_arriving
                    .iter()
                    .map(|f| {
                        let (name, data) = f;
                        (
                            name.to_string(),
                            proto::debug::MoverArrivingMoverData {
                                actor_name: name.to_string(),
                                actor_type: data.actor_type.to_string(),
                                arriving_actor_id: data.arriving_actor_id,
                            },
                        )
                    })
                    .collect(),
                registered_movable_actors: self
                    .registered_movable
                    .keys()
                    .map(|k| k.to_string())
                    .collect(),
            }
            .encode_to_vec()
        }
    }
}
