use std::sync::Arc;

use fxhash::FxHashMap;
use tracing::Instrument;

use crate::ActorMarker;
use crate::actor::message::Message;
use crate::actor::{ActorContext, RemoteActorInfo, Sender};
use crate::net::proto;
use crate::{
    NodeId,
    actor::{Actor, RemoteActor},
    snowflake::Snowflake,
};

use super::{
    ActorType, GetNamedActor, PublishSystemEvent, SetNamedActor, SystemTopic, UnsetNamedActor,
};

#[derive(Default)]
pub struct RemoteRegistryActor {
    named_actors: FxHashMap<Arc<str>, (ActorType, Snowflake<ActorMarker>)>,
}

impl Actor for RemoteRegistryActor {
    #[cfg(feature = "smallbox")]
    type HandlerStorage = [usize; 75];
}
impl RemoteActor for RemoteRegistryActor {
    fn info() -> &'static RemoteActorInfo {
        &RemoteActorInfo {
            actor_type: "RemoteRegistry",
        }
    }
}

#[handler(manual)]
impl RemoteRegistryActor {
    async fn handle_proto_remote_named_actor_discovery_request(
        &mut self,
        message: proto::remote::NamedActorDiscoveryRequest,
        _ctx: &mut ActorContext<Self>,
    ) -> proto::remote::NamedActorDiscoveryResponse {
        let res = match self.named_actors.get(message.actor_name.as_str()) {
            Some(actor_id) => match actor_id.0 {
                None => {
                    error!(
                        "named actor locality mismatch (name={}, expected=remote, actual=local)",
                        message.actor_name,
                    );
                    None
                }
                Some(actor_type) => {
                    if let Some(atype_check) = message.actor_type {
                        if actor_type != atype_check.as_str() {
                            error!(
                                "named actor type mismatch (name={}, expected={}, actual={})",
                                message.actor_name, atype_check, actor_type
                            );
                            None
                        } else {
                            Some((actor_id.1, actor_type))
                        }
                    } else {
                        Some((actor_id.1, actor_type))
                    }
                }
            },
            None => None,
        };

        if let Some((id, _type)) = res {
            proto::remote::NamedActorDiscoveryResponse {
                actor_id: Some(id),
                actor_type: Some(_type.into()),
            }
        } else {
            proto::remote::NamedActorDiscoveryResponse {
                actor_id: None,
                actor_type: None,
            }
        }
    }

    async fn handle_get_named_actor(
        &mut self,
        message: GetNamedActor,
        _ctx: &mut ActorContext<Self>,
    ) -> Option<(Snowflake<ActorMarker>, Option<&'static str>)> {
        self.named_actors
            .get(&message.actor_name)
            .map(|actor_id| (actor_id.1, actor_id.0))
    }

    async fn handle_set_named_actor(
        &mut self,
        message: SetNamedActor,
        _ctx: &mut ActorContext<Self>,
    ) -> <SetNamedActor as Message>::Result {
        self.named_actors
            .insert(message.name, (message.actor_type, message.id));
    }

    async fn handle_unset_named_actor(
        &mut self,
        message: UnsetNamedActor,
        _ctx: &mut ActorContext<Self>,
    ) -> <UnsetNamedActor as Message>::Result {
        self.named_actors.remove(&message.name);
    }

    async fn handle_publish_system_event(
        &mut self,
        message: PublishSystemEvent,
        ctx: &mut ActorContext<Self>,
    ) -> <PublishSystemEvent as Message>::Result {
        ctx.pubsub().publish_local::<SystemTopic>(message.message);
    }

    #[handler(delayed)]
    async fn handle_proto_pubsub_pubsub_subscribe(
        &mut self,
        message: proto::pubsub::PubsubSubscribe,
        ctx: &mut ActorContext<Self>,
        sender: Sender<<proto::pubsub::PubsubSubscribe as Message>::Result>,
    ) {
        let nucleus = ctx.nucleus().clone();
        tokio::spawn(
            async move { sender.send(nucleus.pubsub().remote_node_subscribe(message).await) }
                .in_current_span(),
        );
    }

    #[handler(delayed)]
    async fn handle_proto_pubsub_pubsub_unsubscribe(
        &mut self,
        message: proto::pubsub::PubsubUnsubscribe,
        ctx: &mut ActorContext<Self>,
        sender: Sender<<proto::pubsub::PubsubUnsubscribe as Message>::Result>,
    ) {
        let nucleus = ctx.nucleus().clone();
        tokio::spawn(
            async move { sender.send(nucleus.pubsub().remote_node_unsubscribe(message)) }
                .in_current_span(),
        );
    }

    #[handler(delayed)]
    async fn handle_proto_pubsub_publisher_moved(
        &mut self,
        message: proto::pubsub::PublisherMoved,
        ctx: &mut ActorContext<Self>,
        sender: Sender<<proto::pubsub::PublisherMoved as Message>::Result>,
    ) {
        let nucleus = ctx.nucleus().clone();
        tokio::spawn(
            async move {
                nucleus
                    .registry()
                    .update_named_actor_id(message.actor_name.as_str(), message.new_publisher_id);

                sender.send(
                    nucleus.pubsub().remote_publisher_moved(
                        message.actor_name.as_str(),
                        message.new_publisher_id,
                    ),
                )
            }
            .in_current_span(),
        );
    }

    async fn handle_proto_pubsub_pubsub_message(
        &mut self,
        message: proto::pubsub::PubsubMessage,
        ctx: &mut ActorContext<Self>,
    ) -> <proto::pubsub::PubsubMessage as Message>::Result {
        ctx.nucleus().pubsub().publish_incoming(message)
    }

    async fn handle_proto_remote_remote_node_register(
        &mut self,
        message: proto::remote::RemoteNodeRegister,
        ctx: &mut ActorContext<Self>,
    ) {
        if message.node_id as NodeId == ctx.nucleus().node_id() {
            // skip on self connect
            return;
        }

        if ctx
            .nucleus()
            .registry()
            .get_remote_node(message.node_id as u16)
            .is_none()
        {
            let nucleus = ctx.nucleus().clone();
            tokio::spawn(
                async move {
                    // only publish if connecting succeeds
                    if nucleus
                        .registry()
                        .connect_remote(message.address.parse().unwrap())
                        .await
                        .is_ok()
                    {
                        nucleus.publish_remote_nodes(message).await
                    }
                }
                .in_current_span(),
            );
        }
    }

    async fn handle_proto_remote_remote_node_unregister(
        &mut self,
        message: proto::remote::RemoteNodeUnregister,
        ctx: &mut ActorContext<Self>,
    ) {
        if message.node_id as NodeId == ctx.nucleus().node_id() {
            warn!("Registry received RemoteNodeUnregister, shutting down system");
            let n = ctx.nucleus().clone();

            tokio::spawn(async move { n.shutdown().await }.in_current_span());
            return;
        }

        let r = ctx
            .nucleus()
            .registry()
            .get_remote_node(message.node_id as u16);

        if let Some(r) = r {
            if let Err(e) = r.actor_ref().notify(message) {
                debug!(
                    "error notifying remote node of unregister, probably already dead - {:?}",
                    e
                );
            }
        }
    }

    async fn handle_proto_remote_remote_node_heartbeat(
        &mut self,
        message: proto::remote::RemoteNodeHeartbeat,
        ctx: &mut ActorContext<Self>,
    ) {
        if let Some(r) = ctx
            .nucleus()
            .registry()
            .get_remote_node(message.node_id as u16)
        {
            let _ = r.actor_ref().notify(message);
        }
    }
}

#[cfg(feature = "debug")]
mod debug_handlers {
    use futures::{StreamExt, stream::FuturesUnordered};

    use crate::{
        NodeId,
        actor::{
            ActorContext, Sender,
            delay::{MaybeDelayed, maybe_delayed},
        },
        net::proto,
        utils::AsyncErrorSquash,
    };

    use super::RemoteRegistryActor;
    use crate::net::registry::Message;

    #[handler(manual)]
    impl RemoteRegistryActor {
        #[handler(delayed)]
        async fn handle_proto_debug_list_actors(
            &mut self,
            message: proto::debug::ListActors,
            ctx: &mut ActorContext<Self>,
            sender: Sender<<proto::debug::ListActors as Message>::Result>,
        ) {
            let f = async {
                let mut actors: Vec<_> = self
                    .named_actors
                    .iter()
                    .map(|(k, v)| proto::debug::ActorInfo {
                        name: k.to_string(),
                        id: v.1,
                        actor_type: v.0.map(|x| x.to_string()),
                    })
                    .collect();

                if !message.global {
                    return MaybeDelayed::Instant(proto::debug::ActorList { actors });
                }

                let nucleus = ctx.nucleus().clone();
                MaybeDelayed::Delayed(async move {
                    let remote_nodes: Vec<NodeId> = nucleus
                        .registry()
                        .remote_nodes
                        .iter()
                        .map(|r| *r.key())
                        .collect();

                    let mut futures = FuturesUnordered::new();

                    for remote in remote_nodes.into_iter() {
                        // let aref = remote.value().clone().0;

                        let nucleus = nucleus.clone();
                        futures.push(async move {
                            nucleus
                                .send_remote_node(
                                    remote,
                                    proto::debug::ListActors { global: false },
                                )
                                .squash()
                                .await
                                .ok()
                        });
                    }

                    while let Some(res) = futures.next().await {
                        if let Some(res) = res {
                            actors.extend(res.actors.into_iter());
                        }
                    }

                    proto::debug::ActorList { actors }
                })
            };

            maybe_delayed(sender, f).await;
        }
    }
}
