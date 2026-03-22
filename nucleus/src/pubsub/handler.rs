use crate::actor::Actor;
use crate::pubsub::Message;

/*

*/

use crate::actor::{ActorContext, Sender, message::Handler};

use super::{PubsubMessage, Topic};

pub(crate) struct PubsubMsgWrapper<T: Topic> {
    pub(crate) msg: PubsubMessage<T>,
}

impl<T> Message for PubsubMsgWrapper<T>
where
    T: Topic,
{
    type Result = ();

    fn name() -> &'static str {
        "PubsubMsgWrapper"
    }
}

#[handler(manual, delayed)]
impl<A, T> Handler<PubsubMsgWrapper<T>> for A
where
    A: Actor + Handler<PubsubMessage<T>>,
    T: Topic,
{
    async fn handle_delayed(
        &mut self,
        message: PubsubMsgWrapper<T>,
        ctx: &mut ActorContext<Self>,
        sender: Sender<()>,
    ) {
        let key = (message.msg.actor_name.clone(), T::name());
        let seq = ctx.sub_seq_ids.entry(key).or_default();

        if message.msg.seq_id != *seq + 1 {
            if message.msg.seq_id == u64::MAX || *seq == 0 {
                // MAX means publisher stopped
                // 0 means first message, so no need to warn
            } else {
                warn!(
                    "Received pubsub message with unexpected message id (topic={}, actor={}, expected={}, received={})",
                    T::name(),
                    message.msg.actor_name,
                    *seq + 1,
                    message.msg.seq_id
                );
            }

            if message.msg.seq_id <= *seq {
                warn!(
                    "Ignoring pubsub message with id {} (topic={}, actor={}), double received! (possibly due to actor being moved)",
                    message.msg.seq_id,
                    T::name(),
                    message.msg.actor_name
                );
                return;
            }
        }

        if message.msg.seq_id == u64::MAX {
            // means publisher stopped, so we reset its seq to 0
            *seq = 0;
        } else {
            *seq = message.msg.seq_id;
        }

        self.handle_delayed(message.msg, ctx, sender).await
    }
}
