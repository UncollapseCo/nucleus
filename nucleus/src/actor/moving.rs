use std::{future::Future, pin::Pin};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{NucleusNode, net::mover::SubRefMap};

use super::{Actor, ActorContext, RemoteActor};
/*
Explanation of the moving protocol - 1s means step 1 on source node, 3d means 3rd step on destination node

1s call move_to(node)
  registers with local mover
  send ActorMoveInIntent to destination node

1d register incoming actor

2s closes mailbox channel + process all pending messages
3s call on_pause hook
4s send serialized actor to new node - ActorMoveInPayload - await for actor id response

2d resume_moved() - deserialize actor
3d run before_resume hook
4d tell local mover new actor id (send WaitResumeMessages to Mover)
   register name with local registry - discovery requests will now succeed
   respond with ActorMoveInResponse with the dest actor id

4s receive destination actor id
5s remove name and id from local registry - name discoveries will now fail on local
6s send all received remote messages to new node and sends responses back in separate tasks - holds AwaitableArc

5d process all messages forwarded by source node

7s when all resumed message responses have been received
  remove local actor leaving entry
  notify all local waiters waiting for move (named refs queue this by WaitForMoveFinish)
  tell remote ActorMoveOutFinished

6d on ActorMoveOutFinished - remove local arriving entry
7d stop processing resume messages
8d call post_resume hook
9d start regular processing loop

some notes - 4d registers new name on dest node, while 5s unregisters the name later on source node - this means discoveries might return two results for a short window of time
this is okay, because until the source one is unregistered it will forward all the messages to the new node when its finished moving
in the implementation, message will be sent to whichever future resolves first

on resume, messages sent to source node are processed first and in order - this creates a tiny potential of unordered messages if they get sent to the two different
nodes at the same time - this should be okay due to the fact that the window is really small, and named actor refs are cached per node, so messages originating
from one node via named actor refs will always be processed in order
*/

#[async_trait_nobox]
pub trait MovableActor: RemoteActor {
    fn serialize(&self) -> Bytes;
    fn deserialize(data: Bytes, nucleus: &NucleusNode, sub_refs: SubRefMap) -> Self;

    async fn on_pause(&mut self, _ctx: &mut ActorContext<Self>) {}
    async fn before_resume(&mut self, _ctx: &mut ActorContext<Self>) {}
    async fn post_resume(&mut self, _ctx: &mut ActorContext<Self>) {}
}

type Nerd<A> =
    for<'a> fn(&'a mut A, &'a mut ActorContext<A>) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub type ActorRx = oneshot::Receiver<(Bytes, SubRefMap)>;

pub(crate) struct MovingVtable<A: Actor> {
    pub(crate) serialize: fn(&A) -> Bytes,
    pub(crate) deserialize: fn(Bytes, nucleus: &NucleusNode, sub_refs: SubRefMap) -> A,
    pub(crate) on_pause: Nerd<A>,
    pub(crate) on_resume: Nerd<A>,
}

impl<A: Actor> MovingVtable<A> {
    pub(crate) fn new() -> Self
    where
        A: MovableActor,
    {
        Self {
            serialize: A::serialize,
            deserialize: A::deserialize,
            on_pause: |actor, ctx| Box::pin(A::on_pause(actor, ctx)),
            on_resume: |actor, ctx| Box::pin(A::before_resume(actor, ctx)),
        }
    }
}

impl<A: Actor> Clone for MovingVtable<A> {
    fn clone(&self) -> Self {
        Self {
            serialize: self.serialize,
            deserialize: self.deserialize,
            on_pause: self.on_pause,
            on_resume: self.on_resume,
        }
    }
}
