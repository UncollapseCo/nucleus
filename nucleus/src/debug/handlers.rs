use crate::{
    actor::{
        ActorContext,
        message::{Handler, Message},
        moving::MovableActor,
    },
    net::proto,
};

#[async_trait_nobox]
impl<A: MovableActor> Handler<proto::debug::GetSerialized> for A {
    async fn handle(
        &mut self,
        _message: proto::debug::GetSerialized,
        _ctx: &mut ActorContext<Self>,
    ) -> <proto::debug::GetSerialized as Message>::Result {
        self.serialize().into()
    }
}
