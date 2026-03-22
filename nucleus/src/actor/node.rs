use std::{
    any::{Any, type_name},
    ops::Deref,
    sync::Arc,
};

use tokio::sync::oneshot;
use tracing::Span;

use crate::{
    NucleusNode,
    actor::{Actor, refs::LocalActorRef},
    errors::SpawnErr,
};

use super::{
    ActorRuntime, RemoteActor,
    moving::{ActorRx, MovableActor},
};

impl NucleusNode {
    #[instrument(skip(self, handler, name), fields(actor_name = tracing::field::Empty))]
    pub async fn spawn_actor_local<A: Actor>(
        &self,
        name: impl Into<Arc<str>>,
        handler: A,
    ) -> Result<LocalActorRef<A>, SpawnErr> {
        if self.is_closing() {
            return Err(SpawnErr::NucleusShuttingDown);
        }

        let actor_type = type_name::<A>();
        let actor_name: Arc<str> = name.into();

        Span::current().record("actor_name", actor_name.deref());

        let (actor_ref, handle) =
            ActorRuntime::<A>::spawn(handler, actor_name.clone(), self.clone(), actor_type).await?;

        self.registry()
            .new_local_actor(actor_name, actor_ref.clone(), handle);

        Ok(actor_ref)
    }

    #[instrument(skip(self, handler, name), fields(actor_name = tracing::field::Empty))]
    pub async fn spawn_actor<A: RemoteActor>(
        &self,
        name: impl Into<Arc<str>>,
        handler: A,
    ) -> Result<LocalActorRef<A>, SpawnErr> {
        if self.is_closing() {
            return Err(SpawnErr::NucleusShuttingDown);
        }

        let actor_type = A::info().actor_type;
        let actor_name: Arc<str> = name.into();

        Span::current().record("actor_name", actor_name.deref());

        let (actor_ref, handle) =
            ActorRuntime::<A>::spawn(handler, actor_name.clone(), self.clone(), actor_type).await?;

        self.registry()
            .new_remotable_actor(actor_name, actor_ref.clone(), handle)
            .await;

        Ok(actor_ref)
    }

    pub(crate) async fn spawn_actor_resumed<A: MovableActor>(
        &self,
        name: impl Into<Arc<str>>,
        actor_rx: ActorRx,
        ref_tx: oneshot::Sender<Box<dyn Any + Send>>,
    ) -> Result<LocalActorRef<A>, SpawnErr> {
        if self.is_closing() {
            return Err(SpawnErr::NucleusShuttingDown);
        }

        let actor_name: Arc<str> = name.into();

        let (actor_ref, handle) =
            ActorRuntime::<A>::spawn_resumed(actor_rx, ref_tx, actor_name.clone(), self.clone())
                .await?;

        self.registry()
            .new_remotable_actor(actor_name, actor_ref.clone(), handle)
            .await;

        Ok(actor_ref)
    }
}
