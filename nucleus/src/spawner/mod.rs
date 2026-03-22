use std::{pin::Pin, sync::Arc};

use tokio::sync::oneshot;

use crate::{
    NucleusNode,
    actor::{
        Actor, ActorContext, RemoteActor,
        message::{Handler, Message},
        refs::{ActorRef, BoxedActorRef, LocalActorRef},
    },
    errors::SpawnErr,
};

pub(crate) struct SpawnerGetOrCreateActor<T: Actor> {
    name: Arc<str>,
    _t: std::marker::PhantomData<T>,
}

impl<T: Actor> SpawnerGetOrCreateActor<T> {
    pub(crate) fn new(name: Arc<str>) -> Self {
        Self {
            name,
            _t: std::marker::PhantomData,
        }
    }
}

impl<T: Actor> Message for SpawnerGetOrCreateActor<T> {
    type Result = Result<ActorRef<T>, SpawnErr>;
    fn name() -> &'static str {
        "System.SpawnerGetOrCreateActor"
    }
}

pub(crate) struct SpawnerGetActor<T: Actor> {
    name: Arc<str>,
    _t: std::marker::PhantomData<T>,
}

impl<T: Actor> SpawnerGetActor<T> {
    pub(crate) fn new(name: Arc<str>) -> Self {
        Self {
            name,
            _t: std::marker::PhantomData,
        }
    }
}

impl<T: Actor> Message for SpawnerGetActor<T> {
    type Result = Option<ActorRef<T>>;
    fn name() -> &'static str {
        "System.SpawnerGetActor"
    }
}

#[async_trait_nobox]
pub trait Spawner: RemoteActor {
    type A: RemoteActor;

    async fn get_or_create_actor(
        &mut self,
        name: Arc<str>,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<ActorRef<Self::A>, SpawnErr>;

    async fn get_actor(
        &mut self,
        name: Arc<str>,
        _ctx: &mut ActorContext<Self>,
    ) -> Option<ActorRef<Self::A>>;
}

#[async_trait_nobox]
impl<T: Spawner> Handler<SpawnerGetOrCreateActor<T::A>> for T {
    async fn handle(
        &mut self,
        msg: SpawnerGetOrCreateActor<T::A>,
        _ctx: &mut ActorContext<T>,
    ) -> <SpawnerGetOrCreateActor<T::A> as Message>::Result {
        self.get_or_create_actor(msg.name, _ctx).await
    }
}

#[async_trait_nobox]
impl<T: Spawner> Handler<SpawnerGetActor<T::A>> for T {
    async fn handle(
        &mut self,
        msg: SpawnerGetActor<T::A>,
        _ctx: &mut ActorContext<T>,
    ) -> <SpawnerGetActor<T::A> as Message>::Result {
        self.get_actor(msg.name, _ctx).await
    }
}

pub(crate) trait SpawnerTrait {
    fn get_or_create_actor_box(
        &self,
        name: Arc<str>,
    ) -> Pin<Box<dyn Future<Output = Result<BoxedActorRef, SpawnErr>> + Send>>;
    fn get_actor_box(
        &self,
        name: Arc<str>,
    ) -> Pin<Box<dyn Future<Output = Option<BoxedActorRef>> + Send>>;
}

impl<T> SpawnerTrait for LocalActorRef<T>
where
    T: Spawner,
{
    fn get_or_create_actor_box(
        &self,
        name: Arc<str>,
    ) -> Pin<Box<dyn Future<Output = Result<BoxedActorRef, SpawnErr>> + Send>> {
        let msg = SpawnerGetOrCreateActor::new(name);
        let (tx, rx) = oneshot::channel();

        self.send_with_sender(msg, tx.into()).unwrap();

        Box::pin(async move {
            let res = rx.await.unwrap();
            match res {
                Ok(actor) => Ok(BoxedActorRef(Arc::new(actor))),
                Err(e) => Err(e),
            }
        })
    }

    fn get_actor_box(
        &self,
        name: Arc<str>,
    ) -> Pin<Box<dyn Future<Output = Option<BoxedActorRef>> + Send>> {
        // let msg = SpawnerGetActor::new(name);
        // let res = self.send(msg).unwrap().await;
        // res.map(|actor| BoxedActorRef(Arc::new(actor)))

        let msg = SpawnerGetActor::new(name);
        let (tx, rx) = oneshot::channel();

        self.send_with_sender(msg, tx.into()).unwrap();

        Box::pin(async move {
            let res = rx.await.unwrap();
            res.map(|actor| BoxedActorRef(Arc::new(actor)))
        })
    }
}

pub(crate) type BoxedSpawner = Box<dyn SpawnerTrait + Send + Sync>;

impl NucleusNode {
    pub async fn add_spawner<T: Spawner>(
        &self,
        spawner: T,
        name: impl Into<Arc<str>>,
    ) -> Result<(), SpawnErr> {
        let r = self.spawn_actor(name, spawner).await?;

        self.registry()
            .spawners
            .insert(T::A::info().actor_type.to_owned(), Box::new(r));

        Ok(())
    }
}
