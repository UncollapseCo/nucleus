use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;

use crate::{
    NodeId, NucleusNode,
    actor::{
        ActorContext,
        guards::RatelimitStore,
        message::{Handler, Message},
        moving::MovableActor,
        refs::NamedActorRef,
        tests::{
            handlers::{
                CheckedIncrMessage, DoSubMessage, GetNumMessage, PubIncrNumber, SetNumMessage,
            },
            setup, shutdown,
        },
    },
    net::mover::SubRefMap,
};

use super::{Numtopic, handlers::TestActor};

struct MoveActor {
    dest: NodeId,
}
impl Message for MoveActor {
    type Result = ();
    fn name() -> &'static str {
        "MoveActor"
    }
}

#[async_trait_nobox]
impl MovableActor for TestActor {
    fn serialize(&self) -> Bytes {
        self.last_num.to_le_bytes().to_vec().into()
    }

    fn deserialize(data: Bytes, _nucleus: &NucleusNode, mut sub_ref_map: SubRefMap) -> Self {
        let last_num = u64::from_le_bytes(data.as_ref().try_into().unwrap());

        let sub_ref = sub_ref_map
            .get_ref::<Numtopic>()
            .and_then(|x| x.into_iter().next());

        Self {
            last_num,
            rl_store: RatelimitStore::default(),
            sub_ref,
        }
    }

    async fn on_pause(&mut self, _ctx: &mut ActorContext<Self>) {
        info!("Pausing test actor");
    }
    async fn before_resume(&mut self, _ctx: &mut ActorContext<Self>) {
        info!("Resuming test actor");
    }
    async fn post_resume(&mut self, _ctx: &mut ActorContext<Self>) {
        info!("Post-resuming test actor");
    }
}

#[async_trait_nobox]
impl Handler<MoveActor> for TestActor {
    async fn handle(&mut self, msg: MoveActor, ctx: &mut ActorContext<Self>) {
        let mut m = ctx.move_to(msg.dest);

        if let Some(r) = self.sub_ref.as_ref() {
            m = m.with_pubsub_ref(&r);
        }

        m.commit().await.unwrap();
    }
}

#[tokio::test]
async fn test_simple_move() {
    let (node1, _node2, actor, _actor2) = setup().await;

    actor.send(SetNumMessage { num: 45 }).await.unwrap();
    actor.send(MoveActor { dest: 1 }).await.unwrap();

    // assert actor is closed and in moving state
    assert!(actor.is_moving_out());

    let named_ref: NamedActorRef<TestActor> = node1.get_named_ref("test").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let num = named_ref.send(GetNumMessage).await.unwrap();
    // assert num is 45
    assert_eq!(num.num, 45);
    assert_eq!(named_ref.actor_id().await.unwrap().worker_id(), 1);
    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_named_ref_spam() {
    let (node1, _node2, actor, _actor2) = setup().await;

    actor.send(SetNumMessage { num: 0 }).await.unwrap();
    let named_ref: NamedActorRef<TestActor> = node1.get_named_ref("test").await.unwrap();
    let named_ref_2 = named_ref.clone();
    let named_ref_3 = named_ref.clone();

    let task = tokio::spawn(async move {
        for i in 0..50 {
            named_ref
                .send(CheckedIncrMessage { expected_num: i })
                .await
                .unwrap();
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    actor.send(MoveActor { dest: 1 }).await.unwrap();

    task.await.unwrap();
    // send some more
    let task = tokio::spawn(async move {
        for i in 50..100 {
            named_ref_2
                .send(CheckedIncrMessage { expected_num: i })
                .await
                .unwrap();
        }
    });

    // assert actor is closed and in moving state
    assert!(actor.is_moving_out());

    task.await.unwrap();
    let num = named_ref_3.send(GetNumMessage).await.unwrap();
    // assert num is 45
    assert_eq!(num.num, 100);
    assert_eq!(named_ref_3.actor_id().await.unwrap().worker_id(), 1);
    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_remote_named_ref_spam() {
    let (_node1, node2, actor, _actor2) = setup().await;

    actor.send(SetNumMessage { num: 0 }).await.unwrap();
    let named_ref: NamedActorRef<TestActor> = node2.get_named_ref("test").await.unwrap();

    let named_ref_3 = named_ref.clone();

    let task = tokio::spawn(async move {
        for i in 0..50 {
            named_ref
                .notify(CheckedIncrMessage { expected_num: i })
                .await
                .unwrap();
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    actor.send(MoveActor { dest: 1 }).await.unwrap();

    task.await.unwrap();
    // send some more

    // assert actor is closed and in moving state
    assert!(actor.is_moving_out());

    let num = named_ref_3.send(GetNumMessage).await.unwrap();
    // assert num is 45
    assert_eq!(num.num, 50);
    assert_eq!(named_ref_3.actor_id().await.unwrap().worker_id(), 1);
    shutdown(_node1, node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_moving_subs() {
    let (node1, node2, actor, actor2) = setup().await;

    // make actor2 subscribe to actor1 on NumTopic
    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    // move actor2 to node1
    actor2.send(SetNumMessage { num: 68 }).await.unwrap();
    actor2.send(MoveActor { dest: 0 }).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    actor.send(SetNumMessage { num: 68 }).await.unwrap();
    actor.send(PubIncrNumber {}).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // now get number from test2, should be 68
    let actor2ref: NamedActorRef<TestActor> = node1.get_named_ref("test2").await.unwrap();
    let last_num = actor2ref.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 69);

    shutdown(node1, node2, actor, actor2).await;
}

#[tokio::test]
async fn test_sub_moving_consistency_basic() {
    // spawn a task to spam publish from actor 2
    // while doing that, move actor 1 to node 2

    let (node1, node2, actor, actor2) = setup().await;

    // make actor2 subscribe to actor1 on NumTopic
    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 5 }).await.unwrap();
    actor2.send(SetNumMessage { num: 5 }).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    let tstop = stop.clone();
    let tactor = actor.clone();
    let task = tokio::spawn(async move {
        let mut incr_count = 0;
        while !tstop.load(Ordering::Relaxed) {
            tactor.notify(PubIncrNumber {}).unwrap();
            incr_count += 1;
            // yield event loop
            tokio::task::yield_now().await;
        }
        incr_count
    });

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    actor2.send(MoveActor { dest: 0 }).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // discover actor2 on node1
    let actor2ref: NamedActorRef<TestActor> = node1.get_named_ref("test2").await.unwrap();

    stop.store(true, Ordering::Relaxed);
    let incr_count = task.await.unwrap();

    // tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let last_num = actor2ref.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 5 + incr_count);

    //sleep 1s
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    shutdown(node1, node2, actor, actor2).await;
}

#[tokio::test]
async fn test_sub_moving_consistency_fromlocal() {
    // spawn a task to spam publish from actor 2, which is on node 1
    // while doing that, move actor 1 to node 2

    let (node1, node2, actor, _) = setup().await;

    let actor2 = node1
        .spawn_actor(
            "test3",
            TestActor {
                last_num: 0,
                rl_store: RatelimitStore::default(),
                sub_ref: None,
            },
        )
        .await
        .unwrap();

    // make actor2 subscribe to actor1 on NumTopic
    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 5 }).await.unwrap();
    actor2.send(SetNumMessage { num: 5 }).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    let tstop = stop.clone();
    let tactor = actor.clone();
    let task = tokio::spawn(async move {
        let mut incr_count = 0;
        while !tstop.load(Ordering::Relaxed) {
            tactor.notify(PubIncrNumber {}).unwrap();
            incr_count += 1;
            // yield event loop
            tokio::task::yield_now().await;
        }
        incr_count
    });

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    actor2.send(MoveActor { dest: 1 }).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // discover actor2 on node1
    let actor2ref: NamedActorRef<TestActor> = node1.get_named_ref("test3").await.unwrap();

    stop.store(true, Ordering::Relaxed);
    let incr_count = task.await.unwrap();

    // tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let last_num = actor2ref.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 5 + incr_count);

    //sleep 1s
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    shutdown(node1, node2, actor, actor2).await;
}

#[tokio::test]
async fn test_moving_publisher() {
    // actor2 subs to actor1, actor 1 publishes 10 msgs, then moves to node2 and then publishes 10 more, actor2 should receive all 20
    let (node1, node2, actor, actor2) = setup().await;

    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 5 }).await.unwrap();
    actor2.send(SetNumMessage { num: 5 }).await.unwrap();

    for _ in 0..10 {
        actor.notify(PubIncrNumber {}).unwrap();
    }

    actor.send(MoveActor { dest: 1 }).await.unwrap();

    // sleep 1s
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // get namedref to moved actor
    let actor1ref: NamedActorRef<TestActor> = node2.get_named_ref("test").await.unwrap();

    for _ in 0..10 {
        actor1ref.notify(PubIncrNumber {}).await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 5 + 20);

    shutdown(node1, node2, actor, actor2).await;
}

#[tokio::test]
async fn test_moving_publisher_fromlocal() {
    // actor2 subs to actor1, both are on node1, actor 1 publishes 10 msgs, then moves to node2 and then publishes 10 more, actor2 should receive all 20
    let (node1, node2, actor, _) = setup().await;

    let actor2 = node1
        .spawn_actor(
            "test2",
            TestActor {
                last_num: 0,
                rl_store: RatelimitStore::default(),
                sub_ref: None,
            },
        )
        .await
        .unwrap();

    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 5 }).await.unwrap();
    actor2.send(SetNumMessage { num: 5 }).await.unwrap();

    for _ in 0..5 {
        actor.notify(PubIncrNumber {}).unwrap();
    }

    actor.send(MoveActor { dest: 1 }).await.unwrap();

    // sleep 1s
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // get namedref to moved actor
    let actor1ref: NamedActorRef<TestActor> = node2.get_named_ref("test").await.unwrap();

    for _ in 0..5 {
        actor1ref.notify(PubIncrNumber {}).await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 5 + 10);

    shutdown(node1, node2, actor, actor2).await;
}
