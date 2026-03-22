use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU16, Ordering};

use handlers::{
    CheckedIncrMessage, GetNumMessage, Numtopic, PubIncrNumber, SetNumMessage, TestActor,
};
use tracing::Instrument;

use crate::NucleusNode;
use crate::actor::refs::LocalActorRef;
use crate::actor::{Actor, ActorContext};
use crate::errors::{ActorProcessingErr, SpawnErr};
use crate::net::remote::HandlerRegistryBuilder;

use super::guards::RatelimitStore;

mod handlers;
mod test_clients;
mod test_guards;
mod test_moving;
mod test_remote;

static TEST_PORT: AtomicU16 = AtomicU16::new(10000);

async fn setup() -> (
    NucleusNode,
    NucleusNode,
    LocalActorRef<TestActor>,
    LocalActorRef<TestActor>,
) {
    // tracing subscriber with trace level logging

    if true {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }
    // if TEST_TELEMETRY
    //     .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
    //     .is_ok()
    // {
    //     NucleusNode::init_telemetry();
    // }

    let registry1 = HandlerRegistryBuilder::default()
        .with_actor::<TestActor>()
        .with_handler::<TestActor, SetNumMessage>()
        .with_handler::<TestActor, GetNumMessage>()
        .with_handler::<TestActor, CheckedIncrMessage>()
        .with_handler::<TestActor, PubIncrNumber>()
        .with_movable_sub_ref::<TestActor, Numtopic>()
        .with_topic(Numtopic);

    let registry2 = HandlerRegistryBuilder::default()
        .with_actor::<TestActor>()
        .with_handler::<TestActor, SetNumMessage>()
        .with_handler::<TestActor, GetNumMessage>()
        .with_handler::<TestActor, CheckedIncrMessage>()
        .with_handler::<TestActor, PubIncrNumber>()
        .with_movable_sub_ref::<TestActor, Numtopic>()
        .with_topic(Numtopic);

    let port1 = TEST_PORT.fetch_add(1, Ordering::Acquire);
    let addr1 = format!("127.0.0.1:{}", port1);
    let port2 = TEST_PORT.fetch_add(1, Ordering::Acquire);
    let addr2 = format!("127.0.0.1:{}", port2);

    let node1 = NucleusNode::new_root(
        0,
        "test",
        (),
        SocketAddr::from_str(&addr1).unwrap(),
        None,
        None,
        registry1,
        0u64,
        None,
    )
    .await
    .unwrap();

    let node2 = NucleusNode::new_root(
        1,
        "test",
        (),
        SocketAddr::from_str(&addr2).unwrap(),
        None,
        Some(addr1),
        registry2,
        0u64,
        None,
    )
    .await
    .unwrap();

    let actor = node1
        .spawn_actor(
            "test".to_string(),
            TestActor {
                last_num: 0,
                rl_store: RatelimitStore::default(),
                sub_ref: None,
            },
        )
        .await
        .unwrap();
    let actor2 = node2
        .spawn_actor(
            "test2".to_string(),
            TestActor {
                last_num: 0,
                rl_store: RatelimitStore::default(),
                sub_ref: None,
            },
        )
        .await
        .unwrap();

    node1.registry().register_movable::<TestActor>();
    node2.registry().register_movable::<TestActor>();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    (node1, node2, actor, actor2)
}

async fn shutdown(
    node1: NucleusNode,
    node2: NucleusNode,
    actor1: LocalActorRef<TestActor>,
    actor2: LocalActorRef<TestActor>,
) {
    actor1.notify_stop();
    actor2.notify_stop();
    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_panic_on_start_captured() {
    struct TestActor;

    #[async_trait_nobox]
    impl Actor for TestActor {
        async fn init(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorProcessingErr> {
            panic!("boom");
        }
    }
    let (node, _, _, _) = setup().await;

    let actor_output = node.spawn_actor_local("test".to_string(), TestActor).await;
    assert!(matches!(actor_output, Err(SpawnErr::StartupFailed(_))));
}

#[tokio::test]
async fn test_error_on_start_captured() {
    struct TestActor;

    #[async_trait_nobox]
    impl Actor for TestActor {
        async fn init(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorProcessingErr> {
            Err(From::from("boom"))
        }
    }
    let (node, _, _, _) = setup().await;

    let actor_output = node.spawn_actor_local("test2".to_string(), TestActor).await;
    assert!(matches!(actor_output, Err(SpawnErr::StartupFailed(_))));
}
