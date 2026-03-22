use std::net::SocketAddr;
use std::str::FromStr;

use crate::NucleusNode;
use crate::actor::RemoteActor;
use crate::actor::refs::NamedActorRef;
use crate::actor::refs::weak::WeakActorRef;
use crate::actor::tests::handlers::{
    CheckedIncrMessage, DoSubMessage, GetNumMessage, PubIncrNumber, SendRequestMessage,
    SetNumMessage, TestActor,
};
use crate::actor::tests::{setup, shutdown};
use crate::net::remote::{DebugInterruptStream, HandlerRegistryBuilder};

#[tokio::test]
async fn test_remote() {
    let (node1, _node2, actor, actor2) = setup().await;
    let actor2_id = actor2.actor_id();

    actor
        .send(SendRequestMessage {
            actor_id: actor2_id,
        })
        .await
        .unwrap();

    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 42);

    info!("Testing named actor discovery");

    let named_ref: NamedActorRef<TestActor> = node1
        .get_named_ref("test2")
        .await
        .expect("actor should be discoverable across nodes");

    named_ref.send(SetNumMessage { num: 43 }).await.unwrap();

    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 43);

    shutdown(node1, _node2, actor, actor2).await;
}

#[tokio::test]
#[tracing::instrument]
async fn test_named_discovery() {
    let (node1, node2, actor, actor2) = setup().await;

    let named_ref: NamedActorRef<TestActor> = node1
        .get_named_ref("test2")
        .await
        .expect("actor should be discoverable across nodes");

    named_ref.send(SetNumMessage { num: 43 }).await.unwrap();

    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 43);

    // do it the other way too, have node2 get a ref to node1's actor
    let named_ref: NamedActorRef<TestActor> = node2
        .get_named_ref("test")
        .await
        .expect("actor should be discoverable across nodes");

    named_ref.send(SetNumMessage { num: 44 }).await.unwrap();

    let last_num = actor.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 44);

    shutdown(node1, node2, actor, actor2).await;
}

#[tokio::test]
async fn test_node_discovery() {
    let (node1, node2, _actor, _actor2) = setup().await;

    let registry3 = HandlerRegistryBuilder::default()
        .with_actor::<TestActor>()
        .with_handler::<TestActor, SetNumMessage>();

    let node3 = NucleusNode::new_root(
        2,
        "test",
        (),
        SocketAddr::from_str("127.0.0.1:8082").unwrap(),
        None,
        Some(node1.public_addr().to_string()),
        registry3,
        0u64,
        None,
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // node 2 should know about node 3
    let remote_nodes = node2.registry().get_remote_addresses();
    assert_eq!(remote_nodes.len(), 2);
    assert!(remote_nodes.iter().any(|r| r.node_id == 2));

    // node 3 should know about node 2
    let remote_nodes = node3.registry().get_remote_addresses();
    assert_eq!(remote_nodes.len(), 2);
    assert!(remote_nodes.iter().any(|r| r.node_id == 1));

    shutdown(node1, node2, _actor, _actor2).await;
    node3.shutdown().await;
}

#[tokio::test]
async fn test_pubsub() {
    let (_node1, _node2, actor, actor2) = setup().await;

    // have actor 2 sub to actor 1's numtopic
    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 68 }).await.unwrap();
    actor2.send(SetNumMessage { num: 68 }).await.unwrap();
    actor.send(PubIncrNumber {}).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // now get number from test2, should be 68
    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, 69);

    shutdown(_node1, _node2, actor, actor2).await;
}

#[tokio::test]
async fn test_reconnects() {
    let (node1, _node2, _actor, _actor2) = setup().await;

    let remote_node = node1
        .registry()
        .get_remote_node(1)
        .unwrap()
        .actor_ref()
        .clone();

    // task to send 10 msgs of increasing numbers, then get and verify the last number, sleep 0.1s between each
    let tnode1 = node1.clone();
    let task = async move {
        let actor2ref = tnode1.get_named_ref::<TestActor>("test2").await.unwrap();

        for i in 0..10 {
            actor2ref.send(SetNumMessage { num: i }).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let num = actor2ref.send(GetNumMessage).await.unwrap().num;
            assert_eq!(num, i);
        }
    };

    let handle = tokio::spawn(task);

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    remote_node.notify(DebugInterruptStream).unwrap();

    handle.await.unwrap();

    shutdown(node1, _node2, _actor, _actor2).await;
}

#[tokio::test]
async fn test_failed_resumes() {
    let (node1, node2, _actor, _actor2) = setup().await;

    let listen_on = node1.public_addr();
    let remote_node = node1
        .registry()
        .get_remote_node(1)
        .unwrap()
        .actor_ref()
        .clone();
    remote_node.notify(DebugInterruptStream).unwrap();
    node1.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // start node 3 with same id and listen addr
    let registry3 = HandlerRegistryBuilder::default()
        .with_actor::<TestActor>()
        .with_handler::<TestActor, SetNumMessage>();

    let node3 = NucleusNode::new_root(
        0,
        "test",
        (),
        listen_on.parse().unwrap(),
        None,
        None,
        registry3,
        0u64,
        None,
    )
    .await
    .unwrap();

    // sleep
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let r: NamedActorRef<TestActor> = node3.get_named_ref("test2").await.unwrap();
    r.send(SetNumMessage { num: 42 }).await.unwrap();
    let num = _actor2.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 42);

    // shutdown 3 and 2
    node3.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_node_graceful_shutdown() {
    // this is less of a test and more for me to see the logs if the other side of a remote node was closed cleanly, but thats hard to verify via tests
    let (node1, node2, _actor, _actor2) = setup().await;

    node1
        .get_named_ref::<TestActor>("test2")
        .await
        .unwrap()
        .send(SetNumMessage { num: 42 })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    node2.shutdown().await;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    node1.shutdown().await;
}

#[tokio::test]
async fn test_reconnect_replays() {
    let (node1, _node2, _actor, actor2) = setup().await;

    let remote_node = node1
        .registry()
        .get_remote_node(1)
        .unwrap()
        .actor_ref()
        .clone();

    // task to send 10 msgs of increasing numbers, then get and verify the last number, sleep 0.1s between each
    let actor2ref = node1.get_ref_by_id::<TestActor>(actor2.actor_id()).unwrap();
    actor2ref.notify(SetNumMessage { num: 0 }).await.unwrap();

    const NUM_MSGS: u64 = 50;

    let task = async move {
        let mut tasks = Vec::new();
        for i in 0..NUM_MSGS {
            let aref = actor2ref.clone();
            let task = tokio::spawn(async move {
                aref.notify(CheckedIncrMessage { expected_num: i })
                    .await
                    .unwrap();
            });
            tasks.push(task);

            debug!("checkedincr sent {}", i);

            // tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            // let num = actor2ref.send(GetNumMessage).await.unwrap().num;
            // assert_eq!(num, i);
        }

        for task in tasks {
            task.await.unwrap();
        }
    };

    let handle = tokio::spawn(task);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    remote_node.notify(DebugInterruptStream).unwrap();
    // actor2
    //     .notify(SleepMsg {
    //         duration_millis: 1000,
    //     })
    //     .unwrap();

    handle.await.unwrap();

    // the last num should be 9
    let last_num = actor2.send(GetNumMessage).await.unwrap().num;
    assert_eq!(last_num, NUM_MSGS);

    shutdown(node1, _node2, _actor, actor2).await;
}

#[tokio::test]
async fn test_shutdowns() {
    let (node1, _node2, actor, actor2) = setup().await;

    // send one setnum message
    actor2.send(SetNumMessage { num: 42 }).await.unwrap();

    shutdown(node1, _node2, actor, actor2).await;
}

#[tokio::test]
async fn test_pubsub_publisher_stop() {
    let (_node1, _node2, actor, actor2) = setup().await;

    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 68 }).await.unwrap();
    actor2.send(SetNumMessage { num: 68 }).await.unwrap();
    actor.send(PubIncrNumber {}).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let num = actor2.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 69);

    // this should send PublisherStopped on numtopic, so actor2 should reset to 0
    actor.notify_stop();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let num = actor2.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 0);

    shutdown(_node1, _node2, actor, actor2).await;
}

#[tokio::test]
async fn test_pubsub_remote_death() {
    let (node1, node2, actor, actor2) = setup().await;

    let remote_node = node1
        .registry()
        .get_remote_node(1)
        .unwrap()
        .actor_ref()
        .clone();

    actor2
        .send(DoSubMessage {
            actor_name: "test".into(),
        })
        .await
        .unwrap();

    actor.send(SetNumMessage { num: 68 }).await.unwrap();
    actor2.send(SetNumMessage { num: 68 }).await.unwrap();
    actor.send(PubIncrNumber {}).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let num = actor2.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 69);

    // --
    remote_node.notify(DebugInterruptStream).unwrap();

    // send msg from node2 to actor
    let actor_id = actor.actor_id();
    let actor_ref = node2.get_ref_by_id::<TestActor>(actor_id).unwrap();
    let sendres = tokio::spawn(async move { actor_ref.send(SetNumMessage { num: 99 }).await });

    let node1clone = node1.clone();
    tokio::spawn(async move { node1clone.shutdown().await });

    if sendres.await.unwrap().is_ok() {
        panic!("expected error")
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --

    let num = actor2.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 10000); // this is 10000 + node id of the dead node

    // shutdown(node1, node2, actor, actor2).await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_selfconnect_circuit_breaker() {
    // spawn one node, with upstream set to itself

    let port1 = (rand::random::<u16>() | 0x8000) - 1;
    let addr1 = format!("127.0.0.1:{}", port1);

    let node = NucleusNode::new_root(
        0,
        "test",
        (),
        SocketAddr::from_str(&addr1).unwrap(),
        None,
        Some(addr1.clone()),
        HandlerRegistryBuilder::default(),
        0u64,
        None,
    )
    .await
    .unwrap();

    // sleep 5s
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    assert_eq!(node.registry().get_remote_nodes().len(), 0);
}

#[tokio::test]
async fn test_weakrefs() {
    let (node1, node2, actor, _actor2) = setup().await;

    let weakref =
        WeakActorRef::new_unchecked(node1, actor.actor_id(), TestActor::info().actor_type);
    weakref.send(SetNumMessage { num: 67 }).await.unwrap();
    let num = actor.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 67);

    // test across node boundaries
    let weakref =
        WeakActorRef::new_unchecked(node2, actor.actor_id(), TestActor::info().actor_type);
    weakref.send(SetNumMessage { num: 88 }).await.unwrap();
    let num = actor.send(GetNumMessage).await.unwrap();
    assert_eq!(num.num, 88);
}
