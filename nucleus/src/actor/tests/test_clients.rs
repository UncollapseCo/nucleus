use bytes::Bytes;

use crate::{
    actor::{
        RemoteActor,
        message::Serializable,
        refs::ActorRefErr,
        tests::{GetNumMessage, SetNumMessage, TestActor, setup},
    },
    client::NucleusClient,
    net::proto,
};

#[tokio::test]
async fn test_client_basic() {
    let (node1, _node2, actor, _actor2) = setup().await;

    let mut client =
        NucleusClient::connect(node1.public_addr().to_owned(), "test_client".to_owned())
            .await
            .unwrap();

    client
        .submit_remote_message::<SetNumMessage>(
            SetNumMessage { num: 5 },
            proto::client::client_remote_message::Recipient::ActorName("test".to_owned()),
            Some(TestActor::info().actor_type.to_owned()),
        )
        .await
        .await
        .unwrap();

    let actual_num = actor.send(GetNumMessage {}).await.unwrap().num;

    assert_eq!(actual_num, 5);
}

// do the same for test2 on node2, to ensure that the client can talk to actors on different nodes
#[tokio::test]
async fn test_client_basic_remoted() {
    let (node1, _node2, _actor, actor2) = setup().await;

    let mut client =
        NucleusClient::connect(node1.public_addr().to_owned(), "test_client".to_owned())
            .await
            .unwrap();

    client
        .submit_remote_message::<SetNumMessage>(
            SetNumMessage { num: 6 },
            proto::client::client_remote_message::Recipient::ActorName("test2".to_owned()),
            Some(TestActor::info().actor_type.to_owned()),
        )
        .await
        .await
        .unwrap();

    let actual_num = actor2.send(GetNumMessage {}).await.unwrap().num;

    assert_eq!(actual_num, 6);
}

#[tokio::test]
async fn test_no_handler_err() {
    let (node1, _node2, _actor, _actor2) = setup().await;

    let mut client =
        NucleusClient::connect(node1.public_addr().to_owned(), "test_client".to_owned())
            .await
            .unwrap();

    let res: proto::client::ClientRemoteMessageResponse = client
        .submit_remote_message_raw(proto::client::ClientRemoteMessage {
            actor_type: Some(TestActor::info().actor_type.to_owned()),
            message_type: "Lalalala :3".to_owned(),
            response_mode: 0,
            payload: Bytes::new(),
            recipient: Some(proto::client::client_remote_message::Recipient::ActorName(
                "test".to_owned(),
            )),
        })
        .await
        .unwrap()
        .await;

    match res.response.unwrap() {
        proto::client::client_remote_message_response::Response::Error(err) => {
            let err = ActorRefErr::deserialize(err, None).unwrap();
            assert!(matches!(err, ActorRefErr::HandlerNotRegistered));
        }
        _ => panic!("Expected error response"),
    }
}
