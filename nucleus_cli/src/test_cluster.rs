use std::{net::SocketAddr, str::FromStr};

use bytes::{Buf, Bytes};
use nucleus::{
    NucleusNode,
    actor::{
        Actor, ActorContext, RemoteActor,
        message::{DeserializationError, Handler, Message, Serializable},
        moving::MovableActor,
    },
    async_trait_nobox,
    net::{SubRefMap, remote::HandlerRegistryBuilder},
};

pub struct CliActor {
    num: u64,
}
impl Actor for CliActor {}
impl RemoteActor for CliActor {
    fn info() -> &'static nucleus::actor::RemoteActorInfo {
        &nucleus::actor::RemoteActorInfo {
            actor_type: "CliActor",
        }
    }
}

impl MovableActor for CliActor {
    fn serialize(&self) -> Bytes {
        self.num.to_le_bytes().to_vec().into()
    }

    fn deserialize(data: Bytes, _nucleus: &NucleusNode, _: SubRefMap) -> Self {
        let num = u64::from_le_bytes(data.as_ref().try_into().unwrap());
        Self { num }
    }
}

pub struct SetNumber {
    pub num: u64,
}
impl Message for SetNumber {
    type Result = u64;
    fn name() -> &'static str {
        "SetNumber"
    }
}
impl Serializable for SetNumber {
    fn serialize(&self) -> Bytes {
        self.num.to_le_bytes().to_vec().into()
    }

    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(Self {
            num: u64::from_le_bytes(data.chunk().try_into().unwrap()),
        })
    }
}

#[async_trait_nobox]
impl Handler<SetNumber> for CliActor {
    async fn handle(&mut self, message: SetNumber, _ctx: &mut ActorContext<Self>) -> u64 {
        self.num = message.num;
        self.num
    }
}

pub struct GetNumber;
impl Message for GetNumber {
    type Result = u64;
    fn name() -> &'static str {
        "GetNumber"
    }
}
impl Serializable for GetNumber {
    fn serialize(&self) -> Bytes {
        Bytes::new()
    }

    fn deserialize<B>(
        _data: B,
        _nucleus: Option<&NucleusNode>,
    ) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        Ok(GetNumber)
    }
}
#[async_trait_nobox]
impl Handler<GetNumber> for CliActor {
    async fn handle(&mut self, _message: GetNumber, _ctx: &mut ActorContext<Self>) -> u64 {
        self.num
    }
}

pub async fn start_cluster(do_subsriber: bool) -> (NucleusNode, NucleusNode) {
    if do_subsriber {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    let r = HandlerRegistryBuilder::default()
        .with_debuggable_actor::<CliActor>()
        .with_handler::<CliActor, SetNumber>()
        .with_handler::<CliActor, GetNumber>();

    let r2 = HandlerRegistryBuilder::default()
        .with_debuggable_actor::<CliActor>()
        .with_handler::<CliActor, SetNumber>()
        .with_handler::<CliActor, GetNumber>();

    let node = NucleusNode::builder(0, "nucleus-cli-test")
        .with_bind_address(SocketAddr::from_str("127.0.0.1:8080").unwrap())
        .with_public_address("127.0.0.1:8080".to_owned())
        .with_registry(r)
        .build()
        .await
        .unwrap();

    let node2 = NucleusNode::builder(1, "nucleus-cli-test")
        .with_bind_address(SocketAddr::from_str("127.0.0.1:8081").unwrap())
        .with_public_address("127.0.0.1:8081".to_owned())
        .with_upstream_address("127.0.0.1:8080".to_owned())
        .with_registry(r2)
        .build()
        .await
        .unwrap();

    node.spawn_actor("cli1", CliActor { num: 0 }).await.unwrap();
    node2
        .spawn_actor("cli2", CliActor { num: 0 })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Test cluster started");
    (node, node2)
}
