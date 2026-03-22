use std::sync::Arc;

use bytes::{Buf, Bytes};
use snowflake::Snowflake;

use crate::actor::guards::RatelimitStore;
use crate::actor::message::{DeserializationError, Handler, Message, Serializable};
use crate::actor::refs::ActorRef;
use crate::actor::{Actor, ActorContext, RemoteActor, RemoteActorInfo};
use crate::errors::ActorProcessingErr;
use crate::pubsub::{self, PubsubMessage, PubsubRef, Topic};
use crate::{ActorMarker, NucleusNode, handle_self};

pub(super) struct TestActor {
    pub(super) last_num: u64,
    pub(super) rl_store: RatelimitStore,
    pub(super) sub_ref: Option<PubsubRef<Numtopic>>,
}

#[async_trait_nobox]
impl Actor for TestActor {
    const MAX_MAILBOX_SIZE: usize = usize::MAX;

    async fn init(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

impl RemoteActor for TestActor {
    fn info() -> &'static RemoteActorInfo {
        &RemoteActorInfo {
            actor_type: "TestActor",
        }
    }
}

pub(super) struct Numtopic;
impl Topic for Numtopic {
    type M = CheckedIncrMessage;

    type Publisher = TestActor;

    fn name() -> &'static str {
        "Numtopic"
    }
}

pub(super) struct SetNumMessage {
    pub(super) num: u64,
}

impl Message for SetNumMessage {
    type Result = ();
    fn name() -> &'static str {
        "SetNumMessage"
    }
}
impl Serializable for SetNumMessage {
    fn serialize(&self) -> Bytes {
        self.num.to_ne_bytes().to_vec().into()
    }

    fn serialize_into<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        buf.put_slice(&self.num.to_ne_bytes());
    }

    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        let n = u64::from_ne_bytes(data.chunk()[0..8].try_into().map_err(Box::new)?);

        Ok(SetNumMessage { num: n })
    }
}

pub(super) struct GetNumMessage;

impl Message for GetNumMessage {
    type Result = SetNumMessage;
    fn name() -> &'static str {
        "GetNumMessage"
    }
}
impl Serializable for GetNumMessage {
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
        Ok(GetNumMessage)
    }
}

pub(super) struct CheckedIncrMessage {
    pub(super) expected_num: u64,
}
impl Message for CheckedIncrMessage {
    type Result = ();
    fn name() -> &'static str {
        "CheckedIncrMessage"
    }
}
impl Serializable for CheckedIncrMessage {
    fn serialize(&self) -> Bytes {
        self.expected_num.to_be_bytes().to_vec().into()
    }

    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        let n = u64::from_be_bytes(data.chunk()[0..8].try_into().map_err(Box::new)?);

        Ok(Self { expected_num: n })
    }
}

pub(super) struct SendRequestMessage {
    pub(super) actor_id: Snowflake<ActorMarker>,
}

impl Message for SendRequestMessage {
    type Result = ();
    fn name() -> &'static str {
        "SendRequestMessage"
    }
}
impl Serializable for SendRequestMessage {
    fn serialize(&self) -> Bytes {
        self.actor_id.id().to_be_bytes().to_vec().into()
    }

    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        let id = Snowflake::<ActorMarker>::try_from(u64::from_be_bytes(
            data.chunk()[0..8].try_into().map_err(Box::new)?,
        ))
        .unwrap();

        Ok(Self { actor_id: id })
    }
}

pub(super) struct DoSubMessage {
    pub(super) actor_name: Arc<str>,
}
impl Message for DoSubMessage {
    type Result = ();
    fn name() -> &'static str {
        "DoSubMessage"
    }
}

pub(super) struct PubIncrNumber {}
impl Message for PubIncrNumber {
    type Result = ();
    fn name() -> &'static str {
        "PubYourNumberMessage"
    }
}
impl Serializable for PubIncrNumber {
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
        Ok(PubIncrNumber {})
    }
}

pub(super) struct SleepMsg {
    duration_millis: u64,
}
impl Message for SleepMsg {
    type Result = ();
    fn name() -> &'static str {
        "SleepMsg"
    }
}

#[handler(manual)]
impl TestActor {
    async fn handle_set_num_message(
        &mut self,
        message: SetNumMessage,
        _ctx: &mut ActorContext<Self>,
    ) {
        info!("Setting to number: {}", message.num);
        self.last_num = message.num;
    }

    async fn handle_send_request_message(
        &mut self,
        message: SendRequestMessage,
        ctx: &mut ActorContext<Self>,
    ) {
        let actor_ref: ActorRef<TestActor> = ctx.nucleus.get_ref_by_id(message.actor_id).unwrap();
        actor_ref.send(SetNumMessage { num: 42 }).await.unwrap();
    }

    #[handler(local)]
    async fn handle_get_num_message(
        &mut self,
        _message: GetNumMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> SetNumMessage {
        SetNumMessage { num: self.last_num }
    }

    async fn handle_checked_incr_message(
        &mut self,
        message: CheckedIncrMessage,
        _ctx: &mut ActorContext<Self>,
    ) {
        assert_eq!(self.last_num, message.expected_num);
        self.last_num += 1;
    }

    async fn handle_do_sub_message(&mut self, message: DoSubMessage, ctx: &mut ActorContext<Self>) {
        // leak the subscription so it doesn't get dropped and unsubbed
        info!("Subscribing to {}", message.actor_name);
        self.sub_ref = Some(ctx.pubsub().subscribe(message.actor_name).await.unwrap());
    }

    async fn handle_pub_incr_number(
        &mut self,
        _message: PubIncrNumber,
        ctx: &mut ActorContext<Self>,
    ) {
        info!("Publishing my number: {}", self.last_num);
        self.last_num += 1;
        ctx.pubsub().publish::<Numtopic>(CheckedIncrMessage {
            expected_num: self.last_num - 1,
        });
    }

    async fn handle_sleep_msg(&mut self, message: SleepMsg, _ctx: &mut ActorContext<Self>) {
        tokio::time::sleep(std::time::Duration::from_millis(message.duration_millis)).await;
    }

    async fn handle_pubsub_message_numtopic(
        &mut self,
        message: PubsubMessage<Numtopic>,
        _ctx: &mut ActorContext<Self>,
    ) {
        match message.payload {
            pubsub::PubsubPayload::Payload(p) => {
                info!("Received pubsub message: {}", p.expected_num);
                handle_self!(
                    self,
                    _ctx,
                    CheckedIncrMessage {
                        expected_num: p.expected_num
                    }
                );
            }
            pubsub::PubsubPayload::PublisherStopped(_) => {
                info!("Publisher stopped");
                self.last_num = 0;
            }
            pubsub::PubsubPayload::RemoteNodeDied(n) => {
                info!("Remote node died: {}", n);
                self.last_num = (n as u64) + 10000;
            }
        }
    }
}
