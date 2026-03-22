use crate::{actor::message::Message, net::proto};

impl Message for proto::debug::GetSerialized {
    type Result = Vec<u8>;
    const RESPECT_BACKPRESSURE: bool = false;

    fn name() -> &'static str {
        "Debug.GetSerialized"
    }
}

impl Message for proto::debug::ListActors {
    type Result = proto::debug::ActorList;

    fn name() -> &'static str {
        "Debug.ListActors"
    }
}
