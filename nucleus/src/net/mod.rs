use bytes::Bytes;
use proto::core::{RemoteHandshake, RemoteHandshakeResponse};

use crate::actor::{message::Serializable, refs::ActorRefErr};

pub mod errors;
pub mod listener;
pub(crate) mod mover;
pub mod registry;
pub mod remote;

pub use mover::SubRefMap;

pub mod proto {
    use crate::actor::message::Message as ActorMessage;

    use core::*;

    use pubsub::*;
    use remote::*;

    pub(crate) mod core {
        use crate::ActorMarker;
        use crate::RequestMarker;

        include_proto!("nucleus.core.rs");

        pub(crate) fn make_wire_msg(
            seq: u64,
            seq_ack: u64,
            message: Option<wire_message::Message>,
        ) -> WireMessage {
            let mut msg = WireMessage {
                seq,
                seq_ack,
                trace_parent: None,
                message,
            };

            #[cfg(feature = "metrics")]
            {
                use tracing::Span;
                use tracing_opentelemetry::OpenTelemetrySpanExt;

                use crate::metrics::TraceContextInjector;

                let mut injector = TraceContextInjector::new(&mut msg.trace_parent);

                opentelemetry::global::get_text_map_propagator(|propagator| {
                    let context = Span::current().context();
                    propagator.inject_context(&context, &mut injector)
                });
            }

            msg
        }
    }

    pub(crate) mod remote {
        use crate::ActorMarker;
        include_proto!("nucleus.remote.rs");
    }

    pub(crate) mod pubsub {
        use crate::ActorMarker;
        include_proto!("nucleus.pubsub.rs");
    }

    pub(crate) mod mover {
        use crate::ActorMarker;
        include_proto!("nucleus.mover.rs");
    }

    pub mod client {
        include_proto!("nucleus.client.rs");
    }

    #[cfg(feature = "debug")]
    pub mod debug {
        pub use super::remote::NamedActorDiscoveryRequest;
        use crate::ActorMarker;
        pub use crate::actor::message::system::ActorStopMessage;
        pub use crate::actor::message::system::PingMsg;
        include_proto!("nucleus.debug.rs");
    }

    macro_rules! impl_sysactor_message {
        ($name:ident -> $result:ty) => {
            impl ActorMessage for $name {
                type Result = $result;

                fn name() -> &'static str {
                    concat!("System.", stringify!($name))
                }
            }
        };
    }

    // core
    impl_sysactor_message!(RemoteMessage -> remote_response::Response);
    impl_sysactor_message!(RemoteHandshake -> RemoteHandshakeResponse);
    impl_sysactor_message!(RemoteHandshakeResponse -> ());
    impl_sysactor_message!(RemoteResume -> ());
    impl_sysactor_message!(RemoteShutdown -> ());

    // remote
    impl_sysactor_message!(NamedActorDiscoveryRequest -> NamedActorDiscoveryResponse);
    impl_sysactor_message!(RemoteNodeRegister -> ());
    impl_sysactor_message!(RemoteNodeUnregister -> ());
    impl_sysactor_message!(RemoteNodeHeartbeat -> ());

    // pubsub
    impl_sysactor_message!(PubsubMessage -> u64);
    impl_sysactor_message!(PubsubSubscribe -> bool);
    impl_sysactor_message!(PubsubUnsubscribe -> bool);
    impl_sysactor_message!(PublisherMoved -> bool);
}

pub(crate) fn convert_response(
    response: proto::core::remote_response::Response,
) -> Result<Option<Bytes>, ActorRefErr> {
    match response {
        // only passed around internally, should never be none (but protos can be weird)
        proto::core::remote_response::Response::Payload(ok) => Ok(Some(ok)),
        proto::core::remote_response::Response::NotifyAck(_) => Ok(None),
        proto::core::remote_response::Response::Error(err) => {
            Err(ActorRefErr::deserialize(err, None).unwrap())
        }
    }
}
// pub(crate) fn unconvert_response(
//     response: Result<Option<Vec<u8>>, ActorRefErr>,
// ) -> Option<proto::core::remote_response::Response> {
//     Some(match response {
//         Ok(Some(ok)) => proto::core::remote_response::Response::Payload(ok),
//         Ok(None) => proto::core::remote_response::Response::NotifyAck(true),
//         Err(err) => proto::core::remote_response::Response::Error(err.serialize()),
//     })
// }

impl From<RemoteHandshakeResponse> for RemoteHandshake {
    fn from(response: RemoteHandshakeResponse) -> Self {
        RemoteHandshake {
            node_id: response.node_id,
            instance_id: response.instance_id,
            seq: response.seq,
            nodes: response.nodes,
            listen_address: response.listen_address,
            node_flags: response.node_flags,
            cluster: response.cluster,
        }
    }
}
