use bytes::BytesMut;
use prost::Message as _;
use rand::RngCore;
use snowflake::Snowflake;
use tokio::io;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::Instrument as _;

use crate::ActorMarker;
use crate::actor::delay::delayed;
use crate::actor::message::{Message, Serializable};
use crate::actor::refs::{ActorRefErr, remote_send};
use crate::errors::ActorProcessingErr;
use crate::net::remote::ResponseMode;
use crate::{
    NucleusNode,
    actor::{Actor, ActorContext, Sender},
    net::proto,
};

use super::{raw_handshake_write, read_raw_message, write_raw_message};

pub(crate) struct ClientHandler {
    nucleus: NucleusNode,
    client_id: Snowflake<ActorMarker>,
    _handshake: proto::core::RemoteClientHandshake,
    writer: OwnedWriteHalf,
    reader: Option<OwnedReadHalf>,
    _seq: u64,
}

impl ClientHandler {
    pub(crate) async fn receive_handshake(
        nucleus: NucleusNode,
        handshake: proto::core::RemoteClientHandshake,
        stream: TcpStream,
    ) -> Result<Self, io::Error> {
        let seq = rand::thread_rng().next_u64() & 0x000FFFFFFFFFFFFF;

        let response = proto::core::RemoteHandshakeResponse {
            node_id: nucleus.node_id() as u32,
            instance_id: nucleus.instance_id(),
            nodes: nucleus.registry().get_remote_addresses(),
            seq,
            listen_address: nucleus.public_addr().to_string(),
            node_flags: nucleus.node_flags(),
            cluster: nucleus.cluster().into(),
        };

        let buf = proto::core::RemoteHandshakeResponse::encode_to_vec(&response);
        let (rx, mut tx) = stream.into_split();
        raw_handshake_write(&mut tx, buf).await?;

        let cid = nucleus.snowflake().generate();
        Ok(Self {
            nucleus,
            client_id: cid,
            _handshake: handshake,
            writer: tx,
            reader: Some(rx),
            _seq: seq,
        })
    }

    pub(crate) fn client_id(&self) -> Snowflake<ActorMarker> {
        self.client_id
    }

    fn read_task(&mut self, ctx: &mut ActorContext<Self>) {
        // read from stream
        let mut rx = self.reader.take().unwrap();
        let self_ref = ctx.get_ref();
        let nucleus = self.nucleus.clone();

        tokio::spawn(async move {
            let mut msg_buffer = BytesMut::new();

            loop {
                if read_raw_message(&mut rx, &mut msg_buffer, || {})
                    .await
                    .is_err()
                {
                    warn!("failed to read message from client, closing connection");
                    break;
                }

                let msg = match proto::client::ClientCommand::decode(&mut msg_buffer) {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("failed to decode message from client: {}", e);
                        break;
                    }
                };

                if msg.inner.is_none() {
                    continue;
                }

                let self_ref = self_ref.clone();
                let nucleus = nucleus.clone();
                tokio::spawn(async move {
                    let command_id = msg.command_id;
                    let inner = msg.inner.unwrap();

                    match inner {
                        proto::client::client_command::Inner::RemoteMessage(msg) => {
                            let r = self_ref.send(msg).await.unwrap();

                            let wire_reply = proto::client::ClientResponseMessage {
                                                    message: Some(proto::client::client_response_message::Message::CommandResponse(
                                                        proto::client::ClientCommandResponse {
                                                            command_id,
                                                            response: Some(r)
                                                        }
                                                    )),
                                                };

                            let _ = self_ref.notify(wire_reply);
                        }
                        proto::client::client_command::Inner::NamedDiscoveryRequest(request) => {
                            let r = nucleus
                                .registry()
                                .discover_named_actor(
                                    request.actor_name.get_counted(),
                                    request.actor_type,
                                    None,
                                )
                                .await;

                            let response = match r {
                                Some((id, _type)) => proto::remote::NamedActorDiscoveryResponse {
                                    actor_id: Some(id),
                                    actor_type: _type.map(|v| v.into()),
                                },
                                None => proto::remote::NamedActorDiscoveryResponse {
                                    actor_id: None,
                                    actor_type: None,
                                },
                            };

                            let wire_reply = proto::client::ClientResponseMessage {
                                                    message: Some(proto::client::client_response_message::Message::CommandResponse(
                                                        proto::client::ClientCommandResponse {
                                                            command_id,
                                                            response: Some(proto::client::ClientRemoteMessageResponse{
                                                                response: Some(proto::client::client_remote_message_response::Response::Payload(
                                                                    response.serialize()
                                                                ))
                                                            })
                                                        }
                                                    )),
                                                };

                            let _ = self_ref.notify(wire_reply);
                        }
                    }
                }.in_current_span());
            }

            info!("client connection closed");
            self_ref.notify_stop();
        }.in_current_span());
    }
}

#[async_trait_nobox]
impl Actor for ClientHandler {
    async fn init(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorProcessingErr> {
        // start read task
        self.read_task(ctx);

        Ok(())
    }
}

impl Message for proto::client::ClientRemoteMessage {
    type Result = proto::client::ClientRemoteMessageResponse;
    fn name() -> &'static str {
        "Client.RemoteMessage"
    }
}

fn send_err(sender: Sender<proto::client::ClientRemoteMessageResponse>, err: ActorRefErr) {
    let _ = sender.send(proto::client::ClientRemoteMessageResponse {
        response: Some(
            proto::client::client_remote_message_response::Response::Error(err.serialize()),
        ),
    });
}

impl Message for proto::client::ClientResponseMessage {
    type Result = ();
    fn name() -> &'static str {
        "Client.ResponseMessage"
    }
}

#[handler(manual)]
impl ClientHandler {
    #[handler(delayed)]
    async fn handle_proto_client_client_remote_message(
        &mut self,
        message: proto::client::ClientRemoteMessage,
        ctx: &mut ActorContext<Self>,
        sender: Sender<<proto::client::ClientRemoteMessage as Message>::Result>,
    ) {
        if message.recipient.is_none() {
            return send_err(sender, ActorRefErr::InvalidRef);
        }

        let mut determined_actor_type: Option<String> = message.actor_type.clone();

        let actor_id = match message.recipient.unwrap() {
            proto::client::client_remote_message::Recipient::ActorId(aid) => {
                Snowflake::new(aid).unwrap()
            }
            proto::client::client_remote_message::Recipient::ActorName(name) => {
                let res = self
                    .nucleus
                    .registry()
                    .discover_named_actor(name.into(), message.actor_type.map(|v| v.into()), None)
                    .await;

                if res.is_none() {
                    return send_err(sender, ActorRefErr::UnknownNode);
                }

                let (actor_id, atype) = res.unwrap();

                if let Some(ret_type) = atype {
                    if determined_actor_type.is_none() {
                        determined_actor_type = Some(ret_type.to_owned());
                    }
                }

                actor_id
            }
            proto::client::client_remote_message::Recipient::Registry(_node_id) => {
                determined_actor_type = Some("RemoteRegistry".to_owned());
                ctx.nucleus().registry().aref().actor_id()
            }
        };

        let (tx, rx) = {
            if message.response_mode == 0 || message.response_mode == 1 {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (Some(tx), Some(rx))
            } else {
                (None, None)
            }
        };

        if determined_actor_type.is_none() {
            return send_err(sender, ActorRefErr::HandlerNotRegistered);
        }
        let determined_actor_type = determined_actor_type.unwrap();

        if actor_id.worker_id() == self.nucleus.node_id() {
            let handler = self.nucleus.registry().get_handler(&(
                determined_actor_type.as_str(),
                message.message_type.as_str(),
            ));

            if handler.is_none() {
                send_err(sender, ActorRefErr::HandlerNotRegistered);
                return;
            }
            let handler = handler.unwrap();

            trace!(
                "remote node received message for actor {} message {}",
                determined_actor_type, message.message_type
            );

            let mut local_tx = None;

            if let Some(tx_inner) = tx {
                let (tx2, rx2) = tokio::sync::oneshot::channel();
                local_tx = Some(tx2);
                tokio::spawn(
                    async move {
                        let r = rx2.await;
                        match r {
                            Ok(pay) => {
                                tx_inner.send(proto::core::remote_response::Response::Payload(pay))
                            }
                            Err(_) => tx_inner.send(proto::core::remote_response::Response::Error(
                                ActorRefErr::ActorPanicked.serialize(),
                            )),
                        }
                    }
                    .in_current_span(),
                );
            }

            if let Err(e) = handler.handle(
                &self.nucleus,
                message.payload,
                actor_id,
                local_tx.map(|tx| tx.into()),
                false,
            ) {
                return send_err(sender, e);
            } else if rx.is_none() && ResponseMode::from(message.response_mode).is_ack_only() {
                // rx.is_none() is same as is_notify
                let _ = sender.send(proto::client::ClientRemoteMessageResponse {
                    response: Some(
                        proto::client::client_remote_message_response::Response::NotifyAck(true),
                    ),
                });
                return;
            }
        } else {
            // send to remote node raw
            if let Err(e) = remote_send(
                &self.nucleus,
                tx.map(|t| t.into()),
                message.response_mode == 1,
                message.message_type.into(),
                message.payload,
                determined_actor_type.into(),
                actor_id,
                false,
            ) {
                return send_err(sender, e);
            }
        };

        if ResponseMode::from(message.response_mode).is_payload() {
            delayed(sender, async move {
                let response = match rx.unwrap().await {
                    Ok(res) => match res {
                        proto::core::remote_response::Response::Payload(payload) => {
                            proto::client::client_remote_message_response::Response::Payload(
                                payload,
                            )
                        }
                        proto::core::remote_response::Response::Error(e) => {
                            proto::client::client_remote_message_response::Response::Error(e)
                        }
                        proto::core::remote_response::Response::NotifyAck(_) => {
                            proto::client::client_remote_message_response::Response::NotifyAck(
                                false,
                            )
                        }
                    },
                    Err(_) => {
                        error!("failed to receive response, remote responding with panicked error");
                        proto::client::client_remote_message_response::Response::Error(
                            ActorRefErr::ActorPanicked.serialize(),
                        )
                    }
                };

                proto::client::ClientRemoteMessageResponse {
                    response: Some(response),
                }
            });
        } else {
            // not sure how to hit this branch but anyway
            let _ = sender.send(proto::client::ClientRemoteMessageResponse {
                response: Some(
                    proto::client::client_remote_message_response::Response::NotifyAck(false),
                ),
            });
        }
    }

    async fn handle_proto_client_client_response_message(
        &mut self,
        message: proto::client::ClientResponseMessage,
        ctx: &mut ActorContext<Self>,
    ) {
        if write_raw_message(&mut self.writer, message.encode_to_vec().into())
            .await
            .is_err()
        {
            ctx.stop();
        }
    }
}
