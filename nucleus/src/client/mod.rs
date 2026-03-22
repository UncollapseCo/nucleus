use std::{future::Future, sync::Arc};

use dashmap::DashMap;
use fxhash::FxBuildHasher;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::Instrument;

use crate::{actor::message::Message, net::remote::client::NucleusClientInner};
use crate::{
    actor::message::Serializable,
    net::{errors::RemoteError, proto},
};
use crate::{
    actor::{
        message::Handler,
        refs::{ActorRefErr, SendError},
    },
    net::registry::actor::RemoteRegistryActor,
};
use tokio::sync::mpsc::error::SendError as MpscSendError;

pub struct NucleusClient {
    upstream_addr: String,
    upstream_name: String,
    handshake: proto::core::RemoteHandshakeResponse,
    cmd_sender: UnboundedSender<proto::client::ClientCommand>,

    response_waiters: Arc<
        DashMap<u64, oneshot::Sender<proto::client::ClientRemoteMessageResponse>, FxBuildHasher>,
    >,

    cmd_seq: u64,
}

impl NucleusClient {
    pub async fn connect(addr: String, name: String) -> Result<Self, RemoteError> {
        let (handshake, sender, reader) =
            NucleusClientInner::run(addr.clone(), name.clone()).await?;

        let s = Self {
            upstream_addr: addr,
            upstream_name: name,
            handshake,
            cmd_sender: sender,
            response_waiters: Arc::new(DashMap::default()),
            cmd_seq: 1,
        };

        s.fulfillment_task(reader);

        Ok(s)
    }

    pub async fn submit_remote_message<M: Message + Serializable>(
        &mut self,
        msg: M,
        recipient: proto::client::client_remote_message::Recipient,
        actor_type: Option<String>,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>> + use<M>
    where
        M::Result: Serializable,
        // TODO: consider adding a trait bound for A: Handler<M>
        // not sure though it would cause any clients requiring to compile in the actual handlers
        // which is a lot of useless code for potentially a small client program
        // rn it should simply return a HandlerNotRegistered error if the user got it wrong
    {
        let remote_msg = proto::client::ClientRemoteMessage {
            actor_type,
            message_type: M::name().to_owned(),
            payload: msg.serialize(),
            recipient: Some(recipient),
            response_mode: 0,
        };

        let submitted = self.submit_remote_message_raw(remote_msg).await;

        async move {
            match submitted {
                Ok(res) => match res.await.response {
                    Some(proto::client::client_remote_message_response::Response::Payload(p)) => {
                        Ok(M::Result::deserialize(p, None).unwrap())
                    }
                    Some(proto::client::client_remote_message_response::Response::Error(e)) => Err(
                        SendError::new(ActorRefErr::deserialize(e, None).unwrap(), msg),
                    ),
                    Some(proto::client::client_remote_message_response::Response::NotifyAck(_)) => {
                        panic!("expected response payload, got NotifyAck")
                    }
                    None => {
                        panic!("expected response payload, got None")
                    }
                },

                Err(_) => Err(SendError::new(ActorRefErr::ResultChannelClosed, msg)),
            }
        }
        .in_current_span()
    }

    pub async fn submit_remote_registry_message<M: Message + Serializable>(
        &mut self,
        msg: M,
    ) -> impl Future<Output = Result<M::Result, SendError<M>>> + use<M>
    where
        M::Result: Serializable,
        RemoteRegistryActor: Handler<M>,
        // TODO: consider adding a trait bound for A: Handler<M>
        // not sure though it would cause any clients requiring to compile in the actual handlers
        // which is a lot of useless code for potentially a small client program
        // rn it should simply return a HandlerNotRegistered error if the user got it wrong
    {
        let remote_msg = proto::client::ClientRemoteMessage {
            actor_type: Some("RemoteRegistry".to_owned()),
            message_type: M::name().to_owned(),
            payload: msg.serialize(),
            recipient: Some(proto::client::client_remote_message::Recipient::Registry(0)),
            response_mode: 0,
        };

        let submit_fut = self.submit_remote_message_raw(remote_msg).await;

        async move {
            match submit_fut {
                Ok(res) => match res.await.response {
                    Some(proto::client::client_remote_message_response::Response::Payload(p)) => {
                        Ok(M::Result::deserialize(p, None).unwrap())
                    }
                    Some(proto::client::client_remote_message_response::Response::Error(e)) => Err(
                        SendError::new(ActorRefErr::deserialize(e, None).unwrap(), msg),
                    ),
                    Some(proto::client::client_remote_message_response::Response::NotifyAck(_)) => {
                        panic!("expected response payload, got NotifyAck")
                    }
                    None => {
                        panic!("expected response payload, got None")
                    }
                },

                Err(_) => Err(SendError::new(ActorRefErr::ResultChannelClosed, msg)),
            }
        }
        .in_current_span()
    }

    pub async fn submit_remote_message_raw(
        &mut self,
        msg: proto::client::ClientRemoteMessage,
    ) -> Result<
        impl Future<Output = proto::client::ClientRemoteMessageResponse> + use<>,
        MpscSendError<proto::client::ClientCommand>,
    > {
        self.submit_command(proto::client::client_command::Inner::RemoteMessage(msg))
            .await
    }

    pub async fn discover_named_actor(
        &mut self,
        actor_name: String,
    ) -> Result<proto::remote::NamedActorDiscoveryResponse, ActorRefErr> {
        match self
            .submit_command(proto::client::client_command::Inner::NamedDiscoveryRequest(
                proto::remote::NamedActorDiscoveryRequest {
                    actor_name: actor_name.into(),
                    actor_type: None,
                },
            ))
            .await
        {
            Ok(res) => match res.await.response {
                Some(proto::client::client_remote_message_response::Response::Payload(p)) => {
                    Ok(proto::remote::NamedActorDiscoveryResponse::deserialize(p, None).unwrap())
                }
                Some(proto::client::client_remote_message_response::Response::Error(e)) => {
                    Err(ActorRefErr::deserialize(e, None).unwrap())
                }
                Some(proto::client::client_remote_message_response::Response::NotifyAck(_)) => {
                    panic!("expected response payload, got NotifyAck")
                }
                None => {
                    panic!("expected response payload, got None")
                }
            },

            Err(_) => Err(ActorRefErr::ResultChannelClosed),
        }
    }

    async fn submit_command(
        &mut self,
        mut command: proto::client::client_command::Inner,
    ) -> Result<
        impl Future<Output = proto::client::ClientRemoteMessageResponse> + use<>,
        MpscSendError<proto::client::ClientCommand>,
    > {
        self.cmd_seq += 1;
        let seq = self.cmd_seq;

        let rx = loop {
            let (tx, rx) = oneshot::channel();
            self.response_waiters.insert(seq, tx);

            if let Err(e) = self.cmd_sender.send(proto::client::ClientCommand {
                command_id: seq,
                inner: Some(command),
            }) {
                command = e.0.inner.as_ref().unwrap().clone();
                if let Err(rec_err) = self.reconnect().await {
                    warn!("Failed to reconnect: {}", rec_err);
                    return Err(e);
                }
                continue;
            }

            break rx;
        };

        Ok(async move {
            match rx.await {
                Ok(r) => r,
                Err(_) => crate::net::proto::client::ClientRemoteMessageResponse {
                    response: Some(
                        proto::client::client_remote_message_response::Response::Error(
                            ActorRefErr::ResultChannelClosed.serialize(),
                        ),
                    ),
                },
            }
        })
    }

    async fn reconnect(&mut self) -> Result<(), &'static str> {
        const MAX_RETRIES: usize = 5;
        for i in 0..MAX_RETRIES {
            match NucleusClientInner::run(self.upstream_addr.clone(), self.upstream_name.clone())
                .await
            {
                Ok(connected) => {
                    let (handshake, sender, reader) = connected;
                    self.handshake = handshake;
                    self.cmd_sender = sender;

                    self.fulfillment_task(reader);

                    return Ok(());
                }
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_secs(i as u64 + 1)).await;
                }
            }
        }

        Err("Failed to reconnect after maximum retries")
    }

    fn fulfillment_task(
        &self,
        mut reader: UnboundedReceiver<proto::client::ClientResponseMessage>,
    ) {
        let waiters = self.response_waiters.clone();

        tokio::spawn(
            async move {
                while let Some(response) = reader.recv().await {
                    if response.message.is_none() {
                        continue;
                    }

                    match response.message.unwrap() {
                        proto::client::client_response_message::Message::CommandResponse(
                            response,
                        ) => {
                            if let Some((_, sender)) = waiters.remove(&response.command_id) {
                                if let Some(r) = response.response {
                                    let _ = sender.send(r);
                                }
                            }
                        }
                    }
                }

                // loop ended, we will not receive any more responses, clear the map to drop all senders
                waiters.clear();
            }
            .in_current_span(),
        );
    }

    pub fn handshake(&self) -> &proto::core::RemoteHandshakeResponse {
        &self.handshake
    }
}
