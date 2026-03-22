use bytes::{Bytes, BytesMut};
use prost::Message as _;
use tokio::{
    io,
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::Instrument as _;

use crate::net::{
    errors::RemoteError,
    proto::{self, core::RemoteHandshakeResponse},
    remote::read_raw_message,
};

use super::{raw_handshake_receive, raw_handshake_write, write_raw_message};

pub(crate) struct NucleusClientInner {
    writer: OwnedWriteHalf,
    reader: OwnedReadHalf,
    cmd_submission_queue_reader: UnboundedReceiver<proto::client::ClientCommand>,
    response_queue_writer: UnboundedSender<proto::client::ClientResponseMessage>,
}

impl NucleusClientInner {
    pub(crate) async fn run(
        addr: String,
        name: String,
    ) -> Result<
        (
            RemoteHandshakeResponse,
            UnboundedSender<proto::client::ClientCommand>,
            UnboundedReceiver<proto::client::ClientResponseMessage>,
        ),
        RemoteError,
    > {
        let stream = TcpStream::connect(addr).await?;

        let (mut reader, mut writer) = stream.into_split();

        let handshake = proto::core::RemoteClientHandshake { name, seq: 0 };
        let msg = proto::core::handshake_message::Message::ClientHandshake(handshake);

        let msg = proto::core::HandshakeMessage { message: Some(msg) };
        let vec = msg.encode_to_vec();
        raw_handshake_write(&mut writer, vec).await?;

        let handshake = raw_handshake_receive(&mut reader).await?;
        let handshake = proto::core::RemoteHandshakeResponse::decode(handshake.as_slice()).unwrap();

        let (cmd_submission_queue_writer, cmd_submission_queue_reader) =
            tokio::sync::mpsc::unbounded_channel();

        let (response_queue_writer, response_queue_reader) = tokio::sync::mpsc::unbounded_channel();

        let client = NucleusClientInner {
            writer,
            reader,
            cmd_submission_queue_reader,
            response_queue_writer,
        };

        client.run_inner();

        Ok((
            handshake,
            cmd_submission_queue_writer,
            response_queue_reader,
        ))
    }

    fn run_inner(mut self) {
        tokio::spawn(
            async move {
                loop {
                    let mut read_buf = BytesMut::new();

                    select! {
                        cmd = self.cmd_submission_queue_reader.recv() => {
                            if let Some(cmd) = cmd {
                                self.handle_cmd_sub(cmd).await?;
                            } else {
                                break;
                            }
                        }
                        response = read_raw_message(&mut self.reader, &mut read_buf, || {}) => {
                            response?;
                            self.handle_recv(read_buf.freeze()).await?;
                        }
                    }
                }

                Ok::<(), RemoteError>(())
            }
            .in_current_span(),
        );
    }

    async fn handle_cmd_sub(
        &mut self,
        cmd: proto::client::ClientCommand,
    ) -> Result<(), RemoteError> {
        let encoded = cmd.encode_to_vec().into();
        write_raw_message(&mut self.writer, encoded).await?;

        Ok(())
    }

    async fn handle_recv(&mut self, response: Bytes) -> Result<(), RemoteError> {
        let msg = proto::client::ClientResponseMessage::decode(response)
            .map_err(|e| RemoteError::ProtocolError(e.to_string()))?;

        self.response_queue_writer.send(msg).map_err(|_| {
            RemoteError::IOError(io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "response channel closed",
            ))
        })?;

        Ok(())
    }
}
