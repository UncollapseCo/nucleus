use std::net::SocketAddr;

use crate::NucleusNode;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info};

pub struct NucleusNodeListener {
    nucleus: NucleusNode,
    listen_addr: SocketAddr,
    cancellation_token: CancellationToken,
}

impl NucleusNodeListener {
    pub async fn start(&self) -> Result<(), tokio::io::Error> {
        info!("Starting node listener on {}", self.listen_addr);

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        tokio::spawn(
            server_loop(
                listener,
                self.cancellation_token.clone(),
                self.nucleus.clone(),
            )
            .in_current_span(),
        );

        Ok(())
    }
}

async fn server_loop(
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    nucleus: NucleusNode,
) {
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                       debug!("remote conn accepted {}", addr,);
                       let nucleus = nucleus.clone();
                       let block = async move {nucleus.registry().new_remote_received(stream).await}.in_current_span();
                       tokio::spawn(block);
                    }
                    Err(e) => {
                        error!("<hazard> error accepting connection: {}", e);
                    }
                }
            }
        }
    }

    info!("node listener stopped");
}

impl NucleusNode {
    pub async fn start_listener(
        &self,
        listener_cancel: CancellationToken,
    ) -> Result<NucleusNodeListener, tokio::io::Error> {
        let listener = NucleusNodeListener {
            nucleus: self.clone(),
            listen_addr: self.inner.bind_addr,
            cancellation_token: listener_cancel,
        };

        listener.start().await?;

        Ok(listener)
    }
}
