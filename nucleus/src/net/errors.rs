#[derive(Debug)]
pub enum RemoteError {
    IOError(std::io::Error),
    ProtocolError(String),
    Resumed,
    ClientHandshake,
    ClusterMismatch,
    ClusterShuttingDown,
    TimedOut,
}

impl From<std::io::Error> for RemoteError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

#[derive(Debug)]
pub enum RemoteInitError {
    IsSelf,
    AddrAlreadyConnected,
    ErrorHandshaking(RemoteError),
    NodeIdAlreadyConnected,
    ActorSpawnError,
    LookupFailed,
}

impl RemoteInitError {
    pub fn is_duplicate(&self) -> bool {
        matches!(
            self,
            Self::IsSelf | Self::AddrAlreadyConnected | Self::NodeIdAlreadyConnected
        )
    }
}
