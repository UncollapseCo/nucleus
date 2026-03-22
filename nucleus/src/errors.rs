use std::{fmt::Display, sync::Arc};

pub type ActorProcessingErr = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum SpawnErr {
    /// Actor panic'd or returned an error during startup
    StartupFailed(ActorProcessingErr),
    /// An actor cannot be started > 1 time
    ActorAlreadyStarted,
    /// The named actor is already registered in the registry
    ActorAlreadyRegistered(Arc<str>),
    SpawnerNotFound(Arc<str>),
    NucleusShuttingDown,
    Generic(String),
}

impl std::error::Error for SpawnErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self {
            Self::StartupFailed(inner) => Some(inner.as_ref()),
            _ => None,
        }
    }
}

impl Display for SpawnErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartupFailed(panic_msg) => {
                if f.alternate() {
                    write!(f, "Actor panicked during startup '{panic_msg:#}'")
                } else {
                    write!(f, "Actor panicked during startup '{panic_msg}'")
                }
            }
            Self::ActorAlreadyStarted => {
                write!(f, "Actor cannot be re-started more than once")
            }
            Self::ActorAlreadyRegistered(actor_name) => {
                write!(
                    f,
                    "Actor '{actor_name}' is already registered in the actor registry"
                )
            }
            Self::SpawnerNotFound(actor_type) => {
                write!(f, "Spawner not found for actor type '{actor_type}'")
            }
            Self::NucleusShuttingDown => {
                write!(f, "Nucleus is shutting down, cannot spawn new actors")
            }
            Self::Generic(msg) => {
                write!(f, "{msg}")
            }
        }
    }
}
