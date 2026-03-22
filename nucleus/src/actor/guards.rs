use crate::{
    NucleusNode,
    actor::{
        Actor, ActorContext, Sender,
        message::{DeserializationError, Handler, Message, Serializable},
    },
    net::proto::mover::{SerializedRatelimitBucket, SerializedRatelimitStore},
};
use bytes::{Buf, Bytes};
use nohash_hasher::NoHashHasher;
use prost::Message as ProstMessage;
use std::time::Duration;
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher},
    time::Instant,
};

#[derive(Debug, Clone)]
pub enum GuardError<T> {
    Restricted(T),
    Ratelimit(RatelimitError),
}

#[derive(Debug, Clone)]
pub struct RatelimitError {
    pub bounds: RatelimitBounds,
    pub try_again_in: Duration,
}

#[derive(Debug, Clone)]
pub struct RatelimitBounds {
    pub call_limit: u64,
    pub time_period: Duration,
}

impl RatelimitBounds {
    pub const fn new(call_limit: u64, seconds: u64) -> Self {
        Self {
            call_limit,
            time_period: Duration::from_secs(seconds),
        }
    }
}

struct RatelimitBucket {
    reset_at: Instant,
    tokens_remaining: u64,
}

#[derive(Default)]
pub struct RatelimitStore {
    store: HashMap<u64, RatelimitBucket, BuildHasherDefault<NoHashHasher<u64>>>,
}
impl RatelimitStore {
    fn limit<M: Message>(
        &mut self,
        _message: &M,
        config: &RatelimitConfig,
    ) -> Result<(), RatelimitError> {
        let now = Instant::now();
        let mut hasher = DefaultHasher::new();

        M::name().hash(&mut hasher);
        config.bucket.hash(&mut hasher);
        let key = hasher.finish();

        let bucket = self.store.entry(key).or_insert_with(|| RatelimitBucket {
            reset_at: now + config.bounds.time_period,
            tokens_remaining: config.bounds.call_limit,
        });

        if bucket.reset_at < now {
            bucket.reset_at = now + config.bounds.time_period;
            bucket.tokens_remaining = config.bounds.call_limit;
        }

        if bucket.tokens_remaining == 0 {
            return Err(RatelimitError {
                bounds: config.bounds.clone(),
                try_again_in: bucket.reset_at - now,
            });
        }

        bucket.tokens_remaining -= 1;

        Ok(())
    }
}

pub struct RatelimitConfig {
    pub bucket: u64,
    pub bounds: RatelimitBounds,
}

impl RatelimitConfig {
    pub fn new(bucket: impl Hash, bounds: RatelimitBounds) -> Self {
        let mut hasher = DefaultHasher::new();
        bucket.hash(&mut hasher);
        Self {
            bucket: hasher.finish(),
            bounds,
        }
    }
}

pub trait GuardedActor: Actor {
    type RestrictionType = ();

    fn get_rl_store(&mut self) -> &mut RatelimitStore;
}

#[crate::async_trait_nobox]
pub trait GuardedHandler<M: Message>
where
    Self: GuardedActor,
    M::Result: From<GuardError<Self::RestrictionType>>,
{
    async fn handle(&mut self, message: M, _ctx: &mut ActorContext<Self>) -> M::Result;

    async fn handle_delayed(
        &mut self,
        message: M,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<M::Result>,
    ) {
        let result = self.handle(message, _ctx).await;
        let _ = sender.send(result);
    }

    fn guard(&self, _message: &M) -> Result<RatelimitConfig, Self::RestrictionType> {
        Ok(RatelimitConfig {
            bucket: u64::MAX,
            bounds: RatelimitBounds::new(0, 0),
        })
    }

    fn restrict(&self, _message: &M) -> Option<Self::RestrictionType> {
        None
    }

    fn ratelimit(&self, _message: &M) -> Option<RatelimitConfig> {
        None
    }
}

impl<E, T: From<GuardError<E>>, Ok> From<GuardError<E>> for Result<Ok, T> {
    fn from(e: GuardError<E>) -> Self {
        Err(e.into())
    }
}

#[async_trait_nobox]
#[diagnostic::do_not_recommend]
impl<A, M> Handler<M> for A
where
    A: GuardedHandler<M>,
    M: Message,
    M::Result: From<GuardError<A::RestrictionType>>,
{
    async fn handle(&mut self, _message: M, _ctx: &mut ActorContext<Self>) -> M::Result {
        panic!("Ratelimited handlers use handle_delayed")
    }

    async fn handle_delayed(
        &mut self,
        message: M,
        _ctx: &mut ActorContext<Self>,
        sender: Sender<M::Result>,
    ) {
        match self.guard(&message) {
            Ok(config) => {
                if config.bucket != u64::MAX {
                    let store = self.get_rl_store();
                    if let Err(e) = store.limit(&message, &config) {
                        let _ = sender.send(GuardError::Ratelimit(e).into());
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = sender.send(GuardError::Restricted(e).into());
                return;
            }
        }

        if let Some(restriction) = self.restrict(&message) {
            let _ = sender.send(GuardError::Restricted(restriction).into());
            return;
        }

        if let Some(config) = self.ratelimit(&message) {
            let store = self.get_rl_store();
            if let Err(e) = store.limit(&message, &config) {
                let _ = sender.send(GuardError::Ratelimit(e).into());
                return;
            }
        }

        self.handle_delayed(message, _ctx, sender).await;
    }
}

impl Serializable for RatelimitStore {
    fn serialize(&self) -> Bytes {
        SerializedRatelimitStore {
            buckets: self
                .store
                .iter()
                .map(|(&key, bucket)| (key, bucket.as_serialized()))
                .collect(),
        }
        .encode_to_vec()
        .into()
    }

    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        let serialized: SerializedRatelimitStore = ProstMessage::decode(data)?;

        let mut store = RatelimitStore::default();
        for (key, bucket) in serialized.buckets {
            store
                .store
                .insert(key, RatelimitBucket::from_serialized(bucket));
        }

        Ok(store)
    }
}

impl RatelimitBucket {
    fn as_serialized(&self) -> SerializedRatelimitBucket {
        SerializedRatelimitBucket {
            time_until_reset: self.reset_at.duration_since(Instant::now()).as_millis() as u64,
            tokens_remaining: self.tokens_remaining,
        }
    }

    fn from_serialized(serialized: SerializedRatelimitBucket) -> Self {
        RatelimitBucket {
            // these RLs will be "paused" while the actor is being moved - maybe worth improving later but shrug
            reset_at: Instant::now() + Duration::from_millis(serialized.time_until_reset),
            tokens_remaining: serialized.tokens_remaining,
        }
    }
}
