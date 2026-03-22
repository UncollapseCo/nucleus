use crate::actor::{
    ActorContext,
    guards::{
        GuardError, GuardedActor, GuardedHandler, RatelimitBounds, RatelimitConfig, RatelimitStore,
    },
    message::Message,
    tests::{setup, shutdown},
};

use super::handlers::TestActor;

struct RatelimitedNumGet {
    bucket: u64,
}

struct RestrictedNumGet {
    restrict: bool,
}

struct RestrictedRatelimitedNumGet {
    bucket: u64,
    restrict: bool,
}

struct GuardedNumGet {
    bucket: u64,
    restrict: bool,
}

#[derive(Debug)]
pub struct ErrorType;
type TestGuardError = GuardError<ErrorType>;

impl Message for RatelimitedNumGet {
    type Result = Result<u64, TestGuardError>;

    fn name() -> &'static str {
        "RatelimitedNumGet"
    }
}

impl Message for RestrictedNumGet {
    type Result = Result<u64, TestGuardError>;

    fn name() -> &'static str {
        "RestrictedNumGet"
    }
}

impl Message for RestrictedRatelimitedNumGet {
    type Result = Result<u64, TestGuardError>;

    fn name() -> &'static str {
        "RestrictedRatelimitedNumGet"
    }
}

impl Message for GuardedNumGet {
    type Result = Result<u64, TestGuardError>;

    fn name() -> &'static str {
        "GuardedNumGet"
    }
}

impl GuardedActor for TestActor {
    type RestrictionType = ErrorType;
    fn get_rl_store(&mut self) -> &mut RatelimitStore {
        &mut self.rl_store
    }
}

#[async_trait_nobox]
impl GuardedHandler<RatelimitedNumGet> for TestActor {
    async fn handle(
        &mut self,
        _message: RatelimitedNumGet,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<u64, TestGuardError> {
        Ok(self.last_num)
    }

    fn ratelimit(&self, message: &RatelimitedNumGet) -> Option<RatelimitConfig> {
        Some(RatelimitConfig {
            bucket: message.bucket,
            bounds: RatelimitBounds::new(5, 5),
        })
    }
}

#[async_trait_nobox]
impl GuardedHandler<RestrictedNumGet> for TestActor {
    async fn handle(
        &mut self,
        _message: RestrictedNumGet,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<u64, TestGuardError> {
        Ok(self.last_num)
    }

    fn restrict(&self, message: &RestrictedNumGet) -> Option<Self::RestrictionType> {
        if message.restrict {
            Some(ErrorType)
        } else {
            None
        }
    }
}

#[async_trait_nobox]
impl GuardedHandler<RestrictedRatelimitedNumGet> for TestActor {
    async fn handle(
        &mut self,
        _message: RestrictedRatelimitedNumGet,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<u64, TestGuardError> {
        Ok(self.last_num)
    }

    fn restrict(&self, message: &RestrictedRatelimitedNumGet) -> Option<Self::RestrictionType> {
        if message.restrict {
            Some(ErrorType)
        } else {
            None
        }
    }

    fn ratelimit(&self, message: &RestrictedRatelimitedNumGet) -> Option<RatelimitConfig> {
        Some(RatelimitConfig {
            bucket: message.bucket,
            bounds: RatelimitBounds::new(5, 5),
        })
    }
}

#[async_trait_nobox]
impl GuardedHandler<GuardedNumGet> for TestActor {
    async fn handle(
        &mut self,
        _message: GuardedNumGet,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<u64, TestGuardError> {
        Ok(self.last_num)
    }

    fn guard(&self, message: &GuardedNumGet) -> Result<RatelimitConfig, Self::RestrictionType> {
        if message.restrict {
            return Err(ErrorType);
        }

        Ok(RatelimitConfig {
            bucket: message.bucket,
            bounds: RatelimitBounds::new(5, 5),
        })
    }
}

#[tokio::test]
async fn test_ratelimits_basic() {
    let (node1, _node2, actor, _actor2) = setup().await;

    for _ in 0..5 {
        actor
            .send(RatelimitedNumGet { bucket: 0 })
            .await
            .unwrap()
            .expect("should NOT be ratelimited");
    }

    actor
        .send(RatelimitedNumGet { bucket: 0 })
        .await
        .unwrap()
        .expect_err("should be ratelimited");

    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_ratelimits_two_buckets() {
    let (node1, _node2, actor, _actor2) = setup().await;

    for _ in 0..5 {
        actor
            .send(RatelimitedNumGet { bucket: 0 })
            .await
            .unwrap()
            .expect("should NOT be ratelimited");
    }

    for _ in 0..5 {
        actor
            .send(RatelimitedNumGet { bucket: 1 })
            .await
            .unwrap()
            .expect("should NOT be ratelimited");
    }

    actor
        .send(RatelimitedNumGet { bucket: 0 })
        .await
        .unwrap()
        .expect_err("should be ratelimited");

    actor
        .send(RatelimitedNumGet { bucket: 1 })
        .await
        .unwrap()
        .expect_err("should be ratelimited");

    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_restriction_basic() {
    let (node1, _node2, actor, _actor2) = setup().await;

    actor
        .send(RestrictedNumGet { restrict: false })
        .await
        .unwrap()
        .expect("should NOT be restricted");

    actor
        .send(RestrictedNumGet { restrict: true })
        .await
        .unwrap()
        .expect_err("should be restricted");

    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_both() {
    let (node1, _node2, actor, _actor2) = setup().await;

    // Test restriction first
    actor
        .send(RestrictedRatelimitedNumGet {
            bucket: 0,
            restrict: true,
        })
        .await
        .unwrap()
        .expect_err("should be restricted");

    // Test rate limiting (not restricted)
    for _ in 0..5 {
        actor
            .send(RestrictedRatelimitedNumGet {
                bucket: 0,
                restrict: false,
            })
            .await
            .unwrap()
            .expect("should NOT be ratelimited");
    }

    actor
        .send(RestrictedRatelimitedNumGet {
            bucket: 0,
            restrict: false,
        })
        .await
        .unwrap()
        .expect_err("should be ratelimited");

    // Test that restriction takes precedence (separate bucket)
    actor
        .send(RestrictedRatelimitedNumGet {
            bucket: 1,
            restrict: true,
        })
        .await
        .unwrap()
        .expect_err("should be restricted before ratelimit check");

    shutdown(node1, _node2, actor, _actor2).await;
}

#[tokio::test]
async fn test_guarded() {
    let (node1, _node2, actor, _actor2) = setup().await;

    // Test restriction first
    actor
        .send(GuardedNumGet {
            bucket: 0,
            restrict: true,
        })
        .await
        .unwrap()
        .expect_err("should be restricted");

    // Test rate limiting (not restricted)
    for _ in 0..5 {
        actor
            .send(GuardedNumGet {
                bucket: 0,
                restrict: false,
            })
            .await
            .unwrap()
            .expect("should NOT be ratelimited");
    }

    actor
        .send(GuardedNumGet {
            bucket: 0,
            restrict: false,
        })
        .await
        .unwrap()
        .expect_err("should be ratelimited");

    // Test that restriction takes precedence (separate bucket)
    actor
        .send(GuardedNumGet {
            bucket: 1,
            restrict: true,
        })
        .await
        .unwrap()
        .expect_err("should be restricted before ratelimit check");

    shutdown(node1, _node2, actor, _actor2).await;
}
