use crate::actor::{Sender, get_panic_string};
use std::convert::Infallible;
use std::ops::{FromResidual, Try};
use std::panic::{AssertUnwindSafe, Location};
use tracing::Instrument;

// takes in sender, and an async move block,
// spawns the block and waits for it to return then sends the result to the sender
#[track_caller]
pub fn delayed<T: Send + 'static>(
    sender: Sender<T>,
    block: impl std::future::Future<Output = T> + Send + 'static,
) {
    let caller = Location::caller();

    tokio::spawn(
        async move {
            let result = futures::FutureExt::catch_unwind(AssertUnwindSafe(block)).await;

            match result {
                Ok(output) => {
                    let _ = sender.send(output);
                }
                Err(e) => {
                    let e = get_panic_string(e);
                    let backtrace = std::backtrace::Backtrace::capture();
                    let caller_info =
                        format!("{}:{}:{}", caller.file(), caller.line(), caller.column());

                    error!(
                        %backtrace,
                        "<hazard> Panic in a delayed block from {} - {:?}", caller_info, e
                    );
                }
            }
        }
        .in_current_span(),
    );
}

pub enum MaybeDelayed<F: Future<Output = T>, T> {
    Delayed(F),
    Instant(T),
}

// impl<T, E, R, F: Future<Output = R>> FromResidual<Result<std::convert::Infallible, E>>
//     for MaybeDelayed<F, R>
// where
//     R: Into<Result<T, E>>,
// {
//     fn from_residual(res: Result<std::convert::Infallible, E>) -> Self {
//         match res {
//             Ok(_) => unreachable!(),
//             Err(e) => MaybeDelayed::Instant(Err(e)),
//         }
//     }
// }

// impl<T, F: Future<Output = T>> FromResidual<T> for MaybeDelayed<F, T> {
//     fn from_residual(res: T) -> Self {
//         MaybeDelayed::Instant(res)
//     }
// }

impl<T, E> Try for MaybeDelayedResult<T, E> {
    type Output = T;
    type Residual = MaybeResultErr<E>;

    fn branch(self) -> std::ops::ControlFlow<Self::Residual, Self::Output> {
        match self {
            MaybeDelayedResult { res: Ok(output) } => std::ops::ControlFlow::Continue(output),
            MaybeDelayedResult { res: Err(residual) } => std::ops::ControlFlow::Break(residual),
        }
    }

    fn from_output(output: T) -> Self {
        MaybeDelayedResult { res: Ok(output) }
    }
}

pub struct MaybeDelayedResult<T, E> {
    res: Result<T, MaybeResultErr<E>>,
}

impl<T, E> MaybeDelayedResult<T, E> {
    pub fn new(res: Result<T, MaybeResultErr<E>>) -> Self {
        MaybeDelayedResult { res }
    }
}

impl<T, E> From<Result<T, E>> for MaybeDelayedResult<T, E> {
    fn from(res: Result<T, E>) -> Self {
        MaybeDelayedResult {
            res: match res {
                Ok(output) => Ok(output),
                Err(err) => Err(MaybeResultErr(err)),
            },
        }
    }
}

impl<T, E> IntoMaybeDelayedResult<T, Result<T, E>> for Result<T, E> {
    fn into_maybe(self) -> MaybeDelayedResult<T, Result<T, E>> {
        match self {
            Ok(output) => MaybeDelayedResult { res: Ok(output) },
            Err(err) => MaybeDelayedResult {
                res: Err(MaybeResultErr(Err(err))),
            },
        }
    }
}

impl<T, E, I: Into<MaybeResultErr<E>>> FromResidual<I> for MaybeDelayedResult<T, E> {
    fn from_residual(residual: I) -> Self {
        MaybeDelayedResult {
            res: Err(residual.into()),
        }
    }
}
impl<T, F: Future<Output = T>> FromResidual<MaybeDelayedResult<Infallible, T>>
    for MaybeDelayed<F, T>
{
    fn from_residual(residual: MaybeDelayedResult<Infallible, T>) -> Self {
        match residual.res {
            Ok(_) => unreachable!(),
            Err(e) => MaybeDelayed::Instant(e.0),
        }
    }
}

impl<E, F: Future<Output = E>> FromResidual<MaybeResultErr<E>> for MaybeDelayed<F, E> {
    fn from_residual(residual: MaybeResultErr<E>) -> Self {
        MaybeDelayed::Instant(residual.0)
    }
}

// impl fromresidual for maybedelayed result for all types that impl Into<maybedelayedresult>
// impl<T, E, I: Into<MaybeDelayedResult<T, E>>> FromResidual<I> for MaybeDelayedResult<T, E> {
//     fn from_residual(residual: I) -> Self {
//         residual.into()
//     }
// }

// impl<T, F: Future<Output = T>> FromResidual<T> for MaybeDelayed<F, T> {
//     fn from_residual(residual: T) -> Self {
//         MaybeDelayed::Instant(residual)
//     }
// }

// pub trait IntoMaybeDelayed<F: Future<Output = T>, T> {
//     fn into_instant(self) -> MaybeDelayed<F, T>;
// }

// impl<F: Future<Output = T>, T> IntoMaybeDelayed<F, T> for T {
//     fn into_instant(self) -> MaybeDelayed<F, T> {
//         MaybeDelayed::Instant(self)
//     }
// }

pub trait IntoMaybeDelayedResult<T, E> {
    fn into_maybe(self) -> MaybeDelayedResult<T, E>;
}

pub struct MaybeResultErr<E>(pub E);

#[track_caller]
pub async fn maybe_delayed<T: Send + 'static>(
    sender: Sender<T>,
    block: impl std::future::Future<Output = MaybeDelayed<impl Future<Output = T> + Send + 'static, T>>
    + Send,
) {
    let res = block.await;
    match res {
        MaybeDelayed::Delayed(fut) => {
            delayed(sender, fut);
        }
        MaybeDelayed::Instant(result) => {
            let _ = sender.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{ControlFlow, FromResidual, Try};

    use crate::actor::delay::{
        IntoMaybeDelayedResult, MaybeDelayed, MaybeDelayedResult, MaybeResultErr, maybe_delayed,
    };

    #[tokio::test]
    async fn test_maybe_delayed_types() {
        enum CustomRes<T, E> {
            Ok(T),
            Err(E),
        }

        impl<T, E> Try for CustomRes<T, E> {
            type Output = T;
            type Residual = E;

            fn from_output(output: Self::Output) -> Self {
                CustomRes::Ok(output)
            }

            fn branch(self) -> std::ops::ControlFlow<Self::Residual, Self::Output> {
                match self {
                    CustomRes::Ok(output) => ControlFlow::Continue(output),
                    CustomRes::Err(residual) => ControlFlow::Break(residual),
                }
            }
        }

        impl<T, E> FromResidual<E> for CustomRes<T, E> {
            fn from_residual(residual: E) -> Self {
                CustomRes::Err(residual)
            }
        }

        impl<T, E> IntoMaybeDelayedResult<T, CustomRes<T, E>> for CustomRes<T, E> {
            fn into_maybe(self) -> MaybeDelayedResult<T, CustomRes<T, E>> {
                match self {
                    CustomRes::Ok(output) => MaybeDelayedResult { res: Ok(output) },
                    CustomRes::Err(err) => MaybeDelayedResult {
                        res: Err(MaybeResultErr(CustomRes::Err(err))),
                    },
                }
            }
        }

        type CustomResult = CustomRes<u32, u32>;

        let (sender, rx) = tokio::sync::oneshot::channel::<CustomResult>();

        maybe_delayed(sender.into(), async {
            let toret = CustomResult::Err(0).into_maybe();
            toret?;

            MaybeDelayed::Delayed(async move { CustomRes::Ok(1) })
        })
        .await;

        let res = rx.await.unwrap();
        match res {
            CustomResult::Ok(_) => panic!("Expected Err, got Ok"),
            CustomResult::Err(val) => assert_eq!(val, 0),
        }
    }

    #[tokio::test]
    async fn test_maybe_delayed() {
        fn ret_err() -> Result<u32, u32> {
            Err(1)
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<u32, u32>>();

        maybe_delayed(tx.into(), async {
            ret_err().into_maybe()?;

            MaybeDelayed::Delayed(async move { Ok(0) })
        })
        .await;

        assert_eq!(rx.await.unwrap(), Err(1));

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<u32, u32>>();

        maybe_delayed(tx.into(), async {
            MaybeDelayed::Delayed(async move { Ok(0) })
        })
        .await;

        assert_eq!(rx.await.unwrap(), Ok(0));
    }
}
