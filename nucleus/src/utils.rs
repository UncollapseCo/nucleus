use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::fmt::Debug;
use std::sync::LazyLock;
use std::time::Duration;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{io, pin, select};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span};

use crate::actor::get_panic_string;

pub struct TaskHolder<T> {
    tasks: VecDeque<JoinHandle<T>>,
}

impl<T> TaskHolder<T> {
    pub fn new() -> Self {
        Self {
            tasks: VecDeque::new(),
        }
    }

    pub fn add_task(&mut self, task: JoinHandle<T>) {
        self.tasks.push_back(task);

        if self.tasks.len().is_multiple_of(1000) {
            self.cleanup();
        }
    }

    fn cleanup(&mut self) {
        self.tasks.retain(|task| !task.is_finished());
    }

    pub fn awaiter(&mut self) -> TaskAwaiter<T> {
        let tasks = self.tasks.drain(..).collect();

        TaskAwaiter { tasks }
    }

    pub fn await_all(&mut self) -> impl Future<Output = bool> {
        let awaiter = self.awaiter();
        awaiter.await_all().in_current_span()
    }

    pub fn force_abort_all(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}
impl<T> Default for TaskHolder<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TaskAwaiter<T> {
    tasks: Vec<JoinHandle<T>>,
}

impl<T> TaskAwaiter<T> {
    pub async fn await_all(mut self) -> bool {
        let mut had_work = false;
        while let Some(task) = self.tasks.pop() {
            if let Err(e) = task.await {
                let panic = e.try_into_panic();
                if let Ok(panic) = panic {
                    error!("Task panicked: {}", get_panic_string(panic));
                }
            }
            had_work = true;
        }

        had_work
    }
}

/// Awaitable arc is a struct that can be awaited and returns when the reference count reaches 0
#[derive(Clone, Debug)]
pub struct AwaitableArc {
    inner: Arc<Mutex<AwaitableArcInner>>,
}

impl AwaitableArc {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AwaitableArcInner { wakers: Vec::new() })),
        }
    }

    pub fn wait_zero(self) -> ArcAwaiter {
        ArcAwaiter {
            inner: Arc::downgrade(&self.inner),
            waker: None,
        }
    }
}

impl Default for AwaitableArc {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct AwaitableArcInner {
    wakers: Vec<Waker>,
}

impl Drop for AwaitableArcInner {
    fn drop(&mut self) {
        self.wakers.drain(..).for_each(|waker| waker.wake());
    }
}

pub struct ArcAwaiter {
    inner: Weak<Mutex<AwaitableArcInner>>,
    waker: Option<Waker>,
}

impl Clone for ArcAwaiter {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            waker: None,
        }
    }
}

impl Future for ArcAwaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(inner) = self.inner.upgrade() {
            let mut inner = inner.lock().unwrap();
            if let Some(waker) = self.get_mut().waker.replace(cx.waker().clone()) {
                inner.wakers.retain(|w| !w.will_wake(&waker));
            }

            inner.wakers.push(cx.waker().clone());

            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[derive(Debug)]
pub struct PeriodSendHandler {
    cancel: JoinHandle<()>,
}
impl PeriodSendHandler {
    pub(crate) fn new(cancel: JoinHandle<()>) -> Self {
        Self { cancel }
    }

    pub fn cancel(&self) {
        self.cancel.abort();
    }
}

pub fn ctrl_c() -> io::Result<CtrlCRecv> {
    static CANCEL_TOKEN: LazyLock<CancellationToken> = LazyLock::new(|| {
        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();

        #[cfg(unix)]
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

        #[cfg(unix)]
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

        #[cfg(unix)]
        let mut quit =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit()).unwrap();

        #[cfg(windows)]
        let mut win_signal = tokio::signal::windows::ctrl_c()?;

        tokio::spawn(async move {
            #[cfg(unix)]
            select! {
                _ = interrupt.recv() => {}
                _ = term.recv() => {}
                _ = quit.recv() => {}
            };

            #[cfg(windows)]
            win_signal.recv().await;

            task_token.cancel();
        });

        cancel_token
    });

    let cancel_token: tokio_util::sync::WaitForCancellationFutureOwned =
        CANCEL_TOKEN.clone().cancelled_owned();

    Ok(CtrlCRecv {
        cancel_token: Box::pin(cancel_token),
    })
}

pub struct CtrlCRecv {
    cancel_token: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}

impl CtrlCRecv {
    pub fn recv(&mut self) -> &mut Self {
        self
    }
}

impl Future for &mut CtrlCRecv {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.cancel_token.as_mut().poll(cx)
    }
}

/// takes a future, duration and a closure
/// if the future does not complete in the duration, the closure is called but future continues running
pub async fn timed_closure<'f, F, C: FnOnce() + Send + Unpin + 'static>(
    future: F,
    now: Option<Instant>,
    duration: std::time::Duration,
    closure: C,
) -> (F::Output, Instant)
where
    F: Future + Send + 'f,
{
    let now = now.unwrap_or_else(Instant::now);

    let deadline = match now.checked_add(duration) {
        Some(deadline) => deadline,
        // this is Instant::far_future but thats a private function :c
        None => now + Duration::from_secs(86400 * 365 * 30),
    };

    let sleep = None;
    pin!(sleep);
    pin!(future);
    let closure = Some(closure);

    (
        TimeoutClosureHolder {
            future,
            sleep_future: sleep,
            closure,
            deadline,
        }
        .in_current_span()
        .await,
        now,
    )
}

struct TimeoutClosureHolder<'f, F: Future + Send + 'f, C: FnOnce() + Send + Unpin + 'static> {
    future: Pin<&'f mut F>,
    sleep_future: Pin<&'f mut Option<tokio::time::Sleep>>,
    closure: Option<C>,
    deadline: Instant,
}

impl<'f, F: Future + Send + 'f, C: FnOnce() + Send + Unpin + 'static> Future
    for TimeoutClosureHolder<'f, F, C>
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll_f = this.future.poll_unpin(cx);

        match poll_f {
            Poll::Ready(v) => Poll::Ready(v),
            Poll::Pending => {
                if this.closure.is_some() {
                    // if this is ever ready once, closure gets taken, so is_some() will be false
                    // and we don't poll the sleep again
                    if this.sleep_future.is_none() {
                        let sleep = tokio::time::sleep_until(this.deadline);
                        this.sleep_future.set(Some(sleep));
                    }

                    let sleep_poll = this.sleep_future.as_mut().as_pin_mut().unwrap().poll(cx);

                    if sleep_poll.is_ready() {
                        let span = Span::current();
                        let _e = span.enter();
                        (this.closure.take().unwrap())();
                    }
                }

                Poll::Pending
            }
        }
    }
}

pub trait AsyncErrorSquash<R, E> {
    fn squash(self) -> impl Future<Output = Result<R, E>>;
}

impl<R, E, F> AsyncErrorSquash<R, E> for Result<F, E>
where
    F: Future<Output = Result<R, E>>,
{
    async fn squash(self) -> Result<R, E> {
        match self {
            Ok(future) => future.await,
            Err(e) => Err(e),
        }
    }
}

pub async fn join_all_unordered<T>(futures: impl IntoIterator<Item = impl Future<Output = T>>) {
    let mut unordered = FuturesUnordered::from_iter(futures);
    while (unordered.next().await).is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_zero() {
        let arc = AwaitableArc::new();
        let clone = arc.clone();

        let waiter = arc.wait_zero();

        // Spawn a task that drops the clone after a delay
        let task = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            drop(clone);
        });

        // Wait for all clones to be dropped
        waiter.await;

        // assert that the task completed
        assert!(task.is_finished());
    }
}
