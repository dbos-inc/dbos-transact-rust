//! Joining and aborting a task, built over a runtime that can only spawn one and forget it.
//!
//! The runtimes disagree about what a task handle is. Tokio's detaches on drop and cancels only
//! through an explicit `abort`; smol's `Task` cancels on drop; async-std's detaches and cannot
//! cancel at all. This crate was written against tokio's — shutdown aborts tasks whose callers
//! dropped their handles long ago, and waits for them to stop — so that is the meaning kept here,
//! on every runtime, by asking each one only to run a future to the end.
//!
//! The future a backend runs is a wrapper around the caller's: it races the body against a
//! [`CancellationToken`] for abort, catches a panic out of the body or out of dropping it, and
//! sends what happened down a `oneshot` to whoever holds the [`JoinHandle`]. A runtime dropping
//! the wrapper unfinished ends the task the same way, through `Running`'s `Drop`.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Wraps `future` into the task a backend runs, and the handle that joins or aborts it.
pub(super) fn joinable<F>(
    future: F,
) -> (
    impl Future<Output = ()> + Send + 'static,
    JoinHandle<F::Output>,
)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let shared = Arc::new(Shared::default());
    let (report, result) = oneshot::channel();
    let mut running = Running {
        body: Some(Box::pin(future)),
        shared: Arc::clone(&shared),
        report: Some(report),
    };
    let task = async move {
        let outcome = {
            let mut aborted = std::pin::pin!(running.shared.abort.cancelled());
            let body = &mut running.body;
            std::future::poll_fn(|cx| {
                // Abort first, so a task aborted before its first poll is never polled.
                if aborted.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(JoinError(Repr::Cancelled)));
                }
                let body = body.as_mut().expect("polled until it ends").as_mut();
                // A panic unwinds out of the body's `poll` — dropping its live locals on the way,
                // inside the unwind — and is stopped here, so it reaches the joiner rather than
                // the runtime.
                match std::panic::catch_unwind(AssertUnwindSafe(|| body.poll(cx))) {
                    Ok(poll) => poll.map(Ok),
                    Err(payload) => Poll::Ready(Err(JoinError(Repr::Panic(payload)))),
                }
            })
            .await
        };
        running.finish(outcome);
    };
    (
        task,
        JoinHandle {
            result,
            abort: AbortHandle(shared),
        },
    )
}

/// A task's body, and what ends it: the one place a task finishes, however it ends.
///
/// The task's own end is not the only way out. A runtime shutting down drops its tasks unfinished,
/// at a suspension point or before their first poll, and then only `Drop` runs. So `Drop` ends the
/// task the same way the task's end does, in the same order, as tokio orders it:
///
/// 1. **The body is dropped**, inside `catch_unwind`. A `Drop` of the body's that panics — an
///    aborted body is dropped mid-flight, guards and all — is the task's panic, as tokio reports
///    it, not one that escapes to the runtime and leaves the joiner told the task was cancelled.
/// 2. **The task is marked finished.** Tokio counts a task its runtime dropped as finished, and so
///    must this: `is_finished` is what the executor's task sets and the system database's listener
///    restart rely on, and a task left unfinished would look running to them forever.
/// 3. **The outcome is sent**, so a joiner that sees it can rely on the body being gone. A task
///    the runtime dropped has none, and is reported as cancelled unless its drop panicked.
///
/// The body is boxed because dropping it here, from `&mut self`, means moving it out of a pinned
/// place, which only a `Box` allows without `unsafe`. It costs an allocation per task.
struct Running<F: Future> {
    body: Option<Pin<Box<F>>>,
    shared: Arc<Shared>,
    report: Option<oneshot::Sender<Result<F::Output, JoinError>>>,
}

impl<F: Future> Running<F> {
    /// Ends the task with `outcome`, unless dropping the body panics, which ends it with that.
    fn finish(&mut self, outcome: Result<F::Output, JoinError>) {
        let outcome = match self.body.take() {
            Some(body) => match std::panic::catch_unwind(AssertUnwindSafe(|| drop(body))) {
                Ok(()) => outcome,
                Err(payload) => Err(JoinError(Repr::Panic(payload))),
            },
            None => outcome,
        };
        self.shared.finished.store(true, Ordering::Release);
        if let Some(report) = self.report.take() {
            // Nobody joining is the ordinary case of a detached task.
            let _ = report.send(outcome);
        }
    }
}

impl<F: Future> Drop for Running<F> {
    fn drop(&mut self) {
        // Still holding the sender only when the runtime dropped the task before it ended.
        if self.report.is_some() {
            self.finish(Err(JoinError(Repr::Cancelled)));
        }
    }
}

#[derive(Default)]
struct Shared {
    abort: CancellationToken,
    finished: AtomicBool,
}

/// An owned permission to join a task: awaiting it gives the task's output, or how it failed.
///
/// Dropping it detaches the task, which runs on.
pub struct JoinHandle<T> {
    result: oneshot::Receiver<Result<T, JoinError>>,
    abort: AbortHandle,
}

impl<T> JoinHandle<T> {
    /// Cancels the task at its next suspension point; at once if it has not started.
    pub fn abort(&self) {
        self.abort.abort();
    }

    /// A handle that can abort the task without being able to join it.
    pub fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }

    /// Whether the task has ended — completed, panicked, or been cancelled.
    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The sender is gone without sending only when the runtime dropped the task unfinished,
        // which is what shutting a runtime down does; tokio reports that as a cancellation too.
        Pin::new(&mut self.result)
            .poll(cx)
            .map(|received| received.unwrap_or(Err(JoinError(Repr::Cancelled))))
    }
}

/// Aborts a task, without the right to join it.
#[derive(Clone)]
pub struct AbortHandle(Arc<Shared>);

impl AbortHandle {
    /// Cancels the task at its next suspension point; at once if it has not started. Aborting
    /// a task that has finished does nothing.
    pub fn abort(&self) {
        self.0.abort.cancel();
    }

    /// Whether the task has ended — completed, panicked, or been cancelled.
    pub fn is_finished(&self) -> bool {
        self.0.finished.load(Ordering::Acquire)
    }
}

/// Why joining a task gave no output: it was aborted, or it panicked.
pub struct JoinError(Repr);

enum Repr {
    Cancelled,
    Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
    pub fn is_cancelled(&self) -> bool {
        matches!(self.0, Repr::Cancelled)
    }

    pub fn is_panic(&self) -> bool {
        matches!(self.0, Repr::Panic(_))
    }

    /// The panic's payload, to resume it with.
    ///
    /// # Panics
    ///
    /// When the task was cancelled rather than panicked.
    #[track_caller]
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.0 {
            Repr::Panic(payload) => payload,
            Repr::Cancelled => panic!("`into_panic` on a task that was cancelled"),
        }
    }
}

impl std::fmt::Debug for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Repr::Cancelled => f.write_str("JoinError::Cancelled"),
            Repr::Panic(_) => f.write_str("JoinError::Panic(..)"),
        }
    }
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Repr::Cancelled => f.write_str("task was cancelled"),
            Repr::Panic(_) => f.write_str("task panicked"),
        }
    }
}

impl std::error::Error for JoinError {}
