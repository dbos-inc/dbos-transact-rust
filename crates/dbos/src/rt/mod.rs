//! The async runtime: the one place this crate spawns a task or waits on a clock.
//!
//! Everything else here runs on any executor. `tokio::sync`'s primitives, `select!`, `join!` and
//! `task_local!` are plain futures that wake through the `Waker` they were polled with, and need
//! no reactor. What does need one is spawning and timers, and those go through this module so
//! that the reactor is the application's choice rather than this crate's.
//!
//! **The runtime is chosen at run time, not by cargo feature alone**, which is what `sqlx` does
//! and for the same reason: features unify across the dependency graph, so an application on
//! another runtime cannot stop some dependency of its own from enabling `runtime-tokio` too.
//! Enabling several is therefore allowed. When the caller is inside a tokio runtime, tokio is
//! used; otherwise `async-global-executor` if it is enabled, then smol — `sqlx`'s own order, so
//! its tasks and this crate's land on the same executor. Only enabling none is an error.
//!
//! What a backend supplies is the operation every runtime agrees on — spawn this future and
//! forget it — and a timer. Joining, aborting and panic reporting are built over that in
//! [`task`], so a [`JoinHandle`] means the same thing whichever runtime ran the task.

mod task;
#[cfg(feature = "runtime-async-global-executor")]
mod with_async_global_executor;
#[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
mod with_async_io;
// Not when `async-global-executor` is enabled too: that one is preferred, and smol's spawn would
// never be called.
#[cfg(all(
    feature = "runtime-smol",
    not(feature = "runtime-async-global-executor")
))]
mod with_smol;
#[cfg(feature = "runtime-tokio")]
mod with_tokio;

/// The executor a task goes to when the caller is not in a tokio runtime.
#[cfg(feature = "runtime-async-global-executor")]
use with_async_global_executor as global;
#[cfg(all(
    feature = "runtime-smol",
    not(feature = "runtime-async-global-executor")
))]
use with_smol as global;

use std::future::Future;
use std::task::Poll;
use std::time::{Duration, Instant};

pub use task::{AbortHandle, JoinError, JoinHandle};

#[cfg(not(any(
    feature = "runtime-tokio",
    feature = "runtime-smol",
    feature = "runtime-async-global-executor"
)))]
compile_error!(
    "dbos needs an async runtime: enable one of the features `runtime-tokio` (the default; check \
     for `default-features = false`), `runtime-smol` or `runtime-async-global-executor`"
);

/// Where a task is spawned: the runtime a caller was on when this was taken.
///
/// Held rather than looked up per spawn by anything that spawns on someone else's behalf. The
/// executor takes one at launch, because recovery and the dequeue loop spawn from background
/// tasks, where "the caller's runtime" is not one answer.
#[derive(Clone)]
pub(crate) struct Spawner(Backend);

#[derive(Clone)]
enum Backend {
    #[cfg(feature = "runtime-tokio")]
    #[allow(clippy::disallowed_types)]
    Tokio(tokio::runtime::Handle),
    /// The process-wide executor of whichever non-tokio runtime is enabled; see `global`.
    #[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
    Global,
}

impl Spawner {
    /// The runtime the caller is on: tokio if it is in a tokio runtime, and otherwise the
    /// enabled non-tokio runtime, whose executor is global and so always there.
    ///
    /// # Panics
    ///
    /// When no enabled runtime is available to the caller: a tokio-only build called from outside
    /// a tokio runtime.
    #[track_caller]
    pub(crate) fn current() -> Self {
        #[cfg(feature = "runtime-tokio")]
        if let Some(handle) = with_tokio::current() {
            return Self(Backend::Tokio(handle));
        }
        #[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
        return Self(Backend::Global);
        #[cfg(not(any(feature = "runtime-smol", feature = "runtime-async-global-executor")))]
        missing_runtime()
    }

    /// Spawns `future` and returns a handle to join or abort it.
    ///
    /// Dropping the handle detaches the task, which runs on.
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, handle) = task::joinable(future);
        match self.0 {
            #[cfg(feature = "runtime-tokio")]
            Backend::Tokio(ref runtime) => with_tokio::spawn_detached(runtime, task),
            #[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
            Backend::Global => global::spawn_detached(task),
        }
        handle
    }
}

/// Spawns `future` on the caller's runtime, returning a handle to join or abort it. Dropping
/// the handle detaches the task, which runs on.
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Spawner::current().spawn(future)
}

/// Waits until `duration` has passed.
///
/// The deadline is taken now, when this is called, rather than when the future is first polled —
/// as `tokio::time::sleep` does.
pub fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
    sleep_until(after(duration))
}

/// Waits until `deadline`; at once if it has passed.
pub async fn sleep_until(deadline: Instant) {
    #[cfg(feature = "runtime-tokio")]
    if with_tokio::available() {
        return with_tokio::sleep_until(deadline).await;
    }
    #[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
    return with_async_io::sleep_until(deadline).await;
    #[cfg(not(any(feature = "runtime-smol", feature = "runtime-async-global-executor")))]
    missing_runtime()
}

/// Runs `future` for up to `duration`, failing with [`Elapsed`] if it has not finished by then.
///
/// The deadline is taken now, when this is called, as `tokio::time::timeout` does. Dropping the
/// future at the deadline is the whole of the cancellation, so this is no stronger than the
/// future's own cancellation safety.
pub fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    timeout_at(after(duration), future)
}

/// [`timeout`], with the deadline given as an instant.
///
/// The future is polled before the timer, so one that is ready at the deadline keeps its output.
pub async fn timeout_at<F: Future>(deadline: Instant, future: F) -> Result<F::Output, Elapsed> {
    let mut future = std::pin::pin!(future);
    let mut expiry = std::pin::pin!(sleep_until(deadline));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        expiry.as_mut().poll(cx).map(|()| Err(Elapsed))
    })
    .await
}

/// A [`timeout`] whose deadline passed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Lets the runtime run other tasks before this one continues.
///
/// Needs no reactor: wake at once and return `Pending` a single time, which puts the task at the
/// back of the run queue on every executor.
///
/// Only the crate's own tests yield, so only they get it. Library code that needs to can drop the
/// `cfg`; an integration test would also need it re-exported through `crate::__rt`.
#[cfg(test)]
pub(crate) async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// The runtime a caller outside tokio gets, by the name of its feature without `runtime-`; `None`
/// in a build with no non-tokio runtime.
///
/// For the test harness, which runs a test off tokio and checks it is landing where it was told.
pub fn fallback() -> Option<&'static str> {
    if cfg!(feature = "runtime-async-global-executor") {
        Some("async-global-executor")
    } else if cfg!(feature = "runtime-smol") {
        Some("smol")
    } else {
        None
    }
}

/// `duration` from now, or a deadline far enough out to mean "never" where adding would overflow
/// — `Duration::MAX` is a legitimate way to ask for no deadline.
fn after(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration)
        .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365 * 30))
}

#[cfg(not(any(feature = "runtime-smol", feature = "runtime-async-global-executor")))]
#[track_caller]
fn missing_runtime() -> ! {
    panic!(
        "dbos needs an async runtime, and none of the enabled ones is available here: call it \
         from within a tokio runtime, or enable `runtime-smol` or \
         `runtime-async-global-executor`"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    /// Runs each scenario twice: in a tokio runtime, and — when a non-tokio runtime is enabled —
    /// on a plain thread, where `rt` has to fall back to that runtime's executor and timers.
    macro_rules! on_each_runtime {
        ($($scenario:ident),* $(,)?) => {
            mod on_tokio {
                $(
                    #[tokio::test]
                    async fn $scenario() {
                        super::$scenario().await;
                    }
                )*
            }
            #[cfg(any(feature = "runtime-smol", feature = "runtime-async-global-executor"))]
            mod off_tokio {
                $(
                    #[test]
                    fn $scenario() {
                        async_io::block_on(super::$scenario());
                    }
                )*
            }
        };
    }

    on_each_runtime!(
        joining_gives_the_output,
        dropping_the_handle_detaches_the_task,
        an_aborted_task_is_dropped_and_reports_finished,
        a_panic_reaches_the_joiner,
        a_panic_dropping_an_aborted_body_reaches_the_joiner,
        a_ready_future_beats_a_passed_deadline,
        a_sleep_waits_out_its_duration,
        an_unbounded_timeout_does_not_overflow,
    );

    async fn joining_gives_the_output() {
        assert_eq!(super::spawn(async { 7 }).await.expect("it completes"), 7);
    }

    async fn dropping_the_handle_detaches_the_task() {
        let (done, finished) = tokio::sync::oneshot::channel();
        drop(super::spawn(async move {
            super::yield_now().await;
            let _ = done.send(());
        }));
        super::timeout(Duration::from_secs(10), finished)
            .await
            .expect("the task ran on without its handle")
            .expect("and finished");
    }

    async fn an_aborted_task_is_dropped_and_reports_finished() {
        let (dropped, gone) = tokio::sync::oneshot::channel::<()>();
        let task = super::spawn(async move {
            let _dropped = dropped;
            std::future::pending::<()>().await;
        });
        let abort = task.abort_handle();
        assert!(!abort.is_finished());
        abort.abort();
        assert!(task.await.expect_err("it was aborted").is_cancelled());
        assert!(abort.is_finished());
        assert!(
            gone.await.is_err(),
            "the body was dropped, and its sender with it"
        );
    }

    async fn a_panic_reaches_the_joiner() {
        let joined = super::spawn(async { panic!("boom") }).await;
        let payload = joined.expect_err("it panicked").into_panic();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    }

    async fn a_panic_dropping_an_aborted_body_reaches_the_joiner() {
        struct PanicsOnDrop;
        impl Drop for PanicsOnDrop {
            fn drop(&mut self) {
                panic!("in drop");
            }
        }
        let (started, running) = tokio::sync::oneshot::channel();
        let task = super::spawn(async move {
            let _guard = PanicsOnDrop;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        running.await.expect("the body started");
        task.abort();
        let payload = task.await.expect_err("it was aborted").into_panic();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"in drop"));
    }

    async fn a_ready_future_beats_a_passed_deadline() {
        let past = Instant::now() - Duration::from_secs(1);
        assert_eq!(super::timeout_at(past, async { 1 }).await, Ok(1));
        assert_eq!(
            super::timeout_at(past, std::future::pending::<()>()).await,
            Err(super::Elapsed)
        );
    }

    async fn a_sleep_waits_out_its_duration() {
        let started = Instant::now();
        super::sleep(Duration::from_millis(20)).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    async fn an_unbounded_timeout_does_not_overflow() {
        assert_eq!(super::timeout(Duration::MAX, async { 1 }).await, Ok(1));
    }

    /// Tokio only, because only tokio's runtime can be dropped: what a runtime shutting down does
    /// to the tasks still on it, whether or not they have been polled.
    #[test]
    fn a_task_its_runtime_drops_reports_finished_and_cancelled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let (unpolled, polled) = runtime.block_on(async {
            let unpolled = super::spawn(std::future::pending::<()>());
            let polled = super::spawn(std::future::pending::<()>());
            // Let the second one run to its first suspension; the first was spawned too, so
            // whether it ran is the scheduler's business — the point is that one surely did.
            super::sleep(std::time::Duration::from_millis(10)).await;
            (unpolled, polled)
        });
        assert!(!unpolled.is_finished() && !polled.is_finished());
        drop(runtime);
        for task in [unpolled, polled] {
            assert!(
                task.is_finished(),
                "a dropped task is finished, as tokio counts it"
            );
            let joined = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("a runtime")
                .block_on(task);
            assert!(joined.expect_err("it never completed").is_cancelled());
        }
    }

    /// Tokio only: a current-thread runtime is what makes "before it starts" certain. On a global
    /// executor the task may be running on another thread before `abort` is reached.
    #[tokio::test]
    async fn a_task_aborted_before_it_starts_is_never_polled() {
        let polled = Arc::new(AtomicBool::new(false));
        let task = {
            let polled = Arc::clone(&polled);
            super::Spawner::current().spawn(async move { polled.store(true, Ordering::SeqCst) })
        };
        task.abort();
        let joined = task.await;
        assert!(joined.expect_err("it was aborted").is_cancelled());
        assert!(!polled.load(Ordering::SeqCst));
    }
}
