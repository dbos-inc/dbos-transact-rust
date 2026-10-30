//! The tokio backend: the one module allowed to call tokio for spawning and timers.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::future::Future;
use std::time::Instant;

use tokio::runtime::Handle;

/// The tokio runtime the caller is in, if it is in one.
pub(super) fn current() -> Option<Handle> {
    Handle::try_current().ok()
}

/// Whether the caller is in a tokio runtime, whose timers it can then use.
pub(super) fn available() -> bool {
    Handle::try_current().is_ok()
}

/// Spawns `task` on `runtime` and drops tokio's own handle, which detaches it.
pub(super) fn spawn_detached<F>(runtime: &Handle, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    drop(runtime.spawn(task));
}

pub(super) async fn sleep_until(deadline: Instant) {
    tokio::time::sleep_until(deadline.into()).await;
}
