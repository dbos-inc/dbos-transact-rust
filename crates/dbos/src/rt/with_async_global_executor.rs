//! The `async-global-executor` backend: a global executor over `async-executor`, like smol's own,
//! and the one async-std ran on. Timers come from `async-io`, since it has none of its own.

use std::future::Future;

pub(super) fn spawn_detached<F>(task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    async_global_executor::spawn(task).detach();
}
