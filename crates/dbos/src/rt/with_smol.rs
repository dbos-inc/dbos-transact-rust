//! The smol backend: tasks on smol's global executor, timers from `async-io`.

use std::future::Future;

pub(super) fn spawn_detached<F>(task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    smol::spawn(task).detach();
}
