//! Recovery on launch: returning what a previous process abandoned to its queue.
//!
//! **Recovery is a re-enqueue, and that is the whole of it.** A `PENDING` row whose executor is
//! gone goes back to `ENQUEUED`, and whichever executor next polls its queue runs it. This module
//! is one write.
//!
//! Python, TypeScript and Go re-enqueue too (Go's is #433); Java alone runs the recovered
//! workflow in the process that found it.
//!
//! Three things the re-enqueue buys:
//!
//! - **A repeat costs nothing.** The queue's atomic `ENQUEUED` → `PENDING` claim admits exactly
//!   one runner, and the `executor_ids` predicate means a second sweep naming the dead executor
//!   matches nothing once a live one has taken the row.
//! - **The fleet shares the backlog.** A re-enqueued workflow can be picked up by any executor on
//!   the version, so one executor returning to a large backlog does not work through all of it
//!   alone.
//! - **Concurrency belongs to the queue, not to this module.** Note that none of the four
//!   references gives [`INTERNAL_QUEUE`] a concurrency default, so the answer for a workflow that
//!   was never queued is "all of them", by choice rather than by oversight.
//!
//! **Synchronous, inside `launch`.** Not backgrounded, because it is one statement rather than a
//! list of workflows to run — and because doing it before `launch` returns is what keeps it from
//! touching a workflow the application starts the instant it does. Such a workflow is `PENDING`
//! under this executor's id too, and indistinguishable from an abandoned one; a sweep running any
//! later would tear it off its runner and offer it to the fleet. Go orders it the same way, and
//! for the second reason as well: it re-enqueues before the queue runner starts, so recovered work
//! is not racing a dequeue pass that is already in flight.

use crate::error::{Error, Result};
use crate::sysdb::{INTERNAL_QUEUE, SystemDatabase};

/// Returns this executor's abandoned workflows to their queues.
///
/// Called by [`Executor::start`](crate::instance::Executor) before `launch` returns, and reports what
/// moved rather than handles: this process may run none of them.
pub(crate) async fn reenqueue(
    sysdb: &dyn SystemDatabase,
    executor_id: &str,
    app_version: &str,
) -> Result<Vec<String>> {
    let recovered = sysdb
        .reenqueue_for_recovery(&[executor_id], app_version, INTERNAL_QUEUE)
        .await
        .map_err(Error::SystemDatabase)?;
    if recovered.is_empty() {
        tracing::debug!("no workflows to recover");
    } else {
        tracing::info!(
            workflows = recovered.len(),
            "re-enqueued workflows a previous run left PENDING"
        );
    }
    Ok(recovered)
}
