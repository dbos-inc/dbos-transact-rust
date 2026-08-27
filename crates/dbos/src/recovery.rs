//! Recovery on launch: returning what a previous process abandoned to its queue.
//!
//! **Recovery is a re-enqueue, and that is the whole of it.** A `PENDING` row whose executor is
//! gone goes back to `ENQUEUED`, and whichever executor next polls its queue runs it. This module
//! is one write.
//!
//! It used to be otherwise: the sweep read the abandoned rows and executed them here, in the
//! process that found them, which was Java's shape and only Java's — `sdk-parity.md` divergence
//! 12, and go #433's conclusion. Python, TypeScript and Go all re-enqueue. That shape was not a
//! preference; it was what was available before the engine had queues, and this module carried a
//! section arguing against itself for as long as it lasted.
//!
//! Three things the re-enqueue buys, none of which was reachable before:
//!
//! - **A repeat costs nothing.** The queue's atomic `ENQUEUED` → `PENDING` claim admits exactly
//!   one runner, and the `executor_ids` predicate means a second sweep naming the dead executor
//!   matches nothing once a live one has taken the row. Executing here was held together by
//!   `init_workflow`'s ownership guard instead, which is a narrower promise.
//! - **The fleet shares the backlog.** A re-enqueued workflow can be picked up by any executor on
//!   the version. One recovered in place ran in place, so an executor returning to a large backlog
//!   worked through all of it alone.
//! - **Concurrency stops being this module's problem.** The old sweep spawned every pending
//!   workflow at once, unbounded, so a process that died holding a backlog contended with itself
//!   for its own connection pool on the next launch. A queue owns that question now — though note
//!   that none of the four references gives [`INTERNAL_QUEUE`] a concurrency default, so the
//!   answer for a workflow that was never queued is still "all of them", by choice rather than by
//!   oversight.
//!
//! **Synchronous, inside `launch`.** Not backgrounded, because it is now one statement rather than
//! a list of workflows to run — and because doing it before `launch` returns is what keeps it from
//! touching a workflow the application starts the instant it does. Such a workflow is `PENDING`
//! under this executor's id too, and indistinguishable from an abandoned one; a sweep running any
//! later would tear it off its runner and offer it to the fleet. Go orders it the same way, and
//! for the second reason as well: it re-enqueues before the queue runner starts, so recovered work
//! is not racing a dequeue pass that is already in flight.

use crate::error::{Error, Result};
use crate::sysdb::{INTERNAL_QUEUE, SystemDatabase};

/// Returns this executor's abandoned workflows to their queues.
///
/// Called by [`Executor::start`](crate::dbos::Executor) before `launch` returns, and reports what
/// moved rather than handles: this process may run none of them.
pub(crate) async fn reenqueue(
    sysdb: &impl SystemDatabase,
    executor_id: &str,
    application_version: &str,
) -> Result<Vec<String>> {
    let recovered = sysdb
        .reenqueue_for_recovery(&[executor_id], application_version, INTERNAL_QUEUE)
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
