//! The workflow-timeout sweep, which records the timeout of workflows nobody is running.
//!
//! Every executor of an application runs this loop. Each pass cancels the application's unsettled
//! workflows whose deadline has passed, whoever owns them and whichever unsettled status they are
//! in, so a workflow nobody is running still times out: an `ENQUEUED` or `DELAYED` child that
//! inherited its parent's deadline, or a `PENDING` workflow whose executor died. See
//! [`cancel_timed_out_workflows`](crate::sysdb::SystemDatabase::cancel_timed_out_workflows).
//!
//! A workflow running in this process usually doesn't wait for the sweep: its own timer ends the
//! body at the deadline, and the execution records the timeout itself once the database's clock
//! agrees it has passed. Only when that clock hasn't yet does the execution wait for the sweep. See
//! `run_until_deadline` and `time_out` in the workflow module.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::Instrument;

use crate::instance::Executor;
use crate::workflow::{Role, spawn_tracked};

/// The most workflows one pass cancels. A full batch means more may be waiting, so the next pass
/// runs at once rather than after the interval.
const BATCH: i64 = 1000;

/// How long the sweep waits after a pass that didn't fill its batch.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);

/// The interval [`set_interval`] chose, in milliseconds, or 0 for [`DEFAULT_INTERVAL`].
static INTERVAL_MS: AtomicU64 = AtomicU64::new(0);

/// Sets how long every sweep started from now on waits between passes, in this process. For
/// tests, which would otherwise wait up to a second for each timeout they check; zero restores the
/// default. Not public API: reached through `dbos::__private`.
pub fn set_interval(interval: Duration) {
    INTERVAL_MS.store(
        u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

/// The interval a sweep started now waits between passes.
fn interval() -> Duration {
    match INTERVAL_MS.load(Ordering::Relaxed) {
        0 => DEFAULT_INTERVAL,
        ms => Duration::from_millis(ms),
    }
}

/// Starts the sweep, as a [`Role::Monitor`]: shutdown aborts it once the workflows have drained.
///
/// After the drain rather than with the producers, because a workflow still running may be waiting
/// on the sweep to record its timeout. Its own task rather than a step of the dequeue supervisor's
/// tick: back-to-back full batches would otherwise hold up queue reconciliation. Aborting the task
/// stops the loop; a statement the server already has may still commit, which cancels only rows
/// that had expired anyway. The next launch starts a fresh sweep.
pub(crate) fn spawn(executor: Arc<Executor>) {
    spawn_tracked(
        &executor,
        Role::Monitor,
        "workflow timeouts".to_owned(),
        {
            let executor = Arc::clone(&executor);
            sweep(executor, interval())
        }
        .instrument(tracing::info_span!("workflow_timeouts")),
    );
}

/// Runs passes until the task is aborted.
///
/// A failed pass is logged and the loop goes on: the next pass finds the same rows, and stopping
/// would leave every later deadline unenforced for the life of the executor.
async fn sweep(executor: Arc<Executor>, interval: Duration) {
    loop {
        let full = match executor.sysdb().cancel_timed_out_workflows(BATCH).await {
            Ok(cancelled) => {
                for workflow_id in &cancelled {
                    tracing::debug!(
                        workflow_id,
                        "the workflow exceeded its deadline and is cancelled"
                    );
                }
                cancelled.len() as i64 >= BATCH
            }
            Err(error) => {
                tracing::warn!(%error, "could not cancel workflows past their deadline");
                false
            }
        };
        if !full {
            tokio::time::sleep(interval).await;
        }
    }
}
