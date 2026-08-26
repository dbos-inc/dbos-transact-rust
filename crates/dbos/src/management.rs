//! Resuming and forking: putting a workflow back on a queue for the fleet to run.
//!
//! Both operations were **inert until the engine had a dequeue loop**, which is why they arrive
//! now rather than earlier. Neither runs a workflow; each writes an `ENQUEUED` row and leaves it
//! for whichever executor next polls that queue. That is what every reference does, and for the
//! same reason: the process asking is usually an operator's tool, not a host that can run the
//! workflow — it may not even have the code.
//!
//! So both return a **polling** [`WorkflowHandle`]. Awaiting one watches the database, because
//! this process is very probably not the one doing the work.

use std::sync::Arc;
use std::time::Duration;

use crate::dbos::DBOS;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::sysdb::types::{Fork, ForkOptions as SysForkOptions, ForkPoint};

/// Where a fork picks up.
///
/// Everything *below* the chosen step is copied to the fork and replays instead of running, so the
/// fork reaches that step in the state the original was in when it got there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForkFrom<'a> {
    /// Step zero: the workflow runs again from the top, carrying nothing across.
    #[default]
    Beginning,
    /// A step the caller has picked out, numbered from zero.
    Step(i32),
    /// The step that failed, or the last recorded step if none did.
    ///
    /// The fallback is what makes this useful on a workflow killed mid-step, which records no
    /// error at all. Python, Java and TypeScript each call their equivalent `fork_from_failure`,
    /// from when failure was the only case; Go renamed it once the others existed, and this
    /// follows Go.
    LastFailure,
    /// The last recorded step, failed or not.
    LastStep,
    /// The last step recorded under this name.
    ///
    /// For a workflow whose shape is known: fork from `charge_card`, wherever it happens to fall.
    StepNamed(&'a str),
}

/// What a fork inherits, and where it goes.
///
/// Every field defaults to "the same as the source", which is what a caller who says nothing
/// means.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions<'a> {
    /// The id the fork gets. `None` generates one.
    ///
    /// Ignored unless [`ForkFrom`] names a step outright: a caller who is not choosing the step is
    /// not choosing the id either, which is the rule the system database enforces.
    pub forked_id: Option<&'a str>,
    /// The version the fork runs under. `None` inherits the source's.
    ///
    /// **This is what forking is usually for.** A workflow that failed against broken code is
    /// forked onto the fixed deployment, and only executors running that version will dequeue it.
    pub application_version: Option<&'a str>,
    /// The queue the fork is enqueued on. `None` is the engine's internal queue.
    pub queue: Option<&'a str>,
    /// How long the fork may run once it starts.
    pub timeout: Option<Duration>,
}

impl DBOS {
    /// Puts a workflow back on a queue, and hands back a handle to watch it.
    ///
    /// **Resuming is a fresh start, not a retry.** The recovery-attempt count and the deadline are
    /// both cleared: a workflow parked at the attempt limit would otherwise be parked again on
    /// sight, and one held to a deadline set before it stalled would expire the moment it ran.
    /// Its recorded steps are kept, so it picks up where it stopped rather than starting over —
    /// that is the difference from [`fork`](Self::fork).
    ///
    /// A workflow that has already succeeded or failed is left alone, and the handle simply
    /// reports the outcome it already has. An id with **no row at all** is
    /// [`Error::SystemDatabase`] carrying `NonExistentWorkflow`, because a mistyped id should say
    /// so rather than silently do nothing — a distinction a zero-row update cannot draw, and one
    /// Python draws the same way.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle = dbos.resume::<u32, dbos::EngineOnly>("stalled-workflow").await?;
    /// # Ok(()) }
    /// ```
    pub async fn resume<R, E>(&self, workflow_id: &str) -> Result<WorkflowHandle<R, E>> {
        let executor = self.executor("resume a workflow")?;
        executor
            .sysdb()
            .resume_workflows(&[workflow_id], None)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(workflow_id, "resumed the workflow onto its queue");
        Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            workflow_id.to_owned(),
        ))
    }

    /// Forks a workflow from `from`, enqueueing the fork and handing back a handle to it.
    ///
    /// [`fork_with`](Self::fork_with) for the options; this is the common case.
    pub async fn fork<R, E>(
        &self,
        workflow_id: &str,
        from: ForkFrom<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        self.fork_with(workflow_id, from, ForkOptions::default())
            .await
    }

    /// Forks a workflow, choosing what the fork inherits and where it runs.
    ///
    /// A fork is a **new workflow** that inherits its source's identity — name, inputs, roles,
    /// attributes — along with the recorded results of every step below the fork point. Those
    /// steps replay rather than run, so the fork arrives at the fork point in the state the
    /// original was in, and carries on from a moment that has already happened. Re-running failed
    /// work against fixed code is what it is for, which is why
    /// [`ForkOptions::application_version`] exists.
    ///
    /// The source is not modified beyond being marked as forked from; the fork gets its own id,
    /// generated unless [`ForkOptions::forked_id`] names one.
    ///
    /// **Enqueued, never started.** The handle is a polling one — see the module documentation.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // Re-run what failed, against the deployment that fixes it.
    /// let handle = dbos.fork_with::<u32, dbos::EngineOnly>(
    ///     "failed-workflow",
    ///     dbos::ForkFrom::LastFailure,
    ///     dbos::ForkOptions { application_version: Some("v2"), ..Default::default() },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub async fn fork_with<R, E>(
        &self,
        workflow_id: &str,
        from: ForkFrom<'_>,
        options: ForkOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        let executor = self.executor("fork a workflow")?;
        let sys_options = SysForkOptions {
            application_version: options.application_version,
            queue_name: options.queue,
            queue_partition_key: None,
            timeout: options.timeout,
            replacement_children: &[],
        };

        // Two system-database calls rather than one, because the two halves of `ForkFrom` are two
        // different questions. A named step is an address and needs no lookup; the other three are
        // searches through the source's own history, and `fork_from` is where that search lives.
        let forked = match from {
            ForkFrom::Beginning | ForkFrom::Step(_) => {
                let start_step = match from {
                    ForkFrom::Step(step) => step,
                    _ => 0,
                };
                executor
                    .sysdb()
                    .fork_workflows(
                        &[Fork {
                            source_id: workflow_id,
                            forked_id: options.forked_id,
                            start_step,
                        }],
                        &sys_options,
                    )
                    .await
            }
            ForkFrom::LastFailure => {
                executor
                    .sysdb()
                    .fork_from(&[workflow_id], ForkPoint::LastFailure, &sys_options)
                    .await
            }
            ForkFrom::LastStep => {
                executor
                    .sysdb()
                    .fork_from(&[workflow_id], ForkPoint::LastStep, &sys_options)
                    .await
            }
            ForkFrom::StepNamed(name) => {
                executor
                    .sysdb()
                    .fork_from(&[workflow_id], ForkPoint::StepNamed(name), &sys_options)
                    .await
            }
        }
        .map_err(Error::SystemDatabase)?;

        let forked_id = forked.into_iter().next().ok_or_else(|| {
            Error::Config(format!("forking `{workflow_id}` produced no workflow"))
        })?;
        tracing::info!(workflow_id, forked_id, "forked the workflow onto its queue");
        Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            forked_id,
        ))
    }
}
