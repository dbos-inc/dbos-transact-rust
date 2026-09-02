//! The management surface: finding workflows, and cancelling, resuming, forking, delaying and
//! deleting them.
//!
//! **The operator's half of the API.** Everything here addresses a workflow by id rather than by
//! calling it, and the process doing the addressing is usually a tool, a Conductor session, or an
//! admin endpoint rather than a host that can run the work — it may not even have the code. The
//! surface is shaped by that: nothing here runs a workflow.
//!
//! [`resume`](DBOS::resume) and [`fork`](DBOS::fork) each write an `ENQUEUED` row and leave it for
//! whichever executor next polls that queue, which is what every reference does — so both hand back
//! a **polling** [`WorkflowHandle`]. Awaiting one watches the database, because this process is very
//! probably not the one doing the work.
//!
//! # Naming
//!
//! A method names its noun exactly when the verb would otherwise be ambiguous in this crate. A
//! queue can be deleted and so can a workflow, so [`delete`](DBOS::delete) sits beside
//! [`delete_queue`](DBOS::delete_queue) and both say what they take; nothing but a workflow can be
//! cancelled, resumed or forked, so those are bare verbs. Listing always says what it lists.
//!
//! The references spell every one of these `*_workflow`/`*_workflows`, because their entry point is
//! a bare `DBOS` class rather than a handle you already hold. The concepts are theirs and the
//! spelling is this crate's, which is the same trade [`ForkFrom`] makes against
//! `fork_from_failure`.
//!
//! # Bulk forms
//!
//! Every operation that has one in the references has one here, suffixed `_all`, and the singular
//! form is a caller with one id. [`retrieve_workflow`](DBOS::retrieve_workflow),
//! [`set_workflow_delay`](DBOS::set_workflow_delay) and
//! [`update_workflow_attributes`](DBOS::update_workflow_attributes) have none, in this crate or in
//! any reference: each addresses one row and none of them is worth a round trip to batch.
//! The bulk form is the primitive: the system database cancels, deletes and forks in batches because a
//! partially applied batch is worse than a slow one, and the singular methods are wrappers that
//! pass a one-element slice.
//!
//! Cascading to descendants lives on the bulk form alone — `cancel_all(&["one"],
//! Children::Include)` is how one workflow and its tree are cancelled. The singular form is the
//! common case, and the common case is one workflow.
//!
//! # Two things this surface deliberately does not do
//!
//! **It does not stop a workflow already running in this process.** Cancelling writes `CANCELLED`
//! and the running execution finds out by reading, not by being interrupted: a preemptible step
//! polls the status and abandons its attempt, and any other step finishes before the workflow's own
//! terminal write is refused by the status gate on `record_workflow_outcome`. That is what makes
//! cancelling work at all across a fleet, where the executor running the workflow is usually not the
//! one being asked to cancel it.
//!
//! That also keeps this crate clear of go #426, where shutdown's context cancellation reached the
//! *durable* cancel path and marked in-flight workflows `CANCELLED` instead of leaving them
//! `PENDING` for recovery. Rust's shutdown aborts tasks and says so — see `Tasks::abort_all`,
//! whose rows stay `PENDING` — and durable cancellation is only ever this module writing to the
//! database. The two paths never meet, so there is no shutdown cause to tag.
//!
//! **It does not record itself as a step.** Python routes every one of these through
//! `call_function_as_step`, so a management call made *from inside a workflow* is checkpointed and
//! replays instead of running twice; TypeScript's `runInternalStep` and Go's `RunAsStep` are the
//! same mechanism. Rust has no equivalent, so calling these from inside a workflow body re-runs
//! them on every replay. Calling them from an operator's tool, which is what they are for, is
//! unaffected.

use std::sync::Arc;
use std::time::Duration;

use crate::dbos::{DBOS, Executor};
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::sysdb::types::{
    Fork, ForkOptions as SysForkOptions, ForkPoint, StepRecord, WorkflowDelay, WorkflowFilter,
    WorkflowRecord,
};

/// Whether an operation reaches a workflow's descendants.
///
/// One type for [`cancel_all`](DBOS::cancel_all) and [`delete_all`](DBOS::delete_all), which the
/// references keep apart as `cancel_children` and `delete_children` booleans. It is the same
/// question in both, and a bare `true` at a call site says neither which question it answers nor
/// which way.
///
/// [`Skip`](Self::Skip) is the default in all four implementations, and the reason is worth
/// knowing: a workflow's children are workflows in their own right, some of them started by code
/// that has no idea it was called from another workflow. Reaching them is a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Children {
    /// The workflows named, and nothing else.
    #[default]
    Skip,
    /// The workflows named, and everything descended from them at any depth.
    ///
    /// The walk is the system database's, and the two operations walk differently on purpose:
    /// cancelling interleaves level by level so a parent cannot spawn behind the sweep, while
    /// deleting collects the whole tree first, because a deleted parent cannot spawn at all.
    Include,
}

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

/// Where a resumed workflow goes.
///
/// A struct for one field, because it is the field every reference has and none of them stopped
/// there — and because `resume_all(&ids, None)` says nothing at a call site about what was
/// declined.
///
/// TODO(dbos-team): UPSTREAM item 28. One field is also all any reference has. Resume takes a
/// queue name and no partition key in all five, and the `UPDATE` behind it moves `queue_name`
/// while leaving `queue_partition_key` untouched — so resuming onto a partitioned queue either
/// carries over a key belonging to whatever queue the workflow was on before, or, for a workflow
/// that never had one, writes the unkeyed row that
/// [`ForkOptions::queue_partition_key`] exists to prevent. Deliberately not closed here alone:
/// the gap is the contract's, and a field no reference has would put this crate's `resume` ahead
/// of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResumeOptions<'a> {
    /// The queue the workflow is re-enqueued on. `None` is the engine's internal queue.
    ///
    /// A named queue is how a resumed workflow is made to wait its turn: the internal queue takes
    /// no limits, so resuming a large backlog onto it re-enqueues everything and then dequeues
    /// everything. Sending it to a queue with a concurrency limit is the way to resume a backlog
    /// without flooding the fleet — and, with an `application_version`-eligible executor pool, the
    /// way to steer resumed work at a particular deployment.
    pub queue: Option<&'a str>,
}

/// What a fork inherits, and where it goes.
///
/// **`None` does not mean the same thing across these fields.**
/// [`application_version`](Self::application_version) is the only one that falls back to the
/// source's; the queue, its partition and the timeout are the *fork's own*, because a fork is
/// enqueued where the caller says rather than where its source ran. Saying nothing about those
/// three asks for the internal queue, no partition, and no bound — not "as before".
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
    /// The partition of that queue, which a partitioned [`queue`](Self::queue) requires.
    ///
    /// **Not inherited.** A source enqueued under a key does not pass it on; the fork's column is
    /// written from this field, so `None` is no partition even when the source had one.
    ///
    /// Leaving it out on a partitioned queue produces a fork that can never run. Such a queue is
    /// swept one partition at a time and every read that does so is keyed — see
    /// `get_queue_partitions`, which selects `WHERE queue_partition_key IS NOT NULL` — so an
    /// unkeyed row belongs to no partition and no sweep will ever see it. It stays `ENQUEUED`,
    /// and the handle waits on a workflow nothing will pick up.
    ///
    /// All four references carry this on their fork options, and for this reason: Python's
    /// `queue_partition_key`, Go's `QueuePartitionKey`, TypeScript's `queuePartitionKey`, and
    /// Java's `ForkFromFailureOptions::queuePartitionKey`.
    pub queue_partition_key: Option<&'a str>,
    /// How long the fork may run once it starts. `None` is unbounded, not the source's bound.
    pub timeout: Option<Duration>,
}

impl DBOS {
    /// A handle to a workflow this process did not start.
    ///
    /// The way a listing becomes something to act on: [`list_workflows`](Self::list_workflows)
    /// hands back rows, and this turns one of their ids into a handle that can be awaited. The
    /// handle is a **polling** one — see the module documentation — because the workflow is
    /// running somewhere else, if it is running at all.
    ///
    /// **The id is not checked, and the call does no I/O.** The references split two-two — Python
    /// verifies the row and raises `DBOSNonExistentWorkflowError`, Go reads it as part of the call,
    /// while TypeScript and Java hand back a handle either way, Java's doc saying "the workflow
    /// exists or not; `getStatus()` can be used to tell the difference".
    ///
    /// **The split is downstream of what awaiting a missing row does**, and this crate is on
    /// TypeScript's and Java's side of it. All five give `await_workflow_result` a
    /// `fail_if_missing` flag whose default is to wait, so an id nothing has seen — which is
    /// exactly what this function hands back — polls for the row to appear rather than reporting
    /// its absence. That is the case the default is for: an id from outside this process, awaited
    /// before whoever owns it has committed the enqueue. The cost is the references': a *mistyped*
    /// id awaited through this handle waits instead of failing.
    ///
    /// So the check would buy something, and what it costs is this call: a verified retrieve is
    /// `async` and fallible on the id as well as the launch, and mapping a listing to handles then
    /// costs a round trip apiece. [`status`](crate::WorkflowHandle::status) is the round trip when
    /// it is wanted — it reports [`Error::WorkflowNotFound`] whatever the handle — and
    /// `tokio::time::timeout` is the bound when a wait should not be open-ended, which is the
    /// escape Python and Java cannot offer at all.
    ///
    /// It is not the same line [`resume`](Self::resume) draws, and deliberately: a resume is a
    /// write that would otherwise silently do nothing, and its handles name rows the call has just
    /// moved.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle = dbos.retrieve_workflow::<u32, dbos::EngineOnly>("started-elsewhere")?;
    /// let status = handle.status().await?;
    /// # Ok(()) }
    /// ```
    pub fn retrieve_workflow<R, E>(&self, workflow_id: &str) -> Result<WorkflowHandle<R, E>> {
        let executor = self.executor("retrieve a workflow")?;
        // Nothing here has seen the row: the id is the caller's, taken on faith, which is the one
        // handle shape that waits for a row to appear rather than reporting it missing.
        Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            workflow_id.to_owned(),
            false,
        ))
    }

    /// Stops a workflow, and any executor running it.
    ///
    /// The row goes to `CANCELLED`, which is terminal: awaiting the workflow raises
    /// [`Error::WorkflowCancelled`] rather than returning a value, and an executor still running it
    /// finds out at its next read — see the module documentation for why cancelling reads rather
    /// than interrupts.
    ///
    /// **A cancelled workflow can be resumed.** [`resume`](Self::resume) puts it back on a queue
    /// with its recorded steps intact, so cancelling is a pause an operator can undo, unlike
    /// [`delete`](Self::delete). That is why cancelling is the graceful form and deleting is not.
    ///
    /// Cancelling a workflow that has already finished does nothing and is not an error, and
    /// neither is cancelling an id with no row: a cancel that finds nothing has the end state it
    /// asked for. [`resume`](Self::resume) draws the opposite line for the opposite reason.
    ///
    /// Children are left alone — [`cancel_all`](Self::cancel_all) with [`Children::Include`] is how
    /// a tree is cancelled.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// dbos.cancel("runaway-workflow").await?;
    /// # Ok(()) }
    /// ```
    pub async fn cancel(&self, workflow_id: &str) -> Result<()> {
        self.cancel_all(&[workflow_id], Children::Skip).await?;
        Ok(())
    }

    /// Cancels workflows, and optionally everything descended from them.
    ///
    /// Returns the ids that actually moved, which is a subset: one already finished is left where
    /// it is, and with [`Children::Include`] the list also carries the descendants that were
    /// cancelled, which the caller never named.
    ///
    /// The cascade is a **level-by-level walk that interleaves with the cancelling**, so a child
    /// spawned while the sweep is running is still caught — the parent is stopped before its
    /// children are looked up. Python and Java do the same; Go collects the whole subtree first and
    /// can miss a workflow spawned during the walk.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // One workflow and its whole tree.
    /// dbos.cancel_all(&["fan-out-root"], dbos::Children::Include).await?;
    /// # Ok(()) }
    /// ```
    pub async fn cancel_all(
        &self,
        workflow_ids: &[&str],
        children: Children,
    ) -> Result<Vec<String>> {
        let executor = self.executor("cancel a workflow")?;
        let cancelled = executor
            .sysdb()
            .cancel_workflows(workflow_ids, children == Children::Include)
            .await
            .map_err(Error::SystemDatabase)?;
        if !cancelled.is_empty() {
            tracing::info!(cancelled = cancelled.len(), "cancelled workflows");
        }
        Ok(cancelled)
    }

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
        self.resume_with(workflow_id, ResumeOptions::default())
            .await
    }

    /// Resumes a workflow onto a queue of the caller's choosing.
    ///
    /// [`resume`](Self::resume) for the common case, which is the engine's internal queue.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // Back on a limited queue, so a resumed backlog does not flood the fleet.
    /// let handle = dbos.resume_with::<u32, dbos::EngineOnly>(
    ///     "stalled-workflow",
    ///     dbos::ResumeOptions { queue: Some("recovery") },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub async fn resume_with<R, E>(
        &self,
        workflow_id: &str,
        options: ResumeOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        self.resume_all(&[workflow_id], options)
            .await?
            .pop()
            .ok_or_else(|| Error::Config(format!("resuming `{workflow_id}` produced no handle")))
    }

    /// Resumes workflows, handing back one handle per id, in the order given.
    ///
    /// One handle per id *asked for*, not per row that moved. A workflow that had already finished
    /// is not re-enqueued, and its handle simply reports the outcome it already had — which is what
    /// [`resume`](Self::resume) promises for one workflow, kept for many.
    ///
    /// Every id must exist. The whole batch is one statement, so an id with no row behind it fails
    /// the call and nothing is enqueued, rather than resuming the ones that happened to be spelled
    /// correctly. Python and Java draw the same line on the batch; **Go deliberately does not**,
    /// and says so — its `ResumeWorkflows` skips a missing id where its `ResumeWorkflow` refuses
    /// one. Following Python here keeps the batch and the single form answering the same way.
    pub async fn resume_all<R, E>(
        &self,
        workflow_ids: &[&str],
        options: ResumeOptions<'_>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        let executor = self.executor("resume a workflow")?;
        executor
            .sysdb()
            .resume_workflows(workflow_ids, options.queue)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(
            count = workflow_ids.len(),
            "resumed workflows onto their queues"
        );
        Ok(workflow_ids
            .iter()
            // `resume_workflows` refuses an id with no row, so each of these named one a moment
            // ago: a row missing from here was deleted, and waiting for it is waiting for nothing.
            .map(|id| {
                WorkflowHandle::polling(Arc::clone(executor.connection()), (*id).to_owned(), true)
            })
            .collect())
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
        let forked =
            fork_batch(&executor, &[workflow_id], from, options.forked_id, &options).await?;

        let forked_id = forked.into_iter().next().ok_or_else(|| {
            Error::Config(format!("forking `{workflow_id}` produced no workflow"))
        })?;
        tracing::info!(workflow_id, forked_id, "forked the workflow onto its queue");
        // The fork's row was written by the call above.
        Ok(WorkflowHandle::polling(
            Arc::clone(executor.connection()),
            forked_id,
            true,
        ))
    }

    /// Forks workflows from the same point, handing back a handle to each fork in the order given.
    ///
    /// The batch is the system database's primitive rather than a loop over
    /// [`fork_with`](Self::fork_with), and the difference is visible: every source must exist and
    /// must have something at the fork point, or the call fails and **no fork is written**. A
    /// half-applied batch would leave forks whose siblings never existed, which for a fan-out being
    /// re-run against fixed code is worse than forking nothing.
    ///
    /// `from` applies to every source, and each source's own history decides where that lands —
    /// [`ForkFrom::LastFailure`] forks each from wherever *it* failed, not from a step number
    /// worked out once.
    ///
    /// [`ForkOptions::forked_id`] is refused here: one id cannot name many forks, and generating
    /// them silently would hand back a fork under an id the caller did not ask for. Every other
    /// option applies to the whole batch, which is what makes
    /// [`application_version`](ForkOptions::application_version) useful — re-running a fan-out
    /// against the deployment that fixes it is the case this method exists for.
    pub async fn fork_all<R, E>(
        &self,
        workflow_ids: &[&str],
        from: ForkFrom<'_>,
        options: ForkOptions<'_>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        // The launch check first, as every other method on this surface does: an unlaunched
        // instance should say so whatever else is wrong with the call.
        let executor = self.executor("fork a workflow")?;
        if options.forked_id.is_some() {
            return Err(Error::Config(
                "ForkOptions::forked_id names a single fork and cannot be used with fork_all"
                    .to_owned(),
            ));
        }
        let forked = fork_batch(&executor, workflow_ids, from, None, &options).await?;
        tracing::info!(count = forked.len(), "forked workflows onto their queues");
        Ok(forked
            .into_iter()
            // Each fork's row was written by the batch above.
            .map(|id| WorkflowHandle::polling(Arc::clone(executor.connection()), id, true))
            .collect())
    }

    /// Removes a workflow and everything recorded against it.
    ///
    /// Steps, events, messages and streams go with the row — the schema cascades — so this is not
    /// a status change and there is nothing left to resume. [`cancel`](Self::cancel) is the form
    /// that stops a workflow and keeps it.
    ///
    /// **No status guard, deliberately.** A running workflow is deleted like any other: naming an
    /// id is an operator saying *this one, now*, and refusing would leave no way to clear a
    /// workflow that is wedged. All four implementations behave the same way — Go's comment on the
    /// same statement reads "Delete all matching workflows regardless of their state".
    ///
    /// The executor running a deleted workflow finds its row gone at the next step boundary and
    /// fails with `NonExistentWorkflow`, rather than stopping cleanly. Cancel first if that matters.
    ///
    /// Deleting an id with no row is not an error, and children are left alone —
    /// [`delete_all`](Self::delete_all) with [`Children::Include`] is how a tree goes.
    pub async fn delete(&self, workflow_id: &str) -> Result<()> {
        self.delete_all(&[workflow_id], Children::Skip).await?;
        Ok(())
    }

    /// Deletes workflows, and optionally everything descended from them.
    ///
    /// Returns how many rows went, descendants included.
    ///
    /// Unlike [`cancel_all`](Self::cancel_all), the tree is collected first and deleted in one
    /// statement rather than level by level. Cancelling has to interleave so a parent cannot spawn
    /// behind the walk; a deleted parent cannot spawn at all.
    pub async fn delete_all(&self, workflow_ids: &[&str], children: Children) -> Result<u64> {
        let executor = self.executor("delete a workflow")?;
        let deleted = executor
            .sysdb()
            .delete_workflows(workflow_ids, children == Children::Include)
            .await
            .map_err(Error::SystemDatabase)?;
        if deleted > 0 {
            tracing::info!(deleted, "deleted workflows");
        }
        Ok(deleted)
    }

    /// Holds a queued workflow back, or lets it go sooner.
    ///
    /// **Only a `DELAYED` row moves.** A workflow that has already been released is running or
    /// queued, and pushing its delay out would not recall it — so this is a way to reschedule work
    /// that is still waiting, not a way to pause work that has started.
    /// [`cancel`](Self::cancel) is the one that stops something.
    ///
    /// [`WorkflowDelay::For`] is resolved against **this process's** clock, the same as the delay
    /// on an enqueue and the same as every reference: Python's `time.time()`, Go's `time.Now()`,
    /// TypeScript's `Date.now()`, Java's `Instant.now()`. So a caller's skew does reach the row.
    ///
    /// **And a second clock decides when the row is acted on.** The supervisor that releases a
    /// `DELAYED` workflow compares the stamp against its *own* reading, so the moment a fleet
    /// observes is two clocks away from the one that set it — the setter's and the releaser's,
    /// neither of them the database's. `init_workflow` carries the reasoning for leaving it
    /// there, and UPSTREAM item 22 the proposal to close it everywhere at once rather than here
    /// alone.
    ///
    /// [`WorkflowDelay::Until`] removes the first of those readings: an absolute instant is
    /// written as given, and nothing on this path consults a clock to do it.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// use std::time::Duration;
    /// // Not before the hour is up.
    /// dbos.set_workflow_delay("scheduled-report", dbos::WorkflowDelay::For(Duration::from_secs(3600))).await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_workflow_delay(&self, workflow_id: &str, delay: WorkflowDelay) -> Result<()> {
        let executor = self.executor("delay a workflow")?;
        executor
            .sysdb()
            .set_workflow_delay(workflow_id, delay)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(workflow_id, "moved the workflow's release time");
        Ok(())
    }

    /// Replaces a workflow's attributes, or clears them with `None`.
    ///
    /// **A replacement, not a merge**, in every implementation — so a caller adding one key must
    /// send the others back with it.
    ///
    /// **A map, not encoded JSON.** The other three take one — Python `Dict[str, Any]`, Go
    /// `map[string]any`, Java `Map<String, Object>` — and encode it in their system database layer.
    /// Rust's cannot: it handles every payload as an opaque string so a host across an FFI boundary
    /// can hand over bytes it already has, which is why `validate_attributes` checks the object
    /// shape down there *"rather than falling out of the type"*. Here it falls out of the type.
    /// This is the layer that encodes, and it is the only one that can.
    ///
    /// The object shape is not decoration: [`WorkflowFilter::attributes`] queries the column with
    /// `@>` containment, and containment against a stored array or scalar silently never matches.
    ///
    /// `serde_json::Value::as_object` is the usual way in — and note that it answers `None` for a
    /// value that is not an object, which **clears** rather than refusing. Build a
    /// `serde_json::Map` directly when the attributes are assembled at runtime.
    ///
    /// **Named for what three of the four call it.** Python's is `update_workflow_attributes` and
    /// Java's `updateWorkflowAttributes`; TypeScript has no attributes method at all; and Go's
    /// method is `SetWorkflowAttributes` while the step it records is `DBOS.updateWorkflowAttributes`
    /// — so Go disagrees with itself, and the stored name is the half that other implementations
    /// read.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let tags = serde_json::json!({ "tenant": "acme", "tier": "gold" });
    /// dbos.update_workflow_attributes("an-order", tags.as_object()).await?;
    /// dbos.update_workflow_attributes("an-order", None).await?; // and cleared
    /// # Ok(()) }
    /// ```
    pub async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Result<()> {
        let executor = self.executor("update a workflow's attributes")?;
        // Plain JSON, never the configured [`Serializer`](crate::Serializer): the column is read by
        // `@>` containment and by every other implementation, so what a workflow chose for its own
        // payloads has no say in it.
        let encoded = attributes
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| Error::Serialization {
                what: "attributes".into(),
                message: error.to_string(),
                source: Some(error),
            })?;
        executor
            .sysdb()
            .update_workflow_attributes(workflow_id, encoded.as_deref())
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(workflow_id, "updated the workflow's attributes");
        Ok(())
    }

    /// Reads the workflows matching a filter, oldest first unless the filter says otherwise.
    ///
    /// **Every filter is one `WHERE` clause**, not a scan the caller narrows afterwards, so
    /// `WorkflowFilter::default()` returns the whole table. A caller that means to page should say
    /// so with [`WorkflowFilter::limit`] — and should know that `created_at` is a millisecond
    /// stamp shared by everything a fan-out creates in the same instant, so a page boundary
    /// falling inside a tie is not stable. That is `UPSTREAM` item 23, and it is four
    /// implementations wide.
    ///
    /// The filter defaults to **this application's workflows plus unclaimed ones**, not to every
    /// application sharing the database; [`Applications::Any`](crate::sysdb::types::Applications)
    /// is the operator's cross-application view. Naming ids explicitly widens it on its own, since
    /// a workflow id is a global address rather than a search.
    ///
    /// There is no separate `list_queued_workflows` as Python and TypeScript have.
    /// [`WorkflowFilter::queues_only`] is that method, and it composes with every other filter
    /// instead of being a second entry point that repeats them.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// use dbos::sysdb::types::{WorkflowFilter, WorkflowStatus};
    ///
    /// let stuck = dbos.list_workflows(&WorkflowFilter {
    ///     status: vec![WorkflowStatus::Pending],
    ///     limit: Some(100),
    ///     load_input: false,
    ///     ..WorkflowFilter::default()
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub async fn list_workflows(&self, filter: &WorkflowFilter<'_>) -> Result<Vec<WorkflowRecord>> {
        let executor = self.executor("list workflows")?;
        executor
            .sysdb()
            .list_workflows(filter)
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Reads one workflow's steps, in execution order.
    ///
    /// Outputs and errors come with them. This is a single workflow's own history, bounded by what
    /// that workflow did, so there is no paging here as there is on
    /// [`list_workflows`](Self::list_workflows) — the system database layer carries limit and
    /// offset for the caller that has a workflow long enough to need them.
    ///
    /// A step whose [`child_workflow_id`](StepRecord::child_workflow_id) is set was a child
    /// workflow call rather than a step body, which is how a tree is walked from a listing.
    ///
    /// An id with no row returns no steps rather than failing, the same as an id whose workflow
    /// has not reached its first step.
    pub async fn list_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepRecord>> {
        let executor = self.executor("list a workflow's steps")?;
        executor
            .sysdb()
            .list_workflow_steps(workflow_id, true, None, None)
            .await
            .map_err(Error::SystemDatabase)
    }
}

/// The two system-database calls a fork picks between, and the rule for picking.
///
/// The halves of [`ForkFrom`] are two different questions. A named step is an address and needs no
/// lookup; the other three are searches through each source's own history, and `fork_from` is where
/// that search lives. Shared by [`DBOS::fork_with`] and [`DBOS::fork_all`] so the rule is written
/// once — the batch form is the primitive, and the single form is a batch of one.
async fn fork_batch(
    executor: &Executor,
    workflow_ids: &[&str],
    from: ForkFrom<'_>,
    forked_id: Option<&str>,
    options: &ForkOptions<'_>,
) -> Result<Vec<String>> {
    let sys_options = SysForkOptions {
        application_version: options.application_version,
        queue_name: options.queue,
        queue_partition_key: options.queue_partition_key,
        timeout: options.timeout,
        replacement_children: &[],
    };

    match from {
        ForkFrom::Beginning | ForkFrom::Step(_) => {
            let start_step = match from {
                ForkFrom::Step(step) => step,
                _ => 0,
            };
            let forks: Vec<Fork<'_>> = workflow_ids
                .iter()
                .map(|source_id| Fork {
                    source_id,
                    forked_id,
                    start_step,
                })
                .collect();
            executor.sysdb().fork_workflows(&forks, &sys_options).await
        }
        ForkFrom::LastFailure => {
            executor
                .sysdb()
                .fork_from(workflow_ids, ForkPoint::LastFailure, &sys_options)
                .await
        }
        ForkFrom::LastStep => {
            executor
                .sysdb()
                .fork_from(workflow_ids, ForkPoint::LastStep, &sys_options)
                .await
        }
        ForkFrom::StepNamed(name) => {
            executor
                .sysdb()
                .fork_from(workflow_ids, ForkPoint::StepNamed(name), &sys_options)
                .await
        }
    }
    .map_err(Error::SystemDatabase)
}
