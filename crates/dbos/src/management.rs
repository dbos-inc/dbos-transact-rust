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
//! # Called from inside a workflow
//!
//! Every operation here is **checkpointed as a step of the workflow that calls it**, so a replay
//! reads back what the first execution did instead of doing it again: a fork keeps the id it
//! generated rather than writing a second one, a cancel or a delete is issued once, and a listing
//! replays the rows it saw.
//!
//! **The checkpoint commits with the operation**, in one transaction, because the step id travels
//! down into the system database rather than wrapping the call here — see
//! [`fork_workflows`](crate::sysdb::SystemDatabase::fork_workflows), and
//! [`caller_for`] for the two lines that spend the id. Nothing in this module records a step of
//! its own, and there is no wrapper left to record one: the atomic version costs the same round
//! trips, and it closes a window the wrapper cannot. A crash between the write and its checkpoint
//! would otherwise leave the work done and unrecorded, which for a fork is a second workflow under
//! a second id; and two executions of one workflow racing here both find no checkpoint, where the
//! transaction lets only the one that wins the step row commit its write.
//!
//! **Go draws the same line, from the other side.** Its writes go through `runAsTxn` and its reads
//! through the plain `RunAsStep`; Python, TypeScript and Java run the whole surface through their
//! non-transactional wrapper — `call_function_as_step`, `runInternalStep`,
//! `runDbosFunctionAsStep` — and each keeps a transactional variant for other surfaces. The reads
//! are transactional here too: a listing that replays the snapshot it recorded is worth the same
//! one transaction, and it leaves one shape for the whole module rather than two.
//!
//! The recorded names are the cross-SDK spellings, in
//! [`step_names`](crate::sysdb::types::step_names) beside every other name this crate records,
//! with what each one cost to settle. One call is one step, and the singular forms delegate to the
//! bulk ones, so `cancel` and `cancel_all` spend the same one step id.
//!
//! Three edges, all shared with the references. Outside a workflow there is nothing to checkpoint
//! against and the call is a plain one, which is what an operator's tool does. Inside a *step* it
//! is also plain, by the leaf rule every other id-allocating call in this crate follows: the
//! step's own checkpoint stands for everything its body did. And a **failure records nothing** —
//! the transaction rolls back — so a replay makes the call again, which is what should happen when
//! what failed was the database being unreachable rather than the operation being wrong.
//!
//! [`retrieve_workflow`](DBOS::retrieve_workflow) is the one member with no step, because it is
//! the one that does no I/O: there is no call to replay. Python checkpoints its equivalent as
//! `DBOS.getStatus` because Python's reads the row.
//!
//! # One thing this surface deliberately does not do
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

use std::sync::Arc;
use std::time::Duration;

use crate::context::Ctx;
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
    /// cancelling interleaves level by level, which narrows the window a parent can spawn behind
    /// the sweep in, while deleting collects the whole tree first — the cascade does in one
    /// statement what cancelling needs one per level for. Neither closes the window: a child
    /// committed after the walk has passed its level survives, in both.
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
    /// without flooding the fleet — and, with an `app_version`-eligible executor pool, the
    /// way to steer resumed work at a particular deployment.
    pub queue: Option<&'a str>,
}

/// What a fork inherits, and where it goes.
///
/// **`None` does not mean the same thing across these fields.**
/// [`app_version`](Self::app_version) is the only one that falls back to the
/// source's; the queue, its partition and the timeout are the *fork's own*, because a fork is
/// enqueued where the caller says rather than where its source ran. Saying nothing about those
/// three asks for the internal queue, no partition, and no bound — not "as before".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions<'a> {
    /// The id the fork gets. `None` generates one.
    ///
    /// **Only with a fork point that names its step** — [`ForkFrom::Beginning`] and
    /// [`ForkFrom::Step`]. The three searched points resolve the step inside the write and always
    /// generate the id; naming one alongside them is an [`Error::Config`] rather than a value
    /// quietly discarded.
    ///
    /// That line is every implementation's, drawn by having no parameter to pass an id through:
    /// Python's `fork_from_failure`, TypeScript's `forkFromFailure` and Go's `ForkFromDBInput`
    /// take none, and Java gives the family a separate `ForkFromFailureOptions` with no
    /// `forkedWorkflowId` at all. This crate merges both halves into one [`ForkFrom`] and one
    /// options struct, so what is missing there has to be refused here.
    ///
    /// [`fork_all`](DBOS::fork_all) refuses it whatever the fork point: one id cannot name many
    /// forks. Go carries it per-source on its `ForkWorkflowSpec` instead, which is the same
    /// difference — its id sits beside each source id, and this one sits beside the batch.
    pub forked_id: Option<&'a str>,
    /// The version the fork runs under. `None` inherits the source's.
    ///
    /// **This is what forking is usually for.** A workflow that failed against broken code is
    /// forked onto the fixed deployment, and only executors running that version will dequeue it.
    pub app_version: Option<&'a str>,
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
    /// This instance, refused if it is not the one running the calling workflow — and that
    /// workflow, which the call is checkpointed against.
    ///
    /// Both halves come back together because the check is what relates them: a checkpointed
    /// management call takes its executor from `self` and its step id from the ambient context,
    /// and the two have to be the same instance. Resolving them apart is what let them disagree.
    /// `operation` names the call in either failure, so taking it once is also what keeps the two
    /// messages from drifting.
    ///
    /// The context is `None` outside a workflow, where a management call is just a call — an
    /// operator's tool, an admin endpoint, a test. `None` inside a *step* as well, by the leaf
    /// rule the rest of the crate follows: the step's own checkpoint stands for everything its
    /// body did, and allocating an id under it would shift every later step onto the wrong replay
    /// slot.
    ///
    /// **A workflow running on another instance is [`Error::WrongInstance`].** This is where the
    /// two halves of a checkpointed management call are combined: the step id comes from the
    /// ambient workflow's counter and the checkpoint is written through *this* instance's system
    /// database, so a handle to some other instance would write the row where the workflow that
    /// allocated the id cannot see it — and, since `operation_outputs` carries a foreign key onto
    /// `workflow_status` from migration 1 onward, usually cannot write it at all.
    /// [`get_event`](Self::get_event) and [`WorkflowRef::parent`](crate::WorkflowRef) refuse the
    /// same combination for the same reason. Inside a step there is nothing to refuse: nothing is
    /// checkpointed, so the two halves are never combined and the call is plain whichever
    /// instance serves it.
    ///
    /// **The launch check comes first**, as every method on this surface expects: an unlaunched
    /// instance should say so whatever else is wrong with the call.
    ///
    /// The context is returned rather than the caller pair, and held in a local at each call
    /// site, because [`caller_for`] borrows from it — and because allocating the step id is what
    /// spends it, which must not happen before a call's own argument checks have passed.
    fn checked_executor(&self, operation: &'static str) -> Result<(Arc<Executor>, Option<Ctx>)> {
        let executor = self.executor(operation)?;
        let ctx = Ctx::current().filter(|ctx| !ctx.in_step());
        if ctx
            .as_ref()
            .is_some_and(|ctx| !Arc::ptr_eq(ctx.executor(), &executor))
        {
            return Err(Error::WrongInstance {
                operation: operation.into(),
            });
        }
        Ok((executor, ctx))
    }

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
    /// Doing no I/O also makes this the one member of the surface that records **no step** when it
    /// is called from inside a workflow: there is nothing to replay. Python checkpoints its
    /// equivalent as `DBOS.getStatus` because Python's reads the row.
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

    /// Marks a workflow cancelled, which is how a running execution is told to stop.
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
    /// can miss a workflow spawned during the walk. Here the walk is also one transaction, so a
    /// tree is either wholly cancelled or untouched, and a failure partway leaves nothing to
    /// finish.
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
        let (executor, ctx) = self.checked_executor("cancel a workflow")?;
        let cancelled = executor
            .sysdb()
            .cancel_workflows(
                workflow_ids,
                children == Children::Include,
                ctx.as_ref().map(caller_for),
            )
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
    /// # Resuming a workflow that is still running
    ///
    /// **Nothing checks for one.** Not being terminal is the whole guard, so a `PENDING` row
    /// passes — and `PENDING` means *some executor owns this*, not *this has stopped*. Resuming a
    /// workflow that is executing right now re-enqueues it underneath its own execution: the next
    /// sweep claims the row and dispatches it, and two executions of one id run concurrently.
    /// Neither is told about the other, and the running one is not cancelled, so nothing stops it
    /// at its next step — see the module documentation on why cancelling is the only thing step
    /// preemption watches for. Both run to a conclusion, one records the outcome and the other's
    /// write is refused; the row is then tidy, but any step neither had checkpointed is performed
    /// twice, side effects included.
    ///
    /// **[`cancel`](Self::cancel) first if the workflow may be live.** That gives the running
    /// execution something to observe, so it abandons its attempt at the next preemptible step
    /// rather than running on. It narrows the window rather than closing it: the two calls are
    /// separate, and the resumed execution can start before the old one has read the
    /// cancellation.
    ///
    /// The guard is not simply missing here. `PENDING` cannot say whether the executor that owns
    /// it is alive, and a workflow left `PENDING` by a node that died is the case an operator most
    /// wants to resume by hand — so a predicate that closes the hazard closes that too. All five
    /// implementations share the two-status deny-list, and UPSTREAM item 27 asks them to settle
    /// what resume should mean for a live row rather than each tightening it alone.
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
        let (executor, ctx) = self.checked_executor("resume a workflow")?;
        let resumed = executor
            .sysdb()
            .resume_workflows(workflow_ids, options.queue, ctx.as_ref().map(caller_for))
            .await
            .map_err(Error::SystemDatabase)?;
        // What moved, not what was asked for: an id that had already finished is not
        // re-enqueued, and it still gets a handle below.
        tracing::info!(
            requested = workflow_ids.len(),
            resumed = resumed.len(),
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
    /// [`ForkOptions::app_version`] exists.
    ///
    /// The source is not modified beyond being marked as forked from; the fork gets its own id,
    /// generated unless [`ForkOptions::forked_id`] names one — which only the fork points that
    /// name their step accept, as in every other implementation.
    ///
    /// **Enqueued, never started.** The handle is a polling one — see the module documentation.
    ///
    /// Called from inside a workflow, the fork is checkpointed as a step **in the transaction
    /// that writes the fork**, so a replay hands back the id the first execution generated rather
    /// than writing a second fork, and no crash in between can leave the two disagreeing. The
    /// whole surface works that way; this is the operation that most needs it, being the only one
    /// that is not idempotent. See the module documentation.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // Re-run what failed, against the deployment that fixes it.
    /// let handle = dbos.fork_with::<u32, dbos::EngineOnly>(
    ///     "failed-workflow",
    ///     dbos::ForkFrom::LastFailure,
    ///     dbos::ForkOptions { app_version: Some("v2"), ..Default::default() },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub async fn fork_with<R, E>(
        &self,
        workflow_id: &str,
        from: ForkFrom<'_>,
        options: ForkOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        let (executor, ctx) = self.checked_executor("fork a workflow")?;
        let forked = fork_batch(
            &executor,
            &[workflow_id],
            from,
            options.forked_id,
            &options,
            ctx,
        )
        .await?;

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
    /// [`app_version`](ForkOptions::app_version) useful — re-running a fan-out
    /// against the deployment that fixes it is the case this method exists for.
    pub async fn fork_all<R, E>(
        &self,
        workflow_ids: &[&str],
        from: ForkFrom<'_>,
        options: ForkOptions<'_>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        // Which instance is serving this, and whether a workflow is asking — before the
        // argument checks, as every other method on this surface does: an unlaunched instance,
        // or one that is not the caller's, should say so whatever else is wrong with the call.
        let (executor, ctx) = self.checked_executor("fork a workflow")?;
        if options.forked_id.is_some() {
            return Err(Error::Config(
                "ForkOptions::forked_id names a single fork and cannot be used with fork_all"
                    .to_owned(),
            ));
        }
        let forked = fork_batch(&executor, workflow_ids, from, None, &options, ctx).await?;
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
    ///
    /// **A workflow cannot delete itself.** Called from inside the workflow it names, this fails
    /// with [`Error::SystemDatabase`] carrying `InvalidInput` and nothing is deleted; see
    /// [`delete_all`](Self::delete_all) for why.
    pub async fn delete(&self, workflow_id: &str) -> Result<()> {
        self.delete_all(&[workflow_id], Children::Skip).await?;
        Ok(())
    }

    /// Deletes workflows, and optionally everything descended from them.
    ///
    /// Returns how many rows went, descendants included.
    ///
    /// Unlike [`cancel_all`](Self::cancel_all), the tree is collected first and deleted in one
    /// statement rather than level by level — the schema's cascade does what cancelling needs a
    /// statement per level for.
    ///
    /// **A tree that is still running can outlive the walk.** A child committed after the walk
    /// has read its parent's level is not in the target set, and survives with
    /// `parent_workflow_id` naming a row that is gone. Interleaving would not fix it and neither
    /// would a wider transaction; [`cancel_all`](Self::cancel_all) with [`Children::Include`]
    /// first, then deleting, is how a tree is stopped before it is removed.
    ///
    /// **A workflow cannot delete itself, or an ancestor it would go down with.** From inside a
    /// workflow this call is a step, and the step's checkpoint is written in the same transaction
    /// as the delete — against a row the cascade has just removed. Rather than let that fail as a
    /// foreign key violation, a target set containing the calling workflow is refused whole:
    /// [`Error::SystemDatabase`] carrying `InvalidInput`, and nothing is deleted. Deleting the
    /// caller's own tree from outside it, or deleting an unrelated tree from inside a workflow,
    /// is unaffected.
    pub async fn delete_all(&self, workflow_ids: &[&str], children: Children) -> Result<u64> {
        let (executor, ctx) = self.checked_executor("delete a workflow")?;
        let deleted = executor
            .sysdb()
            .delete_workflows(
                workflow_ids,
                children == Children::Include,
                ctx.as_ref().map(caller_for),
            )
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
    /// [`WorkflowDelay::For`] is resolved against **this process's** clock, once, before the write
    /// — the same as the delay on an enqueue, and the same as every reference: Python's
    /// `time.time()`, Go's `resolveDelayUntil`, TypeScript's `Date.now()`, Java's `Instant.now()`.
    /// So a caller's skew does reach the row.
    ///
    /// **And a second clock decides when the row is acted on.** The workflow is released by
    /// whichever supervisor next runs, comparing the stamp against *its* reading — so the moment a
    /// fleet honours is two clocks away from the one that set it, and a skewed operator host moves
    /// a release time the whole fleet obeys. `init_workflow` carries the reasoning for leaving it
    /// there, and UPSTREAM item 22 the proposal to close it: the database's clock is the answer,
    /// in every implementation at once rather than here alone at the cost of a round trip none of
    /// them spends.
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
        let (executor, ctx) = self.checked_executor("delay a workflow")?;
        executor
            .sysdb()
            .set_workflow_delay(workflow_id, delay, ctx.as_ref().map(caller_for))
            .await
            .map_err(Error::SystemDatabase)?;
        // Phrased as the request rather than the effect. The statement is guarded on the row
        // still being `DELAYED` and reports no count, so this call cannot tell a workflow that
        // was rescheduled from one that had already been released — unlike `cancel_all` and
        // `delete_all`, which log what they counted.
        tracing::info!(workflow_id, "asked to move the workflow's release time");
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
        let (executor, ctx) = self.checked_executor("update a workflow's attributes")?;
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
            .update_workflow_attributes(
                workflow_id,
                encoded.as_deref(),
                ctx.as_ref().map(caller_for),
            )
            .await
            .map_err(Error::SystemDatabase)?;
        // As on `set_workflow_delay`: no count comes back, so an id with no row behind it
        // reaches here indistinguishable from one whose attributes were replaced.
        tracing::info!(workflow_id, "asked to replace the workflow's attributes");
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
        let (executor, ctx) = self.checked_executor("list workflows")?;
        executor
            .sysdb()
            .list_workflows(filter, ctx.as_ref().map(caller_for))
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
        let (executor, ctx) = self.checked_executor("list a workflow's steps")?;
        executor
            .sysdb()
            .list_workflow_steps(workflow_id, true, None, None, ctx.as_ref().map(caller_for))
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
///
/// `ctx` is the calling workflow, from [`DBOS::checked_executor`], passed in rather than read here:
/// both entry points have already resolved it alongside the executor it has to agree with, and
/// reading it again would name this operation a second time in a second place.
async fn fork_batch(
    executor: &Executor,
    workflow_ids: &[&str],
    from: ForkFrom<'_>,
    forked_id: Option<&str>,
    options: &ForkOptions<'_>,
    ctx: Option<Ctx>,
) -> Result<Vec<String>> {
    let sys_options = SysForkOptions {
        application_version: options.app_version,
        queue_name: options.queue,
        queue_partition_key: options.queue_partition_key,
        timeout: options.timeout,
        replacement_children: &[],
    };

    // A chosen id belongs to the half of the surface that names its step. **Every reference draws
    // the same line**, by giving the search half no parameter to pass one through: Python's
    // `fork_from_failure` (`_sys_db.py:1680`) and TypeScript's `forkFromFailure`
    // (`system_database.ts:2004`) generate a UUID per source and take no id; Go's `ForkFromDBInput`
    // (`system_database.go:2773`) has no id field and leaves `ForkedWorkflowIDs` unset; Java splits
    // the options type outright, `ForkFromFailureOptions` carrying only the version, queue and
    // partition key where its `ForkOptions` leads with `forkedWorkflowId`.
    //
    // Java's split makes the mistake unrepresentable, which is the better shape and not one this
    // crate can have: [`ForkFrom`] is one enum and [`ForkOptions`] is one struct, deliberately —
    // see the module documentation on the naming. So the field exists on a call it cannot serve,
    // and saying so is the merged shape's version of Java's missing field. It was silently dropped
    // before, which is the one behaviour no reference has.
    //
    // The other half of [`DBOS::fork_all`]'s refusal, which turns the same field down for the
    // other reason: one id cannot name many forks, whatever the fork point.
    if forked_id.is_some() && !matches!(from, ForkFrom::Beginning | ForkFrom::Step(_)) {
        return Err(Error::Config(
            "ForkOptions::forked_id needs a fork point that names its step: use ForkFrom::Step, \
             or ForkFrom::Beginning, and let the searched fork points generate the id"
                .to_owned(),
        ));
    }

    // After the refusal above, so a fork this call is going to turn down spends no step id.
    let caller = ctx.as_ref().map(caller_for);

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
            executor
                .sysdb()
                .fork_workflows(&forks, &sys_options, caller)
                .await
        }
        ForkFrom::LastFailure => {
            executor
                .sysdb()
                .fork_from(workflow_ids, ForkPoint::LastFailure, &sys_options, caller)
                .await
        }
        ForkFrom::LastStep => {
            executor
                .sysdb()
                .fork_from(workflow_ids, ForkPoint::LastStep, &sys_options, caller)
                .await
        }
        ForkFrom::StepNamed(name) => {
            executor
                .sysdb()
                .fork_from(
                    workflow_ids,
                    ForkPoint::StepNamed(name),
                    &sys_options,
                    caller,
                )
                .await
        }
    }
    .map_err(Error::SystemDatabase)
}

/// Where the caller stands, for `sysdb` to commit the checkpoint against.
///
/// **Allocates the step id, so it is called exactly once per management call.** Mapping it over
/// an `Option<Ctx>` is what keeps that true: the id is spent only when there is a workflow to
/// spend it in.
fn caller_for(ctx: &Ctx) -> (&str, i32) {
    (ctx.workflow_id(), ctx.next_step_id())
}
