//! The management surface: finding workflows, and cancelling, resuming, forking, rewinding,
//! delaying and deleting them.
//!
//! **The operator's half of the API.** Everything here addresses a workflow by id rather than by
//! calling it, and the process doing the addressing is usually a tool, a Conductor session, or an
//! admin endpoint rather than a host that can run the work — it may not even have the code. The
//! surface is shaped by that: nothing here runs a workflow.
//!
//! [`resume`](DBOS::resume), [`fork`](DBOS::fork) and [`rewind`](DBOS::rewind) each write an
//! `ENQUEUED` row and leave it for whichever executor next polls that queue — so all three hand
//! back a **polling** [`WorkflowHandle`]. Awaiting one watches the database, because this process
//! is very probably not the one doing the work.
//!
//! **The whole surface exists twice**, on [`DBOS`] for an application managing its own workflows
//! and on [`Client`](crate::Client) for a process outside it — which, given the paragraph above, is
//! where an operator usually stands. The operations are the same and share one implementation on
//! the connection beneath both; what differs is the checkpointing described below, and that a
//! client has no launch to check.
//!
//! **And every operation in this module has a third form, a free function, for a workflow
//! body** — see [below](#free-forms-for-a-workflow-body).
//!
//! # Naming
//!
//! A method names its noun exactly when the verb would otherwise be ambiguous in this crate. A
//! queue can be deleted and so can a workflow, so [`delete`](DBOS::delete) sits beside
//! [`delete_queue`](DBOS::delete_queue) and both say what they take; nothing but a workflow can be
//! cancelled, resumed, forked or rewound, so those are bare verbs. Listing always says what it
//! lists.
//!
//! **The free forms spell the noun out.** A free function has no receiver to say what it acts on,
//! and `dbos::cancel(id)` at a call site says nothing about what is being cancelled —
//! `dbos::delete(id)` less still, beside queues and schedules. So they are [`cancel_workflow`],
//! [`resume_workflow`], [`fork_workflow`], [`rewind_workflow`] and [`delete_workflow`], with
//! `_with` where the method has one, and the bulk forms are the plurals [`cancel_workflows`],
//! [`resume_workflows`], [`fork_workflows`] and [`delete_workflows`] where the method says `_all`.
//! A method whose name already says what it acts on keeps it:
//! [`set_workflow_delay`](fn@set_workflow_delay),
//! [`update_workflow_attributes`](fn@update_workflow_attributes),
//! [`merge_workflow_attributes`](fn@merge_workflow_attributes),
//! [`list_workflows`](fn@list_workflows), [`list_workflow_steps`](fn@list_workflow_steps) and
//! [`retrieve_workflow`](fn@retrieve_workflow) are spelled the same both ways.
//!
//! # Bulk forms
//!
//! Every operation that addresses a set of workflows has a bulk form, suffixed `_all` on [`DBOS`]
//! and [`Client`](crate::Client) and pluralised on the free forms, and the singular form is a
//! caller with a one-element batch. [`retrieve_workflow`](DBOS::retrieve_workflow),
//! [`set_workflow_delay`](DBOS::set_workflow_delay),
//! [`update_workflow_attributes`](DBOS::update_workflow_attributes) and
//! [`merge_workflow_attributes`](DBOS::merge_workflow_attributes) have none: each addresses one
//! row and none of them is worth a round trip to batch. The bulk form is the primitive: the
//! system database cancels, deletes and forks in batches because a partially applied batch is
//! worse than a slow one, and the singular methods are wrappers that pass a one-element slice.
//!
//! Cascading to descendants lives on the bulk form alone — `cancel_all(&["one"],
//! Children::Include)` is how one workflow and its tree are cancelled. The singular form is the
//! common case, and the common case is one workflow.
//!
//! # Called from inside a workflow
//!
//! Every operation **on [`DBOS`], and its free form,** is **checkpointed as a step of the workflow
//! that calls it**, so a replay reads back what the first execution did instead of doing it again:
//! a fork keeps the id it generated rather than writing a second one, a cancel or a delete is
//! issued once, and a listing replays the rows it saw.
//!
//! **Each takes its step id where it is written, not where it is first polled**, which is why every
//! one of them is a plain `fn` handing back a [`PendingStep`] — or, for a fork, a resume or a
//! rewind, a [`PendingStart`] wrapping one — rather than an `async fn`; see [`PendingStep`] for
//! what that buys. A management call built beside a step and driven with it by
//! `tokio::join!` therefore takes the same slot on every execution, and one that is refused before
//! it reaches the counter — an unlaunched instance, another instance's handle, an argument the call
//! cannot answer — moves no slot at all.
//!
//! **The same call on a [`Client`](crate::Client) is not**, and cannot be. The step id would have to
//! come from the ambient context, which belongs to a `DBOS` instance the client is not — possibly
//! against another database entirely — so a client's management call runs again on replay. That is
//! the line already drawn for a client's handle awaited inside a workflow
//! ([`WorkflowHandle::result`](crate::WorkflowHandle::result)) and for its
//! [`get_event`](crate::Client::get_event): a client is a guest, and a guest does not write into its
//! host's step sequence.
//!
//! **The checkpoint commits with the operation**, in one transaction, because the step id travels
//! down into the system database rather than wrapping the call here — see
//! [`fork_workflows`](crate::sysdb::SystemDatabase::fork_workflows). Nothing in this module records
//! a step of its own, and there is no wrapper left to record one: the atomic version costs the same
//! round trips, and it closes a window the wrapper cannot. A crash between the write and its
//! checkpoint would otherwise leave the work done and unrecorded, which for a fork is a second
//! workflow under a second id; and an execution that has been handed off is refused by the owner
//! check before the write, in the same transaction, so its work never lands.
//!
//! **The reads are transactional too**, though they write nothing but their checkpoint: a listing
//! that replays the snapshot it recorded is worth the same one transaction, and it leaves one shape
//! for the whole module rather than two.
//!
//! The recorded names are in [`step_names`] beside every other name this crate records, with what
//! each one cost to settle. One call is one step, and the singular forms
//! delegate to the bulk ones, so `cancel` and `cancel_all` spend the same one step id.
//!
//! Three edges. Outside a workflow there is nothing to checkpoint against and the method is a plain
//! call, which is what an operator's tool does; the free form, with no handle and no context to
//! take a connection from, is [`Error::NotInWorkflow`] there. Inside a *step* it is also plain, by
//! the leaf rule every other id-allocating call in this crate follows: the step's own checkpoint
//! stands for everything its body did. And a **failure is recorded only when the database decided
//! it**: "already exists" or "no such workflow" is replayed as the same error, so the replay takes
//! the branch the original run took, while a database that was unreachable records nothing and
//! the replay makes the call again.
//!
//! [`retrieve_workflow`](DBOS::retrieve_workflow) is the one member of the `DBOS` surface with no
//! step, because it is the one that does no I/O: there is no call to replay.
//!
//! # Free forms, for a workflow body
//!
//! **A workflow that manages another workflow should not have to hold a [`DBOS`] to do it.** The
//! registry lives on the instance, so a registered closure that captures a `DBOS` is stored inside
//! the very `Arc` it holds a strong reference to: a cycle, which keeps the instance, its executor
//! and its connection pool alive for the life of the process. A supervisor that cancels a stuck
//! child or forks a failed one from the step that failed would otherwise have had to capture one —
//! the cycle [`get_event`](crate::get_event), [`send`](crate::send()) and
//! [`select_workflow`](fn@crate::select_workflow) already have free forms to avoid, reached by the
//! one door still open. Nothing would reclaim the cycle, and there is no global instance to reach
//! instead, so this surface has free forms.
//!
//! **Each is the method, reached the other way.** A free function takes its connection from the
//! ambient context, so outside a workflow there is none and the call is [`Error::NotInWorkflow`]
//! — that is where the methods and the client are. Inside a step it is plain, as the method is,
//! by the leaf rule. A call with no handle to hand back answers in the caller's error channel, so
//! a workflow with its own error type needs no conversion. A fork, a resume or a rewind hands back
//! a [`PendingStart`], which answers in the handle's channel unless
//! [`lift`](PendingStart::lift) re-declares it — the caller's own whenever the two fail the same
//! way. Their bulk forms answer in the engine's channel, as the methods do, and
//! [`lift`](PendingStep::lift) puts them in the caller's.
//!
//! **One body behind both surfaces.** Each operation is written once, as a private function that
//! takes the connection and builds the [`PendingStep`] — the step name, the argument checks that
//! refuse before an id is spent, the placement, and the run — and the method and the free form
//! are each a line over it, differing only in which connection they hand it and which refusal
//! stands in for not having one ([`Error::NotLaunched`] or [`Error::NotInWorkflow`]). So the two
//! record under the same step names, and a workflow that switches from one to the other between
//! deployments replays the history it recorded. Only the method can raise
//! [`Error::WrongInstance`]: the free form takes its connection and its step id from the one
//! context, so they cannot disagree.
//!
//! [`retrieve_workflow`](fn@retrieve_workflow) has a free form too, for the same reason — a
//! supervisor that has an id and wants to await it needs a handle — and records no step either.
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
//! That also keeps shutdown from marking in-flight workflows `CANCELLED`, which would take them
//! away from recovery instead of leaving them `PENDING` for it. Shutdown waits for workflows or
//! aborts their tasks, and says so — see `Tasks::abort_all`, whose rows stay `PENDING` — and
//! durable cancellation is only ever this module, or a timeout (the timeout sweep, or an execution
//! whose deadline has passed), writing to the database. Shutdown reaches neither, so there is no
//! shutdown cause to tag.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::checkpoint::{Built, PendingStep, StepPlacement, StepWriter};
use crate::connection::Connection;
use crate::error::{DurableError, Error, Result};
use crate::handle::WorkflowHandle;
use crate::instance::{DBOS, Executor};
use crate::sysdb::types::{
    AttributeUpdate, Fork as SysFork, ForkOptions as SysForkOptions,
    RewindOptions as SysRewindOptions, StepRecord, WorkflowDelay, WorkflowFilter, WorkflowRecord,
    duration_to_ms, step_names,
};
use crate::workflow::PendingStart;

/// Whether an operation reaches a workflow's descendants.
///
/// One type for [`cancel_all`](DBOS::cancel_all) and [`delete_all`](DBOS::delete_all), rather than
/// a boolean on each. It is the same question in both, and a bare `true` at a call site says
/// neither which question it answers nor which way.
///
/// [`Skip`](Self::Skip) is the default, and the reason is worth knowing: a workflow's children are
/// workflows in their own right, some of them started by code that has no idea it was called from
/// another workflow. Reaching them is a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Children {
    /// The workflows named, and nothing else.
    #[default]
    Skip,
    /// The workflows named, and everything descended from them at any depth.
    ///
    /// The walk is the system database's, and the two operations walk differently on purpose:
    /// cancelling interleaves level by level, which narrows the window a parent can spawn behind
    /// the sweep in, while deleting collects the whole tree first and removes it in one
    /// transaction, where cancelling needs a statement per level. Neither closes the window: a
    /// child committed after the walk has passed its level survives, in both.
    Include,
}

/// Where a resumed workflow goes.
///
/// A struct for one field, so that options can be added without changing every call site — and
/// because `resume_all(&ids, None)` says nothing at a call site about what was declined.
///
/// **Resume takes a queue name and no partition key.** The `UPDATE` behind it moves `queue_name`
/// and leaves `queue_partition_key` untouched, so resuming onto a partitioned queue either carries
/// over a key belonging to whatever queue the workflow was on before or, for a workflow that never
/// had one, writes the unkeyed row that [`ForkOptions::queue_partition_key`] exists to prevent.
/// A known open gap, shared with the other SDKs on the same database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResumeOptions<'a> {
    /// The queue the workflow is re-enqueued on. `None` is the engine's internal queue.
    ///
    /// A named queue is how a resumed workflow is made to wait its turn: the internal queue takes
    /// no limits, so resuming a large backlog onto it re-enqueues everything and then dequeues
    /// everything. Sending it to a queue with a concurrency limit is the way to resume a backlog
    /// without flooding the fleet — and, with an `app_version`-eligible executor pool, the
    /// way to steer resumed work at a particular deployment.
    pub queue_name: Option<&'a str>,
}

/// One fork: the workflow it is forked from, the step it restarts at, and the id it gets.
///
/// **The element type of [`fork_all`](DBOS::fork_all), and the argument of
/// [`fork_with`](DBOS::fork_with).** These are the fields that vary per fork. A chosen id names
/// exactly one fork, and a batch re-running a fan-out may need each member restarted at its own
/// step, so both travel with the source they belong to; what is uniform across the call is in
/// [`ForkOptions`]. That is the split [`SendMessage`](crate::SendMessage) and
/// [`SendOptions`](crate::SendOptions) make for a send.
///
/// [`Fork::new`] plus functional update, like everything else that takes options here:
///
/// ```no_run
/// # use dbos::Fork;
/// let forks = [
///     Fork { forked_id: Some("order-42-retry"), ..Fork::new("order-42", 3) },
///     Fork::new("order-43", 0),
/// ];
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fork<'a> {
    /// The workflow to fork. It is not modified beyond being marked as forked from.
    ///
    /// Empty is an [`Error::InvalidArgument`].
    pub original_id: &'a str,
    /// The first step the fork runs.
    ///
    /// Every step below it replays from the original's checkpoint, and this step and every one
    /// after it run again. Step ids start at 0, so 0 re-runs the whole workflow. Negative is an
    /// [`Error::InvalidArgument`], and in a batch it refuses the whole call.
    pub start_step: i32,
    /// The id the fork gets. `None` generates one.
    ///
    /// `Some("")` is an [`Error::InvalidArgument`], and so is one id on two forks of a batch, or
    /// an id that is also an original id in the batch.
    ///
    /// An id some existing workflow already has is refused by the database when the fork is
    /// written, and the call fails with [`Error::SystemDatabase`]. The batch is one transaction,
    /// so that usually means nothing was written — but a commit whose acknowledgement was lost is
    /// retried, and the retry hits the forks the commit wrote. A caller that chose its ids can
    /// look them up with [`WorkflowHandle::status`] to tell the two apart.
    pub forked_id: Option<&'a str>,
}

impl<'a> Fork<'a> {
    /// A fork of `original_id` restarting at `start_step`, under a generated id.
    pub fn new(original_id: &'a str, start_step: i32) -> Self {
        Self {
            original_id,
            start_step,
            forked_id: None,
        }
    }

    /// Refuses what the system database would refuse of this fork, as an
    /// [`Error::InvalidArgument`] naming the field as it is spelled here and the workflow it is
    /// about, so a caller can tell which fork in a batch was wrong.
    fn validate(&self) -> Result<()> {
        self.to_sys().validate().map_err(|error| {
            // The rule is the system database's; the value is added here, where it is known.
            let error = match error {
                crate::sysdb::Error::InvalidInput { field, detail } if field == "start_step" => {
                    crate::sysdb::Error::InvalidInput {
                        detail: format!("{detail}, got {}", self.start_step),
                        field,
                    }
                }
                other => other,
            };
            refused_fork(error, |field| {
                if self.original_id.is_empty() {
                    format!("Fork::{field}")
                } else {
                    format!("Fork::{field} of `{}`", self.original_id)
                }
            })
        })
    }

    /// This fork as the system database takes it.
    fn to_sys(self) -> SysFork<'a> {
        SysFork {
            original_id: self.original_id,
            forked_id: self.forked_id,
            start_step: self.start_step,
        }
    }
}

/// What every fork of a call inherits, and where it goes.
///
/// The per-fork fields — the source, the start step and the chosen id — are on [`Fork`].
///
/// **`None` does not mean the same thing across these fields.**
/// [`app_version`](Self::app_version) is the only one that falls back to the
/// source's; the queue, its partition and the timeout are the *fork's own*, because a fork is
/// enqueued where the caller says rather than where its source ran. Saying nothing about those
/// three asks for the internal queue, no partition, and no bound — not "as before".
///
/// A field given as an empty string, or a zero [`timeout`](Self::timeout), is an
/// [`Error::InvalidArgument`]: say `None` to leave it out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions<'a> {
    /// The version the fork runs under. `None` inherits the source's.
    ///
    /// **This is what forking is usually for.** A workflow that failed against broken code is
    /// forked onto the fixed deployment, and only executors running that version will dequeue it.
    pub app_version: Option<&'a str>,
    /// The queue the fork is enqueued on. `None` is the engine's internal queue.
    pub queue_name: Option<&'a str>,
    /// The partition of that queue, which a partitioned [`queue_name`](Self::queue_name) requires.
    ///
    /// **Not inherited.** A source enqueued under a key does not pass it on; the fork's column is
    /// written from this field, so `None` is no partition even when the source had one.
    ///
    /// Leaving it out on a partitioned queue produces a fork that can never run. Such a queue is
    /// swept one partition at a time and every read that does so is keyed — see
    /// `get_queue_partitions`, which selects `WHERE queue_partition_key IS NOT NULL` — so an
    /// unkeyed row belongs to no partition and no sweep will ever see it. It stays `ENQUEUED`,
    /// and the handle waits on a workflow nothing will pick up.
    pub queue_partition_key: Option<&'a str>,
    /// How long the fork may run once it starts. `None` is unbounded, not the source's bound.
    pub timeout: Option<Duration>,
    /// Children to swap in as the fork's steps are copied, as `(original, replacement)` pairs.
    ///
    /// Every copied step that names child `original` is rewritten to name `replacement`. This is
    /// how a fork picks up a corrected child: re-run the failed child (often by forking it), then
    /// fork the parent from the step that awaits it, mapping the old child to the new one. The
    /// fork replays the step that started the child, now naming the replacement, then waits on
    /// the replacement and carries on with its result. The steps before it don't run again.
    ///
    /// **Only the id is rewritten, never a recorded result.** Steps below the start step are
    /// copied, and a copied step that already awaited `original` keeps the result it recorded,
    /// now under the replacement's id. So start the fork at the step that awaits the child, or
    /// earlier: a fork that starts after it returns the original's result. From the start step
    /// on, the fork runs as usual and starts its own children.
    ///
    /// An `original` no copied step names changes nothing. The fork doesn't create
    /// `replacement`: it must exist by the time the fork waits on it.
    /// [`fork_all`](DBOS::fork_all) applies the pairs to every fork.
    ///
    /// Each `original` may appear once. Naming one twice, or giving an empty id on either side,
    /// is an [`Error::InvalidArgument`], and nothing is forked.
    pub replacement_children: &'a [(&'a str, &'a str)],
}

impl<'a> ForkOptions<'a> {
    /// Refuses what the system database would refuse of these options, as an
    /// [`Error::InvalidArgument`] naming the field as it is spelled here.
    fn validate(&self) -> Result<()> {
        self.to_sys().validate().map_err(|error| {
            refused_fork(error, |field| {
                let field = match field {
                    "application_version" => "app_version",
                    other => other,
                };
                format!("ForkOptions::{field}")
            })
        })
    }

    /// These options as the system database takes them.
    fn to_sys(self) -> SysForkOptions<'a> {
        SysForkOptions {
            application_version: self.app_version,
            queue_name: self.queue_name,
            queue_partition_key: self.queue_partition_key,
            timeout: self.timeout,
            replacement_children: self.replacement_children,
        }
    }
}

/// Where a rewind cuts a workflow's history, and where the rewound workflow goes.
///
/// **`None` does not mean the same thing across these fields**, as on [`ForkOptions`]:
/// [`app_version`](Self::app_version) keeps the workflow's own, while the queue and its partition
/// are the rewind's. Saying nothing about those two asks for the internal queue and no partition,
/// not the ones the workflow ran on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RewindOptions<'a> {
    /// The first step to discard. Every step below it replays from its checkpoint; this one and
    /// every one after it run again.
    ///
    /// Step ids start at 0, so the default rewinds the whole workflow. Negative is an
    /// [`Error::InvalidArgument`].
    pub start_step: i32,
    /// The version the workflow is re-run under. `None` keeps the one it has.
    ///
    /// As with a fork, this is how a workflow that failed against broken code is re-run on the
    /// deployment that fixes it: only executors running that version will dequeue it.
    pub app_version: Option<&'a str>,
    /// The queue the workflow is re-enqueued on. `None` is the engine's internal queue.
    pub queue_name: Option<&'a str>,
    /// The partition of that queue, which a partitioned [`queue_name`](Self::queue_name) requires.
    ///
    /// **Not kept.** `None` clears whatever key the workflow had, so a key from the queue it ran
    /// on does not follow it onto another. A partitioned queue sweeps one partition at a time and
    /// never reads an unkeyed row, so leaving this out there produces a workflow that never runs.
    pub queue_partition_key: Option<&'a str>,
}

/// What a step listing reads, and which page of it.
///
/// Constructed with `..Default::default()`, which reads every step, outputs included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListWorkflowStepsOptions {
    /// Whether to read each step's [`output`](StepRecord::output) and
    /// [`error`](StepRecord::error). On by default.
    ///
    /// Off, the listing reads only the steps' metadata — id, name, child workflow, serialization
    /// and timings — and leaves both `None`. The payload columns are left out of the query, so a
    /// workflow whose steps returned large values costs no more to list than one whose steps
    /// returned nothing. A step that returned nothing and a step whose output wasn't read look the
    /// same, and so do a failed step and a successful one: to find the step that failed, list with
    /// this on.
    ///
    /// Called from inside a workflow, the listing is checkpointed, and a replay returns what the
    /// first execution recorded, whatever this says on the replay.
    pub load_output: bool,
    /// The most steps to return, counting from [`offset`](Self::offset). `None` returns them all.
    pub limit: Option<i64>,
    /// How many steps to skip, in step order. `None` skips none.
    pub offset: Option<i64>,
}

impl Default for ListWorkflowStepsOptions {
    fn default() -> Self {
        Self {
            load_output: true,
            limit: None,
            offset: None,
        }
    }
}

/// Marks a workflow cancelled, from inside another workflow, without a handle.
///
/// The free form of [`DBOS::cancel`], for a workflow body: a supervisor that stops a stuck child
/// or a runaway sibling. It takes the executor from the ambient context, so the closure the
/// supervisor is registered as captures no [`DBOS`] — and a captured `DBOS` is a cycle with the
/// registry that holds the closure, which keeps the instance, its executor and its connection pool
/// alive for the life of the process. That is the whole reason this exists beside the method.
///
/// Otherwise it is the method exactly: the same body builds both, so the cancel is checkpointed
/// under the same step name (`DBOS.cancelWorkflow`) and a workflow that switches from one to the
/// other replays the history it recorded. See [`DBOS::cancel`] for what cancelling does — it marks
/// the row, it does not interrupt — and for why a workflow cannot cancel itself.
///
/// The error is the *workflow's* channel, like [`step`](crate::step())'s and
/// [`send`](crate::send())'s, so `?` needs no conversion. From inside a *step* the cancel is plain,
/// with no checkpoint of its own, the step's checkpoint standing for everything its body did.
/// Outside a workflow there is no context to take an executor from, so this is
/// [`Error::NotInWorkflow`]; there [`DBOS::cancel`] and [`Client::cancel`](crate::Client::cancel)
/// are the calls.
///
/// **Named for what it cancels**, where the method is a bare verb: `dbos.cancel(id)` has a receiver
/// to say what is being managed, and a free `dbos::cancel(id)` would not.
///
/// ```no_run
/// # async fn supervise(stuck: String) -> dbos::Result<()> {
/// dbos::cancel_workflow(&stuck).await?;
/// # Ok(()) }
/// ```
pub fn cancel_workflow<'a, E: DurableError + 'a>(workflow_id: &'a str) -> PendingStep<'a, (), E> {
    pending_cancel(
        StepPlacement::ambient_connection("cancel_workflow"),
        workflow_id,
    )
}

/// Cancels workflows, and optionally everything descended from them, from inside a workflow.
///
/// The free form of [`DBOS::cancel_all`], and the bulk form of [`cancel_workflow`]: it exists for
/// the reason that one gives, needing no handle and so leaving the workflow's registered closure
/// with nothing to capture. Recorded as `DBOS.cancelWorkflow`, one step for the whole batch, as
/// the method is. Returns the ids that actually moved — see [`DBOS::cancel_all`] for the cascade.
///
/// Named `cancel_workflows` where the method is `cancel_all`: a free function has no receiver to
/// say what *all* ranges over, so it takes the plural noun.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
///
/// ```no_run
/// # async fn supervise(root: String) -> dbos::Result<()> {
/// // A whole fan-out, root and children.
/// dbos::cancel_workflows(&[root.as_str()], dbos::Children::Include).await?;
/// # Ok(()) }
/// ```
pub fn cancel_workflows<'a, E: DurableError + 'a>(
    workflow_ids: &'a [&'a str],
    children: Children,
) -> PendingStep<'a, Vec<String>, E> {
    pending_cancel_all(
        StepPlacement::ambient_connection("cancel_workflows"),
        workflow_ids,
        children,
    )
}

/// Puts a workflow back on a queue, from inside another workflow, and hands back a handle to it.
///
/// The free form of [`DBOS::resume`], for a workflow body that restarts a cancelled or stalled
/// workflow without capturing a [`DBOS`] in its registered closure — see [`cancel_workflow`] for
/// the cycle that capture would make. The same body builds both, so the resume is checkpointed
/// under the same step name (`DBOS.resumeWorkflow`) and replays as the method's would. See
/// [`DBOS::resume`] for what resuming does to a workflow that is still running.
///
/// **A [`PendingStart`], as [`WorkflowRef::start`](crate::WorkflowRef::start) hands back**: it
/// hands back a workflow to wait on, and answers in the handle's error channel, `E`, by default —
/// the caller's own whenever the two fail the same way, so `?` needs nothing. Where they differ,
/// [`lift`](PendingStart::lift) re-declares the channel before the await; nothing is lost, since
/// the call itself can only fail in the engine's terms. And like a start it is not a
/// [`select_step!`](crate::select_step) branch: it changes what runs, and only the winner of a
/// race is polled on replay.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step.
///
/// ```no_run
/// # async fn supervise(stalled: String) -> dbos::Result<u32> {
/// let handle = dbos::resume_workflow::<u32, dbos::EngineOnly>(&stalled).await?;
/// handle.result().await
/// # }
/// ```
pub fn resume_workflow<'a, R: 'a, E: 'a>(workflow_id: &'a str) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_resume(
        StepPlacement::ambient_connection("resume_workflow"),
        workflow_id,
        ResumeOptions::default(),
    ))
}

/// [`resume_workflow`] onto a queue of the caller's choosing.
///
/// The free form of [`DBOS::resume_with`]; everything [`resume_workflow`] says holds here.
pub fn resume_workflow_with<'a, R: 'a, E: 'a>(
    workflow_id: &'a str,
    options: ResumeOptions<'a>,
) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_resume(
        StepPlacement::ambient_connection("resume_workflow_with"),
        workflow_id,
        options,
    ))
}

/// Resumes workflows from inside a workflow, handing back one handle per id, in the order given.
///
/// The free form of [`DBOS::resume_all`] and the bulk form of [`resume_workflow`], named for the
/// plural as [`cancel_workflows`] is. One step, `DBOS.resumeWorkflow`, for the whole batch, and
/// every id must exist or nothing is enqueued — see [`DBOS::resume_all`].
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. **The call answers in the
/// engine's channel**, as the method does, so [`lift`](PendingStep::lift) puts it in the caller's
/// before the await: `.lift().await?`. A batch is not a [`select_step!`](crate::select_step)
/// branch either, though its type does not stop one; see [`resume_workflow`].
pub fn resume_workflows<'a, R: 'a, E: 'a>(
    workflow_ids: &'a [&'a str],
    options: ResumeOptions<'a>,
) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
    pending_resume_all(
        StepPlacement::ambient_connection("resume_workflows"),
        workflow_ids,
        options,
    )
}

/// Forks a workflow from `start_step`, from inside another workflow, and hands back the fork's
/// handle.
///
/// The free form of [`DBOS::fork`], for a workflow body — a supervisor re-running a failed child
/// from the step that failed — that has no [`DBOS`] to call it on and should not capture one: see
/// [`cancel_workflow`] for the cycle that capture would make.
///
/// **Checkpointed in the transaction that writes the fork**, under `DBOS.forkWorkflow`, because
/// the same body builds this and the method. That is what matters most here: a fork generates a
/// new id, so a replay without the checkpoint would write a second fork. A replay hands back the
/// id the first execution recorded.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. A [`PendingStart`], as
/// [`resume_workflow`] is.
///
/// ```no_run
/// # async fn supervise(failed: String) -> dbos::Result<u32> {
/// let steps = dbos::list_workflow_steps::<dbos::EngineOnly>(&failed).await?;
/// let at = steps.iter().rev().find(|s| s.error.is_some()).map_or(0, |s| s.step_id);
/// let fork = dbos::fork_workflow::<u32, dbos::EngineOnly>(&failed, at).await?;
/// fork.result().await
/// # }
/// ```
pub fn fork_workflow<'a, R: 'a, E: 'a>(
    workflow_id: &'a str,
    start_step: i32,
) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_fork(
        StepPlacement::ambient_connection("fork_workflow"),
        Fork::new(workflow_id, start_step),
        ForkOptions::default(),
    ))
}

/// [`fork_workflow`], choosing the fork's id, what it inherits and where it runs.
///
/// The free form of [`DBOS::fork_with`]; everything [`fork_workflow`] says holds here. An invalid
/// [`Fork`] or [`ForkOptions`] is refused before the call takes its step id, as the method's is.
pub fn fork_workflow_with<'a, R: 'a, E: 'a>(
    fork: Fork<'a>,
    options: ForkOptions<'a>,
) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_fork(
        StepPlacement::ambient_connection("fork_workflow_with"),
        fork,
        options,
    ))
}

/// Forks workflows from inside a workflow, handing back a handle to each fork in the order given.
///
/// The free form of [`DBOS::fork_all`] and the bulk form of [`fork_workflow`], named for the plural
/// as [`cancel_workflows`] is. Every source must exist or no fork is written, and the batch is one
/// step, `DBOS.forkWorkflow` — see [`DBOS::fork_all`].
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. **The call answers in the
/// engine's channel**, as the method does, so [`lift`](PendingStep::lift) puts it in the caller's
/// before the await: `.lift().await?`. A batch is not a [`select_step!`](crate::select_step)
/// branch either, though its type does not stop one; see [`resume_workflow`].
pub fn fork_workflows<'a, R: 'a, E: 'a>(
    forks: &'a [Fork<'a>],
    options: ForkOptions<'a>,
) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
    pending_fork_all(
        StepPlacement::ambient_connection("fork_workflows"),
        forks,
        options,
    )
}

/// Re-runs a finished workflow from its first step under the same id, from inside another
/// workflow.
///
/// The free form of [`DBOS::rewind`], for a workflow body with no [`DBOS`] to call it on — see
/// [`cancel_workflow`] for why it should not capture one. Checkpointed under
/// `DBOS.rewindWorkflow`, as the method is, in the transaction that performs the rewind, so a
/// recovered caller does not rewind the target again. See [`DBOS::rewind_with`] for what is
/// discarded, and why a workflow still running cannot be rewound.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. A [`PendingStart`], as
/// [`resume_workflow`] is.
///
/// ```no_run
/// // A workflow body, registered with `DBOS::register_workflow`.
/// async fn run_again(workflow_id: String) -> dbos::Result<u32> {
///     dbos::rewind_workflow::<u32, dbos::EngineOnly>(&workflow_id)
///         .await?
///         .result()
///         .await
/// }
/// ```
pub fn rewind_workflow<'a, R: 'a, E: 'a>(workflow_id: &'a str) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_rewind(
        StepPlacement::ambient_connection("rewind_workflow"),
        workflow_id,
        RewindOptions::default(),
    ))
}

/// [`rewind_workflow`], choosing the step it re-runs from and where it runs.
///
/// The free form of [`DBOS::rewind_with`]; everything [`rewind_workflow`] says holds here. Invalid
/// [`RewindOptions`] are refused before the call takes its step id, as the method's are.
pub fn rewind_workflow_with<'a, R: 'a, E: 'a>(
    workflow_id: &'a str,
    options: RewindOptions<'a>,
) -> PendingStart<'a, R, E> {
    PendingStart::from_step(pending_rewind(
        StepPlacement::ambient_connection("rewind_workflow_with"),
        workflow_id,
        options,
    ))
}

/// Removes a workflow and everything recorded against it, from inside another workflow.
///
/// The free form of [`DBOS::delete`], for a workflow body with no [`DBOS`] to call it on — see
/// [`cancel_workflow`] for why it should not capture one. Checkpointed under
/// `DBOS.deleteWorkflow`, as the method is. See [`DBOS::delete`] for what goes with the row, and
/// [`DBOS::delete_all`] for why a workflow cannot delete itself.
///
/// **Named for what it deletes**, where the method is too: a queue and a schedule can be deleted
/// as well, and a free `dbos::delete(id)` would not say which.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
///
/// ```no_run
/// // A workflow body, registered with `DBOS::register_workflow`.
/// async fn clean_up(workflow_id: String) -> dbos::Result<()> {
///     dbos::delete_workflow(&workflow_id).await
/// }
/// ```
pub fn delete_workflow<'a, E: DurableError + 'a>(workflow_id: &'a str) -> PendingStep<'a, (), E> {
    pending_delete(
        StepPlacement::ambient_connection("delete_workflow"),
        workflow_id,
    )
}

/// Deletes workflows, and optionally everything descended from them, from inside a workflow.
///
/// The free form of [`DBOS::delete_all`] and the bulk form of [`delete_workflow`], named for the
/// plural as [`cancel_workflows`] is. Returns how many rows went, descendants included; one step,
/// `DBOS.deleteWorkflow`, for the whole batch.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
pub fn delete_workflows<'a, E: DurableError + 'a>(
    workflow_ids: &'a [&'a str],
    children: Children,
) -> PendingStep<'a, u64, E> {
    pending_delete_all(
        StepPlacement::ambient_connection("delete_workflows"),
        workflow_ids,
        children,
    )
}

/// Holds a queued workflow back, or lets it go sooner, from inside another workflow.
///
/// The free form of [`DBOS::set_workflow_delay`], for a workflow body with no [`DBOS`] to call it
/// on — see [`cancel_workflow`] for why it should not capture one. Checkpointed under
/// `DBOS.setWorkflowDelay`, as the method is, and a delay too long to store is refused before the
/// call takes its step id. Only a `DELAYED` row moves; see the method for how the delay is timed.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
pub fn set_workflow_delay<'a, E: DurableError + 'a>(
    workflow_id: &'a str,
    delay: WorkflowDelay,
) -> PendingStep<'a, (), E> {
    pending_set_workflow_delay(
        StepPlacement::ambient_connection("set_workflow_delay"),
        workflow_id,
        delay,
    )
}

/// Replaces a workflow's attributes, or clears them with `None`, from inside another workflow.
///
/// The free form of [`DBOS::update_workflow_attributes`], for a workflow body with no [`DBOS`] to
/// call it on — see [`cancel_workflow`] for why it should not capture one. Checkpointed under
/// `DBOS.updateWorkflowAttributes`, as the method is, so a refusal for a missing workflow
/// replays as the same refusal. A replacement, not a merge:
/// [`merge_workflow_attributes`](fn@merge_workflow_attributes) adds keys.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
pub fn update_workflow_attributes<'a, E: DurableError + 'a>(
    workflow_id: &'a str,
    attributes: Option<&serde_json::Map<String, serde_json::Value>>,
) -> PendingStep<'a, (), E> {
    pending_update_workflow_attributes(
        StepPlacement::ambient_connection("update_workflow_attributes"),
        workflow_id,
        attributes,
    )
}

/// Merges keys into a workflow's attributes, in one statement, from inside another workflow.
///
/// The free form of [`DBOS::merge_workflow_attributes`], for a workflow body with no [`DBOS`] to
/// call it on — see [`cancel_workflow`] for why it should not capture one. Recorded under the same
/// step name as a replacement, `DBOS.updateWorkflowAttributes`, as the method is.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
pub fn merge_workflow_attributes<'a, E: DurableError + 'a>(
    workflow_id: &'a str,
    attributes: &serde_json::Map<String, serde_json::Value>,
) -> PendingStep<'a, (), E> {
    pending_merge_workflow_attributes(
        StepPlacement::ambient_connection("merge_workflow_attributes"),
        workflow_id,
        attributes,
    )
}

/// Reads the workflows matching a filter, from inside a workflow.
///
/// The free form of [`DBOS::list_workflows`], for a supervisor that finds what to act on — stuck
/// workflows, a tenant's backlog — without a [`DBOS`] to call it on; see [`cancel_workflow`] for
/// why it should not capture one. Checkpointed under `DBOS.listWorkflows`, as the method is, so a
/// replay sees the rows the first execution saw and acts on the same ones, rather than on whatever
/// matches by then.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
///
/// ```no_run
/// # async fn supervise() -> dbos::Result<()> {
/// use dbos::sysdb::types::{WorkflowFilter, WorkflowStatus};
///
/// let filter = WorkflowFilter {
///     status: vec![WorkflowStatus::Pending],
///     limit: Some(100),
///     ..WorkflowFilter::default()
/// };
/// for stuck in dbos::list_workflows::<dbos::EngineOnly>(&filter).await? {
///     dbos::cancel_workflow::<dbos::EngineOnly>(&stuck.workflow_id).await?;
/// }
/// # Ok(()) }
/// ```
pub fn list_workflows<'a, E: DurableError + 'a>(
    filter: &'a WorkflowFilter<'a>,
) -> PendingStep<'a, Vec<WorkflowRecord>, E> {
    pending_list_workflows(StepPlacement::ambient_connection("list_workflows"), filter)
}

/// Reads one workflow's steps, in execution order, from inside a workflow.
///
/// The free form of [`DBOS::list_workflow_steps`], for a supervisor that finds where another
/// workflow failed — to fork it from there — without a [`DBOS`] to call it on; see
/// [`cancel_workflow`] for why it should not capture one. Checkpointed under
/// `DBOS.listWorkflowSteps`, as the method is, so a replay reads the steps the first execution
/// read.
///
/// [`Error::NotInWorkflow`] outside a workflow; plain inside a step. The error is the workflow's
/// channel.
pub fn list_workflow_steps<'a, E: DurableError + 'a>(
    workflow_id: &'a str,
) -> PendingStep<'a, Vec<StepRecord>, E> {
    pending_list_workflow_steps(
        StepPlacement::ambient_connection("list_workflow_steps"),
        workflow_id,
        ListWorkflowStepsOptions::default(),
    )
}

/// [`list_workflow_steps`](fn@list_workflow_steps), choosing what each step carries and which page
/// to read.
///
/// The free form of [`DBOS::list_workflow_steps_with`]; everything
/// [`list_workflow_steps`](fn@list_workflow_steps) says holds here.
pub fn list_workflow_steps_with<'a, E: DurableError + 'a>(
    workflow_id: &'a str,
    options: ListWorkflowStepsOptions,
) -> PendingStep<'a, Vec<StepRecord>, E> {
    pending_list_workflow_steps(
        StepPlacement::ambient_connection("list_workflow_steps_with"),
        workflow_id,
        options,
    )
}

/// A handle to a workflow this workflow did not start, from inside a workflow.
///
/// The free form of [`DBOS::retrieve_workflow`]. A supervisor that has an id — from
/// [`list_workflows`](fn@list_workflows), from an event, from its input — and wants to await or
/// inspect that workflow needs a handle, and without this the only way to one is a captured
/// [`DBOS`], the cycle [`cancel_workflow`] describes.
///
/// **No step, and not a [`PendingStep`]**, exactly as the method: the call does no I/O, so there
/// is nothing to replay, and the id is taken on faith. What the handle then does is checkpointed in
/// the usual way — awaiting its [`result`](WorkflowHandle::result) inside a workflow is a step.
///
/// The one refusal is [`Error::NotInWorkflow`], outside a workflow, where
/// [`DBOS::retrieve_workflow`] is the call. Inside a step it hands back a handle like anywhere
/// else. It answers in the handle's error channel, as [`resume_workflow`] does by default; unlike
/// it, a plain `Result` has nowhere to re-declare that, so a caller that fails another way maps
/// the refusal itself.
///
/// ```no_run
/// # async fn supervise(child: String) -> dbos::Result<u32> {
/// let handle = dbos::retrieve_workflow::<u32, dbos::EngineOnly>(&child)?;
/// handle.result().await
/// # }
/// ```
pub fn retrieve_workflow<R, E: DurableError>(workflow_id: &str) -> Result<WorkflowHandle<R, E>, E> {
    let conn = StepPlacement::ambient_connection("retrieve_workflow").map_err(Error::lift)?;
    Ok(retrieved(conn, workflow_id))
}

impl DBOS {
    /// This instance, refused if it is not the one running the calling workflow — and where the
    /// call stands in that workflow.
    ///
    /// Both halves come back together because the check is what relates them: a checkpointed
    /// management call takes its executor from `self` and its step id from the ambient context,
    /// and the two have to be the same instance. Resolving them apart is what let them disagree.
    /// `operation` names the call in either failure, so taking it once is also what keeps the two
    /// messages from drifting.
    ///
    /// The placement records nothing outside a workflow, where a management call is just a call —
    /// an operator's tool, an admin endpoint, a test. Nothing inside a *step* either, by the leaf
    /// rule the rest of the crate follows: the step's own checkpoint stands for everything its
    /// body did, and allocating an id under it would shift every later step onto the wrong replay
    /// slot.
    ///
    /// **A workflow running on another instance is [`Error::WrongInstance`].** This is where the
    /// two halves of a checkpointed management call are combined: the step id comes from the
    /// ambient workflow's counter and the checkpoint is written through *this* instance's system
    /// database, so a handle to some other instance would write the row where the workflow that
    /// allocated the id cannot see it, usually as an orphan, since that database has no such
    /// workflow.
    /// [`get_event`](Self::get_event) and [`WorkflowRef::parent`](crate::WorkflowRef) refuse the
    /// same combination for the same reason. Inside a step there is nothing to refuse: nothing is
    /// checkpointed, so the two halves are never combined and the call is plain whichever
    /// instance serves it.
    ///
    /// **The launch check comes first**, as every method on this surface expects: an unlaunched
    /// instance should say so whatever else is wrong with the call — and, coming before the
    /// placement, it also means such a call moves no step counter.
    ///
    /// **The id is spent here, at the call**, which is why every method that uses this is a plain
    /// `fn` handing back a [`PendingStep`] rather than an `async fn`: a call built beside a step
    /// and driven with it takes the same slot on every execution.
    ///
    /// The schedule surface places through this. The management methods below do not: each shares
    /// its body with a free form that has no `self`, so they hand that body a connection through
    /// [`connection`](Self::connection) and the body places for itself, the same way, after the
    /// argument checks that must refuse before an id is spent.
    pub(crate) fn placed(&self, operation: &'static str) -> Result<(Arc<Executor>, StepPlacement)> {
        StepPlacement::taken(self.executor(operation), operation)
    }

    /// This instance's connection, or [`Error::NotLaunched`] naming the operation: what every
    /// method below hands the body it shares with its free form.
    ///
    /// Only the connection, not a placement, because the body places for itself — after the
    /// argument checks that must refuse before an id is spent. Whether the ambient workflow is
    /// this instance's is decided there, by [`StepPlacement::of`], which is where
    /// [`Error::WrongInstance`] comes from.
    fn connection(&self, operation: &'static str) -> Result<Arc<Connection>> {
        Ok(Arc::clone(self.executor(operation)?.connection()))
    }

    /// A handle to a workflow this process did not start.
    ///
    /// **From a workflow body, reach for the free [`retrieve_workflow`](fn@retrieve_workflow)
    /// instead.** It is this call with no handle to capture; a captured [`DBOS`] is a cycle with
    /// the registry that holds the workflow's closure.
    ///
    /// The way a listing becomes something to act on: [`list_workflows`](Self::list_workflows)
    /// hands back rows, and this turns one of their ids into a handle that can be awaited. The
    /// handle is a **polling** one — see the module documentation — because the workflow is
    /// running somewhere else, if it is running at all.
    ///
    /// **The id is not checked, and the call does no I/O.** A handle is handed back whether or not
    /// the workflow exists.
    ///
    /// **That follows from what awaiting a missing row does.** Awaiting an id nothing has seen —
    /// which is exactly what this function hands back — polls for the row to appear rather than
    /// reporting its absence. That is the case the wait is for: an id from outside this process,
    /// awaited before whoever owns it has committed the enqueue. The cost is that a *mistyped* id
    /// awaited through this handle waits instead of failing.
    ///
    /// So the check would buy something, and what it costs is this call: a verified retrieve is
    /// `async` and fallible on the id as well as the launch, and mapping a listing to handles then
    /// costs a round trip apiece. [`status`](crate::WorkflowHandle::status) is the round trip when
    /// it is wanted — it reports [`Error::WorkflowNotFound`] whatever the handle — and
    /// `tokio::time::timeout` is the bound when a wait should not be open-ended.
    ///
    /// It is not the same line [`resume`](Self::resume) draws, and deliberately: a resume is a
    /// write that would otherwise silently do nothing, and its handles name rows the call has just
    /// moved.
    ///
    /// Doing no I/O also makes this the one member of the surface that records **no step** when it
    /// is called from inside a workflow: there is nothing to replay.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle = dbos.retrieve_workflow::<u32, dbos::EngineOnly>("started-elsewhere")?;
    /// let status = handle.status().await?;
    /// # Ok(()) }
    /// ```
    pub fn retrieve_workflow<R, E>(&self, workflow_id: &str) -> Result<WorkflowHandle<R, E>> {
        Ok(retrieved(
            Arc::clone(self.executor("retrieve a workflow")?.connection()),
            workflow_id,
        ))
    }

    /// Marks a workflow cancelled, which is how a running execution is told to stop.
    ///
    /// **From a workflow body, reach for the free [`cancel_workflow`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
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
    /// **A workflow cannot cancel itself**, from its own code or from inside one of its steps:
    /// [`Error::SystemDatabase`] carrying `InvalidInput`, and nothing is cancelled. A workflow that
    /// wants to stop returns or fails instead. The check knows the caller from the workflow's
    /// context, so it does not reach a cancel through a [`Client`](crate::Client), which is outside
    /// every workflow, or one from a task the workflow spawned with `tokio::spawn`, which leaves
    /// that context behind.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// dbos.cancel("runaway-workflow").await?;
    /// # Ok(()) }
    /// ```
    pub fn cancel<'a>(&self, workflow_id: &'a str) -> PendingStep<'a, ()> {
        pending_cancel(self.connection("cancel a workflow"), workflow_id)
    }

    /// Cancels workflows, and optionally everything descended from them.
    ///
    /// **From a workflow body, reach for the free [`cancel_workflows`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// Returns the ids that actually moved, which is a subset: one already finished is left where
    /// it is, and with [`Children::Include`] the list also carries the descendants that were
    /// cancelled, which the caller never named.
    ///
    /// The cascade is a **level-by-level walk that interleaves with the cancelling**, so a child
    /// spawned while the sweep is running is still caught — the parent is stopped before its
    /// children are looked up; collecting the whole subtree first could miss a workflow spawned
    /// during the walk. The walk is also one transaction, so a tree is either wholly cancelled or
    /// untouched, and a failure partway leaves nothing to finish.
    ///
    /// **A workflow cannot cancel itself here either**, by naming its own id, or with
    /// [`Children::Include`] by naming an ancestor whose tree it is inside. Either refuses the
    /// whole call, as [`cancel`](Self::cancel) describes, and nothing moves. Its own descendants, a
    /// sibling's tree, and its parent with [`Children::Skip`] are fair targets.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // One workflow and its whole tree.
    /// dbos.cancel_all(&["fan-out-root"], dbos::Children::Include).await?;
    /// # Ok(()) }
    /// ```
    pub fn cancel_all<'a>(
        &self,
        workflow_ids: &'a [&'a str],
        children: Children,
    ) -> PendingStep<'a, Vec<String>> {
        pending_cancel_all(self.connection("cancel a workflow"), workflow_ids, children)
    }

    /// Puts a workflow back on a queue, and hands back a handle to watch it.
    ///
    /// **From a workflow body, reach for the free [`resume_workflow`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
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
    /// so rather than silently do nothing — a distinction a zero-row update cannot draw.
    ///
    /// # Resuming a workflow that is still running
    ///
    /// **Allowed, except a workflow resuming itself, and it hands the workflow over.** Resuming
    /// itself is refused as [`Error::SystemDatabase`] carrying `InvalidInput`, from the workflow's
    /// own code or one of its steps, though not through a [`Client`](crate::Client) or from a task
    /// it spawned, as [`cancel`](Self::cancel) describes. Otherwise not being terminal is the whole
    /// guard, so a `PENDING` row passes — and `PENDING` means *some executor owns this*, not *this
    /// has stopped*. Resuming a workflow that is executing right now re-enqueues it and takes it
    /// from its execution: the next sweep claims the row and dispatches a new one, and the old
    /// one's next write is refused, so it stops there and its caller gets the new one's outcome. A
    /// step the old one was running when the hand-off came runs again in the new one, side effects
    /// included.
    ///
    /// **[`cancel`](Self::cancel) first if the workflow may be live.** That gives the running
    /// execution something to observe, so it abandons its attempt at the next preemptible step
    /// rather than running on. It narrows the window rather than closing it: the two calls are
    /// separate, and the resumed execution can start before the old one has read the
    /// cancellation.
    ///
    /// The guard is not simply missing here. `PENDING` cannot say whether the executor that owns
    /// it is alive, and a workflow left `PENDING` by a node that died is the case an operator most
    /// wants to resume by hand — so a predicate that closes the hazard closes that too. The
    /// ownership fence is what bounds it: the new execution takes the row over and the old one's
    /// next write is refused, so only one records, though a step in flight at the hand-off runs
    /// in both. Whether resume should refuse a live workflow is an open question shared with the
    /// other SDKs on the same database.
    ///
    /// **A [`PendingStart`] in the engine's channel**, as a start is: [`lift`](PendingStart::lift)
    /// re-declares the channel for a caller that fails another way, and it is not a
    /// [`select_step!`](crate::select_step) branch, since it sets a workflow running.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle = dbos.resume::<u32, dbos::EngineOnly>("stalled-workflow").await?;
    /// # Ok(()) }
    /// ```
    pub fn resume<'a, R: 'a, E: 'a>(
        &self,
        workflow_id: &'a str,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        self.resume_with(workflow_id, ResumeOptions::default())
    }

    /// Resumes a workflow onto a queue of the caller's choosing.
    ///
    /// **From a workflow body, reach for the free [`resume_workflow_with`] instead.** It is this
    /// call with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a
    /// cycle with the registry that holds the workflow's closure.
    ///
    /// [`resume`](Self::resume) for the common case, which is the engine's internal queue.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // Back on a limited queue, so a resumed backlog does not flood the fleet.
    /// let handle = dbos.resume_with::<u32, dbos::EngineOnly>(
    ///     "stalled-workflow",
    ///     dbos::ResumeOptions { queue_name: Some("recovery") },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub fn resume_with<'a, R: 'a, E: 'a>(
        &self,
        workflow_id: &'a str,
        options: ResumeOptions<'a>,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        PendingStart::from_step(pending_resume(
            self.connection("resume a workflow"),
            workflow_id,
            options,
        ))
    }

    /// Resumes workflows, handing back one handle per id, in the order given.
    ///
    /// **From a workflow body, reach for the free [`resume_workflows`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// One handle per id *asked for*, not per row that moved. A workflow that had already finished
    /// is not re-enqueued, and its handle simply reports the outcome it already had — which is what
    /// [`resume`](Self::resume) promises for one workflow, kept for many.
    ///
    /// Every id must exist. The whole batch is one statement, so an id with no row behind it fails
    /// the call and nothing is enqueued, rather than resuming the ones that happened to be spelled
    /// correctly, which keeps the batch and the single form answering the same way. Naming the
    /// calling workflow among them fails the call the same way, as [`resume`](Self::resume)
    /// describes.
    pub fn resume_all<'a, R: 'a, E: 'a>(
        &self,
        workflow_ids: &'a [&'a str],
        options: ResumeOptions<'a>,
    ) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
        pending_resume_all(self.connection("resume a workflow"), workflow_ids, options)
    }

    /// Forks a workflow from `start_step`, enqueueing the fork and handing back a handle to it.
    ///
    /// **From a workflow body, reach for the free [`fork_workflow`] instead.** It is this call with
    /// no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle with
    /// the registry that holds the workflow's closure.
    ///
    /// Every step below `start_step` replays from the source's checkpoint, and that step and every
    /// one after it run again. Step ids start at 0, so 0 re-runs the whole workflow. Negative is an
    /// [`Error::InvalidArgument`]. To fork from a failure, find the failed step's id with
    /// [`list_workflow_steps`](Self::list_workflow_steps).
    ///
    /// [`fork_with`](Self::fork_with) for a chosen id and the options; this is the common case.
    ///
    /// **A [`PendingStart`] in the engine's channel**, as a start is: [`lift`](PendingStart::lift)
    /// re-declares the channel for a caller that fails another way, and it is not a
    /// [`select_step!`](crate::select_step) branch, since it sets a workflow running.
    pub fn fork<'a, R: 'a, E: 'a>(
        &self,
        workflow_id: &'a str,
        start_step: i32,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        self.fork_with(Fork::new(workflow_id, start_step), ForkOptions::default())
    }

    /// Forks a workflow, choosing the fork's id, what it inherits and where it runs.
    ///
    /// **From a workflow body, reach for the free [`fork_workflow_with`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// A fork is a **new workflow** that inherits its source's identity — name, inputs, roles,
    /// attributes — along with the recorded results of every step below
    /// [`start_step`](Fork::start_step). Those steps replay rather than run, so the fork
    /// arrives at the start step in the state the original was in, and carries on from a moment
    /// that has already happened. Re-running failed work against fixed code is what it is for,
    /// which is why [`ForkOptions::app_version`] exists.
    ///
    /// The source is not modified beyond being marked as forked from; the fork gets its own id,
    /// generated unless [`Fork::forked_id`] names one.
    ///
    /// Takes a [`Fork`] rather than the id and step as arguments, because the chosen id sits
    /// beside them there: it is the same per-fork triple a [`fork_all`](Self::fork_all) batch is
    /// made of, and the single fork is that batch with one element.
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
    /// // Re-run from the step that failed, against the deployment that fixes it.
    /// let steps = dbos.list_workflow_steps("failed-workflow").await?;
    /// let failed = steps.iter().rev().find(|s| s.error.is_some()).map_or(0, |s| s.step_id);
    /// let handle = dbos.fork_with::<u32, dbos::EngineOnly>(
    ///     dbos::Fork::new("failed-workflow", failed),
    ///     dbos::ForkOptions { app_version: Some("v2"), ..Default::default() },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub fn fork_with<'a, R: 'a, E: 'a>(
        &self,
        fork: Fork<'a>,
        options: ForkOptions<'a>,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        PendingStart::from_step(pending_fork(self.connection(FORK_OPERATION), fork, options))
    }

    /// Forks workflows, handing back a handle to each fork in the order given.
    ///
    /// **From a workflow body, reach for the free [`fork_workflows`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// The batch is the system database's primitive rather than a loop over
    /// [`fork_with`](Self::fork_with), and the difference is visible: every source must exist, or
    /// the call fails and **no fork is written**. A half-applied batch would leave forks whose
    /// siblings never existed, which for a fan-out being re-run against fixed code is worse than
    /// forking nothing.
    ///
    /// Each [`Fork`] names its own source, start step and, optionally, the fork's id. The
    /// options apply to the whole batch, which is what makes
    /// [`app_version`](ForkOptions::app_version) useful — re-running a fan-out
    /// against the deployment that fixes it is the case this method exists for.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// use dbos::{Fork, ForkOptions};
    ///
    /// let forks = [
    ///     Fork { forked_id: Some("shard-1-retry"), ..Fork::new("shard-1", 2) },
    ///     Fork { forked_id: Some("shard-2-retry"), ..Fork::new("shard-2", 5) },
    /// ];
    /// let handles = dbos
    ///     .fork_all::<u32, dbos::EngineOnly>(
    ///         &forks,
    ///         ForkOptions { app_version: Some("v2"), ..Default::default() },
    ///     )
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn fork_all<'a, R: 'a, E: 'a>(
        &self,
        forks: &'a [Fork<'a>],
        options: ForkOptions<'a>,
    ) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
        pending_fork_all(self.connection(FORK_OPERATION), forks, options)
    }

    /// Re-runs a finished workflow from its first step, under the same id.
    ///
    /// **From a workflow body, reach for the free [`rewind_workflow`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// [`rewind_with`](Self::rewind_with) for choosing the step and where it runs.
    ///
    /// **A [`PendingStart`] in the engine's channel**, as a start is: [`lift`](PendingStart::lift)
    /// re-declares the channel for a caller that fails another way, and it is not a
    /// [`select_step!`](crate::select_step) branch, since it sets a workflow running.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle = dbos.rewind::<u32, dbos::EngineOnly>("failed-workflow").await?;
    /// let rerun = handle.result().await?;
    /// # Ok(()) }
    /// ```
    pub fn rewind<'a, R: 'a, E: 'a>(
        &self,
        workflow_id: &'a str,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        self.rewind_with(workflow_id, RewindOptions::default())
    }

    /// Discards a finished workflow's history from a step on, and re-enqueues it under the same
    /// id.
    ///
    /// **From a workflow body, reach for the free [`rewind_workflow_with`] instead.** It is this
    /// call with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a
    /// cycle with the registry that holds the workflow's closure.
    ///
    /// The steps below [`RewindOptions::start_step`] replay from their checkpoints and the rest
    /// run again. **The id is kept, which is the difference from [`fork`](Self::fork)**: a fork is
    /// a new workflow, so peers addressing the original by id — a sender, an event reader, a
    /// parent awaiting it — never see its re-run, and a rewind is how the original itself is
    /// repaired.
    ///
    /// What the discarded steps did is undone with them. Events they published revert to their
    /// last value below the cut, or are removed. Messages they consumed are deleted, and so is
    /// every message not yet consumed, so the replayed `recv` waits for a new one. Stream entries
    /// are kept, since a reader may already hold their offsets, and the re-run appends after
    /// them; only a close is removed, which reopens the stream, and the re-run's first write
    /// takes the offset the close held. See
    /// [`SystemDatabase::rewind_workflow`](crate::sysdb::SystemDatabase::rewind_workflow) for the
    /// details, including messages consumed before the schema recorded which step took them.
    ///
    /// **Only a workflow that is not running or waiting to run can be rewound.** One that is
    /// `PENDING`, `ENQUEUED` or `DELAYED` is refused with `WorkflowNotRewindable`. An id with no
    /// row is `NonExistentWorkflow`. Both arrive as [`Error::SystemDatabase`], and either way
    /// nothing is written. `SUCCESS`, `ERROR`, `CANCELLED` and `MAX_RECOVERY_ATTEMPTS_EXCEEDED`
    /// are all rewindable.
    ///
    /// # Rewinding a workflow that is still running
    ///
    /// **[`cancel`](Self::cancel) first, but know that it does not wait.** Cancelling writes
    /// `CANCELLED`, and a running execution finds out at its next step boundary. A rewind issued
    /// before then sets the row back to `ENQUEUED`, so that execution never sees the
    /// cancellation and runs on beside the re-run, both writing at the same step ids. Cancelling
    /// first is what makes the rewind possible at all, but it narrows the window rather than
    /// closing it. To close it, wait until the cancelled execution has stopped — its last step has
    /// been recorded and it has had a step boundary to observe the cancellation at — before
    /// rewinding.
    ///
    /// **Children are not rewound.** The parent replays to each child's id and adopts the row
    /// already there, result included. To repair a failed child, rewind the child, then rewind
    /// the parent to the step that awaited it.
    ///
    /// **Enqueued, never started.** The handle is a polling one — see the module documentation —
    /// and its result is the re-run's.
    ///
    /// Called from inside a workflow, the rewind is checkpointed as a step in the transaction that
    /// performs it, so a recovered caller replays the step and does not rewind the target again.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// // Re-run from step 3 on the deployment with the fix.
    /// let handle = dbos.rewind_with::<u32, dbos::EngineOnly>(
    ///     "failed-workflow",
    ///     dbos::RewindOptions { start_step: 3, app_version: Some("v2"), ..Default::default() },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub fn rewind_with<'a, R: 'a, E: 'a>(
        &self,
        workflow_id: &'a str,
        options: RewindOptions<'a>,
    ) -> PendingStart<'a, R, E, crate::EngineOnly> {
        PendingStart::from_step(pending_rewind(
            self.connection("rewind a workflow"),
            workflow_id,
            options,
        ))
    }

    /// Removes a workflow and everything recorded against it.
    ///
    /// **From a workflow body, reach for the free [`delete_workflow`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// Steps, inputs, outcome, events, messages and streams go with the row, so this is not a
    /// status change and there is nothing left to resume. [`cancel`](Self::cancel) is the form
    /// that stops a workflow and keeps it.
    ///
    /// **No status guard, deliberately.** A running workflow is deleted like any other: naming an
    /// id is an operator saying *this one, now*, and refusing would leave no way to clear a
    /// workflow that is wedged.
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
    pub fn delete<'a>(&self, workflow_id: &'a str) -> PendingStep<'a, ()> {
        pending_delete(self.connection("delete a workflow"), workflow_id)
    }

    /// Deletes workflows, and optionally everything descended from them.
    ///
    /// **From a workflow body, reach for the free [`delete_workflows`] instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// Returns how many rows went, descendants included.
    ///
    /// Unlike [`cancel_all`](Self::cancel_all), the tree is collected first and deleted in one
    /// transaction rather than level by level.
    ///
    /// **A tree that is still running can outlive the walk.** A child committed after the walk
    /// has read its parent's level is not in the target set, and survives with
    /// `parent_workflow_id` naming a row that is gone. Interleaving would not fix it and neither
    /// would a wider transaction; [`cancel_all`](Self::cancel_all) with [`Children::Include`]
    /// first, then deleting, is how a tree is stopped before it is removed.
    ///
    /// **A workflow cannot delete itself, or an ancestor it would go down with.** From inside a
    /// workflow this call is a step, and the step's checkpoint is written in the same transaction
    /// as the delete — for a row the delete has just removed, leaving an orphan checkpoint and a
    /// workflow running with no row to finish on. So a target set containing the calling
    /// workflow is refused whole:
    /// [`Error::SystemDatabase`] carrying `InvalidInput`, and nothing is deleted. Deleting the
    /// caller's own tree from outside it, or deleting an unrelated tree from inside a workflow,
    /// is unaffected.
    pub fn delete_all<'a>(
        &self,
        workflow_ids: &'a [&'a str],
        children: Children,
    ) -> PendingStep<'a, u64> {
        pending_delete_all(self.connection("delete a workflow"), workflow_ids, children)
    }

    /// Holds a queued workflow back, or lets it go sooner.
    ///
    /// **From a workflow body, reach for the free [`set_workflow_delay`](fn@set_workflow_delay)
    /// instead.** It is this call with no handle to capture, recorded under the same step name; a
    /// captured [`DBOS`] is a cycle with the registry that holds the workflow's closure.
    ///
    /// **Only a `DELAYED` row moves.** A workflow that has already been released is running or
    /// queued, and pushing its delay out would not recall it — so this is a way to reschedule work
    /// that is still waiting, not a way to pause work that has started.
    /// [`cancel`](Self::cancel) is the one that stops something.
    ///
    /// [`WorkflowDelay::For`] is resolved by the write against the **database's** clock, the same
    /// clock the release is decided by, as the delay on an enqueue is. So the release lands the
    /// delay after the write (counted from the start of its transaction) whichever executor runs
    /// it, and no caller's skew reaches the row. An
    /// instant past the largest one storable saturates to it, a release that never comes.
    ///
    /// [`WorkflowDelay::Until`] is written as given; it is an instant on the database clock, so a
    /// caller computing it from its own clock carries its skew into the row.
    ///
    /// A [`WorkflowDelay::For`] too long to store as milliseconds in an `i64` is
    /// [`Error::InvalidArgument`], refused before the call takes a step id.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// use std::time::Duration;
    /// // Not before the hour is up.
    /// dbos.set_workflow_delay("scheduled-report", dbos::WorkflowDelay::For(Duration::from_secs(3600))).await?;
    /// # Ok(()) }
    /// ```
    pub fn set_workflow_delay<'a>(
        &self,
        workflow_id: &'a str,
        delay: WorkflowDelay,
    ) -> PendingStep<'a, ()> {
        pending_set_workflow_delay(self.connection(DELAY_OPERATION), workflow_id, delay)
    }

    /// Replaces a workflow's attributes, or clears them with `None`.
    ///
    /// **From a workflow body, reach for the free
    /// [`update_workflow_attributes`](fn@update_workflow_attributes) instead.** It is this call
    /// with no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle
    /// with the registry that holds the workflow's closure.
    ///
    /// **A replacement, not a merge**, so a caller adding one key must send the others back with
    /// it, and two callers adding different keys race to drop each other's.
    /// [`merge_workflow_attributes`](Self::merge_workflow_attributes) adds keys without either
    /// problem.
    ///
    /// **A map, not encoded JSON**, encoded here rather than in the system database layer, which
    /// cannot: it handles every payload as an opaque string so a host across an FFI boundary can
    /// hand over bytes it already has, which is why `validate_attributes` checks the object shape
    /// down there *"rather than falling out of the type"*. Here it falls out of the type. This is
    /// the layer that encodes, and it is the only one that can.
    ///
    /// The object shape is not decoration: [`WorkflowFilter::attributes`] queries the column with
    /// `@>` containment, and containment against a stored array or scalar silently never matches.
    ///
    /// **An id no workflow has is refused** with
    /// [`NonExistentWorkflow`](crate::sysdb::Error::NonExistentWorkflow), rather than reported as
    /// done with nothing written. Inside a workflow the refusal is the step's recorded outcome, so
    /// a replay sees it too.
    ///
    /// `serde_json::Value::as_object` is the usual way in — and note that it answers `None` for a
    /// value that is not an object, which **clears** rather than refusing. Build a
    /// `serde_json::Map` directly when the attributes are assembled at runtime.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let tags = serde_json::json!({ "tenant": "acme", "tier": "gold" });
    /// dbos.update_workflow_attributes("an-order", tags.as_object()).await?;
    /// dbos.update_workflow_attributes("an-order", None).await?; // and cleared
    /// # Ok(()) }
    /// ```
    pub fn update_workflow_attributes<'a>(
        &self,
        workflow_id: &'a str,
        attributes: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> PendingStep<'a, ()> {
        pending_update_workflow_attributes(
            self.connection("update a workflow's attributes"),
            workflow_id,
            attributes,
        )
    }

    /// Merges keys into a workflow's attributes, in one statement.
    ///
    /// **From a workflow body, reach for the free
    /// [`merge_workflow_attributes`](fn@merge_workflow_attributes) instead.** It is this call with
    /// no handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle with
    /// the registry that holds the workflow's closure.
    ///
    /// **A shallow merge of the top-level keys.** A key already there takes the new value, and the
    /// others stay as they are. A key set to JSON `null` is stored as `null`, not removed. If the
    /// workflow has no attributes, or has ones that are not an object, the result is just these,
    /// so an empty map leaves `{}` where there were none.
    ///
    /// **Atomic**, so two callers merging different keys into the same workflow both land, which a
    /// read followed by [`update_workflow_attributes`](Self::update_workflow_attributes) cannot
    /// promise. Inside a workflow it is recorded under the same step name as a replacement.
    ///
    /// An id no workflow has is refused, as by
    /// [`update_workflow_attributes`](Self::update_workflow_attributes).
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let more = serde_json::json!({ "reviewed_by": "ops" });
    /// dbos.merge_workflow_attributes("an-order", more.as_object().unwrap()).await?;
    /// # Ok(()) }
    /// ```
    pub fn merge_workflow_attributes<'a>(
        &self,
        workflow_id: &'a str,
        attributes: &serde_json::Map<String, serde_json::Value>,
    ) -> PendingStep<'a, ()> {
        pending_merge_workflow_attributes(
            self.connection("merge a workflow's attributes"),
            workflow_id,
            attributes,
        )
    }

    /// Reads the workflows matching a filter, oldest first unless the filter says otherwise.
    ///
    /// **From a workflow body, reach for the free [`list_workflows`](fn@list_workflows) instead.**
    /// It is this call with no handle to capture, recorded under the same step name; a captured
    /// [`DBOS`] is a cycle with the registry that holds the workflow's closure.
    ///
    /// **Every filter is one `WHERE` clause**, not a scan the caller narrows afterwards, so
    /// `WorkflowFilter::default()` returns the whole table. A caller that means to page should say
    /// so with [`WorkflowFilter::limit`] — and should know that `created_at` is a millisecond
    /// stamp shared by everything a fan-out creates in the same instant, so a page boundary
    /// falling inside a tie is not stable.
    ///
    /// The filter defaults to **this application's workflows plus unclaimed ones**, not to every
    /// application sharing the database; [`AppScope::Any`](crate::sysdb::types::AppScope)
    /// is the operator's cross-application view. Naming ids explicitly widens it on its own, since
    /// a workflow id is a global address rather than a search.
    ///
    /// There is no separate listing of queued workflows. [`WorkflowFilter::queues_only`] is that
    /// method, and it composes with every other filter instead of being a second entry point that
    /// repeats them.
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
    pub fn list_workflows<'a>(
        &self,
        filter: &'a WorkflowFilter<'a>,
    ) -> PendingStep<'a, Vec<WorkflowRecord>> {
        pending_list_workflows(self.connection("list workflows"), filter)
    }

    /// Reads one workflow's steps, in execution order.
    ///
    /// **From a workflow body, reach for the free [`list_workflow_steps`](fn@list_workflow_steps)
    /// instead.** It is this call with no handle to capture, recorded under the same step name; a
    /// captured [`DBOS`] is a cycle with the registry that holds the workflow's closure.
    ///
    /// Every step, with its output or error.
    /// [`list_workflow_steps_with`](Self::list_workflow_steps_with) can leave the outputs out, or
    /// read one page of a long workflow's steps.
    ///
    /// A step whose [`child_workflow_id`](StepRecord::child_workflow_id) is set was a child
    /// workflow call rather than a step body, which is how a tree is walked from a listing.
    ///
    /// An id with no row returns no steps rather than failing, the same as an id whose workflow
    /// has not reached its first step.
    pub fn list_workflow_steps<'a>(
        &self,
        workflow_id: &'a str,
    ) -> PendingStep<'a, Vec<StepRecord>> {
        self.list_workflow_steps_with(workflow_id, ListWorkflowStepsOptions::default())
    }

    /// Reads one workflow's steps, choosing what each step carries and which page to read.
    ///
    /// **From a workflow body, reach for the free
    /// [`list_workflow_steps_with`](fn@list_workflow_steps_with) instead.** It is this call with no
    /// handle to capture, recorded under the same step name; a captured [`DBOS`] is a cycle with
    /// the registry that holds the workflow's closure.
    ///
    /// See [`list_workflow_steps`](Self::list_workflow_steps).
    /// [`load_output`](ListWorkflowStepsOptions::load_output) off reads only the steps' metadata,
    /// for a listing of a workflow whose step outputs are large;
    /// [`limit`](ListWorkflowStepsOptions::limit) and [`offset`](ListWorkflowStepsOptions::offset)
    /// page through a workflow with many steps.
    pub fn list_workflow_steps_with<'a>(
        &self,
        workflow_id: &'a str,
        options: ListWorkflowStepsOptions,
    ) -> PendingStep<'a, Vec<StepRecord>> {
        pending_list_workflow_steps(
            self.connection("list a workflow's steps"),
            workflow_id,
            options,
        )
    }
}

/// The same surface from outside the application, where an operator's tool usually stands.
///
/// Every method here is [`DBOS`]'s, and the differences are the two a client always has. **There is
/// no launch check**, because a client is connected or it does not exist — `connect` hands back a
/// usable client or an error, so none of these can fail with [`Error::NotLaunched`]. And **nothing
/// is checkpointed**: called from inside a workflow, a client's management call runs again on
/// replay, where the same call on `DBOS` would replay its recorded step. A client has no step
/// counter of its own to agree with the workflow's, and the ambient context belongs to an instance
/// this client is not — the line [`WorkflowHandle::result`](crate::WorkflowHandle::result) already
/// draws for a client's handle awaited inside a workflow.
///
/// **Scope follows the client's application name.** A nameless client reads and writes across every
/// application sharing the database, which is what a cross-application operator's tool wants and
/// what a client acting for one application must not do by accident — see
/// [`ClientConfig::app_name`](crate::ClientConfig::app_name).
impl crate::Client {
    /// Stops a workflow, and any executor running it.
    ///
    /// See [`DBOS::cancel`]. A cancelled workflow stays in the database and can be
    /// [`resume`](Self::resume)d.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// client.cancel("stuck-workflow").await?;
    /// # Ok(()) }
    /// ```
    pub async fn cancel(&self, workflow_id: &str) -> Result<()> {
        self.cancel_all(&[workflow_id], Children::Skip).await?;
        Ok(())
    }

    /// Cancels workflows, and optionally everything descended from them.
    ///
    /// See [`DBOS::cancel_all`] for what the cascade does and why it is one transaction. Returns
    /// the ids that actually moved.
    pub async fn cancel_all(
        &self,
        workflow_ids: &[&str],
        children: Children,
    ) -> Result<Vec<String>> {
        self.connection()
            .cancel_all(workflow_ids, children, None, None)
            .await
    }

    /// Puts a workflow back on a queue, and hands back a handle to watch it.
    ///
    /// See [`DBOS::resume`]. The handle polls, as every handle a client holds does.
    pub async fn resume<R, E>(&self, workflow_id: &str) -> Result<WorkflowHandle<R, E>> {
        self.resume_with(workflow_id, ResumeOptions::default())
            .await
    }

    /// [`resume`](Self::resume), naming the queue the workflow goes back on.
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

    /// Resumes workflows, handing back a handle to each in the order given.
    pub async fn resume_all<R, E>(
        &self,
        workflow_ids: &[&str],
        options: ResumeOptions<'_>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        self.connection()
            .resume_all(workflow_ids, options, None, None)
            .await
    }

    /// Forks a workflow from `start_step`, enqueueing the fork and handing back a handle to it.
    ///
    /// See [`DBOS::fork`]. **This is what an operator's tool exists for**: a workflow that failed
    /// against broken code is forked onto a fixed deployment, and
    /// [`ForkOptions::app_version`] is how it names one.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let steps = client.list_workflow_steps("failed-workflow").await?;
    /// let failed = steps.iter().rev().find(|s| s.error.is_some()).map_or(0, |s| s.step_id);
    /// let handle = client
    ///     .fork_with::<u32, dbos::EngineOnly>(
    ///         dbos::Fork::new("failed-workflow", failed),
    ///         dbos::ForkOptions { app_version: Some("1.4.0"), ..Default::default() },
    ///     )
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn fork<R, E>(
        &self,
        workflow_id: &str,
        start_step: i32,
    ) -> Result<WorkflowHandle<R, E>> {
        self.fork_with(Fork::new(workflow_id, start_step), ForkOptions::default())
            .await
    }

    /// [`fork`](Self::fork), with the chosen id, version, queue, timeout and replacement children
    /// spelled out. See [`DBOS::fork_with`].
    pub async fn fork_with<R, E>(
        &self,
        fork: Fork<'_>,
        options: ForkOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        refuse_invalid_forks(std::slice::from_ref(&fork), &options)?;
        self.connection()
            .fork_one(fork.to_sys(), &options.to_sys(), None)
            .await
    }

    /// Forks workflows, handing back a handle to each fork in the order given.
    ///
    /// See [`DBOS::fork_all`]: every source must exist, or the call fails and no fork is written.
    /// A fan-out re-run against fixed code is exactly the job an operator reaches for a client to
    /// do.
    pub async fn fork_all<R, E>(
        &self,
        forks: &[Fork<'_>],
        options: ForkOptions<'_>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        refuse_invalid_forks(forks, &options)?;
        let forks: Vec<SysFork<'_>> = forks.iter().map(|fork| fork.to_sys()).collect();
        self.connection()
            .fork_all(&forks, &options.to_sys(), None)
            .await
    }

    /// Re-runs a finished workflow from its first step, under the same id.
    ///
    /// See [`DBOS::rewind`]. The handle polls, as every handle a client holds does.
    pub async fn rewind<R, E>(&self, workflow_id: &str) -> Result<WorkflowHandle<R, E>> {
        self.rewind_with(workflow_id, RewindOptions::default())
            .await
    }

    /// [`rewind`](Self::rewind), choosing the step it re-runs from and where it runs.
    ///
    /// See [`DBOS::rewind_with`] for what is discarded and what is kept.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let handle = client
    ///     .rewind_with::<u32, dbos::EngineOnly>(
    ///         "failed-workflow",
    ///         dbos::RewindOptions { start_step: 2, ..Default::default() },
    ///     )
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn rewind_with<R, E>(
        &self,
        workflow_id: &str,
        options: RewindOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        refuse_invalid_rewind_options(&options)?;
        self.connection().rewind(workflow_id, options, None).await
    }

    /// Removes a workflow and everything recorded against it.
    ///
    /// See [`DBOS::delete`]. Anyone awaiting the deleted workflow goes on waiting — deleting a row
    /// and never creating it are indistinguishable to a waiter, which
    /// [`WorkflowHandle::result`](crate::WorkflowHandle::result) records.
    pub async fn delete(&self, workflow_id: &str) -> Result<()> {
        self.delete_all(&[workflow_id], Children::Skip).await?;
        Ok(())
    }

    /// Deletes workflows, and optionally everything descended from them. Returns the rows removed.
    pub async fn delete_all(&self, workflow_ids: &[&str], children: Children) -> Result<u64> {
        self.connection()
            .delete_all(workflow_ids, children, None)
            .await
    }

    /// Moves a delayed workflow's release time.
    ///
    /// See [`DBOS::set_workflow_delay`]. Only a `DELAYED` row moves: a workflow already released is
    /// running or queued, and pushing its delay out would not recall it.
    pub async fn set_workflow_delay(&self, workflow_id: &str, delay: WorkflowDelay) -> Result<()> {
        self.connection()
            .set_workflow_delay(workflow_id, delay, None)
            .await
    }

    /// Replaces the attributes attached to a workflow, or clears them.
    ///
    /// See [`DBOS::update_workflow_attributes`]. `None` clears.
    pub async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Result<()> {
        let encoded = crate::workflow::encode_attributes(attributes)?;
        self.connection()
            .update_workflow_attributes(
                workflow_id,
                AttributeUpdate::Replace(encoded.as_deref()),
                None,
            )
            .await
    }

    /// Merges keys into a workflow's attributes, in one statement.
    ///
    /// See [`DBOS::merge_workflow_attributes`].
    pub async fn merge_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        let encoded = crate::workflow::encode_attribute_map(attributes)?;
        self.connection()
            .update_workflow_attributes(workflow_id, AttributeUpdate::Merge(&encoded), None)
            .await
    }

    /// Reads the workflows matching a filter.
    ///
    /// See [`DBOS::list_workflows`]. Queued workflows are listed with
    /// [`WorkflowFilter::queues_only`] rather than a second method.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// use dbos::sysdb::types::{WorkflowFilter, WorkflowStatus};
    ///
    /// let stuck = client
    ///     .list_workflows(&WorkflowFilter {
    ///         status: vec![WorkflowStatus::Pending],
    ///         ..WorkflowFilter::default()
    ///     })
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn list_workflows(&self, filter: &WorkflowFilter<'_>) -> Result<Vec<WorkflowRecord>> {
        self.connection().list_workflows(filter, None).await
    }

    /// Reads the steps a workflow has recorded, in the order it recorded them, each with its output
    /// or error.
    ///
    /// [`list_workflow_steps_with`](Self::list_workflow_steps_with) can leave the outputs out, or
    /// read one page of a long workflow's steps.
    pub async fn list_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepRecord>> {
        self.list_workflow_steps_with(workflow_id, ListWorkflowStepsOptions::default())
            .await
    }

    /// Reads the steps a workflow has recorded, choosing what each step carries and which page to
    /// read.
    ///
    /// See [`DBOS::list_workflow_steps_with`].
    pub async fn list_workflow_steps_with(
        &self,
        workflow_id: &str,
        options: ListWorkflowStepsOptions,
    ) -> Result<Vec<StepRecord>> {
        self.connection()
            .list_workflow_steps(workflow_id, options, None)
            .await
    }
}

/// The operator surface, on the connection every caller reaches it through.
///
/// Each method here is the whole operation — the system-database call, its logging, and the handles
/// it hands back — and takes `caller` for the one thing the surfaces above it disagree about.
/// [`DBOS`] passes the workflow it is being called from, so the call checkpoints as a step and a
/// replay does not run it twice; [`Client`](crate::Client) always passes `None`, because a client
/// has no step counter of its own to agree with a workflow's. That is the same line
/// [`WorkflowHandle::result`](crate::WorkflowHandle::result) draws for a client's handle awaited
/// inside a workflow, and `Connection::get_event` for a client's read.
///
/// Cancel and resume also take `from_workflow`, the workflow making the call, which they refuse to
/// target. It is not `caller` with the step dropped: inside a step body the call records nothing
/// and `caller` is `None`, but the workflow is still the one calling. A client passes `None` for
/// both.
impl Connection {
    /// Cancels workflows, optionally cascading, and reports which ones moved.
    pub(crate) async fn cancel_all(
        &self,
        workflow_ids: &[&str],
        children: Children,
        from_workflow: Option<&str>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<Vec<String>> {
        let cancelled = self
            .sysdb()
            .cancel_workflows(
                workflow_ids,
                children == Children::Include,
                from_workflow,
                self.step_caller(caller),
            )
            .await
            .map_err(Error::from_sysdb)?;
        if !cancelled.is_empty() {
            tracing::info!(cancelled = cancelled.len(), "cancelled workflows");
        }
        Ok(cancelled)
    }

    /// Puts workflows back on a queue, and hands back a handle to each.
    ///
    /// `self: &Arc<Self>` because the handles hold the connection they poll through, which is this
    /// one — the same reason `WorkflowHandle::polling` takes one by `Arc`.
    pub(crate) async fn resume_all<R, E>(
        self: &Arc<Self>,
        workflow_ids: &[&str],
        options: ResumeOptions<'_>,
        from_workflow: Option<&str>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        let resumed = self
            .sysdb()
            .resume_workflows(
                workflow_ids,
                options.queue_name,
                from_workflow,
                self.step_caller(caller),
            )
            .await
            .map_err(Error::from_sysdb)?;
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
            .map(|id| WorkflowHandle::polling(Arc::clone(self), (*id).to_owned(), true))
            .collect())
    }

    /// Forks workflows, and hands back a handle to each fork.
    pub(crate) async fn fork_all<R, E>(
        self: &Arc<Self>,
        forks: &[SysFork<'_>],
        options: &SysForkOptions<'_>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        let forked = self
            .sysdb()
            .fork_workflows(forks, options, self.step_caller(caller))
            .await
            .map_err(Error::from_sysdb)?;

        match forked.as_slice() {
            [only] => tracing::info!(forked_id = only, "forked the workflow onto its queue"),
            many => tracing::info!(count = many.len(), "forked workflows onto their queues"),
        }
        Ok(forked
            .into_iter()
            // Each fork's row was written by the batch above.
            .map(|id| WorkflowHandle::polling(Arc::clone(self), id, true))
            .collect())
    }

    /// Forks one workflow, as a batch of one, and hands back a handle to its fork.
    pub(crate) async fn fork_one<R, E>(
        self: &Arc<Self>,
        fork: SysFork<'_>,
        options: &SysForkOptions<'_>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<WorkflowHandle<R, E>> {
        self.fork_all(std::slice::from_ref(&fork), options, caller)
            .await?
            .pop()
            .ok_or_else(|| {
                Error::Config(format!(
                    "forking `{}` produced no workflow",
                    fork.original_id
                ))
            })
    }

    /// Rewinds a workflow, and hands back a handle to its re-run.
    pub(crate) async fn rewind<R, E>(
        self: &Arc<Self>,
        workflow_id: &str,
        options: RewindOptions<'_>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<WorkflowHandle<R, E>> {
        let sys_options = SysRewindOptions {
            application_version: options.app_version,
            queue_name: options.queue_name,
            queue_partition_key: options.queue_partition_key,
        };
        self.sysdb()
            .rewind_workflow(
                workflow_id,
                options.start_step,
                &sys_options,
                self.step_caller(caller),
            )
            .await
            .map_err(Error::from_sysdb)?;
        tracing::info!(
            workflow_id,
            start_step = options.start_step,
            "rewound the workflow onto its queue"
        );
        // The rewind refuses an id with no row, so this names one it has just re-enqueued.
        Ok(WorkflowHandle::polling(
            Arc::clone(self),
            workflow_id.to_owned(),
            true,
        ))
    }

    /// Removes workflows and everything recorded against them, and reports how many rows went.
    pub(crate) async fn delete_all(
        &self,
        workflow_ids: &[&str],
        children: Children,
        caller: Option<StepWriter<'_>>,
    ) -> Result<u64> {
        let deleted = self
            .sysdb()
            .delete_workflows(
                workflow_ids,
                children == Children::Include,
                self.step_caller(caller),
            )
            .await
            .map_err(Error::from_sysdb)?;
        if deleted > 0 {
            tracing::info!(deleted, "deleted workflows");
        }
        Ok(deleted)
    }

    /// Moves a delayed workflow's release time.
    pub(crate) async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay: WorkflowDelay,
        caller: Option<StepWriter<'_>>,
    ) -> Result<()> {
        self.sysdb()
            .set_workflow_delay(workflow_id, delay, self.step_caller(caller))
            .await
            .map_err(|error| Error::refused_argument(DELAY_OPERATION, error))?;
        // Phrased as the request rather than the effect. The statement is guarded on the row
        // still being `DELAYED` and reports no count, so this call cannot tell a workflow that
        // was rescheduled from one that had already been released — unlike `cancel_all` and
        // `delete_all`, which log what they counted.
        tracing::info!(workflow_id, "asked to move the workflow's release time");
        Ok(())
    }

    /// Replaces a workflow's attributes, clears them, or merges keys into them.
    ///
    /// The JSON in `update` is plain JSON, never the configured
    /// [`Serializer`](crate::Serializer): the column is read by `@>` containment and by other
    /// SDKs on the same database, so what a workflow chose for its own payloads has no say in it.
    /// Encoded by the caller rather than here, because [`DBOS::update_workflow_attributes`] and
    /// [`DBOS::merge_workflow_attributes`] have to encode before they take their step ids.
    pub(crate) async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        update: AttributeUpdate<'_>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<()> {
        self.sysdb()
            .update_workflow_attributes(workflow_id, update, self.step_caller(caller))
            .await
            .map_err(Error::from_sysdb)?;
        // "Asked to", because a replay reads back the recorded outcome and writes nothing. An id
        // with no row behind it was refused above.
        let verb = match update {
            AttributeUpdate::Replace(_) => "replace",
            AttributeUpdate::Merge(_) => "merge into",
        };
        tracing::info!(workflow_id, "asked to {verb} the workflow's attributes");
        Ok(())
    }

    /// Reads workflow rows matching a filter.
    pub(crate) async fn list_workflows(
        &self,
        filter: &WorkflowFilter<'_>,
        caller: Option<StepWriter<'_>>,
    ) -> Result<Vec<WorkflowRecord>> {
        self.sysdb()
            .list_workflows(filter, self.step_caller(caller))
            .await
            .map_err(Error::from_sysdb)
    }

    /// Reads the steps one workflow has recorded, in the order it recorded them.
    pub(crate) async fn list_workflow_steps(
        &self,
        workflow_id: &str,
        options: ListWorkflowStepsOptions,
        caller: Option<StepWriter<'_>>,
    ) -> Result<Vec<StepRecord>> {
        self.sysdb()
            .list_workflow_steps(
                workflow_id,
                options.load_output,
                options.limit,
                options.offset,
                self.step_caller(caller),
            )
            .await
            .map_err(Error::from_sysdb)
    }
}

// The one body each operation has, behind its two surfaces.
//
// Each `pending_*` below is the whole of an operation as a step: its recorded name, the argument
// checks that must refuse before an id is spent, the placement, and the run. The method on `DBOS`
// and the free function are each one line over it, and differ only in what they hand it as `conn`
// — the handle's executor's connection, refused as `NotLaunched` before launch, or the ambient
// workflow's, refused as `NotInWorkflow` outside one — and in the error channel they ask for. That
// is what makes a workflow that switches from one to the other replay the history it recorded:
// there is no second copy of the step name or of the order of refusals to drift.
//
// The ambient connection is the context's own, so `StepPlacement::of` never sees two instances
// there and `WrongInstance` is the method's alone; the `operation` passed to it names the call for
// that error.

/// Where a management call stands: the connection refused first, then the id.
fn place(conn: Result<Arc<Connection>>, operation: &'static str) -> Built<Arc<Connection>> {
    conn.and_then(|conn| Connection::placed(&conn, operation))
}

/// [`place`] with a check between the connection and the id, for the calls with arguments they
/// can refuse — or a payload they must encode — before a step id is spent. What the check produces
/// travels beside the connection into the run.
fn place_after<C>(
    conn: Result<Arc<Connection>>,
    operation: &'static str,
    check: impl FnOnce() -> Result<C>,
) -> Built<(Arc<Connection>, C)> {
    let conn = conn?;
    let checked = check()?;
    let placement = StepPlacement::of(&conn, operation)?;
    Ok(((conn, checked), placement))
}

/// A handle to a workflow by id, taken on faith: the body of both `retrieve_workflow`s.
fn retrieved<R, E>(conn: Arc<Connection>, workflow_id: &str) -> WorkflowHandle<R, E> {
    // Nothing here has seen the row: the id is the caller's, taken on faith, which is the one
    // handle shape that waits for a row to appear rather than reporting it missing.
    WorkflowHandle::polling(conn, workflow_id.to_owned(), false)
}

fn pending_cancel<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::CANCEL_WORKFLOW,
        place(conn, "cancel a workflow"),
        move |conn, placement| async move {
            conn.cancel_all(
                &[workflow_id],
                Children::Skip,
                placement.workflow_id(),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)?;
            Ok(())
        },
    )
}

fn pending_cancel_all<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_ids: &'a [&'a str],
    children: Children,
) -> PendingStep<'a, Vec<String>, E> {
    PendingStep::placed(
        step_names::CANCEL_WORKFLOW,
        place(conn, "cancel a workflow"),
        move |conn, placement| async move {
            conn.cancel_all(
                workflow_ids,
                children,
                placement.workflow_id(),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)
        },
    )
}

/// `R` and `E` are the handle's; the call answers in the engine's channel, which the surfaces
/// re-declare.
fn pending_resume<'a, R: 'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    options: ResumeOptions<'a>,
) -> PendingStep<'a, WorkflowHandle<R, E>> {
    PendingStep::placed(
        step_names::RESUME_WORKFLOW,
        place(conn, "resume a workflow"),
        move |conn, placement| async move {
            conn.resume_all(
                &[workflow_id],
                options,
                placement.workflow_id(),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)?
            .pop()
            .ok_or_else(|| Error::Config(format!("resuming `{workflow_id}` produced no handle")))
        },
    )
}

fn pending_resume_all<'a, R: 'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_ids: &'a [&'a str],
    options: ResumeOptions<'a>,
) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
    PendingStep::placed(
        step_names::RESUME_WORKFLOW,
        place(conn, "resume a workflow"),
        move |conn, placement| async move {
            conn.resume_all(
                workflow_ids,
                options,
                placement.workflow_id(),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)
        },
    )
}

/// The fork is checked whole before the id is taken, so a refused fork moves the workflow's
/// counter no more than an unlaunched instance does.
fn pending_fork<'a, R: 'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    fork: Fork<'a>,
    options: ForkOptions<'a>,
) -> PendingStep<'a, WorkflowHandle<R, E>> {
    let built = place_after(conn, FORK_OPERATION, || {
        refuse_invalid_forks(std::slice::from_ref(&fork), &options)
    })
    .map(|((conn, ()), placement)| (conn, placement));
    PendingStep::placed(
        step_names::FORK_WORKFLOW,
        built,
        move |conn, placement| async move {
            conn.fork_one(fork.to_sys(), &options.to_sys(), placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

fn pending_fork_all<'a, R: 'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    forks: &'a [Fork<'a>],
    options: ForkOptions<'a>,
) -> PendingStep<'a, Vec<WorkflowHandle<R, E>>> {
    let built = place_after(conn, FORK_OPERATION, || {
        refuse_invalid_forks(forks, &options)
    })
    .map(|((conn, ()), placement)| (conn, placement));
    PendingStep::placed(
        step_names::FORK_WORKFLOW,
        built,
        move |conn, placement| async move {
            let forks: Vec<SysFork<'_>> = forks.iter().map(|fork| fork.to_sys()).collect();
            conn.fork_all(&forks, &options.to_sys(), placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

fn pending_rewind<'a, R: 'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    options: RewindOptions<'a>,
) -> PendingStep<'a, WorkflowHandle<R, E>> {
    let built = place_after(conn, "rewind a workflow", || {
        refuse_invalid_rewind_options(&options)
    })
    .map(|((conn, ()), placement)| (conn, placement));
    PendingStep::placed(
        step_names::REWIND_WORKFLOW,
        built,
        move |conn, placement| async move {
            conn.rewind(workflow_id, options, placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

fn pending_delete<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::DELETE_WORKFLOW,
        place(conn, "delete a workflow"),
        move |conn, placement| async move {
            conn.delete_all(&[workflow_id], Children::Skip, placement.step_writer())
                .await
                .map_err(Error::lift)?;
            Ok(())
        },
    )
}

fn pending_delete_all<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_ids: &'a [&'a str],
    children: Children,
) -> PendingStep<'a, u64, E> {
    PendingStep::placed(
        step_names::DELETE_WORKFLOW,
        place(conn, "delete a workflow"),
        move |conn, placement| async move {
            conn.delete_all(workflow_ids, children, placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

/// A delay too long to store refuses the call without moving the workflow's step counter.
fn pending_set_workflow_delay<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    delay: WorkflowDelay,
) -> PendingStep<'a, (), E> {
    let built = place_after(conn, DELAY_OPERATION, || refuse_unstorable_delay(delay))
        .map(|((conn, ()), placement)| (conn, placement));
    PendingStep::placed(
        step_names::SET_WORKFLOW_DELAY,
        built,
        move |conn, placement| async move {
            conn.set_workflow_delay(workflow_id, delay, placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

/// Encoded before the id is taken, as every other call in the crate encodes before it places:
/// attributes that cannot be encoded are a call that never happens, and a call that never happens
/// must not move the workflow's counter.
fn pending_update_workflow_attributes<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    attributes: Option<&serde_json::Map<String, serde_json::Value>>,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::UPDATE_WORKFLOW_ATTRIBUTES,
        place_after(conn, "update a workflow's attributes", || {
            crate::workflow::encode_attributes(attributes)
        }),
        move |(conn, encoded), placement| async move {
            conn.update_workflow_attributes(
                workflow_id,
                AttributeUpdate::Replace(encoded.as_deref()),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)
        },
    )
}

/// Encoded before the id is taken, as in [`pending_update_workflow_attributes`].
fn pending_merge_workflow_attributes<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    attributes: &serde_json::Map<String, serde_json::Value>,
) -> PendingStep<'a, (), E> {
    PendingStep::placed(
        step_names::UPDATE_WORKFLOW_ATTRIBUTES,
        place_after(conn, "merge a workflow's attributes", || {
            crate::workflow::encode_attribute_map(attributes)
        }),
        move |(conn, encoded), placement| async move {
            conn.update_workflow_attributes(
                workflow_id,
                AttributeUpdate::Merge(&encoded),
                placement.step_writer(),
            )
            .await
            .map_err(Error::lift)
        },
    )
}

fn pending_list_workflows<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    filter: &'a WorkflowFilter<'a>,
) -> PendingStep<'a, Vec<WorkflowRecord>, E> {
    PendingStep::placed(
        step_names::LIST_WORKFLOWS,
        place(conn, "list workflows"),
        move |conn, placement| async move {
            conn.list_workflows(filter, placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

fn pending_list_workflow_steps<'a, E: 'a>(
    conn: Result<Arc<Connection>>,
    workflow_id: &'a str,
    options: ListWorkflowStepsOptions,
) -> PendingStep<'a, Vec<StepRecord>, E> {
    PendingStep::placed(
        step_names::LIST_WORKFLOW_STEPS,
        place(conn, "list a workflow's steps"),
        move |conn, placement| async move {
            conn.list_workflow_steps(workflow_id, options, placement.step_writer())
                .await
                .map_err(Error::lift)
        },
    )
}

/// Refuses a negative rewind start step. `field` names the step in the message
/// as the caller passed it.
///
/// **Made by the surfaces rather than inside the connection**, because a refused call must not
/// spend a step id and the connection is only reached once one has been taken. The system
/// database checks it again for its own callers.
fn refuse_negative_start_step(operation: &'static str, field: &str, start_step: i32) -> Result<()> {
    if start_step < 0 {
        return Err(Error::InvalidArgument {
            operation: operation.into(),
            detail: format!("{field} must not be negative, got {start_step}"),
        });
    }
    Ok(())
}

/// What a refused delay names as the operation.
const DELAY_OPERATION: &str = "delay a workflow";

/// Refuses a relative delay too long to store as milliseconds.
///
/// Made by the surface rather than inside the connection, as [`refuse_negative_start_step`] is,
/// so a refused call spends no step id. It is the only refusal a delay can meet: one that fits is
/// resolved by the system database on its own clock and saturates rather than failing.
fn refuse_unstorable_delay(delay: WorkflowDelay) -> Result<()> {
    match delay {
        WorkflowDelay::For(duration) => duration_to_ms("delay", duration)
            .map(drop)
            .map_err(|error| Error::refused_argument(DELAY_OPERATION, error)),
        WorkflowDelay::Until(_) => Ok(()),
    }
}

/// What a refused fork names as the operation.
const FORK_OPERATION: &str = "fork a workflow";

/// Refuses a fork call the system database would refuse, or one that cannot succeed.
///
/// Made by the surfaces rather than inside the connection, as [`refuse_negative_start_step`] is,
/// so a refused call spends no step id. The first bad fork refuses the whole batch, the valid
/// ones beside it included.
///
/// The per-fork and option rules are the system database's own, run through
/// [`Fork::validate`] and [`ForkOptions::validate`], so the two layers cannot drift apart. What
/// this adds is what only the whole batch can see: one forked id on two forks, and a forked id
/// that is also an original id in the batch. An original must exist, so forking onto its id is
/// certain to hit the primary key. An id taken by a workflow outside the batch is left to the
/// database.
fn refuse_invalid_forks(forks: &[Fork<'_>], options: &ForkOptions<'_>) -> Result<()> {
    let refuse = |detail: String| {
        Err(Error::InvalidArgument {
            operation: FORK_OPERATION.into(),
            detail,
        })
    };
    let original_ids: HashSet<&str> = forks.iter().map(|fork| fork.original_id).collect();
    let mut forked_ids = HashSet::with_capacity(forks.len());
    for fork in forks {
        fork.validate()?;
        if let Some(id) = fork.forked_id {
            if original_ids.contains(id) {
                return refuse(format!("`{id}` is both a forked id and an original id"));
            }
            if !forked_ids.insert(id) {
                return refuse(format!("`{id}` is the forked id of more than one fork"));
            }
        }
    }
    options.validate()
}

/// A system database refusal of a fork's arguments, as the surface reports it: an
/// [`Error::InvalidArgument`] whose detail names the field through `field`.
fn refused_fork(error: crate::sysdb::Error, field: impl FnOnce(&str) -> String) -> Error {
    Error::from_refusal(error, |name, detail| Error::InvalidArgument {
        operation: FORK_OPERATION.into(),
        detail: format!("{}: {detail}", field(name)),
    })
}

/// Refuses a negative rewind step, and an option given as an empty string.
///
/// Made by the surfaces rather than inside the connection, as
/// [`refuse_negative_start_step`] is, so a refused call spends no step id. The system
/// database checks the same things again for its own callers.
fn refuse_invalid_rewind_options(options: &RewindOptions<'_>) -> Result<()> {
    refuse_negative_start_step(
        "rewind a workflow",
        "RewindOptions::start_step",
        options.start_step,
    )?;
    for (field, value) in [
        ("app_version", options.app_version),
        ("queue_name", options.queue_name),
        ("queue_partition_key", options.queue_partition_key),
    ] {
        if value == Some("") {
            return Err(Error::InvalidArgument {
                operation: "rewind a workflow".into(),
                detail: format!("RewindOptions::{field} must be absent rather than empty"),
            });
        }
    }
    Ok(())
}
