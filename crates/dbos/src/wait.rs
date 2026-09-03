//! Waiting on several workflows at once: the first to finish, or all of them.
//!
//! A fan-out starts N workflows and then has to learn when they finish. Awaiting each handle in
//! turn already works and costs nothing in wall-clock — the caller takes about as long as the
//! slowest, which [`WorkflowRef::start_with`](crate::WorkflowRef::start_with) documents — but it
//! answers only in the order the caller happened to ask, and it holds N waits open where one would
//! do. These two calls are the other shape: **one query per interval whatever N is**, answering
//! either *which one finished first* or *tell me when they all have*.
//!
//! ```no_run
//! # async fn drain(dbos: &dbos::DBOS, handles: Vec<dbos::WorkflowHandle<u32>>) -> dbos::Result<()> {
//! let mut handles = handles;
//! while !handles.is_empty() {
//!     // The borrow ends with the block, so the set can be narrowed below.
//!     let id = {
//!         let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
//!         dbos.wait_first(&ids).await?
//!     };
//!     // Removed before it is awaited, so the next pass waits on the rest.
//!     let at = handles.iter().position(|h| h.workflow_id() == id).expect("in the set");
//!     let winner = handles.swap_remove(at);
//!     println!("{id} finished: {:?}", winner.result().await);
//! }
//! # Ok(()) }
//! ```
//!
//! # What "finished" means here
//!
//! **Settled, not succeeded.** A workflow counts as finished once its status leaves `PENDING`,
//! `ENQUEUED` and `DELAYED` — so a cancelled or dead-lettered member ends the wait like any other.
//! Python and TypeScript exclude exactly those three, and the reason is the same in all three: the
//! caller asked which race finished first, and a set that has to be re-waited because one member
//! was cancelled is a wait that can hang on a workflow nobody is running any more.
//!
//! Neither call returns an outcome, and that is not an omission. What a workflow *returned* comes
//! back through its own [`WorkflowHandle::result`](crate::WorkflowHandle::result), typed, and a
//! wait over a set has no one type to give. So these answer *when*, and the handle answers *what*
//! — which is exactly how Python and TypeScript split it too.
//!
//! # Where this differs from the references
//!
//! **They return handles; these return ids.** `DBOS.waitFirst` hands back the handle that won,
//! which in a language without ownership costs nothing: the caller still holds the others. Handing
//! back an owned [`WorkflowHandle`] here would mean taking the whole set by value and dropping
//! every loser, which is precisely the wrong thing for the loop the call exists for. So the answer
//! is the winner's **id** — the identity the handle carried anyway, and the same thing the
//! checkpoint stores, so nothing is projected on the way out and re-derived on replay.
//!
//! A position in the slice was the other candidate and is worse on every count that matters: it is
//! the only positional return anywhere in this crate (`cancel_all` and its neighbours take ids and
//! give back ids), it is meaningful only against the exact slice it came from and so can be
//! misapplied to a drifted one with no error, and it is what would force a set with no repeats.
//! The one thing it buys is skipping a
//! [`position`](std::iter::Iterator::position) lookup in the drain loop above.
//!
//! `wait_all` returns nothing for the same reason its references return their inputs unchanged:
//! there is nothing to hand back that the caller did not already have.
//!
//! **They take handles; these take ids.** Every bulk call in this crate takes `&[&str]` —
//! [`cancel_all`](crate::DBOS::cancel_all), [`delete_all`](crate::DBOS::delete_all),
//! [`fork_all`](crate::DBOS::fork_all) — and a wait is not different enough to spell its argument
//! a second way. It also makes the heterogeneous case expressible: a fan-out whose members return
//! different types has no common `WorkflowHandle<R, E>` to put in a slice, and TypeScript only
//! avoids that by erasing to `WorkflowHandle<unknown>`.
//!
//! **`wait_all` is TypeScript's alone, and it is here anyway.** Python has `wait_first` and no
//! `wait_all`; Go and Java have neither. Both are here because a fan-out wants both questions and
//! because having one without the other would leave the natural pair half-built — the same
//! argument [`Client::fork_all`](crate::Client::fork_all) makes for existing where no reference
//! client has it.
//!
//! # Called from inside a workflow
//!
//! Both are **checkpointed as a step of the calling workflow**, under the cross-SDK names
//! [`WAIT_FIRST`](crate::sysdb::types::step_names::WAIT_FIRST) and
//! [`WAIT_ALL`](crate::sysdb::types::step_names::WAIT_ALL). What that buys differs between them,
//! and the difference is the whole reason `wait_first` records a payload and `wait_all` does not:
//!
//! - **`wait_first` records a choice.** A replay that raced again could see a different member
//!   finish first and take a different branch, which would make the workflow nondeterministic in
//!   the one way a workflow may never be. So the winner's id is the step's output, and a replay
//!   hands back the same id without waiting.
//! - **`wait_all` records only that it happened.** Every member has settled by the time it
//!   returns, in whatever order, so there is no choice to pin — the checkpoint exists to skip the
//!   poll on replay, which is what TypeScript's records too.
//!
//! **A replay checks what its payload lets it check, and no more.** `wait_first` can ask whether
//! the recorded winner is still in the set, because the winner is what it recorded anyway; the
//! all-wait recorded no set and so cannot ask the same of one. A workflow resumed or forked with a
//! member the first execution never waited on therefore skips the wait for it, exactly as a
//! [`sleep`](crate::sleep) whose duration changed keeps the deadline it recorded. Step *inputs*
//! are not checkpointed anywhere in DBOS — no implementation's step row has a column for them — so
//! a replay whose arguments changed reads back the answer to the question it asked the first time.
//! This is that rule rather than an exception to it.
//!
//! Outside a workflow neither is checkpointed and both are plain waits, which is the operator's
//! and the client's case. Inside a *step* they are plain too, by the leaf rule every id-allocating
//! call in this crate follows. [`Placement`] owns those rules and the argument for each.
//!
//! # Three surfaces, split by where the caller stands
//!
//! Exactly as [`event`](crate::event) splits its reader, and for the same reason rather than for
//! symmetry. The free [`wait_first`] and [`wait_all`] are what a **workflow body** calls: they take
//! the executor from the ambient context, so a workflow that waits needs no [`DBOS`] handle — and
//! a registered closure that captured one would be stored inside the very `Arc` it holds a strong
//! reference to, keeping the instance, its executor and its pool alive for the life of the
//! process. [`DBOS::wait_first`] and [`DBOS::wait_all`] are for code **outside** a workflow, where
//! there is nothing ambient to take an executor from; [`Client::wait_first`](crate::Client::wait_first)
//! and [`Client::wait_all`](crate::Client::wait_all) are that same caller from outside the
//! application altogether.
//!
//! # Bounding the wait
//!
//! Neither takes a timeout, for the reason
//! [`await_workflow_result`](crate::sysdb::SystemDatabase::await_workflow_result) gives at length:
//! `tokio::time::timeout` around the future, or dropping it, ends the poll. No reference offers one
//! here either — Python's `wait_first` and TypeScript's `waitFirst`/`waitAll` take a polling
//! interval and nothing else — but where they would have to grow a parameter to bound the wait,
//! the language already supplies it.

use crate::checkpoint::Placement;
use crate::connection::Connection;
use crate::context::Ctx;
use crate::dbos::DBOS;
use crate::error::{Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, Timestamp, step_names};

/// Waits until one of these workflows finishes, and reports **which**.
///
/// The waiter for a workflow body: it takes the executor from the ambient context, so a workflow
/// that waits on a fan-out needs no [`DBOS`] handle and its registered closure captures nothing.
/// That is what keeps the registry free of strong references back to the instance holding it, and
/// it is why [`get_event`](crate::get_event) is a free function too.
///
/// The wait is checkpointed as a step, so a replay returns the same winner instead of racing
/// again. The error is the *workflow's* channel, like [`step`](crate::step)'s, so `?` needs no
/// conversion. From inside a *step* it waits plainly with no checkpoint, the step's own checkpoint
/// standing for everything its body did.
///
/// ```no_run
/// # async fn fan_out(child: dbos::WorkflowRef<u32, u32>) -> dbos::Result<u32> {
/// let mut handles = Vec::new();
/// for n in 0..3 {
///     handles.push(child.start(n).await?);
/// }
/// let id = {
///     let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
///     dbos::wait_first(&ids).await?
/// };
/// let at = handles.iter().position(|h| h.workflow_id() == id).expect("in the set");
/// handles.swap_remove(at).result().await
/// # }
/// ```
///
/// Outside a workflow there is no context to read, so this is [`Error::NotInWorkflow`]. That is
/// where [`DBOS::wait_first`] is the call.
pub async fn wait_first<E: crate::DurableError>(workflow_ids: &[&str]) -> Result<String, E> {
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "wait_first".into(),
        });
    };
    ctx.executor()
        .connection()
        .wait_first(workflow_ids)
        .await
        .map_err(Error::lift)
}

/// Waits until every one of these workflows has finished.
///
/// The all-form of the free [`wait_first`], and the waiter a fan-out that needs every answer
/// reaches for. Same context rules, same checkpoint, same error channel.
///
/// ```no_run
/// # async fn fan_out(child: dbos::WorkflowRef<u32, u32>) -> dbos::Result<u32> {
/// let mut handles = Vec::new();
/// for n in 0..3 {
///     handles.push(child.start(n).await?);
/// }
/// let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
/// dbos::wait_all(&ids).await?;
/// // Every handle now resolves without waiting.
/// let mut total = 0;
/// for handle in handles {
///     total += handle.result().await?;
/// }
/// # Ok(total) }
/// ```
pub async fn wait_all<E: crate::DurableError>(workflow_ids: &[&str]) -> Result<(), E> {
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "wait_all".into(),
        });
    };
    ctx.executor()
        .connection()
        .wait_all(workflow_ids)
        .await
        .map_err(Error::lift)
}

impl DBOS {
    /// Waits until one of these workflows finishes, and reports **which**.
    ///
    /// The waiter for code outside a workflow — an operator's tool, or an HTTP handler watching a
    /// batch it kicked off. **Inside a workflow, reach for the free [`wait_first`] instead**: it
    /// needs no handle, so the closure a workflow is registered as captures nothing, and a captured
    /// [`DBOS`] is a cycle with the registry that holds the closure. Called from inside one anyway,
    /// it behaves as the free function does, except that it reports in the engine's own error
    /// channel — and that a handle to some *other* instance is [`Error::WrongInstance`], because
    /// this takes its executor from `self` and its step id from the ambient context.
    ///
    /// **"Finishes" means settled, not succeeded**: a workflow counts once its status leaves
    /// `PENDING`, `ENQUEUED` and `DELAYED`, so a cancelled or dead-lettered member ends the wait
    /// like any other. Python and TypeScript exclude exactly those three. What a workflow
    /// *returned* comes back through its own
    /// [`WorkflowHandle::result`](crate::WorkflowHandle::result), typed — this answers *when*, and
    /// the handle answers *what*.
    ///
    /// **The winner's id, where the references hand back its handle.** Returning an owned
    /// [`WorkflowHandle`](crate::WorkflowHandle) would mean taking the whole set by value and
    /// dropping every loser, which is the wrong thing for the loop this call exists for. The id is
    /// the identity that handle carried, it is what the checkpoint stores, and it is what every
    /// other bulk call in this crate deals in.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS, a: &str, b: &str) -> dbos::Result<()> {
    /// let first = dbos.wait_first(&[a, b]).await?;
    /// println!("{first} got there first");
    /// # Ok(()) }
    /// ```
    ///
    /// **A tie is broken arbitrarily.** Two workflows that settle within the same poll interval
    /// are not ordered by anything — the interval is the resolution at which this can observe
    /// finishing, and neither Python nor TypeScript orders its own query either. Inside a workflow
    /// that costs nothing, because the winner is recorded and a replay reads it back instead of
    /// racing again; outside one, a second call over the same settled set may answer differently.
    /// [`await_first_workflow_id`](crate::sysdb::SystemDatabase::await_first_workflow_id) sets out
    /// why an `ORDER BY` would make this worse rather than better.
    ///
    /// **Duplicate ids are accepted**, here and in [`wait_all`](Self::wait_all). Python and
    /// TypeScript both refuse them, for a reason that does not reach a Rust caller: they return the
    /// winning *handle*, so they key a map by id, and a repeat would put two handles under one key.
    /// An id has no such collision — a set with `a` twice answers `a`, which names one workflow
    /// however many entries pointed at it. A caller-supplied id that
    /// [`start`](crate::WorkflowRef::start) joined to a run already going produces exactly that
    /// set, legitimately, and there is nothing here for it to break.
    ///
    /// **An empty slice is refused** rather than waited on — the one input either wait rejects.
    /// Python raises here too; a wait for one of nothing has no answer it could ever give, where a
    /// wait for *all* of nothing is already satisfied.
    ///
    /// **An id naming no workflow is waited for, not reported**, which is what makes a set of ids
    /// usable before every enqueue has committed — and what makes a mistyped id a wait that never
    /// ends. That is the trade every reference makes here, and the bound on it is dropping the
    /// future.
    pub async fn wait_first(&self, workflow_ids: &[&str]) -> Result<String> {
        let executor = self.executor("wait_first")?;
        executor.connection().wait_first(workflow_ids).await
    }

    /// Waits until every one of these workflows has finished.
    ///
    /// The all-form of [`DBOS::wait_first`], with the same note about the free [`wait_all`] being
    /// the call a workflow body should make.
    ///
    /// **"Finishes" means settled, not succeeded** — see [`wait_first`](Self::wait_first), which
    /// shares this call's definition of it. Nothing is returned because there is nothing to hand
    /// back that the caller did not already have: TypeScript's `waitAll` returns its input
    /// unchanged for the same reason.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS, handles: Vec<dbos::WorkflowHandle<u32>>) -> dbos::Result<()> {
    /// let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
    /// dbos.wait_all(&ids).await?;
    /// // Every handle now resolves without waiting.
    /// # Ok(()) }
    /// ```
    ///
    /// **Duplicates are accepted**, as they are in [`wait_first`](Self::wait_first): settling is a
    /// property of an id, so a repeated one is simply satisfied twice. **An empty slice returns at
    /// once** — nothing to wait for is a satisfied wait, where an empty first-wait has no answer
    /// and is refused.
    pub async fn wait_all(&self, workflow_ids: &[&str]) -> Result<()> {
        let executor = self.executor("wait_all")?;
        executor.connection().wait_all(workflow_ids).await
    }
}

/// The same two waits from outside the application.
///
/// Identical to [`DBOS`]'s, with the two differences a client always has: there is no launch
/// check, because a client is connected or it does not exist; and **nothing is checkpointed**,
/// because a client has no step counter of its own to agree with a workflow's. Called from inside
/// a workflow body, a client's wait runs again on replay — the line
/// [`WorkflowHandle::result`](crate::WorkflowHandle::result) already draws for a client's handle
/// awaited there.
///
/// **This is the union of the reference clients rather than an invention.** Python's client
/// carries `wait_first` and no `wait_all`; TypeScript's carries both; Go and Java have neither
/// call anywhere. It is the surface an operator's tool wants most: a client is the caller most
/// likely to hold a set of ids it did not start.
impl crate::Client {
    /// Waits until one of these workflows finishes, and reports which.
    ///
    /// See [`DBOS::wait_first`]. Nothing is checkpointed, because a client has nothing to
    /// checkpoint against.
    pub async fn wait_first(&self, workflow_ids: &[&str]) -> Result<String> {
        self.connection().wait_first(workflow_ids).await
    }

    /// Waits until every one of these workflows has finished.
    ///
    /// See [`DBOS::wait_all`]. Nothing is checkpointed, for the same reason.
    pub async fn wait_all(&self, workflow_ids: &[&str]) -> Result<()> {
        self.connection().wait_all(workflow_ids).await
    }
}

/// Names an id set for an error message, without letting a fan-out of thousands *become* the
/// message.
///
/// The first few and a count: enough to see which set is meant, where the whole of a wide one
/// would be a wall of ids that says no more than its first line did.
fn summarize(workflow_ids: &[&str]) -> String {
    const SHOWN: usize = 5;
    if workflow_ids.len() <= SHOWN {
        workflow_ids.join(", ")
    } else {
        format!(
            "{}, and {} more",
            workflow_ids[..SHOWN].join(", "),
            workflow_ids.len() - SHOWN
        )
    }
}

impl Connection {
    /// The first-wait itself, shared by both surfaces.
    ///
    /// On the connection because that is what it needs — a poll interval, a serializer for the
    /// recorded winner, and the database — which is what lets a client reach it. Named as its
    /// surface is, like every other shared internal here.
    pub(crate) async fn wait_first(
        self: &std::sync::Arc<Self>,
        workflow_ids: &[&str],
    ) -> Result<String> {
        // Before the placement, because a call that cannot be answered should not move the
        // workflow's step counter: a workflow that fails here and is fixed to pass a non-empty set
        // would otherwise replay onto a different slot than it recorded.
        //
        // The only thing refused. Nothing here cares whether an id repeats — the answer is the id
        // itself, which names one workflow however many entries pointed at it.
        if workflow_ids.is_empty() {
            return Err(Error::Config(
                "wait_first was given no workflow ids to wait for".to_owned(),
            ));
        }

        let placement = Placement::of(self, "wait_first")?;
        if let Some(recorded) = placement.check(self, step_names::WAIT_FIRST).await? {
            let winner: String =
                decode(recorded.output.as_deref(), "the id that won a wait_first")?;
            tracing::debug!(
                workflow_id = winner,
                "replaying wait_first; the same workflow wins again"
            );
            // **The recorded winner has to still be in the set**, which is a determinism check and
            // not bookkeeping: a workflow that changed which ids it waits on has changed what this
            // position of its code means, and handing back a winner it no longer waits on would
            // have it act on an answer to a question it stopped asking. The same check
            // `Awaiting::check` makes against a recorded child id, for the same reason.
            //
            // **Both references make it too, without writing it down**, because returning a
            // *handle* forces the lookup that catches it: Python's `handle_map[completed_id]` is a
            // `KeyError` on exactly this (`_dbos.py:1636`), and TypeScript's
            // `handleMap.get(completedId)!` is an assertion that is false on it, so the caller is
            // handed `undefined` as a handle and learns about it somewhere else. Answering with
            // the id means nothing here dereferences it against the set, so the check that comes
            // free there has to be spelled — which is the whole cost of it. It reads no extra
            // state: the winner is the payload this call records anyway.
            if !workflow_ids.contains(&winner.as_str()) {
                let (workflow_id, step_id) = placement.step().unwrap_or(("", 0));
                // `expected` is what this run is asking for and `recorded` what the row holds,
                // which is the order `Error::UnexpectedStep` prints them in and the order
                // `Awaiting::check` builds them in.
                return Err(Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                    workflow_id: workflow_id.to_owned(),
                    step_id,
                    expected: format!("a wait_first over {}", summarize(workflow_ids)),
                    recorded: format!("a wait_first won by {winner}"),
                }));
            }
            return Ok(winner);
        }

        // Taken before the wait, so a workflow's timeline shows the waiting rather than the
        // instant the answer was written down — the placement every other recorded call in this
        // crate takes its `started_at` from.
        let started_at = Timestamp::now();
        let winner = self
            .sysdb()
            .await_first_workflow_id(workflow_ids, self.outcome_poll_interval())
            .await
            .map_err(Error::SystemDatabase)?;

        let encoded = encode(&winner, "the id that won a wait_first")?;
        placement
            .record(
                self,
                step_names::WAIT_FIRST,
                Outcome::Output(Some(&encoded)),
                started_at,
            )
            .await?;
        Ok(winner)
    }

    /// The all-wait itself, shared by both surfaces.
    pub(crate) async fn wait_all(self: &std::sync::Arc<Self>, workflow_ids: &[&str]) -> Result<()> {
        // Nothing to wait for is a satisfied wait — and, unlike the first-wait, one with an
        // answer. Returned before the placement so an empty call spends no step id, which matches
        // TypeScript short-circuiting its empty handle list before `runInternalStep`.
        if workflow_ids.is_empty() {
            return Ok(());
        }

        let placement = Placement::of(self, "wait_all")?;
        // The row is the whole of the answer, and there is nothing in it to check the current set
        // against: an all-wait records no set, so a replay of one whose set has *grown* skips the
        // wait for the member it never waited on. That is the ordinary reading of a step whose
        // arguments changed — no implementation checkpoints step inputs — and the module doc says
        // so where a caller will read it.
        if placement.check(self, step_names::WAIT_ALL).await?.is_some() {
            tracing::debug!("replaying wait_all; every member had already settled");
            return Ok(());
        }

        let started_at = Timestamp::now();
        self.sysdb()
            .await_workflow_ids(workflow_ids, self.outcome_poll_interval())
            .await
            .map_err(Error::SystemDatabase)?;

        // **No payload**, which is the shape of the thing rather than an economy: an all-wait
        // decides nothing, so there is nothing a replay could take a different branch on. The row
        // records that the wait happened, and that is all a replay needs to skip it — the same
        // thing TypeScript's `runInternalStep` around `awaitWorkflowIds` writes.
        placement
            .record(
                self,
                step_names::WAIT_ALL,
                Outcome::Output(None),
                started_at,
            )
            .await
    }
}
