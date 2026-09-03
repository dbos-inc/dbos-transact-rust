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
//!     let first = {
//!         let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
//!         dbos.wait_first(&ids).await?
//!     };
//!     // Removed before it is awaited, so the next pass waits on the rest.
//!     let winner = handles.swap_remove(first);
//!     let id = winner.workflow_id().to_owned();
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
//! **They return handles; these return positions.** `DBOS.waitFirst` hands back the handle that
//! won, which in a language without ownership costs nothing: the caller still holds the others.
//! Handing back an owned [`WorkflowHandle`] here would mean taking the whole set by value and
//! dropping every loser, which is precisely the wrong thing for the loop the call exists for. An
//! index leaves the caller holding everything and composes with
//! [`Vec::swap_remove`](std::vec::Vec::swap_remove), as above. `wait_all` returns nothing for the
//! same reason its references return their inputs unchanged: there is nothing to hand back that
//! the caller did not already have.
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
//!   returns the same position without waiting.
//! - **`wait_all` records only that it happened.** Every member has settled by the time it
//!   returns, in whatever order, so there is no choice to pin — the checkpoint exists to skip the
//!   poll on replay, which is what TypeScript's records too.
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
//! `tokio::time::timeout` around the future, or dropping it, ends the poll. That is the bound Go
//! and TypeScript had to add a parameter for and Python and Java cannot offer at all.

use crate::checkpoint::Placement;
use crate::connection::Connection;
use crate::context::Ctx;
use crate::dbos::DBOS;
use crate::error::{Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, Timestamp, step_names};

/// Waits until one of these workflows finishes, and reports **which position** in the slice it was.
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
/// let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
/// let first = dbos::wait_first(&ids).await?;
/// handles.swap_remove(first).result().await
/// # }
/// ```
///
/// Outside a workflow there is no context to read, so this is [`Error::NotInWorkflow`]. That is
/// where [`DBOS::wait_first`] is the call.
pub async fn wait_first<E: crate::DurableError>(workflow_ids: &[&str]) -> Result<usize, E> {
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
    /// Waits until one of these workflows finishes, and reports **which position** in the slice it
    /// was.
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
    /// **A position, where the references hand back a handle.** Returning an owned
    /// [`WorkflowHandle`](crate::WorkflowHandle) would mean taking the whole set by value and
    /// dropping every loser, which is the wrong thing for the loop this call exists for. An index
    /// leaves the caller holding everything and composes with
    /// [`Vec::swap_remove`](std::vec::Vec::swap_remove).
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS, a: &str, b: &str) -> dbos::Result<()> {
    /// let first = dbos.wait_first(&[a, b]).await?;
    /// println!("{} got there first", [a, b][first]);
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
    /// **Duplicate ids are refused**, because the answer is a position and a repeated id has more
    /// than one. Python and TypeScript both reject the same input at the same point, and for the
    /// same reason: their handle map cannot hold two entries under one key.
    ///
    /// **An empty slice is refused** rather than waited on. Python raises here too; a wait for one
    /// of nothing has no answer it could ever give.
    ///
    /// **An id naming no workflow is waited for, not reported**, which is what makes a set of ids
    /// usable before every enqueue has committed — and what makes a mistyped id a wait that never
    /// ends. That is the trade every reference makes here, and the bound on it is dropping the
    /// future.
    pub async fn wait_first(&self, workflow_ids: &[&str]) -> Result<usize> {
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
    /// **Duplicates are accepted**, unlike in [`wait_first`](Self::wait_first): settling is a
    /// property of an id rather than a choice between ids, so a repeated one is simply satisfied
    /// twice. **An empty slice returns at once** — nothing to wait for is a satisfied wait, where
    /// an empty first-wait has no answer and is refused.
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
/// **No reference client has either call.** Python's `DBOSClient.wait_first` is the exception that
/// proves it — Python's client *does* carry `wait_first`, and TypeScript's carries both — so this
/// is the union of the two that have them rather than an invention. It is the surface an
/// operator's tool wants most: a client is the caller most likely to hold a set of ids it did not
/// start.
impl crate::Client {
    /// Waits until one of these workflows finishes, and reports which position it was.
    ///
    /// See [`DBOS::wait_first`]. Nothing is checkpointed, because a client has nothing to
    /// checkpoint against.
    pub async fn wait_first(&self, workflow_ids: &[&str]) -> Result<usize> {
        self.connection().wait_first(workflow_ids).await
    }

    /// Waits until every one of these workflows has finished.
    ///
    /// See [`DBOS::wait_all`]. Nothing is checkpointed, for the same reason.
    pub async fn wait_all(&self, workflow_ids: &[&str]) -> Result<()> {
        self.connection().wait_all(workflow_ids).await
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
    ) -> Result<usize> {
        // Before the placement, because a call that cannot be answered should not move the
        // workflow's step counter: a workflow that fails here and is fixed to pass a non-empty set
        // would otherwise replay onto a different slot than it recorded.
        if workflow_ids.is_empty() {
            return Err(Error::Config(
                "wait_first was given no workflow ids to wait for".to_owned(),
            ));
        }
        if let Some(duplicate) = first_duplicate(workflow_ids) {
            return Err(Error::Config(format!(
                "wait_first was given the workflow id `{duplicate}` more than once, so a winner \
                 would name more than one position"
            )));
        }

        let placement = Placement::of(self, "wait_first")?;
        if let Some(recorded) = placement.recorded(self, step_names::WAIT_FIRST).await? {
            let winner: String =
                decode(recorded.output.as_deref(), "the id that won a wait_first")?;
            tracing::debug!(
                workflow_id = winner,
                "replaying wait_first; the same workflow wins again"
            );
            // **The recorded winner has to still be in the set.** A workflow that changed which
            // ids it waits on between runs has changed the meaning of this position, and adopting
            // a stale winner would silently take the branch the old code took. This is the same
            // check `Awaiting::recorded` makes against a recorded child id, for the same reason.
            return position_of(&winner, workflow_ids).ok_or_else(|| {
                Error::SystemDatabase(crate::sysdb::Error::UnexpectedStep {
                    workflow_id: placement
                        .step()
                        .map(|(id, _)| id.to_owned())
                        .unwrap_or_default(),
                    step_id: placement.step().map(|(_, step)| step).unwrap_or_default(),
                    expected: format!("a wait_first over a set containing {winner}"),
                    recorded: format!("a wait_first won by {winner}, which is no longer waited on"),
                })
            });
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

        // The database answered with an id it read out of the set this call sent, so a position
        // always exists; the `expect` is the invariant rather than a case.
        Ok(position_of(&winner, workflow_ids)
            .expect("the winner came from the set that was waited on"))
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
        if placement
            .recorded(self, step_names::WAIT_ALL)
            .await?
            .is_some()
        {
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
        // records that the wait happened, and that is all a replay needs to skip it.
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

/// The first id that appears twice, if any.
///
/// A quadratic scan over a list short enough to name in one query, which is what these waits are
/// for — building a hash set would cost more than it saves, and this runs once per call rather
/// than once per poll.
fn first_duplicate<'a>(workflow_ids: &[&'a str]) -> Option<&'a str> {
    workflow_ids
        .iter()
        .enumerate()
        .find(|(index, id)| workflow_ids[..*index].contains(id))
        .map(|(_, id)| *id)
}

/// Where `winner` sits in the set that was waited on.
fn position_of(winner: &str, workflow_ids: &[&str]) -> Option<usize> {
    workflow_ids.iter().position(|id| *id == winner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_id_is_found_wherever_it_sits() {
        assert_eq!(first_duplicate(&["a", "b", "c"]), None);
        assert_eq!(first_duplicate(&["a", "b", "a"]), Some("a"));
        assert_eq!(first_duplicate(&["a", "a"]), Some("a"));
        assert_eq!(first_duplicate(&[]), None);
        // The *second* occurrence is what is reported, so the message names the id rather than a
        // position the caller would have to count to.
        assert_eq!(first_duplicate(&["a", "b", "b", "a"]), Some("b"));
    }

    #[test]
    fn a_winner_maps_back_to_the_first_position_holding_it() {
        assert_eq!(position_of("b", &["a", "b", "c"]), Some(1));
        assert_eq!(position_of("z", &["a", "b", "c"]), None);
        assert_eq!(position_of("a", &[]), None);
    }
}
