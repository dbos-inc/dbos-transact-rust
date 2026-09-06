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
//!         dbos.select_workflow(&ids).await?
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
//! `join_workflows` returns nothing for the same reason its references return their inputs
//! unchanged: there is nothing to hand back that the caller did not already have.
//!
//! **They take handles; these take ids.** Every bulk call in this crate takes `&[&str]` —
//! [`cancel_all`](crate::DBOS::cancel_all), [`delete_all`](crate::DBOS::delete_all),
//! [`fork_all`](crate::DBOS::fork_all) — and a wait is not different enough to spell its argument
//! a second way. It also makes the heterogeneous case expressible: a fan-out whose members return
//! different types has no common `WorkflowHandle<R, E>` to put in a slice, and TypeScript only
//! avoids that by erasing to `WorkflowHandle<unknown>`.
//!
//! **The all-wait is TypeScript's alone, and it is here anyway.** Python has `wait_first` and no
//! `wait_all`; Go and Java have neither. Both are here because a fan-out wants both questions and
//! because having one without the other would leave the natural pair half-built — the same argument
//! [`Client::fork_all`](crate::Client::fork_all) makes for existing where no reference client has
//! it.
//!
//! **They are named for the concurrency primitives, not for the references.** Python's `wait_first`
//! and TypeScript's `waitFirst`/`waitAll` describe the wait; `select_workflow` and `join_workflows`
//! name the *shape* a Rust caller already knows it by, from `tokio::select!` and `tokio::join!` —
//! one branch of several wins, or every branch is joined. That is the name the macros over them
//! take, and a function and its macro that do the same thing under two different names is a seam to
//! learn for nothing.
//!
//! **The recorded step names follow the calls**, which is the one place in
//! [`step_names`](crate::sysdb::types::step_names) that a reference's string is not taken:
//! `DBOS.selectWorkflow` and `DBOS.joinWorkflows` where Python and TypeScript write
//! `DBOS.waitFirst` and `DBOS.waitAll`. A step listing should name the call the caller wrote, and
//! nothing across the SDKs reads another's step names to decide anything — a replay checks its own
//! workflow's rows. The cost is that one operation has two names when steps are read across
//! implementations, and the constants say so.
//!
//! # Called from inside a workflow
//!
//! Both are **checkpointed as a step of the calling workflow**, under
//! [`SELECT_WORKFLOW`](crate::sysdb::types::step_names::SELECT_WORKFLOW) and
//! [`JOIN_WORKFLOWS`](crate::sysdb::types::step_names::JOIN_WORKFLOWS). What that buys differs
//! between them, and the difference is the whole reason `select_workflow` records a payload and
//! `join_workflows` does not:
//!
//! - **`select_workflow` records a choice.** A replay that raced again could see a different
//!   member finish first and take a different branch, which would make the workflow
//!   nondeterministic in the one way a workflow may never be. So the winner's id is the step's
//!   output, and a replay hands back the same id without waiting.
//! - **`join_workflows` records only that it happened.** Every member has settled by the time it
//!   returns, in whatever order, so there is no choice to pin — the checkpoint exists to skip the
//!   poll on replay, which is what TypeScript's records too.
//!
//! **A replay checks what its payload lets it check, and no more.** `select_workflow` can ask
//! whether the recorded winner is still in the set, because the winner is what it recorded anyway;
//! the all-wait recorded no set and so cannot ask the same of one. A workflow resumed or forked
//! with a member the first execution never waited on therefore skips the wait for it, exactly as a
//! [`sleep`](crate::sleep) whose duration changed keeps the deadline it recorded. Step *inputs* are
//! not checkpointed anywhere in DBOS — no implementation's step row has a column for them — so a
//! replay whose arguments changed reads back the answer to the question it asked the first time.
//! This is that rule rather than an exception to it.
//!
//! Outside a workflow neither is checkpointed and both are plain waits, which is the operator's
//! and the client's case. Inside a *step* they are plain too, by the leaf rule every id-allocating
//! call in this crate follows. [`Placement`] owns those rules and the argument for each.
//!
//! # Three surfaces, split by where the caller stands
//!
//! Exactly as [`event`](crate::event) splits its reader, and for the same reason rather than for
//! symmetry. The free [`select_workflow`](fn@select_workflow) and
//! [`join_workflows`](fn@join_workflows) are what a **workflow body** calls: they take the executor
//! from the ambient context, so a workflow that waits needs no [`DBOS`] handle — and a registered
//! closure that captured one would be stored inside the very `Arc` it holds a strong reference to,
//! keeping the instance, its executor and its pool alive for the life of the process.
//! [`DBOS::select_workflow`] and [`DBOS::join_workflows`] are for code **outside** a workflow,
//! where there is nothing ambient to take an executor from;
//! [`Client::select_workflow`](crate::Client::select_workflow) and
//! [`Client::join_workflows`](crate::Client::join_workflows) are that same caller from outside the
//! application altogether.
//!
//! # Two macros over the same two calls
//!
//! [`select_workflow!`](macro@crate::select_workflow) and
//! [`join_workflows!`](macro@crate::join_workflows) are the same two waits taking **handles** and
//! giving back **typed results**, which is what the id-taking calls cannot do: a set of ids has no
//! one type, where a fixed list of handles has one per branch. **Each shares its name with the
//! function it wraps**, which costs nothing — macros live in their own namespace, and
//! `futures::join!` sits beside `futures::future::join` on the same terms — and says the true
//! thing: they are one operation in two spellings, taking ids or taking handles. Neither macro
//! records anything of its own; each expands to the wait beside it plus
//! [`result`](crate::WorkflowHandle::result) on the handles it needs, so the checkpoint that pins
//! the choice is the [`select_workflow`](fn@select_workflow) the macro already made.
//!
//! They are macros because a select's answer is a *sum* over branches that may differ in type, and
//! Rust has no anonymous sum type to return; a function would have to give back nested `Either`s,
//! or force one type on every branch. That is why `select!` is a macro everywhere it exists. Both
//! are `macro_rules!`, exported from this crate rather than a companion one, as `tokio::select!`
//! and `tokio::join!` are.
//!
//! Both take an optional instance before a semicolon — `select_workflow!(dbos; ..)` — which is the
//! three-surface split above spelled at the call site, because a macro has no ambient context of
//! its own to consult. The expansion names the *method* rather than a type, so a [`DBOS`] and a
//! [`Client`](crate::Client) are the same two lines.
//!
//! The id-taking calls are not superseded by any of this. A macro is fixed-arity and needs the
//! handles, so a set built in a loop, a set of ids read from somewhere else, and a
//! [`Client`](crate::Client) that never had handles are all still theirs.
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
use crate::error::{Error, Result};
use crate::instance::DBOS;
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
///     dbos::select_workflow(&ids).await?
/// };
/// let at = handles.iter().position(|h| h.workflow_id() == id).expect("in the set");
/// handles.swap_remove(at).result().await
/// # }
/// ```
///
/// Outside a workflow there is no context to read, so this is [`Error::NotInWorkflow`]. That is
/// where [`DBOS::select_workflow`] is the call.
pub async fn select_workflow<E: crate::DurableError>(workflow_ids: &[&str]) -> Result<String, E> {
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "select_workflow".into(),
        });
    };
    ctx.executor()
        .connection()
        .select_workflow(workflow_ids)
        .await
        .map_err(Error::lift)
}

/// Waits until every one of these workflows has finished.
///
/// The all-form of the free [`select_workflow`](fn@select_workflow), and the waiter a fan-out that
/// needs every answer reaches for. Same context rules, same checkpoint, same error channel.
///
/// ```no_run
/// # async fn fan_out(child: dbos::WorkflowRef<u32, u32>) -> dbos::Result<u32> {
/// let mut handles = Vec::new();
/// for n in 0..3 {
///     handles.push(child.start(n).await?);
/// }
/// let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
/// dbos::join_workflows(&ids).await?;
/// // Every handle now resolves without waiting.
/// let mut total = 0;
/// for handle in handles {
///     total += handle.result().await?;
/// }
/// # Ok(total) }
/// ```
pub async fn join_workflows<E: crate::DurableError>(workflow_ids: &[&str]) -> Result<(), E> {
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "join_workflows".into(),
        });
    };
    ctx.executor()
        .connection()
        .join_workflows(workflow_ids)
        .await
        .map_err(Error::lift)
}

/// Races these workflow handles and runs the arm belonging to the one that finishes first.
///
/// The macro form of [`select_workflow`](fn@select_workflow). Where that answers with the winner's
/// **id**, this hands the winning arm the winner's **typed result** — so a fan-out over children
/// that return different types is one expression rather than an id, a lookup against the set, and a
/// cast.
///
/// ```no_run
/// # async fn race(
/// #     counter: dbos::WorkflowRef<u32, u32>,
/// #     namer: dbos::WorkflowRef<(), String>,
/// # ) -> dbos::Result<String> {
/// let count = counter.start(3).await?;
/// let name = namer.start(()).await?;
/// dbos::select_workflow! {
///     n = count => format!("the counter won with {}", n?),
///     s = name  => format!("the namer won with {}", s?),
/// }
/// # }
/// ```
///
/// # Inside a workflow
///
/// **Nothing new is recorded.** The expansion is one [`select_workflow`](fn@select_workflow) over
/// the handles' ids — the checkpoint that already makes this choice survive a replay — and then
/// [`result`](crate::WorkflowHandle::result) on the winner alone. A replayed body reads the same
/// winner back out of that checkpoint and awaits the same handle, so the arm taken the second time
/// is the arm taken the first.
///
/// **Only the winner is awaited**, and that is what keeps the step ids stable rather than being an
/// optimisation: a loser's `result` is called on neither the run nor the replay, so no id is taken
/// on one and not the other. Dropping the losing handles does not stop those workflows — a
/// workflow runs on whether or not anything is watching it, which is the difference between racing
/// workflows and racing steps.
///
/// # Outside a workflow
///
/// An instance before a semicolon is the other form, and it is the [`DBOS::select_workflow`]
/// surface where the bare form is the free [`select_workflow`](fn@select_workflow) one — the same
/// split the calls themselves make, spelled at the call site because a macro has no ambient context
/// of its own to consult:
///
/// ```no_run
/// # async fn race(dbos: &dbos::DBOS, a: dbos::WorkflowHandle<u32>, b: dbos::WorkflowHandle<u32>)
/// # -> dbos::Result<u32> {
/// dbos::select_workflow! { dbos;
///     first = a => first?,
///     second = b => second?,
/// }
/// # }
/// ```
///
/// Anything with the two waits on it goes there, which today is a [`DBOS`] or a
/// [`Client`](crate::Client) — the macro names the method rather than a type, so a client's
/// undurable wait and an instance's checkpointed one are the same expansion. **Inside a workflow,
/// prefer the bare form**: it needs no handle, for the reason a registered closure that captured a
/// [`DBOS`] would keep the instance alive for the life of the process, and a client's wait inside a
/// workflow body is not checkpointed and so runs again on every replay.
///
/// One wrinkle belongs to this form alone: an instance answers in the engine's own channel, so the
/// expansion lifts that error into the caller's, and where nothing else says which channel that is
/// — a test, or any caller that never `?`s the result — it has to be annotated. Inside a workflow
/// the body's own return type settles it and the question does not arise.
///
/// # The shape of an arm
///
/// **A branch is a variable holding a handle, not an arbitrary expression.** The handle is named
/// twice, once for its id and once to consume it, and an expression would be evaluated twice —
/// which for `child.start(n).await?` would start the workflow twice. A workflow body starts its
/// children one at a time anyway, so they are already bound.
///
/// **Two branches naming the same workflow are answered by the first**, in the order the arms are
/// written. [`select_workflow`](fn@select_workflow) accepts a repeated id for the same reason: the
/// answer names one workflow however many entries pointed at it.
///
/// The value is a [`Result`](crate::Result), because the wait itself can fail. Each arm binds its
/// own handle's result, so an arm decides for itself whether to `?` it, match it, or report it.
#[macro_export]
macro_rules! select_workflow {
    // The two public forms differ only in which wait they reach for, so both hand the same arms to
    // this one along with the call that produces the winner. Internal rules come first because a
    // rule that fails on a literal backtracks cleanly, where one that fails inside a `pat` or
    // `expr` fragment need not.
    ( @race { $wait:expr } $( $bind:pat = $handle:ident => $arm:expr ),+ ) => {
        'dbos_select: {
            let winner = match $wait {
                ::core::result::Result::Ok(winner) => winner,
                ::core::result::Result::Err(failed) => {
                    break 'dbos_select ::core::result::Result::Err(failed);
                }
            };
            $(
                if $handle.workflow_id() == winner {
                    let $bind = $handle.result().await;
                    break 'dbos_select ::core::result::Result::Ok($arm);
                }
            )+
            // `select_workflow` checks its recorded winner against the set it was given, so an id
            // from outside it has already been reported as `UnexpectedStep` before this is reached.
            ::core::unreachable!(
                "select_workflow answered with {winner}, which is none of the handles it was given"
            )
        }
    };
    ( $( $bind:pat = $handle:ident => $arm:expr ),+ $(,)? ) => {
        $crate::select_workflow!(
            @race { $crate::select_workflow(&[$( $handle.workflow_id() ),+]).await }
            $( $bind = $handle => $arm ),+
        )
    };
    ( $instance:expr ; $( $bind:pat = $handle:ident => $arm:expr ),+ $(,)? ) => {
        // `lift` because an instance and a client answer in the engine's own channel, where the
        // arms speak the application's: the free form is generic over that channel and needs no
        // conversion, and this one is the `map_err(Error::lift)` every other engine-channel call
        // asks of its caller.
        $crate::select_workflow!(
            @race {
                $instance
                    .select_workflow(&[$( $handle.workflow_id() ),+])
                    .await
                    .map_err($crate::Error::lift)
            }
            $( $bind = $handle => $arm ),+
        )
    };
    // Last, so it speaks only for input no other rule claimed. Worth the rule because the two
    // shapes it covers are the two mistakes available here, and rustc's own answer to both is
    // "no rules expected this token".
    ( $( $bad:tt )* ) => {
        ::core::compile_error!(
            "select_workflow! takes arms of the form `binding = handle => expression`, where each \
             handle is a variable holding a WorkflowHandle rather than an expression. An instance \
             to wait through goes before a *semicolon*: `select_workflow!(dbos; won = a => won?)`."
        )
    };
}

/// Waits for all of these workflows and returns what each of them returned.
///
/// The macro form of [`join_workflows`](fn@join_workflows), and the reason to reach for it rather
/// than awaiting each handle in turn: one poll loop settles the whole set — **one query per
/// interval whatever N is** — and only then is each result read, so N handles cost one wait and N
/// row reads instead of N waits.
///
/// ```no_run
/// # async fn both(
/// #     counter: dbos::WorkflowRef<u32, u32>,
/// #     namer: dbos::WorkflowRef<(), String>,
/// # ) -> dbos::Result<String> {
/// let count = counter.start(3).await?;
/// let name = namer.start(()).await?;
/// let (count, name) = dbos::join_workflows!(count, name)?;
/// # Ok(format!("{name} counted to {count}")) }
/// ```
///
/// **Nothing new is recorded, and the awaits stay sequential.** The expansion is one
/// [`join_workflows`](fn@join_workflows) and then [`result`](crate::WorkflowHandle::result) on each
/// handle in source order. Sequential is not a concession here: the set is already settled by the
/// time the first result is read, so every one of them is a row read that does not wait — and
/// taking them in source order is what keeps each `DBOS.getResult` on the step id its replay
/// expects.
///
/// **The first failure ends it**, in source order, as `try_join!` does: the tuple holds values
/// rather than results, so a failed child is the value of the whole call. Where each child's own
/// failure matters, [`join_workflows`](fn@join_workflows) followed by a `result` per handle reports
/// all of them.
///
/// An instance before a semicolon is the outside-a-workflow form, exactly as it is for
/// [`select_workflow!`](macro@crate::select_workflow), which sets out when to reach for which:
///
/// ```no_run
/// # async fn both(dbos: &dbos::DBOS, a: dbos::WorkflowHandle<u32>, b: dbos::WorkflowHandle<u32>)
/// # -> dbos::Result<u32> {
/// let (a, b) = dbos::join_workflows!(dbos; a, b)?;
/// # Ok(a + b) }
/// ```
///
/// A branch is a variable holding a handle, for the reason
/// [`select_workflow!`](macro@crate::select_workflow) gives.
///
/// **A comma there is not caught.** `join_workflows!(dbos, a, b)` is a perfectly good list of three
/// idents, so it is read as three handles and fails inside the expansion, where the instance turns
/// out to have no `workflow_id`. That ambiguity is why the separator is a semicolon at all: unlike
/// [`select_workflow!`](macro@crate::select_workflow), whose arms cannot be mistaken for an
/// instance, nothing distinguishes the two shapes here.
#[macro_export]
macro_rules! join_workflows {
    ( @join { $wait:expr } $( $handle:ident ),+ ) => {
        'dbos_join: {
            if let ::core::result::Result::Err(failed) = $wait {
                break 'dbos_join ::core::result::Result::Err(failed);
            }
            ::core::result::Result::Ok((
                $(
                    match $handle.result().await {
                        ::core::result::Result::Ok(value) => value,
                        ::core::result::Result::Err(failed) => {
                            break 'dbos_join ::core::result::Result::Err(failed);
                        }
                    },
                )+
            ))
        }
    };
    ( $( $handle:ident ),+ $(,)? ) => {
        $crate::join_workflows!(
            @join { $crate::join_workflows(&[$( $handle.workflow_id() ),+]).await }
            $( $handle ),+
        )
    };
    ( $instance:expr ; $( $handle:ident ),+ $(,)? ) => {
        $crate::join_workflows!(
            @join {
                $instance
                    .join_workflows(&[$( $handle.workflow_id() ),+])
                    .await
                    .map_err($crate::Error::lift)
            }
            $( $handle ),+
        )
    };
    // The comma form this cannot catch is called out by name: `join_workflows!(dbos, a, b)` is a
    // valid list of three idents, so it matches the bare rule above and fails later, inside the
    // expansion, on an instance that has no `workflow_id`.
    ( $( $bad:tt )* ) => {
        ::core::compile_error!(
            "join_workflows! takes variables holding WorkflowHandles: `join_workflows!(a, b)`. An \
             instance to wait through goes before a *semicolon*: `join_workflows!(dbos; a, b)` — \
             with a comma there, the instance is read as one more handle."
        )
    };
}

impl DBOS {
    /// Waits until one of these workflows finishes, and reports **which**.
    ///
    /// The waiter for code outside a workflow — an operator's tool, or an HTTP handler watching a
    /// batch it kicked off. **Inside a workflow, reach for the free
    /// [`select_workflow`](fn@select_workflow) instead**: it needs no handle, so the closure a
    /// workflow is registered as captures nothing, and a captured [`DBOS`] is a cycle with the
    /// registry that holds the closure. Called from inside one anyway, it behaves as the free
    /// function does, except that it reports in the engine's own error channel — and that a handle
    /// to some *other* instance is [`Error::WrongInstance`], because this takes its executor from
    /// `self` and its step id from the ambient context.
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
    /// let first = dbos.select_workflow(&[a, b]).await?;
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
    /// **Duplicate ids are accepted**, here and in [`join_workflows`](Self::join_workflows). Python
    /// and TypeScript both refuse them, for a reason that does not reach a Rust caller: they return
    /// the winning *handle*, so they key a map by id, and a repeat would put two handles under one
    /// key. An id has no such collision — a set with `a` twice answers `a`, which names one
    /// workflow however many entries pointed at it. A caller-supplied id that
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
    pub async fn select_workflow(&self, workflow_ids: &[&str]) -> Result<String> {
        let executor = self.executor("select_workflow")?;
        executor.connection().select_workflow(workflow_ids).await
    }

    /// Waits until every one of these workflows has finished.
    ///
    /// The all-form of [`DBOS::select_workflow`], with the same note about the free
    /// [`join_workflows`](fn@join_workflows) being the call a workflow body should make.
    ///
    /// **"Finishes" means settled, not succeeded** — see
    /// [`select_workflow`](Self::select_workflow), which shares this call's definition of it.
    /// Nothing is returned because there is nothing to hand back that the caller did not already
    /// have: TypeScript's `waitAll` returns its input unchanged for the same reason.
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS, handles: Vec<dbos::WorkflowHandle<u32>>) -> dbos::Result<()> {
    /// let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
    /// dbos.join_workflows(&ids).await?;
    /// // Every handle now resolves without waiting.
    /// # Ok(()) }
    /// ```
    ///
    /// **Duplicates are accepted**, as they are in [`select_workflow`](Self::select_workflow):
    /// settling is a property of an id, so a repeated one is simply satisfied twice. **An empty
    /// slice returns at once** — nothing to wait for is a satisfied wait, where an empty first-wait
    /// has no answer and is refused.
    pub async fn join_workflows(&self, workflow_ids: &[&str]) -> Result<()> {
        let executor = self.executor("join_workflows")?;
        executor.connection().join_workflows(workflow_ids).await
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
    /// See [`DBOS::select_workflow`]. Nothing is checkpointed, because a client has nothing to
    /// checkpoint against.
    pub async fn select_workflow(&self, workflow_ids: &[&str]) -> Result<String> {
        self.connection().select_workflow(workflow_ids).await
    }

    /// Waits until every one of these workflows has finished.
    ///
    /// See [`DBOS::join_workflows`]. Nothing is checkpointed, for the same reason.
    pub async fn join_workflows(&self, workflow_ids: &[&str]) -> Result<()> {
        self.connection().join_workflows(workflow_ids).await
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
    pub(crate) async fn select_workflow(
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
                "select_workflow was given no workflow ids to wait for".to_owned(),
            ));
        }

        let placement = Placement::of(self, "select_workflow")?;
        if let Some(recorded) = placement.check(self, step_names::SELECT_WORKFLOW).await? {
            let winner: String = decode(
                recorded.output.as_deref(),
                "the id that won a select_workflow",
            )?;
            tracing::debug!(
                workflow_id = winner,
                "replaying select_workflow; the same workflow wins again"
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
                    expected: format!("a select_workflow over {}", summarize(workflow_ids)),
                    recorded: format!("a select_workflow won by {winner}"),
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

        let encoded = encode(&winner, "the id that won a select_workflow")?;
        placement
            .record(
                self,
                step_names::SELECT_WORKFLOW,
                Outcome::Output(Some(&encoded)),
                started_at,
            )
            .await?;
        Ok(winner)
    }

    /// The all-wait itself, shared by both surfaces.
    pub(crate) async fn join_workflows(
        self: &std::sync::Arc<Self>,
        workflow_ids: &[&str],
    ) -> Result<()> {
        // Nothing to wait for is a satisfied wait — and, unlike the first-wait, one with an
        // answer. Returned before the placement so an empty call spends no step id, which matches
        // TypeScript short-circuiting its empty handle list before `runInternalStep`.
        if workflow_ids.is_empty() {
            return Ok(());
        }

        let placement = Placement::of(self, "join_workflows")?;
        // The row is the whole of the answer, and there is nothing in it to check the current set
        // against: an all-wait records no set, so a replay of one whose set has *grown* skips the
        // wait for the member it never waited on. That is the ordinary reading of a step whose
        // arguments changed — no implementation checkpoints step inputs — and the module doc says
        // so where a caller will read it.
        if placement
            .check(self, step_names::JOIN_WORKFLOWS)
            .await?
            .is_some()
        {
            tracing::debug!("replaying join_workflows; every member had already settled");
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
                step_names::JOIN_WORKFLOWS,
                Outcome::Output(None),
                started_at,
            )
            .await
    }
}
