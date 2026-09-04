//! Steps: the checkpoints that make a workflow resumable.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use tokio_util::sync::CancellationToken;

use crate::context::{Ctx, StepMarker};
use crate::error::{DurableError, EngineOnly, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{AwaitedOutcome, Outcome, StepTiming, Timestamp};

/// A step's retry predicate: given a failure, whether to try again.
///
/// Named as a type because it appears in [`StepOptions::should_retry`]'s field, where the bare
/// `Arc<dyn Fn(..)>` is more punctuation than signature. Callers rarely write it —
/// [`StepOptions::should_retry`](StepOptions::should_retry) takes the closure and wraps it.
pub type ShouldRetry<E> = Arc<dyn Fn(&Error<E>) -> bool + Send + Sync>;

/// What a caller may say about a step, beyond its name and body.
///
/// A struct rather than a builder, matching [`StartOptions`](crate::StartOptions) and how `sysdb`
/// spells optional arguments. The common case pays nothing because [`step`] keeps its no-option
/// form. Deliberately **not** `#[non_exhaustive]`, so `..Default::default()` keeps working as
/// fields are added.
///
/// **The defaults are the three-way majority, not Go's.** Python, TypeScript and Java all use a
/// one-second base with a 2.0 rate; Go uses 100ms with a five-second cap, which is a different
/// feature — that one is for a flaky network call, this one is for an upstream that is down.
///
/// **The predicate is synchronous**, unlike Python's and TypeScript's, which also accept an
/// `async` one. Go's `WithStepRetryPredicate` and Java's `StepShouldRetry` are both synchronous, so
/// this is 2-of-4 rather than a deviation from consensus — and in Rust the two are not equally
/// priced. An async predicate cannot be stored in a struct field without boxing its future, so
/// every caller would write `Arc::new(|e| Box::pin(async move { .. }))` to express what is almost
/// always a pure match on an error. A check that genuinely has to await belongs in the body, which
/// can return an error the predicate then declines.
pub struct StepOptions<E = EngineOnly> {
    /// How many times the body may run before the step is failed. Defaults to **1**: no retrying.
    ///
    /// All four references agree that a plain step does not retry, and spell it three different
    /// ways — Python and TypeScript with a `retries_allowed` boolean beside a count, Go with
    /// `maxRetries == 0`, Java by flooring `maxAttempts` at 1. The count alone is taken here
    /// because a boolean beside it can express `retries_allowed: false, max_attempts: 5`, which
    /// means nothing, and an option that cannot state a contradiction is worth more than one that
    /// matches a reference's field list.
    ///
    /// Zero is treated as one: a step whose body never runs is not a step.
    pub max_attempts: u32,
    /// How long to wait after the first failed attempt.
    pub interval: Duration,
    /// What the interval is multiplied by after each failure.
    pub backoff_rate: f64,
    /// The longest the interval may grow to, however many failures precede it.
    pub max_interval: Duration,
    /// The longest **one attempt** may run. `None` lets it run as long as it likes.
    ///
    /// On expiry the attempt's future is dropped — which is how Rust stops work, and stops it at
    /// the next suspension point while running destructors on the way out — and the step fails
    /// with [`Error::StepTimeout`]. That is an ordinary retryable failure: it is offered to
    /// [`should_retry`](Self::should_retry) and counts against
    /// [`max_attempts`](Self::max_attempts), as TypeScript's `timeoutMS` also specifies.
    ///
    /// **Each attempt gets its own timeout, and backoff is not charged against it.** Three
    /// attempts at five seconds may spend fifteen seconds in the body. Python states the same
    /// layering where it puts the supervisor inside the retry loop.
    ///
    /// Before the future is dropped, [`Ctx::cancellation`](crate::Ctx::cancellation) fires, so work
    /// the runtime cannot reach by dropping a future — a `spawn_blocking` thread, a client holding
    /// its own cancel handle — can still be told. Ordinary `async` bodies need nothing.
    ///
    /// **Unlike Python, this is not restricted to some kinds of step.** py #826 rejects a timeout
    /// on a sync step because *"Python has no preemption mechanism for sync steps"*; every step
    /// here is a future, so there is no second case to exclude and no error to raise.
    pub timeout: Option<Duration>,
    /// Whether to stop this step as soon as the workflow is seen to be `CANCELLED` elsewhere.
    ///
    /// Off by default, and worth turning on for a long step whose work is wasted once the workflow
    /// is cancelled — a large transfer, a slow report. A cancellation raised in **this** process
    /// does not need it, since that path stops the workflow directly; what this observes is a
    /// cancellation raised somewhere else, by Conductor, a client, or another SDK sharing the
    /// system database.
    ///
    /// The cost is a periodic status read per running step, at
    /// [`Config::outcome_poll_interval`](field@crate::Config::outcome_poll_interval), under the same
    /// polling-concurrency cap as every other database-backed wait. Python's `preemptible` does
    /// the same and hardcodes its interval.
    ///
    /// **A preempted step records nothing** and runs again on resume, because a cancellation is a
    /// control signal rather than the step's result — the step did not fail, it was interrupted.
    pub preemptible: bool,
    /// Decides whether a failure is worth retrying. `None` retries every failure.
    ///
    /// Returning `false` ends the step immediately with that error, even with attempts left — so a
    /// 4xx from an API is not hammered three times while a 5xx is. Named for the majority: Python's
    /// `should_retry`, TypeScript's `shouldRetry` and Java's `StepShouldRetry` agree, and only Go
    /// spells it `WithStepRetryPredicate`.
    ///
    /// **It is evaluated before the backoff sleep**, so declining an error costs no wait. That is
    /// Go's documented behaviour and the only sensible order: waiting to find out whether to wait
    /// helps nobody.
    ///
    /// **A control signal never reaches it.** Cancellation, shutdown and system-database failures
    /// end the step before the policy is consulted, so a predicate cannot elect to retry against a
    /// database that is down, and does not need an arm for a case it will never see. Use
    /// [`should_retry`](Self::should_retry) rather than writing the `Arc` by hand.
    pub should_retry: Option<ShouldRetry<E>>,
}

impl<E> Default for StepOptions<E> {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            interval: Duration::from_secs(1),
            backoff_rate: 2.0,
            max_interval: Duration::from_secs(3600),
            timeout: None,
            preemptible: false,
            should_retry: None,
        }
    }
}

/// Hand-written because a closure is not [`Debug`], and deriving would demand `E: Debug` besides.
impl<E> std::fmt::Debug for StepOptions<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StepOptions")
            .field("max_attempts", &self.max_attempts)
            .field("interval", &self.interval)
            .field("backoff_rate", &self.backoff_rate)
            .field("max_interval", &self.max_interval)
            .field("timeout", &self.timeout)
            .field("preemptible", &self.preemptible)
            .field("should_retry", &self.should_retry.is_some())
            .finish()
    }
}

/// Hand-written because deriving would demand `E: Clone`, which no caller owes.
impl<E> Clone for StepOptions<E> {
    fn clone(&self) -> Self {
        Self {
            max_attempts: self.max_attempts,
            interval: self.interval,
            backoff_rate: self.backoff_rate,
            max_interval: self.max_interval,
            timeout: self.timeout,
            preemptible: self.preemptible,
            should_retry: self.should_retry.clone(),
        }
    }
}

impl<E> StepOptions<E> {
    /// Sets [`should_retry`](Self::should_retry), wrapping the closure.
    ///
    /// A method beside the struct literal rather than instead of it: the other four fields are
    /// plain values and read better set directly, while this one would otherwise make every call
    /// site spell `Some(Arc::new(..))` around a one-line match.
    ///
    /// ```no_run
    /// # #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    /// # #[error("boom")] struct ApiError { status: u16 }
    /// use dbos::StepOptions;
    /// let options = StepOptions {
    ///     max_attempts: 3,
    ///     ..Default::default()
    /// }
    /// .should_retry(|error| match error {
    ///     dbos::Error::Application(ApiError { status }) => *status >= 500,
    ///     _ => true,
    /// });
    /// ```
    pub fn should_retry(
        mut self,
        predicate: impl Fn(&Error<E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.should_retry = Some(Arc::new(predicate));
        self
    }

    /// The wait before the attempt following `failures` failures.
    ///
    /// `interval * rate^failures`, capped. Saturating rather than panicking on a nonsensical rate:
    /// a negative or `NaN` `backoff_rate` yields a non-finite or negative product, and a large one
    /// yields a product past what a `Duration` can hold — all three go to the cap rather than to a
    /// constructor that would panic. A retry that waits too long is a misconfiguration; a retry
    /// that aborts the process is a bug.
    ///
    /// **The cap is applied to the `f64`, not to the `Duration` built from it.** Clamping
    /// afterwards reads the same and is not: `Duration::from_secs_f64` panics on any value that
    /// overflows a `Duration`, so `interval: 1s, backoff_rate: 1e10` reaches the panic on its
    /// second failure with the clamp sitting unreachable behind it.
    fn backoff(&self, failures: u32) -> Duration {
        let grown = self.interval.as_secs_f64() * self.backoff_rate.powi(failures as i32);
        if !grown.is_finite() || grown < 0.0 || grown >= self.max_interval.as_secs_f64() {
            return self.max_interval;
        }
        Duration::from_secs_f64(grown)
    }
}

/// Runs `body` once per workflow, recording what it returned.
///
/// On a first run the body executes and its result is written to the database. On a replay the
/// recorded result is returned **without entering the body at all** — which is the whole of durable
/// execution, and why a step may do things that must not happen twice.
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// let charge = dbos::step("charge_card", || async { dbos::Result::Ok(42) }).await?;
/// # Ok(()) }
/// ```
///
/// The error type is a type parameter so that a recorded failure can be decoded back into it. It
/// is the *workflow's* error type rather than one of the step's own, which is what lets an
/// infallible body write a bare `Ok(..)`: there is nothing to infer, because the surrounding `?`
/// already fixed it. A step failing with some other error type converts at the boundary, with
/// `.map_err(MyError::from)?`.
///
/// Outside a workflow the body simply runs, undurably. That makes a function built from steps
/// ordinarily callable and ordinarily testable, and it is what Python does. Inside another step the
/// same applies: a step is a leaf, so a nested one is a plain call rather than a second checkpoint.
///
/// **Steps may run concurrently, and the id is what makes that sound.** This call takes the id
/// from the workflow's counter *here*, in the caller's own sequential order, and hands back a
/// [`PendingStep`] that has not run — so a set of steps built and then driven together gets the
/// same slots on a replay however their bodies interleave. `tokio::join!` over steps is therefore
/// ordinary code: it builds every branch before polling any, which is exactly the order the ids
/// were taken in. An id allocated at the first poll would instead depend on which future reached
/// the counter first, which is not something a replay reproduces.
///
/// **What is still a rule: one step per branch.** The flag that makes a step a leaf is one
/// `AtomicBool` on the workflow rather than one per call stack, so a step *built while another
/// step's body is running* sees that flag and takes the plain, uncheckpointed path — and a sibling
/// finishing clears it for a body still running. Branches that are each a single step never meet
/// either case. Work needing several steps in one branch is a child workflow, which has a counter
/// of its own. Making the marker per-call-stack is a later change and is what would lift this.
///
/// **A step built and dropped has still spent its id**, which is why [`PendingStep`] is
/// `#[must_use]`. It is deterministic — the same construction sequence burns the same ids on the
/// replay — but it is no longer the no-op it was when the id was taken at the first poll.
///
/// The name is explicit and it matters: it is checked on replay, so a step whose name changed is
/// reported rather than silently matched against the recorded result of whatever used to be there.
///
/// **The body is `FnMut` rather than `FnOnce` because a step may be attempted more than once.**
/// Retries call it again, and a bound that permits exactly one call cannot express that. The cost
/// to a caller is nothing in the ordinary case — a closure written inline at the call site is
/// `FnMut` unless it moves a captured value out — and a body that genuinely consumes what it
/// captured fails to compile here rather than at its second attempt, which is where the mistake
/// should be reported.
pub fn step<'a, T, E, F, Fut>(name: &str, body: F) -> PendingStep<'a, T, E>
where
    T: Serialize + DeserializeOwned + Send + 'a,
    E: DurableError + Send + 'a,
    F: FnMut() -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, E>> + Send + 'a,
{
    step_with(name, StepOptions::default(), body)
}

/// Runs `body` as a step, retrying it as `options` allows.
///
/// [`step`] with a retry policy — one mechanism, and [`step`] is this called with the defaults.
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// use dbos::StepOptions;
/// let charge = dbos::step_with(
///     "charge_card",
///     StepOptions { max_attempts: 3, ..Default::default() },
///     || async { dbos::Result::Ok(42) },
/// )
/// .await?;
/// # Ok(()) }
/// ```
///
/// **Every attempt shares one checkpoint.** The recorded result is looked for once, before the
/// first attempt, and written once, after the last; a replay of a step that took three attempts
/// sees one outcome, and the intermediate failures live only in logs and spans. That is why
/// `operation_outputs` has no attempt column in any of the five implementations.
///
/// **A control signal is never retried.** Cancellation, shutdown, and any system-database failure
/// end the step immediately with nothing recorded, so the workflow stays `PENDING` and is
/// recovered — a database blip is not evidence that the body is wrong, and retrying against a
/// database that is down would burn the whole policy before the first useful attempt.
///
/// The recorded `started_at` covers the **whole sequence**, from before the recorded-result check
/// to after the final attempt, rather than the last attempt alone. Python takes its
/// `step_start_time` in the same place, and Go moved to it in #442.
pub fn step_with<'a, T, E, F, Fut>(
    name: &str,
    options: StepOptions<E>,
    body: F,
) -> PendingStep<'a, T, E>
where
    T: Serialize + DeserializeOwned + Send + 'a,
    E: DurableError + Send + 'a,
    F: FnMut() -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, E>> + Send + 'a,
{
    // **The one thing that happens at the call rather than at the run.** The counter is read in
    // the caller's own sequential order, so the same step takes the same slot on every execution
    // however the bodies interleave once something drives them. The `filter` is the rule a step has
    // always followed: outside a workflow, and inside another step, there is no checkpoint to make,
    // so no id is taken and the counter does not move.
    //
    // **The context is kept beside the id rather than read again when the step runs**, because the
    // two are one claim — *this position, in this workflow*. Reading the context a second time
    // would let them come apart: a step built here and polled inside some other workflow would
    // write this position under that workflow's id and leave this one's slot empty for good.
    let built = Built::here();
    let mut body = body;
    let name: Arc<str> = Arc::from(name);
    let step_id = built.step_id();
    // `run` is an `async fn`, so building its future captures these and does nothing else. That
    // laziness is what makes a step *pending*.
    //
    // **The identity is kept on the value as well as inside the run**, which is the one thing here
    // that is stored twice. The run needs it to do its work; a caller holding the step needs it to
    // *say what this is* — which branch of a race a stale checkpoint names, which reservation a
    // dropped step burned, what a `Debug` prints. Sealed inside an `async fn`'s state none of that
    // is reachable, and an `Arc<str>` makes the second copy a pointer rather than a string.
    PendingStep {
        name: Arc::clone(&name),
        step_id,
        running: Box::pin(run(
            built,
            name,
            options,
            Box::new(move || Box::pin(body())),
        )),
    }
}

/// A step that has taken its id and has not run.
///
/// Returned by [`step`] and [`step_with`], and awaiting one runs it — so `step(..).await?` reads as
/// it always did. What changed is that the id is spent at the call rather than at the first poll,
/// which is what lets a set of steps be built first and driven together.
///
/// **`Future` rather than `IntoFuture`**, because a step has to be accepted everywhere a future is:
/// `tokio::time::timeout` around one, a combinator holding several. `IntoFuture` only ever reaches
/// the `.await` itself.
///
/// **`#[must_use]` is load-bearing rather than tidy.** A built step that is never polled has still
/// taken its id, so dropping one silently shifts nothing — every later id is what it would have
/// been — but the step itself never runs and never records.
///
/// **`Unpin`, and that is part of the contract rather than an accident.** The run is already
/// behind a `Pin<Box<..>>` and the other two fields are plain data, so a combinator can hold one
/// by value, move it into a `Vec`, and poll it through `&mut` without pinning it first. A
/// combinator that wants a whole set of branches is the caller this is for, and requiring it to
/// pin each one would be the difference between a poll loop and a `pin!` per branch.
#[must_use = "a step that is not awaited has spent its id without running; await it, or hand it to               a combinator"]
pub struct PendingStep<'a, T, E> {
    /// What the step is called, shared with the run rather than copied for it.
    name: Arc<str>,
    /// The id this step claimed when it was built, or `None` where it claimed none — outside a
    /// workflow, or inside another step, where there is no checkpoint to make.
    step_id: Option<i32>,
    /// The run, built by the constructor and driven by whatever polls this.
    ///
    /// An `async fn` body does not begin until it is polled, so the future is built where the id
    /// is taken and this field is the whole of what runs. The two above it are identity, not
    /// state: nothing reads them to decide what happens, and nothing mutates them.
    running: Running<'a, T, E>,
}

/// The erased run behind a [`PendingStep`].
type Running<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// The erased body, rebuilt per attempt — which is why it is `FnMut` and not `FnOnce`.
type Body<'a, T, E> = Box<dyn FnMut() -> Running<'a, T, E> + Send + 'a>;

impl<'a, T, E> PendingStep<'a, T, E> {
    /// Unwraps to the run inside, for a combinator that has to hold several branches pinned.
    ///
    /// `pub(crate)` because handing out the erased future would let a caller build a race over
    /// arbitrary futures, and a race over branches that checkpoint nothing is the failure the
    /// durable select exists to prevent.
    // Called by the select core's poll loop, which lands with `select_step!`.
    #[allow(dead_code)]
    pub(crate) fn into_running(self) -> Running<'a, T, E> {
        self.running
    }

    /// What this step is called — the name it will be checked against on replay.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The id this step claimed when it was built, or `None` if it claimed none.
    ///
    /// `None` is not a failure: outside a workflow, and inside another step, a step is a plain call
    /// with no checkpoint to make, so it takes no id and the counter does not move.
    ///
    /// **Readable here because the run cannot be asked.** Once the step is a future the id is
    /// sealed inside it, and the callers that need to *name* a step are all outside it — a race
    /// reporting which branch a stale checkpoint meant, a dropped reservation saying which id it
    /// burned, a `Debug` that says something.
    #[must_use]
    pub fn step_id(&self) -> Option<i32> {
        self.step_id
    }
}

impl<T, E> Future for PendingStep<'_, T, E> {
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.get_mut().running.as_mut().poll(cx)
    }
}

impl<T, E> std::fmt::Debug for PendingStep<'_, T, E> {
    /// Hand-written because the run is a boxed closure with nothing to show. What is worth showing
    /// is the identity, which is why it is on the value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingStep")
            .field("name", &self.name)
            .field("step_id", &self.step_id)
            .finish_non_exhaustive()
    }
}

/// Where a step was built, which is what its id is a claim about.
///
/// Three states rather than `Option<(Ctx, i32)>`, because "took no id" collapses two situations
/// that have to be told apart when the step is polled: *there was no workflow*, and *there was a
/// workflow but we were nested inside one of its steps*. Both take no id; only the first may be
/// polled outside a workflow.
enum Built {
    /// Took an id: this position, in this workflow.
    Claimed { ctx: Ctx, step_id: i32 },
    /// Inside a workflow but nested in one of its step bodies, so a plain call by the leaf rule.
    ///
    /// Carries *which* body, so a step carried out of it and awaited in the workflow proper is
    /// refused rather than quietly running undurably. `marker` is `None` where the in-step flag
    /// said we were nested but no marker was bound — the two disagree only under the concurrency
    /// the shared flag cannot describe, and treating that as its own place keeps the comparison
    /// exact either way.
    ///
    /// Named `marker` rather than `body` because in this file a step's *body* is its closure, and
    /// the field would shadow it wherever both are in scope.
    Nested {
        workflow_id: String,
        marker: Option<StepMarker>,
    },
    /// No workflow context at all, so a plain call and ordinarily testable.
    Outside,
}

impl Built {
    /// Reads where we are, taking an id if this is a place that checkpoints.
    fn here() -> Self {
        match Ctx::current() {
            Some(ctx) if ctx.in_step() => Built::Nested {
                workflow_id: ctx.workflow_id().to_owned(),
                marker: ctx.step_marker(),
            },
            Some(ctx) => {
                let step_id = ctx.next_step_id();
                Built::Claimed { ctx, step_id }
            }
            None => Built::Outside,
        }
    }

    fn step_id(&self) -> Option<i32> {
        match self {
            Built::Claimed { step_id, .. } => Some(*step_id),
            _ => None,
        }
    }

    /// How to describe this place in [`Error::StepBuiltElsewhere`].
    fn whereabouts(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Built::Claimed { ctx, .. } => format!("in workflow {}", ctx.workflow_id()).into(),
            Built::Nested {
                workflow_id,
                marker: Some(_),
            } => format!("inside a step of workflow {workflow_id}").into(),
            Built::Nested { workflow_id, .. } => format!("in workflow {workflow_id}").into(),
            Built::Outside => "outside a workflow".into(),
        }
    }
}

/// How to describe where a step is being polled.
///
/// **The in-step flag is deliberately not consulted.** It is one `AtomicBool` for the whole
/// workflow, so a sibling branch's running body sets it while this one is first polled — reading it
/// here would refuse exactly the concurrent steps the eager id exists to permit.
fn polled_in(ctx: Option<&Ctx>) -> std::borrow::Cow<'static, str> {
    match ctx {
        Some(ctx) if ctx.step_marker().is_some() => {
            format!("inside a step of workflow {}", ctx.workflow_id()).into()
        }
        Some(ctx) => format!("in workflow {}", ctx.workflow_id()).into(),
        None => "outside a workflow".into(),
    }
}

/// The step itself, once something polls it.
///
/// **The first thing it does is check that it is where it was built**, because the id it carries is
/// a claim on one position in one workflow and nothing else can honour it.
async fn run<'a, T, E>(
    built: Built,
    name: Arc<str>,
    options: StepOptions<E>,
    mut body: Body<'a, T, E>,
) -> Result<T, E>
where
    T: Serialize + DeserializeOwned,
    E: DurableError,
{
    let name = &*name;
    // Compared by workflow identity only — `polled_in` says why the in-step flag cannot be part of
    // this.
    let ambient = Ctx::current();
    let (ctx, step_id) = match (&built, ambient.as_ref()) {
        // The ordinary durable case: built at a step boundary of this workflow, polled at one.
        // No step body may be in scope on either side, or this is a step claimed in the workflow
        // proper and carried *into* a step body, where its checkpoint would sit beneath a step
        // whose own row already covers whatever its body did.
        (Built::Claimed { ctx, step_id }, Some(here))
            if here.workflow_id() == ctx.workflow_id() && here.step_marker().is_none() =>
        {
            (ctx.clone(), *step_id)
        }
        // Took no id, and is polled in the same step body it was built in. Plain, as it always was.
        (
            Built::Nested {
                workflow_id,
                marker,
            },
            Some(here),
        ) if here.workflow_id() == workflow_id && here.step_marker() == *marker => {
            tracing::debug!(
                step_name = name,
                "the step body runs plainly: it was built inside another step"
            );
            return body().await;
        }
        (Built::Outside, None) => {
            tracing::debug!(
                step_name = name,
                "the step body runs plainly: it was built and polled outside a workflow"
            );
            return body().await;
        }
        // Everything else is a claim nobody here can honour.
        (built, here) => {
            return Err(Error::StepBuiltElsewhere {
                step: name.to_owned(),
                built: built.whereabouts(),
                polled: polled_in(here),
            });
        }
    };
    let ctx = &ctx;

    let executor = ctx.executor();
    let workflow_id = ctx.workflow_id();

    // Before the check, not after it: the recorded duration covers the whole step, including the
    // round trip that asks whether it has already run. Go moved to this in #442 and Python has
    // always taken `step_start_time` here.
    let started_at = Timestamp::now();

    let recorded = executor
        .sysdb()
        .check_step(workflow_id, step_id, name)
        .await
        .map_err(Error::SystemDatabase)?;
    if let Some(recorded) = recorded {
        tracing::debug!(
            step_id,
            step_name = name,
            "the step replays from its checkpoint; the body does not run"
        );
        return match recorded.error {
            Some(error) => Err(revive(&error, name)),
            None => decode(recorded.output.as_deref(), "step result"),
        };
    }

    let attempts = options.max_attempts.max(1);
    // Only ever pushed to when retrying, so a step at the default of one attempt allocates nothing.
    let mut failures: Vec<Error<E>> = Vec::new();
    let outcome = loop {
        let attempt = failures.len() as u32 + 1;
        // The span nests inside the workflow's, so anything the body logs carries both ids.
        let span = tracing::info_span!("step", step_id, step_name = name, attempt);
        match supervise(ctx, name, &options, body(), span).await {
            Ok(value) => break Ok(value),
            // Not the step's result and not retryable: a cancelled workflow, a shutdown, or a
            // database that is down says nothing about whether the body would succeed. Returning
            // here leaves the row untouched, so the workflow stays `PENDING` and is recovered.
            Err(error) if error.control().is_some() => {
                tracing::debug!(
                    step_id,
                    step_name = name,
                    attempt,
                    "a control signal ended the step; it is not retried and nothing is checkpointed"
                );
                return Err(error);
            }
            Err(error) if attempt >= attempts => {
                if failures.is_empty() {
                    // No retry policy to report on, so the error is the step's outcome as it
                    // stands. Wrapping here would put every ordinary failure inside a collection
                    // of one.
                    break Err(error);
                }
                failures.push(error);
                break Err(Error::MaxStepRetriesExceeded {
                    step: name.to_owned(),
                    attempts,
                    errors: std::mem::take(&mut failures),
                });
            }
            // Declined by the policy: this failure is the step's outcome, whatever attempts
            // remain. Checked before the backoff so refusing to retry costs no wait, which is Go's
            // documented order for the same hook.
            Err(error)
                if options
                    .should_retry
                    .as_ref()
                    .is_some_and(|should_retry| !should_retry(&error)) =>
            {
                tracing::debug!(
                    step_id,
                    step_name = name,
                    attempt,
                    error = %error,
                    "the retry predicate declined this failure; the step is not retried"
                );
                if failures.is_empty() {
                    break Err(error);
                }
                failures.push(error);
                break Err(Error::MaxStepRetriesExceeded {
                    step: name.to_owned(),
                    attempts: attempt,
                    errors: std::mem::take(&mut failures),
                });
            }
            Err(error) => {
                let backoff = options.backoff(attempt - 1);
                tracing::warn!(
                    step_id,
                    step_name = name,
                    attempt,
                    attempts,
                    backoff_secs = backoff.as_secs_f64(),
                    error = %error,
                    "the step failed and will be retried"
                );
                failures.push(error);
                tokio::time::sleep(backoff).await;
            }
        }
    };

    // Built once and held, not rebuilt per attempt: `record_step` compares the stored completion
    // time against this one to tell its own retried write from another execution's, and a fresh
    // timestamp on a retry would read as somebody else.
    let timing = Some(StepTiming {
        started_at,
        completed_at: Timestamp::now(),
    });
    let serialization = Some(executor.serializer().name());

    match &outcome {
        Ok(value) => {
            let output = encode(value, "step result")?;
            executor
                .sysdb()
                .record_step(
                    workflow_id,
                    step_id,
                    name,
                    Outcome::Output(Some(&output)),
                    serialization,
                    timing,
                )
                .await
                .map_err(Error::SystemDatabase)?;
            tracing::debug!(
                step_id,
                step_name = name,
                "the step ran; its output is recorded"
            );
        }
        Err(error) => {
            // A control error cannot arrive here: the loop above returns on one without leaving
            // itself, precisely so that a cancelled workflow's step is not recorded as *failed* and
            // replayed as permanently so. What reaches this arm is the step's own outcome.
            debug_assert!(error.control().is_none());
            // The error itself, encoded, not a description of it: a replay gives back the error
            // that failed exactly as it gives back the value that succeeded.
            let encoded = encode(error, "step error")?;
            executor
                .sysdb()
                .record_step(
                    workflow_id,
                    step_id,
                    name,
                    Outcome::Error(&encoded),
                    serialization,
                    timing,
                )
                .await
                .map_err(Error::SystemDatabase)?;
            tracing::debug!(
                step_id,
                step_name = name,
                "the step failed; its error is recorded"
            );
        }
    }
    outcome
}

/// Runs one attempt under the watchdogs its options ask for.
///
/// One place decides whether the attempt completed or blew its deadline, which is the shape
/// py #826 arrived at when it merged Python's preemption poller and its new step timeout into a
/// single `_supervise_step`. Preemption joins here rather than beside it.
///
/// **Without a timeout this is the body and nothing else** — no timer, no token, no `select!` —
/// so a step that does not ask for one pays nothing for the option existing.
///
/// With one, the attempt races a timer. On expiry the token fires *first* and the body's future is
/// then dropped, and the order matters: dropping stops the future at its next suspension point, so
/// anything that would only learn from the token has to be told before it goes. Dropping is also
/// the half TypeScript cannot do — it abandons a timed-out attempt and discards whatever the
/// abandoned promise eventually settles to, while a dropped Rust future stops *and* runs its
/// destructors, returning connections and releasing guards without the body saying so.
async fn supervise<T, E, Fut>(
    ctx: &Ctx,
    name: &str,
    options: &StepOptions<E>,
    body: Fut,
    span: tracing::Span,
) -> Result<T, E>
where
    E: DurableError,
    Fut: Future<Output = Result<T, E>>,
{
    if options.timeout.is_none() && !options.preemptible {
        return ctx.in_step_scope(None, body).instrument(span).await;
    }

    let token = CancellationToken::new();
    let attempt = ctx
        .in_step_scope(Some(token.clone()), body)
        .instrument(span);

    // A deadline that never arrives, so the arm can be unconditional rather than duplicating the
    // whole `select!` for each combination of watchdogs.
    let deadline = async {
        match options.timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    };
    let cancelled = async {
        match options.preemptible {
            true => observe_cancellation(ctx).await,
            false => std::future::pending().await,
        }
    };

    // `attempt` is passed by value, so losing the race is what drops it — and dropping it is what
    // stops the body. Taking it by `&mut` under `tokio::pin!` would leave the future alive for the
    // rest of this scope, and an explicit `drop` of the pinned reference would not touch it.
    //
    // **Biased, and the order is the decision.** A completed attempt keeps its outcome whatever
    // else happened; a cancellation outranks a deadline that arrived in the same instant, because
    // not checkpointing a cancelled workflow's step is the safer error — a timeout is recorded and
    // replays as a failure, while a cancellation records nothing and runs again on resume. Getting
    // the tie wrong this way costs one re-execution; getting it wrong the other way durably fails a
    // workflow that was never given a chance. Python states the same rule for the same reason.
    tokio::select! {
        biased;
        outcome = attempt => outcome,
        () = cancelled => {
            token.cancel();
            tracing::debug!(
                step_name = name,
                "the workflow was cancelled elsewhere; the step is preempted and records nothing"
            );
            Err(Error::WorkflowCancelled { workflow_id: ctx.workflow_id().to_owned() })
        }
        () = deadline => {
            // Cancelled before this arm returns, and so before the losing future is dropped at the
            // end of the `select!`. A body that could only learn from the token — work on another
            // task, or on a blocking thread — would otherwise never learn at all.
            token.cancel();
            let timeout = options.timeout.expect("the deadline arm only fires with a timeout set");
            tracing::debug!(
                step_name = name,
                timeout_ms = timeout.as_millis(),
                "the step attempt exceeded its timeout and was stopped"
            );
            Err(Error::StepTimeout { step: name.to_owned(), timeout })
        }
    }
}

/// Completes once this workflow is observed `CANCELLED`, and never otherwise.
///
/// **Reuses `await_workflow_result` rather than adding a status channel**, which is the whole of
/// what preemption needs from the database. That method already polls the status row, already
/// reports a cancellation as a *value* rather than an error, already releases its connection
/// between polls, and already runs under the polling-concurrency cap that stops a fan-out of
/// waiters from starving the control plane. Python's poller is the same loop over
/// `get_workflow_status`, with an interval it hardcodes and this one takes from config.
///
/// A terminal outcome that is *not* a cancellation means another executor wrote this workflow's
/// result while this process was still running it. There is nothing to preempt for — the step's
/// own outcome is no longer wanted either way — so this parks rather than reporting a cancellation
/// that did not happen, and lets the attempt finish on its own terms.
async fn observe_cancellation(ctx: &Ctx) {
    let executor = ctx.executor();
    let interval = executor.outcome_poll_interval();
    match executor
        .sysdb()
        // This workflow's own row, which it is running out of, so an absence is a delete.
        .await_workflow_result(ctx.workflow_id(), interval, true)
        .await
    {
        Ok(AwaitedOutcome::Cancelled) => (),
        // Terminal but not a cancellation: another executor wrote this workflow's outcome while
        // this process was still running it. There is nothing to preempt for, so park rather than
        // completing — completing *is* the cancellation signal, and reporting one that did not
        // happen is worse than not watching.
        Ok(_) => std::future::pending().await,
        // **Not retried here, deliberately.** `await_workflow_result` polls in a loop with
        // `with_retry` inside it, and that policy has no attempt limit for transient or connection
        // failures — a database that is merely unreachable never reaches this arm, because the
        // wait blocks until it comes back. What does reach it is the class `sysdb::retry` returns
        // immediately: `Permanent` and non-backend errors, which will fail again identically. So
        // this parks too, and the step finishes on its own terms — the same outcome as not having
        // asked for preemption. Looping here would re-decide a question the layer below owns, and
        // would turn one unparseable status row into a warning every interval for the life of the
        // step.
        Err(error) => {
            tracing::warn!(
                workflow_id = ctx.workflow_id(),
                %error,
                "could not read status for a preemptible step; it will not be preempted"
            );
            std::future::pending().await
        }
    }
}

/// Rebuilds the error a recorded step failed with.
///
/// The same error, not a description of it: an application failure comes back as its own variant
/// with its own fields, and an engine failure as the variant it was. The only payloads that do not
/// survive are the `serde_json::Error` sources, which arrive absent rather than different.
///
/// Falls back to a plain message when the column does not hold one of ours, which is what a row
/// written by another SDK looks like — its serializer chose its own shape, and the `serialization`
/// column says so. A readable message beats a decode failure standing in for somebody else's error.
fn revive<E: DurableError>(recorded: &str, step: &str) -> Error<E> {
    serde_json::from_str(recorded).unwrap_or_else(|_| Error::StepFailed {
        step: step.to_owned(),
        message: recorded.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The growth curve, and that the cap is a cap.
    #[test]
    fn backoff_grows_geometrically_and_stops_at_the_cap() {
        let options = StepOptions::<EngineOnly> {
            interval: Duration::from_secs(1),
            backoff_rate: 2.0,
            max_interval: Duration::from_secs(10),
            ..Default::default()
        };
        // The exponent is the number of failures *before* this wait, so the first is un-multiplied.
        assert_eq!(options.backoff(0), Duration::from_secs(1));
        assert_eq!(options.backoff(1), Duration::from_secs(2));
        assert_eq!(options.backoff(2), Duration::from_secs(4));
        assert_eq!(options.backoff(3), Duration::from_secs(8));
        assert_eq!(options.backoff(4), Duration::from_secs(10), "capped");
        assert_eq!(options.backoff(40), Duration::from_secs(10), "still capped");
    }

    /// A rate that cannot produce a duration yields the cap rather than a panic.
    ///
    /// `Duration::from_secs_f64` panics on a negative or non-finite value **and on a finite one
    /// past `u64::MAX` seconds**, and a step retry is the worst place to discover any of the three:
    /// it would take the process down on a config typo, mid-workflow. `1e10` is the case a clamp
    /// applied to the `Duration` rather than to the `f64` misses — `1e20` is finite, positive, and
    /// still unrepresentable.
    #[test]
    fn a_nonsensical_backoff_rate_yields_the_cap() {
        let cap = Duration::from_secs(30);
        // An odd exponent, so a negative rate is still negative by the time it is measured.
        for rate in [-2.0, f64::NAN, f64::INFINITY, 1e10, f64::MAX] {
            let options = StepOptions::<EngineOnly> {
                interval: Duration::from_secs(1),
                backoff_rate: rate,
                max_interval: cap,
                ..Default::default()
            };
            assert_eq!(options.backoff(3), cap, "rate {rate}");
        }
        // The overflow the guard alone does not catch: `1e10^2` is finite and positive, and
        // `1e20` seconds is past `u64::MAX`.
        let overflows = StepOptions::<EngineOnly> {
            interval: Duration::from_secs(1),
            backoff_rate: 1e10,
            max_interval: cap,
            ..Default::default()
        };
        assert_eq!(overflows.backoff(2), cap);
    }

    /// The default is the three-way majority, and it does not retry.
    #[test]
    fn the_defaults_match_python_typescript_and_java() {
        let options = StepOptions::<EngineOnly>::default();
        assert_eq!(options.max_attempts, 1, "a plain step does not retry");
        assert_eq!(options.interval, Duration::from_secs(1));
        assert_eq!(options.backoff_rate, 2.0);
        assert_eq!(options.max_interval, Duration::from_secs(3600));
    }
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::sysdb::types::{NewWorkflow, Submission};
    use crate::{Config, DBOS};

    /// A launched instance and a workflow row to hang steps off.
    ///
    /// The row has to exist because a step re-stamps its workflow's executor id, and there is no
    /// `run` here on purpose: entering the same workflow id under two contexts is precisely what a
    /// replay is, and it is the only way to test one before recovery exists to do it for real.
    async fn workflow(id: &str) -> (DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = DBOS::new(Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..Config::new("step-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .init_workflow(&NewWorkflow::new(id), None, Submission::Fresh)
            .await
            .expect("could not create the workflow row");
        (dbos, db)
    }

    fn ctx(dbos: &DBOS, id: &str) -> Ctx {
        Ctx::new(dbos.executor("test").expect("launched"), id, None)
    }

    #[tokio::test]
    async fn a_replayed_step_returns_its_recorded_result_without_entering_the_body() {
        let (dbos, _db) = workflow("wf-replay").await;
        let entered = AtomicU32::new(0);
        // Borrows `entered`, so the closure is `Copy` and both runs can use the same body.
        let body = || async {
            entered.fetch_add(1, Ordering::SeqCst);
            Ok::<_, crate::Error>(7u32)
        };

        // First run: the body executes and the result is recorded.
        let first = Ctx::scope(ctx(&dbos, "wf-replay"), async {
            step("compute", body).await
        })
        .await;
        assert_eq!(first.unwrap(), 7);
        assert_eq!(entered.load(Ordering::SeqCst), 1);

        // Replay: a fresh context over the same workflow id, step ids starting again from zero.
        let again = Ctx::scope(ctx(&dbos, "wf-replay"), async {
            step("compute", body).await
        })
        .await;
        assert_eq!(again.unwrap(), 7, "the recorded result");
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "the body must not run a second time: that is the whole of durable execution"
        );

        dbos.shutdown().await;
    }

    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, PartialEq)]
    #[error("the card was declined after {attempts} attempts")]
    struct CardDeclined {
        attempts: u32,
    }

    #[tokio::test]
    async fn a_replayed_step_that_failed_reports_what_it_recorded() {
        let (dbos, _db) = workflow("wf-failed-step").await;

        let failing = || async { Err::<(), _>(CardDeclined { attempts: 3 }.into()) };

        let first = Ctx::scope(ctx(&dbos, "wf-failed-step"), async {
            step("boom", failing).await
        })
        .await;
        let first = first.unwrap_err();
        let Error::Application(error) = &first else {
            panic!("expected an application error, got {first}")
        };
        assert_eq!(error, &CardDeclined { attempts: 3 });

        // Replay gives back the *same error*, decoded, rather than a sentence about it — the same
        // fidelity a successful step's output gets, fields and all.
        let again = Ctx::scope(ctx(&dbos, "wf-failed-step"), async {
            step("boom", failing).await
        })
        .await;
        let again = again.unwrap_err();
        let Error::Application(replayed) = &again else {
            panic!("expected an application error, got {again}")
        };
        assert_eq!(replayed, error, "the failure survives the round trip whole");
        assert_eq!(again.to_string(), first.to_string());

        dbos.shutdown().await;
    }

    /// A database failure is not the step's result either, so nothing is checkpointed.
    ///
    /// The step did not fail; the database did. Recording a failure here would replay as
    /// *permanently* failed, and the next attempt would never re-enter a body that never ran.
    #[tokio::test]
    async fn a_database_failure_is_not_the_steps_result() {
        use crate::sysdb::{BackendError, BackendErrorKind};

        let (dbos, _db) = workflow("wf-blip").await;

        let failed = Ctx::scope(ctx(&dbos, "wf-blip"), async {
            step("charge", || async {
                Err::<(), crate::Error>(Error::SystemDatabase(crate::sysdb::Error::Backend(
                    BackendError {
                        message: "connection reset by peer".to_owned(),
                        sqlstate: None,
                        kind: BackendErrorKind::Connection,
                    },
                )))
            })
            .await
        })
        .await
        .unwrap_err();
        assert!(matches!(failed, Error::SystemDatabase(_)), "{failed}");

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-blip", true, None, None, None)
            .await
            .expect("read failed");
        assert!(
            steps.is_empty(),
            "a database failure must not be checkpointed as the step's outcome: {steps:?}"
        );

        dbos.shutdown().await;
    }

    /// The one case full fidelity cannot cover: a row this SDK did not write.
    ///
    /// A portable invocation leaves an error encoded in somebody else's shape, and no type
    /// parameter reconstructs a type that was never Rust's. The replay degrades to the message
    /// rather than failing to decode, so the workflow reports what happened instead of reporting
    /// that it could not read what happened.
    #[tokio::test]
    async fn a_step_recorded_in_a_foreign_shape_degrades_to_its_message() {
        let (dbos, _db) = workflow("wf-foreign").await;

        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .record_step(
                "wf-foreign",
                0,
                "charge",
                Outcome::Error(r#"{"pythonModule":"app.errors","type":"CardDeclined"}"#),
                Some("pickle"),
                None,
            )
            .await
            .expect("could not record the step");

        let replayed = Ctx::scope(ctx(&dbos, "wf-foreign"), async {
            step("charge", || async {
                Err::<(), _>(CardDeclined { attempts: 1 }.into())
            })
            .await
        })
        .await
        .unwrap_err();

        match &replayed {
            Error::StepFailed { step, message } => {
                assert_eq!(step, "charge");
                assert!(message.contains("CardDeclined"), "{message}");
            }
            other => panic!("expected a degraded step failure, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// A failure that is one of *ours* replays as itself, variant and payload intact — not
    /// flattened into an application error carrying its message.
    #[tokio::test]
    async fn a_replayed_step_gives_back_the_same_variant_it_recorded() {
        let (dbos, _db) = workflow("wf-variant").await;

        let failing = || async {
            Err::<(), crate::Error>(Error::NotRegistered {
                key: "checkout/Checkout/eu".to_owned(),
            })
        };
        let first = Ctx::scope(ctx(&dbos, "wf-variant"), async {
            step("boom", failing).await
        })
        .await;
        let first = first.unwrap_err();

        let again = Ctx::scope(ctx(&dbos, "wf-variant"), async {
            step("boom", failing).await
        })
        .await;
        let again = again.unwrap_err();

        match &again {
            Error::NotRegistered { key } => assert_eq!(key, "checkout/Checkout/eu"),
            other => panic!("the variant did not survive the round trip: {other:?}"),
        }
        assert_eq!(again.to_string(), first.to_string());

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_step_whose_name_changed_is_reported_rather_than_silently_matched() {
        let (dbos, _db) = workflow("wf-renamed").await;

        let first = Ctx::scope(ctx(&dbos, "wf-renamed"), async {
            step("old_name", || async { Ok::<_, crate::Error>(1u32) }).await
        })
        .await;
        assert_eq!(first.unwrap(), 1);

        // Step 0 is recorded under a different name, so its result is not this step's result.
        let renamed = Ctx::scope(ctx(&dbos, "wf-renamed"), async {
            step("new_name", || async { Ok::<_, crate::Error>(1u32) }).await
        })
        .await;
        assert!(
            renamed.is_err(),
            "a changed step name must not match a recorded result"
        );

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn steps_are_recorded_in_order_under_the_ids_they_allocated() {
        let (dbos, _db) = workflow("wf-order").await;

        Ctx::scope(ctx(&dbos, "wf-order"), async {
            step("one", || async { Ok::<_, crate::Error>(1u32) })
                .await
                .unwrap();
            step("two", || async { Ok::<_, crate::Error>(2u32) })
                .await
                .unwrap();
            step("three", || async { Ok::<_, crate::Error>(3u32) })
                .await
                .unwrap();
        })
        .await;

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-order", true, None, None, None)
            .await
            .expect("read failed");
        let seen: Vec<(i32, &str)> = steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect();
        assert_eq!(seen, [(0, "one"), (1, "two"), (2, "three")]);
        assert_eq!(steps[2].output.as_deref(), Some("3"));

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_nested_step_is_a_plain_call_and_allocates_no_id() {
        let (dbos, _db) = workflow("wf-nested").await;

        Ctx::scope(ctx(&dbos, "wf-nested"), async {
            step("outer", || async {
                // Go #420's rule: a step is a leaf. Were this to allocate an id, every step after
                // it would replay against the wrong slot.
                let inner = step("inner", || async { Ok::<_, crate::Error>(1u32) })
                    .await
                    .unwrap();
                Ok::<_, crate::Error>(inner + 1)
            })
            .await
            .unwrap();
            step("after", || async { Ok::<_, crate::Error>(9u32) })
                .await
                .unwrap();
        })
        .await;

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-nested", true, None, None, None)
            .await
            .expect("read failed");
        let seen: Vec<(i32, &str)> = steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect();
        assert_eq!(
            seen,
            [(0, "outer"), (1, "after")],
            "`inner` is not a checkpoint"
        );

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_step_outside_a_workflow_runs_plainly() {
        let entered = Arc::new(AtomicU32::new(0));
        let value = step("standalone", || {
            let entered = Arc::clone(&entered);
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                Ok::<_, crate::Error>(3u32)
            }
        })
        .await
        .expect("a step with no workflow around it just runs");

        assert_eq!(value, 3);
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert!(Ctx::current().is_none());
    }
}
