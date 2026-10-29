//! Debouncing: many requests within a period collapse into one workflow run.
//!
//! Debouncing a key starts a workflow that waits `DELAYED` on a queue, holding a deduplication id
//! derived from the key. Each later debounce of the key pushes that workflow's release out by the
//! period it names and replaces its input, so it runs **once**, with the **last** input, when the
//! calls stop — or at the debounce timeout, which caps how far a steady stream of calls can push it.
//! Once the workflow is released to its queue the key is free again, and the next debounce starts a
//! fresh workflow.
//!
//! [`Debouncer`] debounces a registered workflow from an app; [`DebouncerClient`] does the same
//! from a [`Client`], naming the workflow as an enqueue does.
//!
//! # The loop
//!
//! A debounce is a loop around two writes, because nothing makes them one:
//!
//! 1. **Bounce.** [`debounce_delayed_workflow`](crate::sysdb::SystemDatabase::debounce_delayed_workflow)
//!    extends the debounced workflow holding the key, if there is one this debounce may extend, and
//!    the caller gets a handle to it.
//! 2. **Otherwise, look at who holds the key.** Nobody: go on to 3. A workflow that is not
//!    debounced, is a different workflow (name, class or configured instance), or belongs to
//!    another app: the key is not this debounce's, and the call fails with the deduplication error
//!    an enqueue under a held key fails with. A debounced holder of the same workflow that the
//!    bounce could not extend is between states; pause briefly and bounce again.
//! 3. **Enqueue** a fresh debounced workflow under the key. A concurrent debounce may enqueue
//!    first, and then this insert fails on the deduplication index: go back to 1, which now
//!    extends the winner.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::client::{Client, EnqueueOptions};
use crate::error::{DurableError, EngineOnly, Error, Result};
use crate::handle::WorkflowHandle;
use crate::identity::validate_app_name;
use crate::instance::Executor;
use crate::registry::{QualifiedWorkflowName, WorkflowRef};
use crate::serialization::{PayloadEncoding, encode_input};
use crate::sysdb::INTERNAL_QUEUE;
use crate::sysdb::types::{
    Debounce, DebounceRequest, Timestamp, step_names, validate_positive_duration,
};
use crate::workflow::{
    Debounced, DuplicationPolicy, Enqueue, OwnedEnqueue, OwnedStart, PendingStart, StartOptions,
    Timeout, create, encode_attributes, placed_workflow_id, refused_start, validate_start,
};

/// What a debounce refuses its arguments as.
const DEBOUNCE_OPERATION: &str = "debounce a workflow";

/// How long a debounce waits before bouncing again when the key's debounced holder is between
/// states, so a holder stuck there does not have the loop spinning against the database.
const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// The deduplication id a debounce of `key` holds for `workflow`: `<workflow>-<key>`, with the
/// workflow spelled `name`, `name/class`, `name/class/config`, or `name//config` for a config with
/// no class.
///
/// The whole name, so two configured instances of one class, or a method and a free function of
/// the same bare name, debounce one key independently rather than colliding on it. The parts are
/// joined without escaping, so names that contain `/` or `-` can still spell the same id — a free
/// function `a/b` and a method `a` of class `b`, or `a` with key `b-c` and `a-b` with key `c`.
/// Such a collision is refused rather than extended: [`classify`] compares the holder's whole
/// name.
///
/// **The one place the spelling is written**, so changing it is a change here and nowhere else.
/// It is stored, and only a debounce that spells the id this way finds a workflow another debounce
/// of the key started, so it is spelled from the key's parts rather than from its
/// [`Display`](std::fmt::Display), which is for people to read.
pub(crate) fn deduplication_id(workflow: &QualifiedWorkflowName, key: &str) -> String {
    let name = workflow.name();
    match (workflow.class_name(), workflow.config_name()) {
        (None, None) => format!("{name}-{key}"),
        (Some(class), None) => format!("{name}/{class}-{key}"),
        (Some(class), Some(config)) => format!("{name}/{class}/{config}-{key}"),
        (None, Some(config)) => format!("{name}//{config}-{key}"),
    }
}

/// What one debounce says beyond its key, period and input — see
/// [`Debouncer::debounce_with`] and [`DebouncerClient::debounce_with`].
///
/// Per call rather than on the debouncer, because what it names belongs to one workflow: a
/// debouncer serves any number of keys, and any number of workflows one key after another.
#[derive(Debug, Clone, Copy, Default)]
pub struct DebounceOptions<'a> {
    /// The id of the workflow this call enqueues, in place of a generated one.
    ///
    /// Used **only when the call enqueues a fresh workflow**. A call that coalesces onto a
    /// workflow already waiting under the key hands back that workflow, whatever id this names.
    /// A fresh enqueue under an id that already names a workflow is not joined to it: the call
    /// fails with [`ConflictingWorkflow`](crate::sysdb::Error::ConflictingWorkflow).
    pub workflow_id: Option<&'a str>,
}

/// What a debounce call does about one bounce's outcome.
#[derive(Debug, PartialEq, Eq)]
enum BounceAction {
    /// The bounce extended this workflow, already waiting under the key; the call coalesces onto
    /// it and hands back a handle to it.
    Coalesce(String),
    /// Nothing holds the key; enqueue a fresh workflow.
    Enqueue,
    /// The key is held by this workflow, which this debounce must not touch; the call fails as a
    /// deduplicated enqueue would.
    Refuse(String),
    /// A debounced holder of this workflow could not be extended this time; bounce again.
    Retry,
}

/// Decides what a debounce of `workflow`, acting for `target_app`, does about `outcome`.
///
/// A holder that is not debounced is an ordinary deduplicated enqueue, and one of a different
/// name, class or configured instance is a key collision between unrelated workflows (`"a"` with
/// key `"b-c"` against `"a-b"` with `"c"`): both are refused rather than hijacked. So is a holder
/// another app owns, which the bounce never extends — retrying it would spin until the holder
/// left. A holder with no owner, or a debounce acting for no app, cannot be told apart that way,
/// and is retried.
fn classify(
    outcome: Debounce,
    workflow: &QualifiedWorkflowName,
    target_app: Option<&str>,
) -> BounceAction {
    match outcome {
        Debounce::Bounced { workflow_id } => BounceAction::Coalesce(workflow_id),
        Debounce::Unheld => BounceAction::Enqueue,
        Debounce::Held(holder) => {
            // Part by part, with `''` absent as `QualifiedWorkflowName::from_row` reads it.
            fn present(part: Option<&str>) -> Option<&str> {
                part.filter(|part| !part.is_empty())
            }
            let same_workflow = holder.workflow_name.as_deref() == Some(workflow.name())
                && present(holder.class_name.as_deref()) == workflow.class_name()
                && present(holder.config_name.as_deref()) == workflow.config_name();
            let another_app = matches!(
                (target_app, holder.application_name.as_deref()),
                (Some(target), Some(owner)) if target != owner
            );
            if !holder.is_debounced || !same_workflow || another_app {
                BounceAction::Refuse(holder.workflow_id)
            } else {
                BounceAction::Retry
            }
        }
    }
}

/// Everything a debounce's bounces share, settled once before the loop: what is debounced, under
/// which key and on which queue, with which input, for which app.
///
/// Both debouncers drive their loops through this, so a bounce is asked for and answered the same
/// way from an app and from a client.
struct Bounce<'a> {
    workflow: &'a QualifiedWorkflowName,
    queue_name: &'a str,
    deduplication_id: String,
    period: Duration,
    /// The input, encoded once: every bounce and the fresh enqueue write the same text.
    input: String,
    encoding: PayloadEncoding,
    target_app: Option<&'a str>,
}

impl Bounce<'_> {
    /// The request for one bounce, releasing the workflow `period` from it.
    ///
    /// The period goes as a duration, which the bounce resolves on the database clock, so each
    /// attempt measures it from when that attempt lands.
    fn request(&self) -> DebounceRequest<'_> {
        DebounceRequest {
            workflow: self.workflow,
            queue_name: self.queue_name,
            deduplication_id: &self.deduplication_id,
            period: self.period,
            inputs: Some(&self.input),
            serialization: Some(self.encoding.name()),
            application_name: self.target_app,
        }
    }

    /// What to do about a bounce's `outcome` — see [`classify`].
    fn action(&self, outcome: Debounce) -> BounceAction {
        classify(outcome, self.workflow, self.target_app)
    }

    /// The error for a key `holder` holds that this debounce must not extend: the one an enqueue
    /// under a held key fails with.
    fn deduplicated(self, holder: String) -> Error {
        Error::SystemDatabase(crate::sysdb::Error::QueueDeduplicated {
            workflow_id: holder,
            queue_name: self.queue_name.to_owned(),
            deduplication_id: self.deduplication_id,
        })
    }
}

/// Whether a fresh enqueue lost the key to a concurrent debounce, which the loop answers by
/// bouncing again — the bounce then extends the winner.
fn lost_the_key(result: &Result<impl Sized>) -> bool {
    matches!(
        result,
        Err(Error::SystemDatabase(
            crate::sysdb::Error::QueueDeduplicated { .. }
        ))
    )
}

/// Refuses an [`app_name`](Debouncer::app_name) no app could have, as [`Error::InvalidArgument`]
/// naming `app_name`, before the name reaches a column.
fn refuse_app_name(app_name: Option<&str>) -> Result<()> {
    match app_name.map(validate_app_name) {
        Some(Err(Error::Config(detail))) => Err(Error::InvalidArgument {
            operation: DEBOUNCE_OPERATION.into(),
            detail,
        }),
        Some(Err(other)) => Err(other),
        Some(Ok(())) | None => Ok(()),
    }
}

/// Refuses a `workflow_id` on a debouncer's stored options, as [`Error::InvalidArgument`].
///
/// A debouncer is reused across keys and across release cycles of one key, so an id stored on it
/// would name every fresh workflow it ever enqueues. The id names one workflow, and belongs on the
/// call that enqueues it.
fn refuse_stored_workflow_id(workflow_id: Option<&str>, method: &str) -> Result<()> {
    match workflow_id {
        Some(_) => Err(Error::InvalidArgument {
            operation: DEBOUNCE_OPERATION.into(),
            detail: format!(
                "`options.workflow_id` cannot be set on a debouncer, which enqueues many \
                 workflows: pass the id for one call in `DebounceOptions::workflow_id`, to \
                 `{method}`"
            ),
        }),
        None => Ok(()),
    }
}

/// Refuses the queue options a debounce owns, as [`Error::InvalidArgument`].
///
/// A debounce writes the workflow's deduplication id (the key) and its delay (the period), so a
/// caller may not set either, nor ask to join a holder of the key the debounce manages. A priority
/// and a partition key are refused too: debounced workflows do not support them.
fn refuse_owned_options(queue: &Enqueue<'_>) -> Result<()> {
    let refuse = |detail: &str| {
        Err(Error::InvalidArgument {
            operation: DEBOUNCE_OPERATION.into(),
            detail: format!("queue `{}`: {detail}", queue.name),
        })
    };
    if queue.deduplication_id.is_some() {
        return refuse(
            "a `deduplication_id` cannot be set: the debounce key is the workflow's deduplication id",
        );
    }
    if queue.delay.is_some() {
        return refuse("a `delay` cannot be set: the debounce period is the workflow's delay");
    }
    if queue.duplication_policy == DuplicationPolicy::ReturnExisting {
        return refuse(
            "`DuplicationPolicy::ReturnExisting` cannot be set: a debounce owns the workflow's \
             deduplication",
        );
    }
    if queue.priority.is_some() {
        return refuse("a `priority` cannot be set: debounced workflows do not support priority");
    }
    if queue.partition_key.is_some() {
        return refuse(
            "a `partition_key` cannot be set: debounced workflows do not support partition keys",
        );
    }
    Ok(())
}

/// Refuses a period or a debounce timeout of zero, or one too long to store.
///
/// A zero period would enqueue the workflow ready to run rather than `DELAYED`, where no later
/// bounce can find it.
fn refuse_durations(period: Duration, debounce_timeout: Option<Duration>) -> Result<()> {
    validate_positive_duration("period", period)
        .map_err(|error| Error::refused_argument(DEBOUNCE_OPERATION, error))?;
    if let Some(timeout) = debounce_timeout {
        validate_positive_duration("debounce_timeout", timeout)
            .map_err(|error| Error::refused_argument(DEBOUNCE_OPERATION, error))?;
    }
    Ok(())
}

/// A registered workflow, debounced.
///
/// Built with [`Debouncer::new`] and adjusted by functional update, and reusable: one debouncer
/// serves any number of keys.
///
/// ```no_run
/// # async fn f(workflow: &dbos::WorkflowRef<String, ()>) -> dbos::Result<()> {
/// use std::time::Duration;
///
/// let debouncer = dbos::Debouncer {
///     debounce_timeout: Some(Duration::from_secs(60)),
///     ..dbos::Debouncer::new(workflow)
/// };
/// // Runs once, with "third", two seconds after the last call — or a minute after the first.
/// for input in ["first", "second", "third"] {
///     debouncer.debounce("user-42", Duration::from_secs(2), input.to_owned()).await?;
/// }
/// # Ok(()) }
/// ```
///
/// # Options
///
/// [`options`](Self::options) is what a [`start_with`](WorkflowRef::start_with) would be given,
/// and applies to every workflow a debounce enqueues:
///
/// - **[`queue`](StartOptions::queue)** is the queue the debounced workflow waits and runs on, and
///   the scope of its key: the same key on two queues is two debounces. `None` is the internal
///   queue every app dequeues. The queue's [`serialization`](Enqueue::serialization) is honoured;
///   its deduplication id, delay, [`DuplicationPolicy::ReturnExisting`], priority and partition
///   key are refused as [`Error::InvalidArgument`], because the debounce owns the first three and
///   debounced workflows support neither of the others.
/// - **[`workflow_id`](StartOptions::workflow_id)** is refused as [`Error::InvalidArgument`]: an
///   id names one workflow, and a debouncer enqueues many. Name one call's workflow with
///   [`debounce_with`](Self::debounce_with) instead.
/// - **[`timeout`](StartOptions::timeout)** is the workflow's budget once it runs. A debounced
///   workflow **never inherits** a calling workflow's deadline: a debounce can wait longer than the
///   caller lives, so [`Timeout::Inherit`] means no budget here, as [`Timeout::None`] does.
/// - **[`attributes`](StartOptions::attributes)** go on the row of a fresh workflow.
///
/// # Inside a workflow
///
/// A debounce called from a workflow body is durable. Each bounce commits with its own step
/// checkpoint, and a fresh enqueue is recorded as a child start, so a replay reads back what the
/// first run did — the same handle, without bouncing or enqueueing again. Called inside a step, it
/// is [`Error::InsideStep`], as a start is.
///
/// **A fresh enqueue makes the debounced workflow the calling workflow's child**: its row names
/// the caller as its parent, as any child start's does. So cancelling the caller cancels the
/// debounced workflow too — and that workflow is shared: later debounces of the key, from other
/// callers, coalesce onto it, and the input it would have run with may be theirs. A debounce that
/// only extends a waiting workflow records no parent on it.
pub struct Debouncer<'a, P, R, E = EngineOnly> {
    /// The workflow to debounce.
    pub workflow: WorkflowRef<P, R, E>,
    /// What to say about every workflow a debounce enqueues — see [Options](Self#options).
    pub options: StartOptions<'a>,
    /// The longest a workflow may be held back, counted from the debounce that enqueued it, on the
    /// database clock.
    ///
    /// Later debounces extend the delay up to this cap and no further, so a steady stream of calls
    /// cannot postpone the workflow forever. `None` is no cap.
    pub debounce_timeout: Option<Duration>,
    /// The app the debounce acts for, or `None` for this one.
    ///
    /// A fresh workflow is written for that app — with no app version, so that app's latest runs
    /// it. A waiting workflow is extended if it is that app's or no app's, and one that was no
    /// app's becomes that app's.
    pub app_name: Option<&'a str>,
}

impl<'a, P, R, E> Debouncer<'a, P, R, E> {
    /// Debounces `workflow` on the internal queue, with no timeout, for this app.
    pub fn new(workflow: &WorkflowRef<P, R, E>) -> Self {
        Self {
            workflow: workflow.clone(),
            options: StartOptions::default(),
            debounce_timeout: None,
            app_name: None,
        }
    }
}

/// Hand-written: the derive would demand `P: Clone, R: Clone`, which a debouncer that holds
/// neither has no business requiring.
impl<P, R, E> Clone for Debouncer<'_, P, R, E> {
    fn clone(&self) -> Self {
        Self {
            workflow: self.workflow.clone(),
            options: self.options.clone(),
            debounce_timeout: self.debounce_timeout,
            app_name: self.app_name,
        }
    }
}

impl<P, R, E> std::fmt::Debug for Debouncer<'_, P, R, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Debouncer")
            .field("workflow", &self.workflow)
            .field("options", &self.options)
            .field("debounce_timeout", &self.debounce_timeout)
            .field("app_name", &self.app_name)
            .finish()
    }
}

impl<P, R, E> Debouncer<'_, P, R, E>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    E: DurableError,
{
    /// Debounces `key`: the workflow runs once, with the last `input`, `period` after the last
    /// debounce of the key — or at the [`debounce_timeout`](Self::debounce_timeout).
    ///
    /// Hands back a handle to the workflow this call fed: the one already waiting under the key,
    /// whose release it pushed out and whose input it replaced, or a fresh one. Every call that
    /// coalesces gets a handle to the same workflow, and its result is the run with the last
    /// input.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidArgument`] for a zero `period` or
    ///   [`debounce_timeout`](Self::debounce_timeout), an invalid [`app_name`](Self::app_name), or
    ///   an option the debounce owns — see [Options](Self#options).
    /// - [`Error::SystemDatabase`] with
    ///   [`QueueDeduplicated`](crate::sysdb::Error::QueueDeduplicated) when the key is held by a
    ///   workflow this debounce must not extend — an ordinary deduplicated enqueue, another
    ///   workflow whose key spells the same, or another app's — naming the holder.
    /// - [`Error::InsideStep`] when called inside a step, and [`Error::WrongInstance`] when called
    ///   from a workflow another instance runs, as a start is.
    ///
    /// # Step ids
    ///
    /// Inside a workflow, a debounce claims two step ids where it is written: the bounce's, and the
    /// fresh enqueue's, which a debounce that extends a waiting workflow leaves unused. When an
    /// attempt neither extends nor enqueues — another debounce of the key enqueued between the
    /// two, or the bounce met a waiting workflow between states — this one claims two more for
    /// each further attempt as it makes it. So a debounce should be driven to completion before
    /// the workflow's next durable call, rather than joined with one.
    pub fn debounce<'b>(&'b self, key: &str, period: Duration, input: P) -> PendingStart<'b, R, E> {
        self.debounce_with(key, period, input, DebounceOptions::default())
    }

    /// [`debounce`](Self::debounce), with something to say about this one call.
    ///
    /// [`options.workflow_id`](DebounceOptions::workflow_id) names the workflow this call enqueues
    /// if it enqueues a fresh one. An id that already names a workflow is
    /// [`Error::SystemDatabase`] with
    /// [`ConflictingWorkflow`](crate::sysdb::Error::ConflictingWorkflow), never a join: a debounce
    /// that joined a finished workflow would drop its input.
    ///
    /// ```no_run
    /// # async fn f(debouncer: &dbos::Debouncer<'_, String, ()>) -> dbos::Result<()> {
    /// use std::time::Duration;
    ///
    /// let handle = debouncer
    ///     .debounce_with(
    ///         "user-42",
    ///         Duration::from_secs(2),
    ///         "latest".to_owned(),
    ///         dbos::DebounceOptions {
    ///             workflow_id: Some("user-42-digest-7"),
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn debounce_with<'b>(
        &'b self,
        key: &str,
        period: Duration,
        input: P,
        options: DebounceOptions<'b>,
    ) -> PendingStart<'b, R, E> {
        let deduplication_id = deduplication_id(self.workflow.qualified_name(), key);
        let built = self
            .place(period)
            .map(|(executor, bounce, start)| ((executor, start), bounce));
        PendingStart::from_step(PendingStep::placed(
            step_names::DEBOUNCE,
            built,
            move |(executor, start), bounce| async move {
                self.debounced(
                    executor,
                    bounce,
                    start,
                    deduplication_id,
                    period,
                    input,
                    options,
                )
                .await
            },
        ))
    }

    /// Everything a debounce settles where it is written: the instance, that its arguments are
    /// ones it can act on, and the two step ids it starts with.
    ///
    /// The checks come first, so a refused debounce moves no step counter.
    fn place(&self, period: Duration) -> Result<(Arc<Executor>, StepPlacement, StepPlacement)> {
        let executor = self.workflow.dbos().executor(DEBOUNCE_OPERATION)?;
        refuse_stored_workflow_id(self.options.workflow_id, "Debouncer::debounce_with")?;
        if let Some(queue) = &self.options.queue {
            refuse_owned_options(queue)?;
        }
        validate_start(self.options.queue.as_ref(), self.options.timeout.budget())?;
        refuse_durations(period, self.debounce_timeout)?;
        refuse_app_name(self.app_name)?;
        let bounce = StepPlacement::of(executor.connection(), DEBOUNCE_OPERATION)?;
        if matches!(bounce, StepPlacement::InsideStep { .. }) {
            return Err(Error::InsideStep {
                operation: "debouncing a workflow".into(),
            });
        }
        let start = StepPlacement::of(executor.connection(), DEBOUNCE_OPERATION)?;
        Ok((executor, bounce, start))
    }

    /// The loop, once something polls the debounce. See the [module documentation](self).
    #[allow(clippy::too_many_arguments)]
    async fn debounced(
        &self,
        executor: Arc<Executor>,
        mut bounce_at: StepPlacement,
        mut start: StepPlacement,
        deduplication_id: String,
        period: Duration,
        input: P,
        call: DebounceOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        let conn = executor.connection();
        let name: &QualifiedWorkflowName = self.workflow.qualified_name();
        let queue = self
            .options
            .queue
            .clone()
            .unwrap_or_else(|| Enqueue::new(INTERNAL_QUEUE));
        // The registration's encoding unless the queue asks for its own, as a start's is. A bounce
        // writes the input in the same one, so the row's tag always describes its input.
        let encoding = PayloadEncoding::resolve(
            queue.serialization,
            PayloadEncoding::resolve(
                self.workflow.serialization(),
                executor.serializer().encoding(),
            ),
        );
        let bounce = Bounce {
            workflow: name,
            queue_name: queue.name,
            deduplication_id,
            period,
            input: encode_input(&input, &encoding)?,
            encoding,
            target_app: Some(self.app_name.unwrap_or(executor.app_name())),
        };
        let attributes = encode_attributes(self.options.attributes)?;
        // Never the caller's deadline: the debounce can hold the workflow back for longer than the
        // caller has left.
        let timeout = match self.options.timeout {
            Timeout::Inherit => Timeout::None,
            other => other,
        };

        loop {
            let outcome = conn
                .sysdb()
                .debounce_delayed_workflow(
                    &bounce.request(),
                    conn.step_caller(bounce_at.step_writer()),
                )
                .await
                .map_err(|error| Error::refused_argument(DEBOUNCE_OPERATION, error))?;

            match bounce.action(outcome) {
                // The bounce matched the row, so a later absence is a deletion.
                BounceAction::Coalesce(workflow_id) => {
                    return Ok(WorkflowHandle::polling(Arc::clone(conn), workflow_id, true));
                }
                BounceAction::Refuse(holder) => return Err(bounce.deduplicated(holder)),
                BounceAction::Retry => tokio::time::sleep(RETRY_PAUSE).await,
                BounceAction::Enqueue => {
                    // The period is the delay and the timeout the deadline, both resolved by the
                    // insert on one reading of the database clock, which caps the release at the
                    // deadline. A replay that re-runs the start after a crash resolves them again,
                    // from when that insert lands.
                    let fresh = Enqueue {
                        deduplication_id: Some(&bounce.deduplication_id),
                        delay: Some(period),
                        ..queue.clone()
                    };
                    let request = OwnedStart {
                        workflow_id: placed_workflow_id(call.workflow_id, &start),
                        timeout,
                        queue: Some(OwnedEnqueue::from(&fresh)),
                        input: Some(bounce.input.clone()),
                        encoding: bounce.encoding.clone(),
                        attributes: attributes.clone(),
                        started_at: Timestamp::now(),
                        debounce: Some(Debounced {
                            timeout: self.debounce_timeout,
                        }),
                        app_name: self.app_name.map(str::to_owned),
                    };
                    // Recorded at the start's step as a child start would be — the workflow it
                    // created, or the deduplication error it lost the race with — so a replay
                    // takes the same branch. The replayed form of that error is the same variant,
                    // read back from the start's record.
                    let created =
                        create::<R, E>(Arc::clone(&executor), start, name.clone(), request)
                            .await
                            .map_err(|error| refused_start(Some(queue.name), error));
                    if !lost_the_key(&created) {
                        return created;
                    }
                }
            }

            // A further attempt, which takes ids of its own where it runs.
            bounce_at = StepPlacement::of(conn, DEBOUNCE_OPERATION)?;
            start = StepPlacement::of(conn, DEBOUNCE_OPERATION)?;
        }
    }
}

/// A workflow debounced through a [`Client`], named as an enqueue names it.
///
/// What [`Debouncer`] is for an app, for a process that does not register the workflow: built
/// with [`DebouncerClient::new`] and adjusted by functional update.
///
/// ```no_run
/// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
/// use std::time::Duration;
///
/// let debouncer = dbos::DebouncerClient {
///     debounce_timeout: Some(Duration::from_secs(60)),
///     ..dbos::DebouncerClient::new(client, "summarise", dbos::EnqueueOptions::new("work"))
/// };
/// let handle: dbos::WorkflowHandle<String> = debouncer
///     .debounce("user-42", Duration::from_secs(2), "the latest text")
///     .await?;
/// # Ok(()) }
/// ```
///
/// [`workflow`](Self::workflow) is the workflow's name, as [`Client::enqueue`] takes it: a bare
/// name for a free function, or a [`QualifiedWorkflowName`] naming its class and configured
/// instance.
/// [`options`](Self::options) describes every workflow a debounce enqueues, as it would for
/// [`Client::enqueue_with`]: its queue (and the key's scope), an app version, a timeout and
/// attributes. Its [`app_name`](EnqueueOptions::app_name), or else the
/// client's, is the app the debounce acts for. The queue options a debounce owns are refused as
/// on [`Debouncer`], and so is a [`workflow_id`](EnqueueOptions::workflow_id): name one call's
/// workflow with [`debounce_with`](Self::debounce_with).
///
/// **Not durable from inside a workflow body**, as a client's enqueue is not: a client is a
/// connection, not an execution, so a replayed body debounces again.
#[derive(Debug, Clone)]
pub struct DebouncerClient<'a> {
    /// The client to debounce through.
    pub client: Client,
    /// The workflow, by the key it is registered under.
    pub workflow: QualifiedWorkflowName,
    /// What to say about every workflow a debounce enqueues.
    pub options: EnqueueOptions<'a>,
    /// The longest a workflow may be held back, counted from the debounce that enqueued it — see
    /// [`Debouncer::debounce_timeout`].
    pub debounce_timeout: Option<Duration>,
}

impl<'a> DebouncerClient<'a> {
    /// Debounces `workflow` as `options` would enqueue it, with no timeout.
    pub fn new(
        client: &Client,
        workflow: impl Into<QualifiedWorkflowName>,
        options: EnqueueOptions<'a>,
    ) -> Self {
        Self {
            client: client.clone(),
            workflow: workflow.into(),
            options,
            debounce_timeout: None,
        }
    }

    /// Debounces `key` — see [`Debouncer::debounce`], whose errors this shares, apart from the
    /// two only a workflow body can meet.
    ///
    /// The input is encoded as [`Client::enqueue_with`] encodes it, and the type parameters are the
    /// caller's assertion, as there.
    pub async fn debounce<P, R, E>(
        &self,
        key: &str,
        period: Duration,
        input: P,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        self.debounce_with(key, period, input, DebounceOptions::default())
            .await
    }

    /// [`debounce`](Self::debounce), with something to say about this one call — see
    /// [`Debouncer::debounce_with`].
    pub async fn debounce_with<P, R, E>(
        &self,
        key: &str,
        period: Duration,
        input: P,
        call: DebounceOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        refuse_stored_workflow_id(self.options.workflow_id, "DebouncerClient::debounce_with")?;
        refuse_owned_options(&self.options.queue)?;
        self.options.validate(&self.workflow)?;
        refuse_durations(period, self.debounce_timeout)?;
        refuse_app_name(self.options.app_name)?;

        let conn = self.client.connection();
        let encoding = PayloadEncoding::resolve(
            self.options.queue.serialization,
            conn.serializer().encoding(),
        );
        let bounce = Bounce {
            workflow: &self.workflow,
            queue_name: self.options.queue.name,
            deduplication_id: deduplication_id(&self.workflow, key),
            period,
            input: encode_input(&input, &encoding)?,
            encoding,
            target_app: self.options.app_name.or(conn.app_name()),
        };
        loop {
            let outcome = conn
                .sysdb()
                .debounce_delayed_workflow(&bounce.request(), None)
                .await
                .map_err(|error| Error::refused_argument(DEBOUNCE_OPERATION, error))?;

            match bounce.action(outcome) {
                BounceAction::Coalesce(workflow_id) => {
                    return Ok(WorkflowHandle::polling(Arc::clone(conn), workflow_id, true));
                }
                BounceAction::Refuse(holder) => return Err(bounce.deduplicated(holder)),
                BounceAction::Retry => tokio::time::sleep(RETRY_PAUSE).await,
                BounceAction::Enqueue => {
                    // The period is the delay and the timeout the deadline, both resolved by the
                    // insert on the database clock, which caps the release at the deadline.
                    let options = EnqueueOptions {
                        queue: Enqueue {
                            deduplication_id: Some(&bounce.deduplication_id),
                            delay: Some(period),
                            ..self.options.queue.clone()
                        },
                        workflow_id: call.workflow_id,
                        app_name: bounce.target_app,
                        ..self.options.clone()
                    };
                    let created = self
                        .client
                        .enqueue_encoded(
                            &self.workflow,
                            &bounce.input,
                            bounce.encoding.clone(),
                            options,
                            Some(Debounced {
                                timeout: self.debounce_timeout,
                            }),
                        )
                        .await;
                    if !lost_the_key(&created) {
                        return created;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sysdb::types::DebounceHolder;

    /// Each shape of key spells its own deduplication id, the stored form a later debounce of the
    /// key must reproduce.
    #[test]
    fn each_key_spells_its_deduplication_id() {
        let id = |workflow: QualifiedWorkflowName| deduplication_id(&workflow, "k");
        assert_eq!(id(QualifiedWorkflowName::new("f")), "f-k");
        assert_eq!(id(QualifiedWorkflowName::method("f", "C")), "f/C-k");
        assert_eq!(
            id(QualifiedWorkflowName::instance("f", "C", "eu")),
            "f/C/eu-k"
        );
        assert_eq!(
            id(QualifiedWorkflowName::instance("f", "", "eu")),
            "f//eu-k"
        );
    }

    fn holder(name: &str, is_debounced: bool, app: Option<&str>) -> Debounce {
        Debounce::Held(DebounceHolder {
            workflow_id: "holder".to_owned(),
            is_debounced,
            workflow_name: Some(name.to_owned()),
            class_name: None,
            config_name: None,
            application_name: app.map(str::to_owned),
        })
    }

    /// The spelling is `<workflow>-<key>`, which is what makes the two collide in
    /// `a_bounce_is_classified_by_its_holder`.
    #[test]
    fn the_deduplication_id_is_the_workflow_then_the_key() {
        let free = |name: &str| QualifiedWorkflowName::new(name);
        assert_eq!(deduplication_id(&free("wf"), "key"), "wf-key");
        assert_eq!(
            deduplication_id(&free("a"), "b-c"),
            deduplication_id(&free("a-b"), "c")
        );
    }

    /// The whole name is in the id, so a method, its configured instances and a free function of
    /// the same bare name each hold a key of their own.
    #[test]
    fn the_deduplication_id_spells_the_whole_name() {
        let eu = QualifiedWorkflowName::instance("wf", "Billing", "eu");
        let us = QualifiedWorkflowName::instance("wf", "Billing", "us");
        let method = QualifiedWorkflowName::method("wf", "Billing");
        assert_eq!(deduplication_id(&eu, "key"), "wf/Billing/eu-key");
        assert_eq!(deduplication_id(&method, "key"), "wf/Billing-key");
        let ids = [
            deduplication_id(&eu, "key"),
            deduplication_id(&us, "key"),
            deduplication_id(&method, "key"),
            deduplication_id(&QualifiedWorkflowName::new("wf"), "key"),
        ];
        let distinct: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(distinct.len(), ids.len(), "{ids:?}");
    }

    #[test]
    fn a_bounce_is_classified_by_its_holder() {
        let wf = QualifiedWorkflowName::new("wf");
        assert_eq!(
            classify(
                Debounce::Bounced {
                    workflow_id: "w".to_owned()
                },
                &wf,
                Some("app")
            ),
            BounceAction::Coalesce("w".to_owned())
        );
        assert_eq!(
            classify(Debounce::Unheld, &wf, Some("app")),
            BounceAction::Enqueue
        );
        // An ordinary deduplicated enqueue, and a colliding key of another workflow.
        assert_eq!(
            classify(holder("wf", false, Some("app")), &wf, Some("app")),
            BounceAction::Refuse("holder".to_owned())
        );
        assert_eq!(
            classify(holder("wf-b", true, Some("app")), &wf, Some("app")),
            BounceAction::Refuse("holder".to_owned())
        );
        // Another configured instance of the same class and name.
        let east = QualifiedWorkflowName::instance("wf", "Billing", "east");
        let west = Debounce::Held(DebounceHolder {
            workflow_id: "holder".to_owned(),
            is_debounced: true,
            workflow_name: Some("wf".to_owned()),
            class_name: Some("Billing".to_owned()),
            config_name: Some("west".to_owned()),
            application_name: None,
        });
        assert_eq!(
            classify(west, &east, Some("app")),
            BounceAction::Refuse("holder".to_owned())
        );
        // A holder that spells an absent class or instance as `""` is the same workflow, which the
        // bounce extends too; one it could not extend this time is between states.
        let blank = Debounce::Held(DebounceHolder {
            workflow_id: "holder".to_owned(),
            is_debounced: true,
            workflow_name: Some("wf".to_owned()),
            class_name: Some(String::new()),
            config_name: Some(String::new()),
            application_name: Some("app".to_owned()),
        });
        assert_eq!(classify(blank, &wf, Some("app")), BounceAction::Retry);
        // Another application's holder is refused, never retried: the bounce cannot extend it.
        assert_eq!(
            classify(holder("wf", true, Some("other")), &wf, Some("app")),
            BounceAction::Refuse("holder".to_owned())
        );
        // Our own, or one no application can be told apart by, is between states.
        assert_eq!(
            classify(holder("wf", true, Some("app")), &wf, Some("app")),
            BounceAction::Retry
        );
        assert_eq!(
            classify(holder("wf", true, Some("other")), &wf, None),
            BounceAction::Retry
        );
        assert_eq!(
            classify(holder("wf", true, None), &wf, Some("app")),
            BounceAction::Retry
        );
    }

    /// Each option a debounce owns is refused by name; the serialization is not one of them.
    #[test]
    fn the_options_a_debounce_owns_are_refused() {
        let refused = |queue: Enqueue<'_>, field: &str| {
            let error = refuse_owned_options(&queue).expect_err(field);
            assert!(
                matches!(&error, Error::InvalidArgument { operation, detail }
                    if operation == DEBOUNCE_OPERATION && detail.contains(field)),
                "{error}"
            );
        };
        refused(
            Enqueue {
                deduplication_id: Some("mine"),
                ..Enqueue::new("q")
            },
            "deduplication_id",
        );
        refused(
            Enqueue {
                delay: Some(Duration::from_secs(1)),
                ..Enqueue::new("q")
            },
            "delay",
        );
        refused(
            Enqueue {
                duplication_policy: DuplicationPolicy::ReturnExisting,
                ..Enqueue::new("q")
            },
            "ReturnExisting",
        );
        refused(
            Enqueue {
                priority: Some(1),
                ..Enqueue::new("q")
            },
            "priority",
        );
        refused(
            Enqueue {
                partition_key: Some("p"),
                ..Enqueue::new("q")
            },
            "partition_key",
        );
        refuse_owned_options(&Enqueue {
            serialization: crate::SerializationFormat::Portable,
            ..Enqueue::new("q")
        })
        .expect("the serialization is the caller's to choose");
    }

    #[test]
    fn a_zero_period_or_timeout_is_refused() {
        assert!(matches!(
            refuse_durations(Duration::ZERO, None),
            Err(Error::InvalidArgument { .. })
        ));
        assert!(matches!(
            refuse_durations(Duration::from_secs(1), Some(Duration::ZERO)),
            Err(Error::InvalidArgument { .. })
        ));
        refuse_durations(Duration::from_secs(1), Some(Duration::from_secs(1))).expect("valid");
    }

    /// A debouncer's stored `workflow_id` is refused by its public name, pointing at the per-call
    /// form; an empty or malformed `app_name` is refused as `app_name`, not as a column.
    #[test]
    fn a_stored_workflow_id_and_a_bad_app_name_are_refused() {
        let error = refuse_stored_workflow_id(Some("pinned"), "Debouncer::debounce_with")
            .expect_err("a stored id is refused");
        assert!(
            matches!(&error, Error::InvalidArgument { detail, .. }
                if detail.contains("options.workflow_id") && detail.contains("debounce_with")),
            "{error}"
        );
        refuse_stored_workflow_id(None, "Debouncer::debounce_with").expect("no id is fine");

        for bad in ["", "No"] {
            let error = refuse_app_name(Some(bad)).expect_err(bad);
            assert!(
                matches!(&error, Error::InvalidArgument { operation, detail }
                    if operation == DEBOUNCE_OPERATION
                        && detail.contains("`app_name`")
                        && !detail.contains("application_name")),
                "{error}"
            );
        }
        refuse_app_name(Some("some-app")).expect("a valid name");
        refuse_app_name(None).expect("no name");
    }
}
