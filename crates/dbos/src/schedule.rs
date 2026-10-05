//! Schedules: cron-driven workflows, stored as rows in `workflow_schedules`.
//!
//! **A schedule is a database object, not a decorator.** It names a workflow, a cron expression
//! and a context, and every executor of its application fires it: each computes the next tick,
//! sleeps until then, and enqueues the workflow under an id derived from the schedule and the
//! tick, so the fleet runs each tick once however many executors woke for it.
//!
//! Schedules are managed at runtime, from [`DBOS`] or from a [`Client`](crate::Client), and the
//! running executors notice within a poll — creating, pausing, changing or deleting one needs no
//! restart. See [`scheduler`](crate::scheduler) for the loop that fires them.
//!
//! # A scheduled workflow
//!
//! Any registered workflow whose argument is a [`ScheduledWorkflowInput`]: the tick it is running
//! for and the schedule's context. [`ScheduleSpec::new`] takes the workflow by its
//! [`WorkflowRef`], which is what holds the context's type to the workflow's — a schedule whose
//! context the workflow cannot read does not compile.
//!
//! ```no_run
//! # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
//! use dbos::{ScheduleSpec, ScheduledWorkflowInput};
//!
//! let report = dbos.register_workflow("nightly-report", |input: ScheduledWorkflowInput<String>| async move {
//!     println!("report for {} at {:?}", input.context, input.scheduled_time);
//!     Ok::<(), dbos::Error>(())
//! })?;
//! dbos.launch().await?;
//!
//! let mut spec = ScheduleSpec::new("nightly", &report, "0 0 2 * * *", &"emea".to_owned())?;
//! spec.cron_timezone = Some("Europe/Paris".to_owned());
//! dbos.create_schedule(&spec).await?;
//! # Ok(()) }
//! ```
//!
//! # Called from inside a workflow
//!
//! As on the rest of the management surface, the single-schedule operations on [`DBOS`] are
//! recorded as steps — `DBOS.createSchedule`, `DBOS.listSchedules` and so on — so a replay reads
//! back what the first run did. [`apply_schedules`](DBOS::apply_schedules),
//! [`backfill_schedule`](DBOS::backfill_schedule) and [`trigger_schedule`](DBOS::trigger_schedule)
//! are refused inside a workflow instead: each writes a batch with no checkpoint, which a replay
//! would write again.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::client::Client;
use crate::connection::Connection;
use crate::context::Ctx;
use crate::cron::{CronSchedule, firing_id, trigger_id};
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::instance::DBOS;
use crate::registry::{WorkflowKey, WorkflowRef};
use crate::serialization::{decode, encode};
use crate::sysdb::INTERNAL_QUEUE;
use crate::sysdb::types::{
    Change, NewSchedule, NewWorkflow, ScheduleFilter, ScheduleRecord, ScheduleStatus,
    ScheduleUpdate, step_names,
};
use crate::workflow::{DuplicationPolicy, Enqueue, init_or_join, new_row};

/// What a scheduled workflow is called with: the tick it is running for, and the schedule's
/// context.
///
/// A struct because a workflow takes one argument. Encoded as `{"scheduled_time": <RFC 3339 text>,
/// "context": <the stored context>}`.
///
/// `scheduled_time` is the **tick**, not the moment the workflow started: a workflow that waited
/// in a queue, or one enqueued by a backfill, still reports the time it was scheduled for. A
/// manual [`trigger`](DBOS::trigger_schedule) reports the time it was triggered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledWorkflowInput<C = ()> {
    /// The cron tick this run is for.
    #[serde(with = "rfc3339")]
    pub scheduled_time: SystemTime,
    /// The schedule's context, as [`ScheduleSpec`] stored it.
    pub context: C,
}

/// `SystemTime` as RFC 3339 text, rather than serde's `{secs_since_epoch, nanos_since_epoch}`.
///
/// Text so that a reader of the row can make sense of it without arithmetic.
mod rfc3339 {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer, de};

    pub(super) fn serialize<S: Serializer>(
        time: &SystemTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let timestamp = jiff::Timestamp::try_from(*time).map_err(serde::ser::Error::custom)?;
        serializer.collect_str(&timestamp)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SystemTime, D::Error> {
        let text = String::deserialize(deserializer)?;
        let timestamp: jiff::Timestamp = text.parse().map_err(de::Error::custom)?;
        Ok(SystemTime::from(timestamp))
    }
}

/// A schedule's definition: what [`create_schedule`](DBOS::create_schedule) creates and
/// [`apply_schedules`](DBOS::apply_schedules) reconciles.
///
/// The context is encoded when the spec is built, so a spec that exists is one whose context could
/// be written; the rest are plain fields to set after construction.
///
/// # The cron dialect
///
/// Five or six fields, seconds first when there are six. A five-field pattern fires at second
/// zero.
///
/// - **Nicknames:** `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`.
/// - **Names:** months and weekdays, three-letter or in full, in any case.
/// - **`?`** in day-of-month or day-of-week, meaning `*`.
/// - **`L`** (the last day of the month), **`LW`** (its last weekday) and **`nW`** (the weekday
///   nearest day *n*) in day-of-month; **`nL`** (the last weekday *n* of the month) and **`n#m`**
///   (the *m*th weekday *n*) in day-of-week. Day-of-week `7` is Sunday, as `0` is.
/// - **Steps** on `*` or on a range: `*/15`, `10-50/10`. A step on a single value (`5/10`) is
///   refused.
/// - **Day-of-month and day-of-week must both match** when both are restricted, so `0 0 13 * 5`
///   is Friday the 13th, not every Friday and every 13th.
/// - **A pattern that can never fire is refused**, such as `0 0 31 2 *`.
///
/// # Not accepted
///
/// `L-n` (the *n*th-from-last day), inverted ranges (`22-2`, `Fri-Mon`), and a seventh field are
/// all refused.
///
/// # Daylight saving
///
/// A wall-clock time skipped at spring-forward fires at the first instant after the gap. At
/// fall-back, a pattern for a fixed time of day fires once, on the first of the two repeated
/// hours, while an interval pattern such as `*/15 * * * *` fires in both, since each is a
/// quarter-hour that happened. A daily job at `02:30` or `01:30` therefore runs exactly once every
/// day of the year.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleSpec {
    /// The schedule's name, unique across every application sharing the database.
    pub schedule_name: String,
    /// The cron expression, in the dialect described above.
    pub schedule: String,
    /// The workflow each tick enqueues.
    ///
    /// A configured instance cannot be scheduled: the table has no column for one.
    pub workflow: WorkflowKey,
    /// The context each tick is called with, encoded.
    context: String,
    /// Whether an executor picking the schedule up — after a restart, or a deploy — first enqueues
    /// the ticks missed since it last fired.
    pub automatic_backfill: bool,
    /// The IANA timezone the cron expression is read in, or `None` for UTC.
    pub cron_timezone: Option<String>,
    /// The queue each tick is enqueued on, or `None` for the engine's internal queue.
    pub queue_name: Option<String>,
    /// The application that owns the schedule and runs its workflows, or `None` for the caller's.
    ///
    /// A client with no application name and no name here writes an **unclaimed** schedule,
    /// which every application sharing the database fires.
    pub application_name: Option<String>,
}

impl ScheduleSpec {
    /// A schedule enqueuing `workflow` on every tick of `schedule`, called with `context`.
    ///
    /// The workflow's argument type is what fixes `C`, so a context the workflow could not read
    /// is a compile error rather than a run that fails on every tick.
    pub fn new<C, R, E>(
        schedule_name: impl Into<String>,
        workflow: &WorkflowRef<ScheduledWorkflowInput<C>, R, E>,
        schedule: impl Into<String>,
        context: &C,
    ) -> Result<Self>
    where
        C: Serialize,
    {
        Self::for_workflow(schedule_name, workflow.key().clone(), schedule, context)
    }

    /// [`new`](Self::new) for a workflow named rather than held, which is what a
    /// [`Client`](crate::Client) has.
    ///
    /// Nothing here can hold the context to the workflow's type, so a mismatch surfaces as a
    /// deserialization failure in each run.
    pub fn for_workflow<C>(
        schedule_name: impl Into<String>,
        workflow: WorkflowKey,
        schedule: impl Into<String>,
        context: &C,
    ) -> Result<Self>
    where
        C: Serialize,
    {
        Ok(Self {
            schedule_name: schedule_name.into(),
            schedule: schedule.into(),
            workflow,
            context: encode(context, "schedule context")?,
            automatic_backfill: false,
            cron_timezone: None,
            queue_name: None,
            application_name: None,
        })
    }

    /// The context, encoded as the row stores it.
    pub fn context(&self) -> &str {
        &self.context
    }

    /// Refuses what no executor could fire: a malformed cron expression, an unknown timezone, a
    /// configured instance, an empty name.
    fn validate(&self, operation: &'static str) -> Result<()> {
        let refuse = |detail: String| Error::InvalidArgument {
            operation: operation.into(),
            detail: format!("schedule `{}`: {detail}", self.schedule_name),
        };
        if self.schedule_name.is_empty() {
            return Err(Error::InvalidArgument {
                operation: operation.into(),
                detail: "a schedule needs a name".to_owned(),
            });
        }
        if self.workflow.config_name.is_some() {
            return Err(refuse(format!(
                "the configured instance {} cannot be scheduled",
                self.workflow
            )));
        }
        CronSchedule::parse(&self.schedule, self.cron_timezone.as_deref()).map_err(refuse)?;
        Ok(())
    }

    /// The row this spec writes. `schedule_id` is generated by the system database.
    fn as_new(&self) -> NewSchedule<'_> {
        NewSchedule {
            workflow_class_name: self.workflow.class_name.as_deref(),
            context: &self.context,
            automatic_backfill: self.automatic_backfill,
            cron_timezone: self.cron_timezone.as_deref(),
            queue_name: self.queue_name.as_deref(),
            application_name: self.application_name.as_deref(),
            ..NewSchedule::new(&self.schedule_name, &self.workflow.name, &self.schedule)
        }
    }
}

/// A change to a schedule's definition, leaving what it does not name.
///
/// The workflow cannot be changed — a schedule for a different workflow is a different schedule —
/// and neither can the status, which is [`pause_schedule`](DBOS::pause_schedule)'s, nor
/// `last_fired_at`, which is the loop's.
///
/// The context is a [`serde_json::Value`] rather than the workflow's type, because nothing here
/// holds the workflow to check it against.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScheduleChange<'a> {
    /// The cron expression.
    pub schedule: Change<&'a str>,
    /// The context each tick is called with.
    pub context: Change<serde_json::Value>,
    /// Whether a restart first enqueues the ticks it missed.
    pub automatic_backfill: Change<bool>,
    /// The timezone the expression is read in; `Set(None)` is UTC.
    pub cron_timezone: Change<Option<&'a str>>,
    /// The queue ticks are enqueued on; `Set(None)` is the internal queue.
    pub queue_name: Change<Option<&'a str>>,
}

impl ScheduleChange<'_> {
    /// Refuses a change that would leave a schedule no executor could fire, and encodes the
    /// context.
    ///
    /// **A new expression is checked against the new timezone or UTC**, not the stored one: the
    /// stored row is not read, and an expression that fires somewhere fires everywhere.
    fn validate(&self, name: &str, operation: &'static str) -> Result<Option<String>> {
        let refuse = |detail: String| Error::InvalidArgument {
            operation: operation.into(),
            detail: format!("schedule `{name}`: {detail}"),
        };
        let timezone = match self.cron_timezone {
            Change::Set(timezone) => timezone,
            Change::Leave => None,
        };
        match self.schedule {
            Change::Set(expression) => {
                CronSchedule::parse(expression, timezone).map_err(refuse)?;
            }
            Change::Leave if timezone.is_some() => {
                CronSchedule::parse("* * * * *", timezone).map_err(refuse)?;
            }
            Change::Leave => {}
        }
        match &self.context {
            Change::Set(context) => Ok(Some(encode(context, "schedule context")?)),
            Change::Leave => Ok(None),
        }
    }
}

impl ScheduleRecord {
    /// The schedule's context, decoded as `C`.
    ///
    /// A schedule written by another SDK may hold a context this one cannot read, which is why
    /// listing hands back the encoded form and this decodes on request.
    pub fn decode_context<C: DeserializeOwned>(&self) -> Result<C> {
        decode(Some(&self.context), "schedule context")
    }
}

/// [`Error::InsideWorkflow`] for a call made from inside a workflow body or one of its steps.
fn refuse_inside_workflow(operation: &'static str) -> Result<()> {
    match Ctx::current() {
        Some(_) => Err(Error::InsideWorkflow {
            operation: operation.into(),
        }),
        None => Ok(()),
    }
}

impl DBOS {
    /// Creates a schedule, which every executor of its application then fires.
    ///
    /// Refused, before anything is written, if the cron expression or the timezone is invalid, if
    /// the pattern never fires, if this instance has no registration for the workflow, or if the
    /// queue is not registered. A name already taken is an error; to create-or-replace, use
    /// [`apply_schedules`](Self::apply_schedules).
    ///
    /// The executors pick the schedule up within their polling interval — a second, for the first
    /// minute after launch, so a schedule created at startup starts promptly.
    pub fn create_schedule<'a>(&self, spec: &'a ScheduleSpec) -> PendingStep<'a, ()> {
        const OPERATION: &str = "create a schedule";
        let built = self.executor(OPERATION).and_then(|executor| {
            spec.validate(OPERATION)?;
            refuse_unregistered(&executor, &spec.workflow, OPERATION)?;
            let placement = StepPlacement::of(executor.connection(), OPERATION)?;
            Ok((executor, placement))
        });
        PendingStep::placed(
            step_names::CREATE_SCHEDULE,
            built,
            move |executor, placement| async move {
                let conn = executor.connection();
                refuse_unknown_queue(
                    conn,
                    &spec.schedule_name,
                    spec.queue_name.as_deref(),
                    OPERATION,
                )
                .await?;
                conn.create_schedule(spec, placement.step()).await
            },
        )
    }

    /// Creates or replaces a set of schedules, in one transaction.
    ///
    /// The declarative form: an application states the schedules it wants at startup, and running
    /// this again — from the same deploy or the next — converges on them. An existing schedule
    /// keeps its id, its status and when it last fired, so re-applying does not resume a paused
    /// schedule or re-run a tick.
    ///
    /// Every spec is checked as [`create_schedule`](Self::create_schedule) checks one before any
    /// is written. **Refused inside a workflow**: it writes many rows with no checkpoint.
    pub async fn apply_schedules(&self, specs: &[ScheduleSpec]) -> Result<()> {
        const OPERATION: &str = "apply schedules";
        let executor = self.executor(OPERATION)?;
        refuse_inside_workflow(OPERATION)?;
        // Each distinct queue read once: a startup declaring many schedules usually puts them on
        // a few queues.
        let mut checked = HashSet::new();
        for spec in specs {
            spec.validate(OPERATION)?;
            refuse_unregistered(&executor, &spec.workflow, OPERATION)?;
            if checked.insert(spec.queue_name.as_deref()) {
                refuse_unknown_queue(
                    executor.connection(),
                    &spec.schedule_name,
                    spec.queue_name.as_deref(),
                    OPERATION,
                )
                .await?;
            }
        }
        executor.connection().apply_schedules(specs).await
    }

    /// The schedules matching `filter`, by name.
    ///
    /// The default filter lists this application's schedules and the unclaimed ones.
    pub fn list_schedules<'a>(
        &self,
        filter: &'a ScheduleFilter<'a>,
    ) -> PendingStep<'a, Vec<ScheduleRecord>> {
        PendingStep::placed(
            step_names::LIST_SCHEDULES,
            self.placed("list schedules"),
            move |executor, placement| async move {
                executor
                    .connection()
                    .list_schedules(filter, placement.step())
                    .await
            },
        )
    }

    /// The schedule with this name, or `None`.
    pub fn get_schedule<'a>(&self, name: &'a str) -> PendingStep<'a, Option<ScheduleRecord>> {
        PendingStep::placed(
            step_names::GET_SCHEDULE,
            self.placed("read a schedule"),
            move |executor, placement| async move {
                executor
                    .connection()
                    .get_schedule(name, placement.step())
                    .await
            },
        )
    }

    /// Changes a schedule's definition, leaving what `change` does not name.
    ///
    /// Running executors restart the schedule with the new definition at their next poll. A name
    /// with no schedule is an error, even for an empty change, and a new queue must be registered,
    /// as [`create_schedule`](Self::create_schedule) requires.
    pub fn update_schedule<'a>(
        &self,
        name: &'a str,
        change: &'a ScheduleChange<'a>,
    ) -> PendingStep<'a, ()> {
        const OPERATION: &str = "update a schedule";
        let built = self.executor(OPERATION).and_then(|executor| {
            let context = change.validate(name, OPERATION)?;
            let placement = StepPlacement::of(executor.connection(), OPERATION)?;
            Ok(((executor, context), placement))
        });
        PendingStep::placed(
            step_names::UPDATE_SCHEDULE,
            built,
            move |(executor, context), placement| async move {
                let conn = executor.connection();
                if let Change::Set(queue) = change.queue_name {
                    refuse_unknown_queue(conn, name, queue, OPERATION).await?;
                }
                conn.update_schedule(name, change, context.as_deref(), placement.step())
                    .await
            },
        )
    }

    /// Stops a schedule firing, until [`resume_schedule`](Self::resume_schedule).
    ///
    /// **With [`automatic_backfill`](ScheduleSpec::automatic_backfill), resuming runs the ticks
    /// missed while paused**, because the schedule catches up from when it last fired whenever an
    /// executor starts firing it — and resuming is that.
    /// Without it, ticks that passed during the pause are not run.
    pub fn pause_schedule<'a>(&self, name: &'a str) -> PendingStep<'a, ()> {
        PendingStep::placed(
            step_names::PAUSE_SCHEDULE,
            self.placed("pause a schedule"),
            move |executor, placement| async move {
                executor
                    .connection()
                    .set_schedule_status(name, ScheduleStatus::Paused, placement.step())
                    .await
            },
        )
    }

    /// Lets a paused schedule fire again, from its next tick.
    pub fn resume_schedule<'a>(&self, name: &'a str) -> PendingStep<'a, ()> {
        PendingStep::placed(
            step_names::RESUME_SCHEDULE,
            self.placed("resume a schedule"),
            move |executor, placement| async move {
                executor
                    .connection()
                    .set_schedule_status(name, ScheduleStatus::Active, placement.step())
                    .await
            },
        )
    }

    /// Deletes a schedule. Deleting one that does not exist is not an error.
    ///
    /// Runs it already enqueued are not touched.
    pub fn delete_schedule<'a>(&self, name: &'a str) -> PendingStep<'a, ()> {
        PendingStep::placed(
            step_names::DELETE_SCHEDULE,
            self.placed("delete a schedule"),
            move |executor, placement| async move {
                executor
                    .connection()
                    .delete_schedule(name, placement.step())
                    .await
            },
        )
    }

    /// Enqueues every tick of a schedule strictly between `start` and `end`, and hands back a
    /// handle to each.
    ///
    /// Each tick is enqueued under the id the loop would have used, so a tick that already ran is
    /// not run again — its handle is returned all the same, so the result covers the whole
    /// window. **Refused inside a workflow**: it enqueues many runs with no checkpoint.
    pub async fn backfill_schedule<R, E>(
        &self,
        name: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        const OPERATION: &str = "backfill a schedule";
        let executor = self.executor(OPERATION)?;
        refuse_inside_workflow(OPERATION)?;
        executor
            .connection()
            .backfill_schedule(name, start, end)
            .await
    }

    /// Enqueues one run of a schedule's workflow now, outside its cron expression.
    ///
    /// The run is called with the time it was triggered as its
    /// [`scheduled_time`](ScheduledWorkflowInput::scheduled_time). **Refused inside a workflow**:
    /// its run id is generated from the clock, so a replay would enqueue a second run.
    pub async fn trigger_schedule<R, E>(&self, name: &str) -> Result<WorkflowHandle<R, E>> {
        const OPERATION: &str = "trigger a schedule";
        let executor = self.executor(OPERATION)?;
        refuse_inside_workflow(OPERATION)?;
        executor.connection().trigger_schedule(name).await
    }
}

/// [`Error::InvalidArgument`] for a workflow this executor has no registration for.
///
/// On [`DBOS`] only: a [`Client`] runs no workflows, and may well be scheduling one for an
/// application written in another language.
fn refuse_unregistered(
    executor: &crate::Executor,
    workflow: &WorkflowKey,
    operation: &'static str,
) -> Result<()> {
    if executor.workflows().contains_key(workflow) {
        return Ok(());
    }
    Err(Error::InvalidArgument {
        operation: operation.into(),
        detail: format!("no workflow is registered as {workflow}"),
    })
}

/// [`Error::InvalidArgument`] for a queue nobody has registered.
///
/// Read rather than taken on faith, because a tick enqueued on a queue with no row is never
/// dequeued. On [`DBOS`] only, like [`refuse_unregistered`]: a client may be scheduling onto a
/// queue its application registers later.
async fn refuse_unknown_queue(
    conn: &Connection,
    schedule_name: &str,
    queue: Option<&str>,
    operation: &'static str,
) -> Result<()> {
    let Some(queue) = queue else {
        return Ok(());
    };
    if queue == INTERNAL_QUEUE || conn.queue(queue).await?.is_some() {
        return Ok(());
    }
    Err(Error::InvalidArgument {
        operation: operation.into(),
        detail: format!(
            "schedule `{schedule_name}`: queue `{queue}` is not registered; register it before \
             scheduling onto it"
        ),
    })
}

/// The same surface for a process outside the application.
///
/// [`DBOS`]'s methods with a client's two differences: nothing is checkpointed, and nothing checks
/// the workflow or the queue against a registration, because a client has neither — see
/// [`ScheduleSpec::for_workflow`]. [`apply_schedules`](Self::apply_schedules),
/// [`backfill_schedule`](Self::backfill_schedule) and [`trigger_schedule`](Self::trigger_schedule)
/// are not refused inside a workflow either: a client's call is never recorded, so there is
/// nothing for a replay to disagree with.
impl Client {
    /// Creates a schedule. See [`DBOS::create_schedule`].
    pub async fn create_schedule(&self, spec: &ScheduleSpec) -> Result<()> {
        spec.validate("create a schedule")?;
        self.connection().create_schedule(spec, None).await
    }

    /// Creates or replaces a set of schedules, in one transaction. See [`DBOS::apply_schedules`].
    pub async fn apply_schedules(&self, specs: &[ScheduleSpec]) -> Result<()> {
        for spec in specs {
            spec.validate("apply schedules")?;
        }
        self.connection().apply_schedules(specs).await
    }

    /// The schedules matching `filter`. See [`DBOS::list_schedules`].
    pub async fn list_schedules(&self, filter: &ScheduleFilter<'_>) -> Result<Vec<ScheduleRecord>> {
        self.connection().list_schedules(filter, None).await
    }

    /// The schedule with this name, or `None`.
    pub async fn get_schedule(&self, name: &str) -> Result<Option<ScheduleRecord>> {
        self.connection().get_schedule(name, None).await
    }

    /// Changes a schedule's definition. See [`DBOS::update_schedule`].
    pub async fn update_schedule(&self, name: &str, change: &ScheduleChange<'_>) -> Result<()> {
        let context = change.validate(name, "update a schedule")?;
        self.connection()
            .update_schedule(name, change, context.as_deref(), None)
            .await
    }

    /// Stops a schedule firing. See [`DBOS::pause_schedule`].
    pub async fn pause_schedule(&self, name: &str) -> Result<()> {
        self.connection()
            .set_schedule_status(name, ScheduleStatus::Paused, None)
            .await
    }

    /// Lets a paused schedule fire again.
    pub async fn resume_schedule(&self, name: &str) -> Result<()> {
        self.connection()
            .set_schedule_status(name, ScheduleStatus::Active, None)
            .await
    }

    /// Deletes a schedule. Deleting one that does not exist is not an error.
    pub async fn delete_schedule(&self, name: &str) -> Result<()> {
        self.connection().delete_schedule(name, None).await
    }

    /// Enqueues every tick of a schedule strictly between `start` and `end`. See
    /// [`DBOS::backfill_schedule`].
    pub async fn backfill_schedule<R, E>(
        &self,
        name: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        self.connection().backfill_schedule(name, start, end).await
    }

    /// Enqueues one run of a schedule's workflow now. See [`DBOS::trigger_schedule`].
    pub async fn trigger_schedule<R, E>(&self, name: &str) -> Result<WorkflowHandle<R, E>> {
        self.connection().trigger_schedule(name).await
    }
}

impl Connection {
    pub(crate) async fn create_schedule(
        &self,
        spec: &ScheduleSpec,
        caller: Option<(&str, i32)>,
    ) -> Result<()> {
        self.sysdb()
            .create_schedule(&spec.as_new(), caller)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(schedule = spec.schedule_name, "created a schedule");
        Ok(())
    }

    pub(crate) async fn apply_schedules(&self, specs: &[ScheduleSpec]) -> Result<()> {
        let rows: Vec<NewSchedule<'_>> = specs.iter().map(ScheduleSpec::as_new).collect();
        self.sysdb()
            .apply_schedules(&rows)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(schedules = specs.len(), "applied schedules");
        Ok(())
    }

    pub(crate) async fn list_schedules(
        &self,
        filter: &ScheduleFilter<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<ScheduleRecord>> {
        self.sysdb()
            .list_schedules(filter, caller)
            .await
            .map_err(Error::SystemDatabase)
    }

    pub(crate) async fn get_schedule(
        &self,
        name: &str,
        caller: Option<(&str, i32)>,
    ) -> Result<Option<ScheduleRecord>> {
        self.sysdb()
            .get_schedule(name, caller)
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Writes a validated change. `context` is the change's context, already encoded.
    pub(crate) async fn update_schedule(
        &self,
        name: &str,
        change: &ScheduleChange<'_>,
        context: Option<&str>,
        caller: Option<(&str, i32)>,
    ) -> Result<()> {
        let update = ScheduleUpdate {
            schedule: change.schedule,
            context: context.map_or(Change::Leave, Change::Set),
            automatic_backfill: change.automatic_backfill,
            cron_timezone: change.cron_timezone,
            queue_name: change.queue_name,
        };
        self.sysdb()
            .update_schedule(name, &update, caller)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(schedule = name, "updated a schedule");
        Ok(())
    }

    pub(crate) async fn set_schedule_status(
        &self,
        name: &str,
        status: ScheduleStatus,
        caller: Option<(&str, i32)>,
    ) -> Result<()> {
        self.sysdb()
            .set_schedule_status(name, status, caller)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(
            schedule = name,
            status = status.as_str(),
            "set a schedule's status"
        );
        Ok(())
    }

    pub(crate) async fn delete_schedule(
        &self,
        name: &str,
        caller: Option<(&str, i32)>,
    ) -> Result<()> {
        self.sysdb()
            .delete_schedule(name, caller)
            .await
            .map_err(Error::SystemDatabase)?;
        tracing::info!(schedule = name, "deleted a schedule");
        Ok(())
    }

    /// Enqueues every tick strictly between `start` and `end` that has not already run.
    ///
    /// `self: &Arc<Self>` because the handles poll through this connection.
    pub(crate) async fn backfill_schedule<R, E>(
        self: &Arc<Self>,
        name: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> Result<Vec<WorkflowHandle<R, E>>> {
        let record = self.schedule_to_fire(name).await?;
        let cron = CronSchedule::parse(&record.schedule, record.cron_timezone.as_deref()).map_err(
            |detail| Error::InvalidArgument {
                operation: "backfill a schedule".into(),
                detail: format!("schedule `{name}`: {detail}"),
            },
        )?;
        let end = to_jiff(end)?;
        let mut cursor = to_jiff(start)?;
        // Once for the whole window: the owner's version and the context are the same for every
        // tick, and a long window would otherwise read them once per tick.
        let firing = self.prepare_firing(&record).await?;
        let mut handles = Vec::new();
        let mut first_failure = None;
        while let Some(tick) = cron.next_after(cursor) {
            cursor = tick.timestamp();
            if cursor >= end {
                break;
            }
            let workflow_id = firing_id(name, &tick);
            // **On past a tick that fails**, so one bad tick does not leave the rest of the window
            // unfilled. The call still fails, with the first error, once the window is walked; a
            // second call over the same window enqueues only what is still missing.
            match self
                .fire_unless_fired(&record, &firing, cursor, &workflow_id)
                .await
            {
                Ok(_) => handles.push(WorkflowHandle::polling(Arc::clone(self), workflow_id, true)),
                Err(error) => {
                    tracing::warn!(workflow_id, error = %error, "could not backfill a tick");
                    first_failure.get_or_insert(error);
                }
            }
        }
        tracing::info!(
            schedule = name,
            ticks = handles.len(),
            "backfilled a schedule"
        );
        match first_failure {
            Some(error) => Err(error),
            None => Ok(handles),
        }
    }

    /// Enqueues one run now, under an id of its own.
    pub(crate) async fn trigger_schedule<R, E>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<WorkflowHandle<R, E>> {
        let record = self.schedule_to_fire(name).await?;
        let now = jiff::Timestamp::now();
        let workflow_id = trigger_id(name, now);
        let firing = self.prepare_firing(&record).await?;
        self.fire(&record, &firing, SystemTime::from(now), &workflow_id)
            .await?;
        tracing::info!(schedule = name, workflow_id, "triggered a schedule");
        Ok(WorkflowHandle::polling(Arc::clone(self), workflow_id, true))
    }

    /// The schedule, or the system database's `NotRegistered` for a name with none.
    async fn schedule_to_fire(&self, name: &str) -> Result<ScheduleRecord> {
        self.get_schedule(name, None).await?.ok_or_else(|| {
            Error::SystemDatabase(crate::sysdb::Error::NotRegistered {
                kind: "Schedule".into(),
                name: name.to_owned(),
            })
        })
    }

    /// Enqueues the tick at `at` under `workflow_id`, unless a workflow already has that id, and
    /// says whether it did.
    ///
    /// Read first, so a tick another executor already enqueued costs a read rather than an insert. The insert would be harmless anyway — an `ENQUEUED` row taken again
    /// is left as it was — but an executor fleet all waking for the same tick is the common case,
    /// and this keeps it to one write. Two executors can both read nothing and both insert, so
    /// `true` means this call wrote the row or tied for it.
    pub(crate) async fn fire_unless_fired(
        &self,
        record: &ScheduleRecord,
        firing: &Firing,
        at: jiff::Timestamp,
        workflow_id: &str,
    ) -> Result<bool> {
        let existing = self
            .sysdb()
            .get_workflow(workflow_id)
            .await
            .map_err(Error::SystemDatabase)?;
        if existing.is_some() {
            return Ok(false);
        }
        self.fire(record, firing, SystemTime::from(at), workflow_id)
            .await?;
        Ok(true)
    }

    /// Enqueues one run of a schedule's workflow, called for `scheduled_time`.
    ///
    /// What a scheduled run is written as:
    ///
    /// - **`ENQUEUED`**, on the schedule's queue or the internal one, with nobody claiming it.
    /// - **The owner's latest application version.** A schedule is fired by every executor of its
    ///   application, of whatever version, and the run should go to the newest: an executor still
    ///   on the old code during a deploy enqueues work only the new code dequeues.
    /// - **`application_name`** the schedule's owner, or this handle's for an unclaimed schedule.
    /// - **`schedule_name`**, which is what lists a schedule's runs.
    /// - **No parent and no creator**: nothing called this, so nothing records having called it.
    async fn fire(
        &self,
        record: &ScheduleRecord,
        firing: &Firing,
        scheduled_time: SystemTime,
        workflow_id: &str,
    ) -> Result<()> {
        let input = encode(
            &ScheduledWorkflowInput {
                scheduled_time,
                context: &firing.context,
            },
            "argument",
        )?;
        let queue = Enqueue::new(record.queue_name.as_deref().unwrap_or(INTERNAL_QUEUE));
        let new = NewWorkflow {
            name: Some(&record.workflow_name),
            class_name: record.workflow_class_name.as_deref(),
            input: Some(&input),
            serialization: Some(self.serializer().name()),
            executor_id: None,
            application_name: firing.owner.as_deref(),
            application_version: firing.version.as_deref(),
            schedule_name: Some(&record.schedule_name),
            ..new_row(workflow_id, Some(&queue))
        };
        init_or_join(self, &new, DuplicationPolicy::Reject, None).await?;
        tracing::debug!(
            schedule = record.schedule_name,
            workflow_id,
            "enqueued a scheduled run"
        );
        Ok(())
    }
}

/// What every run of a schedule shares, read once rather than per tick.
pub(crate) struct Firing {
    /// The application the runs belong to: the schedule's owner, or the firing handle's for an
    /// unclaimed schedule.
    owner: Option<String>,
    /// The owner's latest application version.
    version: Option<String>,
    /// The schedule's context, decoded.
    context: serde_json::Value,
}

impl Connection {
    /// Reads what [`fire`](Self::fire) stamps on a run of `record`.
    ///
    /// **The latest version is read at the time of firing**, not at the time the schedule was
    /// loaded: a deploy that lands while a schedule is running should get that schedule's next
    /// tick. The loop refreshes this at most once a second, so a long catch-up does not read it
    /// once per tick either.
    pub(crate) async fn prepare_firing(&self, record: &ScheduleRecord) -> Result<Firing> {
        let owner = record
            .application_name
            .as_deref()
            .or(self.app_name())
            .map(str::to_owned);
        // `None` when the owner has registered no version — a schedule created by a client before
        // its application ever launched. An unversioned row is dequeued by whichever executor is
        // on the latest version once there is one, which is what stamping the latest would mean.
        let version = self
            .sysdb()
            .get_latest_application_version(owner.as_deref())
            .await
            .map_err(Error::SystemDatabase)?
            .map(|version| version.version_name);
        // Decoded to a value and encoded again inside each input, rather than spliced in as text,
        // so a context that is not JSON fails here rather than in every run.
        let context = decode(Some(&record.context), "schedule context")?;
        Ok(Firing {
            owner,
            version,
            context,
        })
    }
}

/// A `SystemTime` as the calendar library's instant.
fn to_jiff(time: SystemTime) -> Result<jiff::Timestamp> {
    jiff::Timestamp::try_from(time).map_err(|error| Error::InvalidArgument {
        operation: "backfill a schedule".into(),
        detail: format!("{time:?} is outside the supported range: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::error::EngineOnly;

    #[test]
    fn the_input_encodes_as_go_does() {
        let input = ScheduledWorkflowInput {
            scheduled_time: SystemTime::UNIX_EPOCH + Duration::from_secs(1_759_665_600),
            context: "emea",
        };
        let encoded = encode::<_, EngineOnly>(&input, "argument").unwrap();
        assert_eq!(
            encoded,
            r#"{"scheduled_time":"2025-10-05T12:00:00Z","context":"emea"}"#
        );
        let decoded: ScheduledWorkflowInput<String> =
            decode::<_, EngineOnly>(Some(&encoded), "argument").unwrap();
        assert_eq!(decoded.scheduled_time, input.scheduled_time);
        assert_eq!(decoded.context, "emea");
    }

    #[test]
    fn a_spec_refuses_what_no_executor_could_fire() {
        let spec = |cron: &str, timezone: Option<&str>| ScheduleSpec {
            cron_timezone: timezone.map(str::to_owned),
            ..ScheduleSpec::for_workflow("s", WorkflowKey::new("w"), cron, &()).unwrap()
        };
        assert!(spec("* * * * *", None).validate("test").is_ok());
        for (cron, timezone, expected) in [
            ("not a cron", None, "invalid cron schedule"),
            ("0 0 31 2 *", None, "never fires"),
            ("* * * * *", Some("Nowhere/Special"), "invalid timezone"),
        ] {
            let error = spec(cron, timezone).validate("test").unwrap_err();
            assert!(error.to_string().contains(expected), "{cron}: {error}");
        }

        let instance = ScheduleSpec::for_workflow(
            "s",
            WorkflowKey::instance("w", "Class", "config"),
            "* * * * *",
            &(),
        )
        .unwrap();
        assert!(
            instance
                .validate("test")
                .unwrap_err()
                .to_string()
                .contains("configured instance")
        );
    }
}
