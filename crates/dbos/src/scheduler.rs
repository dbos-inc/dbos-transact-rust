//! The loop that fires schedules: one task per active schedule, reconciled against the table.
//!
//! **Python's `dynamic_scheduler_loop`, and TypeScript's equivalent.** A reconciler lists this
//! application's schedules every polling interval and keeps one task per active schedule, starting
//! one for a new schedule, stopping it for a paused or deleted one, and restarting it when the
//! definition changes. Each task sleeps to its schedule's next tick and enqueues the run.
//!
//! Every executor of an application runs this loop, so every executor fires every tick. The run is
//! enqueued under an id derived from the schedule and the tick, which is what makes the fleet
//! enqueue it once — see [`firing_id`](crate::cron::firing_id).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp as Instant;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::Executor;
use crate::cron::{CronSchedule, firing_id};
use crate::error::Result;
use crate::sysdb::types::{ScheduleFilter, ScheduleRecord, ScheduleStatus, Timestamp};
use crate::workflow::spawn_tracked;

/// How long after launch the reconciler polls fast.
///
/// A schedule created during startup — by the application itself, just after `launch` — would
/// otherwise wait out a whole polling interval before its first tick. Python and TypeScript both
/// poll every second for the first minute for this reason.
const STARTUP_FAST_POLL_DURATION: Duration = Duration::from_secs(60);

/// How often the reconciler polls during [`STARTUP_FAST_POLL_DURATION`].
const STARTUP_FAST_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The ceiling on how long a tick is delayed to spread a fleet out.
const MAX_JITTER: Duration = Duration::from_secs(10);

/// Starts the reconciler. Called once, by `launch`.
pub(crate) fn spawn(executor: Arc<Executor>, polling_interval: Duration) {
    spawn_tracked(
        &executor,
        reconcile(Arc::clone(&executor), polling_interval)
            .instrument(tracing::info_span!("scheduler")),
    );
}

/// What a schedule's task was started for. A change to any of these restarts it.
///
/// Python's thread signature, and TypeScript's: the definition, and nothing the loop itself
/// writes. `status` is not here because a paused schedule has no task to compare; `last_fired_at`
/// because the task writes it every tick; `automatic_backfill` because it only matters when a task
/// starts; the id and the owner because neither changes what is fired.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Signature {
    workflow_name: String,
    workflow_class_name: Option<String>,
    schedule: String,
    context: String,
    cron_timezone: Option<String>,
    queue_name: Option<String>,
}

impl Signature {
    fn of(record: &ScheduleRecord) -> Self {
        Self {
            workflow_name: record.workflow_name.clone(),
            workflow_class_name: record.workflow_class_name.clone(),
            schedule: record.schedule.clone(),
            context: record.context.clone(),
            cron_timezone: record.cron_timezone.clone(),
            queue_name: record.queue_name.clone(),
        }
    }
}

/// A schedule's running task, and the definition it was started for.
struct Running {
    signature: Signature,
    task: JoinHandle<()>,
}

/// Lists the schedules and brings the running tasks into line with them, forever.
///
/// Ended by shutdown aborting it, which aborts the per-schedule tasks with it: every one is
/// spawned through [`spawn_tracked`].
async fn reconcile(executor: Arc<Executor>, polling_interval: Duration) {
    let fast_until = std::time::Instant::now() + STARTUP_FAST_POLL_DURATION;
    let mut running: HashMap<String, Running> = HashMap::new();
    loop {
        // This application's schedules and the unclaimed ones — the default scope, and the one
        // every reference fires.
        match executor
            .sysdb()
            .list_schedules(&ScheduleFilter::default(), None)
            .await
        {
            Ok(schedules) => reconcile_once(&executor, &mut running, schedules).await,
            Err(error) => tracing::warn!(error = %error, "could not list schedules"),
        }
        let wait = if std::time::Instant::now() < fast_until {
            STARTUP_FAST_POLL_INTERVAL.min(polling_interval)
        } else {
            polling_interval
        };
        tokio::time::sleep(wait).await;
    }
}

/// One pass of the reconciler.
async fn reconcile_once(
    executor: &Arc<Executor>,
    running: &mut HashMap<String, Running>,
    schedules: Vec<ScheduleRecord>,
) {
    // Keyed by id rather than name, as Python keys its threads: a schedule deleted and created
    // again under the same name between two polls is a new schedule.
    let present: HashSet<&str> = schedules.iter().map(|s| s.schedule_id.as_str()).collect();
    running.retain(|id, schedule| {
        let keep = present.contains(id.as_str());
        if !keep {
            schedule.task.abort();
        }
        keep
    });

    for record in schedules {
        if record.status != ScheduleStatus::Active {
            if let Some(stopped) = running.remove(&record.schedule_id) {
                tracing::debug!(schedule = record.schedule_name, "the schedule is paused");
                stopped.task.abort();
            }
            continue;
        }
        let signature = Signature::of(&record);
        match running.get(&record.schedule_id) {
            // **Including a task that has ended.** One ends only on a schedule it cannot fire —
            // an expression a peer stored without checking, or one that has stopped firing — and
            // restarting it would fail the same way on every poll. A change to the definition
            // is what gives it another chance.
            Some(current) if current.signature == signature => continue,
            Some(_) => {
                tracing::info!(
                    schedule = record.schedule_name,
                    "the schedule's definition changed; restarting it"
                );
                if let Some(stale) = running.remove(&record.schedule_id) {
                    stale.task.abort();
                }
            }
            None => catch_up(executor, &record).await,
        }
        let id = record.schedule_id.clone();
        let task = spawn_tracked(
            executor,
            fire_forever(Arc::clone(executor), record.clone())
                .instrument(tracing::info_span!("schedule", name = record.schedule_name)),
        );
        running.insert(id, Running { signature, task });
    }
}

/// Enqueues the ticks a schedule missed while no task here was firing it, if it asks for that.
///
/// From `last_fired_at` to now, which is the downtime as this schedule saw it. Only when a task
/// *starts*: a definition change restarts the task without catching up, as in Python.
async fn catch_up(executor: &Arc<Executor>, record: &ScheduleRecord) {
    let Some(last_fired_at) = record.last_fired_at.and_then(Timestamp::to_system_time) else {
        return;
    };
    if !record.automatic_backfill {
        return;
    }
    let now = std::time::SystemTime::now();
    if last_fired_at >= now {
        return;
    }
    match executor
        .connection()
        .backfill_schedule::<(), crate::EngineOnly>(&record.schedule_name, last_fired_at, now)
        .await
    {
        Ok(handles) if !handles.is_empty() => tracing::info!(
            schedule = record.schedule_name,
            ticks = handles.len(),
            "caught up on ticks missed while the schedule was not running"
        ),
        Ok(_) => {}
        Err(error) => tracing::warn!(
            schedule = record.schedule_name,
            error = %error,
            "could not catch up on missed ticks"
        ),
    }
}

/// Sleeps to each tick of a schedule and enqueues its run, until aborted.
///
/// **Ticks are walked from the previous tick, not from the clock**, so a task that wakes late —
/// a stalled runtime, a slow enqueue — fires the ticks it overslept rather than skipping them, as
/// Python's does.
async fn fire_forever(executor: Arc<Executor>, record: ScheduleRecord) {
    let cron = match CronSchedule::parse(&record.schedule, record.cron_timezone.as_deref()) {
        Ok(cron) => cron,
        Err(detail) => {
            tracing::error!("cannot run schedule `{}`: {detail}", record.schedule_name);
            return;
        }
    };
    let mut cursor = Instant::now();
    loop {
        let Some(tick) = cron.next_after(cursor) else {
            tracing::error!(
                "schedule `{}` no longer fires; stopping it",
                record.schedule_name
            );
            return;
        };
        cursor = tick.timestamp();
        let until = Duration::try_from(cursor.duration_since(Instant::now())).unwrap_or_default();
        tokio::time::sleep(until + jitter(until)).await;

        let workflow_id = firing_id(&record.schedule_name, &tick);
        if let Err(error) = fire(&executor, &record, cursor, &workflow_id).await {
            tracing::error!(
                workflow_id,
                error = %error,
                "could not fire schedule `{}`",
                record.schedule_name
            );
        }
    }
}

/// Enqueues one tick and records it as the schedule's last.
async fn fire(
    executor: &Executor,
    record: &ScheduleRecord,
    tick: Instant,
    workflow_id: &str,
) -> Result<()> {
    let conn = executor.connection();
    conn.fire_unless_fired(record, tick, workflow_id).await?;
    // The tick, not the clock: it is what automatic backfill resumes from, and a backfill from the
    // clock would skip whatever fired between the tick and the write.
    conn.sysdb()
        .update_schedule_last_fired_at(
            &record.schedule_name,
            Timestamp::from_epoch_ms(tick.as_millisecond()),
        )
        .await
        .map_err(crate::Error::SystemDatabase)
}

/// A delay of up to a tenth of `until`, capped at [`MAX_JITTER`], so a fleet does not stampede.
///
/// Every executor of an application wakes for the same tick, and they race to enqueue one row.
/// Spreading them out costs a tick a few seconds of latency at most. Python's formula; TypeScript
/// and Go cap it the same way.
///
/// Drawn from a v4 UUID, as the dequeue loop's jitter is, rather than a dependency added for it.
fn jitter(until: Duration) -> Duration {
    let ceiling = (until / 10).min(MAX_JITTER);
    let bits = uuid::Uuid::new_v4().as_u128() as u32;
    ceiling.mul_f64(f64::from(bits) / f64::from(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_is_at_most_a_tenth_and_at_most_ten_seconds() {
        for _ in 0..1000 {
            assert!(jitter(Duration::from_secs(5)) <= Duration::from_millis(500));
            assert!(jitter(Duration::from_secs(3600)) <= MAX_JITTER);
        }
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn the_signature_ignores_what_the_loop_writes() {
        let record = ScheduleRecord {
            schedule_id: "id".into(),
            schedule_name: "s".into(),
            workflow_name: "w".into(),
            workflow_class_name: None,
            schedule: "* * * * *".into(),
            status: ScheduleStatus::Active,
            context: "null".into(),
            last_fired_at: None,
            automatic_backfill: false,
            cron_timezone: None,
            queue_name: None,
            application_name: None,
        };
        let same = ScheduleRecord {
            schedule_id: "other-id".into(),
            status: ScheduleStatus::Paused,
            last_fired_at: Some(Timestamp::from_epoch_ms(1)),
            automatic_backfill: true,
            application_name: Some("app".into()),
            ..record.clone()
        };
        assert_eq!(Signature::of(&record), Signature::of(&same));

        for changed in [
            ScheduleRecord {
                workflow_name: "w2".into(),
                ..record.clone()
            },
            ScheduleRecord {
                workflow_class_name: Some("C".into()),
                ..record.clone()
            },
            ScheduleRecord {
                schedule: "0 * * * *".into(),
                ..record.clone()
            },
            ScheduleRecord {
                context: "1".into(),
                ..record.clone()
            },
            ScheduleRecord {
                cron_timezone: Some("UTC".into()),
                ..record.clone()
            },
            ScheduleRecord {
                queue_name: Some("q".into()),
                ..record.clone()
            },
        ] {
            assert_ne!(Signature::of(&record), Signature::of(&changed));
        }
    }
}
