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
use crate::dequeue::random_unit;
use crate::error::Result;
use crate::schedule::Firing;
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

/// A schedule's task, and the definition it was started for.
struct Running {
    signature: Signature,
    task: Task,
}

/// Where a schedule's task stands, as far as the reconciler knows.
enum Task {
    /// Running, or finished and not yet looked at.
    Live(JoinHandle<Ended>),
    /// Ended because the schedule cannot be fired. A state of its own because a finished task can
    /// be awaited only once, and this outcome has to outlive that.
    Unfireable,
}

impl Task {
    fn abort(&self) {
        if let Task::Live(handle) = self {
            handle.abort();
        }
    }
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
        let catch_up = match running.get_mut(&record.schedule_id) {
            Some(current) if current.signature == signature => {
                let Task::Live(handle) = &mut current.task else {
                    // **Not restarted.** A schedule the task could not fire — an expression a peer
                    // stored without checking, a context that is not JSON, a pattern that has
                    // stopped firing — would fail the same way on every poll. A change to the
                    // definition is what gives it another chance.
                    continue;
                };
                if !handle.is_finished() {
                    continue;
                }
                // Finished, so awaiting it hands back how it ended without waiting.
                match handle.await {
                    Ok(Ended::Unfireable) => {
                        current.task = Task::Unfireable;
                        continue;
                    }
                    // It found its row replaced or redefined before this poll saw it, and the row
                    // is back as it was: restarted like any start.
                    Ok(Ended::Superseded) | Err(_) => true,
                }
            }
            // A definition change restarts the task without catching up, as in Python: the ticks
            // since the last firing belonged to the old definition.
            Some(_) => {
                tracing::info!(
                    schedule = record.schedule_name,
                    "the schedule's definition changed; restarting it"
                );
                if let Some(stale) = running.remove(&record.schedule_id) {
                    stale.task.abort();
                }
                false
            }
            // New to this executor — just launched, just created, or just resumed.
            None => true,
        };
        let id = record.schedule_id.clone();
        let task = spawn_tracked(
            executor,
            fire_forever(Arc::clone(executor), record.clone(), catch_up)
                .instrument(tracing::info_span!("schedule", name = record.schedule_name)),
        );
        running.insert(
            id,
            Running {
                signature,
                task: Task::Live(task),
            },
        );
    }
}

/// Why a schedule's task stopped on its own.
enum Ended {
    /// The schedule cannot be fired: its expression does not parse, it has no next tick, or its
    /// context is not JSON.
    Unfireable,
    /// The row under the schedule's name is no longer the one the task was started for — deleted,
    /// replaced or redefined since the reconciler last looked.
    Superseded,
}

/// How long a task trusts what it last read about its schedule before reading it again.
///
/// A task re-reads its row before firing — it may have been deleted, paused or redefined since the
/// reconciler last looked, which is up to a polling interval ago — and the owner's latest version
/// with it. Once a second at most, so a catch-up walking a backlog of ticks does not cost two extra
/// reads per tick.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// The longest wait between attempts at a tick that failed.
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Sleeps to each tick of a schedule and enqueues its run, until aborted or superseded.
///
/// **Ticks are walked from the previous tick, not from the clock**, so a task that wakes late —
/// a stalled runtime, a slow enqueue — fires the ticks it overslept rather than skipping them, as
/// Python's does.
///
/// **Catching up is the same walk, started earlier.** With `catch_up` and the schedule's
/// [`automatic_backfill`](ScheduleRecord::automatic_backfill), the walk starts from `last_fired_at`
/// rather than now: the missed ticks are all due, so they fire back to back, and the walk carries
/// on into the live ticks with nothing between them. A backfill that ended at its own "now" and a
/// loop that started from a later one would drop whatever fell between the two.
async fn fire_forever(executor: Arc<Executor>, record: ScheduleRecord, catch_up: bool) -> Ended {
    let cron = match CronSchedule::parse(&record.schedule, record.cron_timezone.as_deref()) {
        Ok(cron) => cron,
        Err(detail) => {
            tracing::error!("cannot run schedule `{}`: {detail}", record.schedule_name);
            return Ended::Unfireable;
        }
    };
    let now = Instant::now();
    let mut cursor = match record.last_fired_at {
        Some(last) if catch_up && record.automatic_backfill => {
            let last = Instant::from_millisecond(last.as_epoch_ms()).unwrap_or(now);
            if last < now {
                tracing::info!(
                    schedule = record.schedule_name,
                    since = %last,
                    "catching up on ticks missed while the schedule was not running"
                );
            }
            last.min(now)
        }
        _ => now,
    };
    let mut fresh: Option<(Firing, std::time::Instant)> = None;
    loop {
        let Some(tick) = cron.next_after(cursor) else {
            tracing::error!(
                "schedule `{}` no longer fires; stopping it",
                record.schedule_name
            );
            return Ended::Unfireable;
        };
        cursor = tick.timestamp();
        let until = Duration::try_from(cursor.duration_since(Instant::now())).unwrap_or_default();
        tokio::time::sleep(until + jitter(until)).await;

        let workflow_id = firing_id(&record.schedule_name, &tick);
        let next_tick = cron.next_after(cursor).map(|next| next.timestamp());
        let mut wait = Duration::from_secs(1);
        loop {
            match fire(&executor, &record, &mut fresh, cursor, &workflow_id).await {
                Ok(Attempt::Fired | Attempt::Paused) => break,
                Ok(Attempt::Superseded) => {
                    tracing::info!(
                        schedule = record.schedule_name,
                        "the schedule was changed or deleted; stopping it"
                    );
                    return Ended::Superseded;
                }
                Ok(Attempt::Unfireable(detail)) => {
                    tracing::error!("cannot run schedule `{}`: {detail}", record.schedule_name);
                    return Ended::Unfireable;
                }
                Err(error) => {
                    if overtaken(next_tick, wait) {
                        tracing::error!(
                            workflow_id,
                            error = %error,
                            "could not fire schedule `{}`; giving up on this tick",
                            record.schedule_name
                        );
                        break;
                    }
                    tracing::warn!(
                        workflow_id,
                        error = %error,
                        "could not fire schedule `{}`; retrying",
                        record.schedule_name
                    );
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(MAX_RETRY_INTERVAL);
                    // Read again on the next attempt: the failure may have been the row changing.
                    fresh = None;
                }
            }
        }
    }
}

/// Whether a failed tick should be given up on, because the next tick will be due before the next
/// attempt.
///
/// **Retried rather than dropped**, because a database away for longer than the system database's
/// own retries would otherwise cost a daily schedule a day. The next tick is the bound for a live
/// tick because past it this one has been overtaken, and two ticks' runs racing each other is
/// worse than one missing.
///
/// **No bound while catching up.** There the next tick is already due — every tick of a backlog is
/// — and giving up would drop the tick for good: the ticks after it move `last_fired_at` past it,
/// so no later catch-up comes back for it. Nothing races a backlog, which is walked one tick at a
/// time. The last tick of a pattern that stops firing has no next tick, and is retried for the same
/// reason.
fn overtaken(next_tick: Option<Instant>, wait: Duration) -> bool {
    let Some(next_tick) = next_tick else {
        return false;
    };
    match Duration::try_from(next_tick.duration_since(Instant::now())) {
        Ok(remaining) => remaining <= wait,
        // Already due: catching up.
        Err(_) => false,
    }
}

/// What one attempt at a tick came to.
enum Attempt {
    /// Enqueued, by this executor or a peer.
    Fired,
    /// The schedule is paused, so the tick is skipped. The task keeps going rather than ending, so
    /// a resume before the reconciler's next poll loses nothing; a pause that lasts is ended by
    /// the reconciler.
    Paused,
    /// The row under the name is gone, a different schedule, or redefined: whatever this task was
    /// started for, it is not that any more.
    Superseded,
    /// The schedule can never be fired as it stands. Carries why.
    Unfireable(String),
}

/// Enqueues one tick and records it as the schedule's last, unless the schedule says otherwise.
async fn fire(
    executor: &Executor,
    record: &ScheduleRecord,
    fresh: &mut Option<(Firing, std::time::Instant)>,
    tick: Instant,
    workflow_id: &str,
) -> Result<Attempt> {
    let conn = executor.connection();
    let firing = match fresh {
        Some((firing, read_at)) if read_at.elapsed() < REFRESH_INTERVAL => firing,
        _ => {
            let Some(current) = conn
                .get_schedule(&record.schedule_name, None)
                .await?
                .filter(|current| {
                    current.schedule_id == record.schedule_id
                        && Signature::of(current) == Signature::of(record)
                })
            else {
                return Ok(Attempt::Superseded);
            };
            if current.status != ScheduleStatus::Active {
                // Not cached: the next tick reads again, to see whether it has been resumed.
                *fresh = None;
                return Ok(Attempt::Paused);
            }
            // From the row as it is now: the definition is the same, but the owner may have
            // claimed an unclaimed schedule since.
            let prepared = match conn.prepare_firing(&current).await {
                Ok(prepared) => prepared,
                // A context this SDK cannot read — written by another one, say — will not become
                // readable by trying again.
                Err(crate::Error::Deserialization { message, .. }) => {
                    return Ok(Attempt::Unfireable(format!(
                        "its context cannot be decoded: {message}"
                    )));
                }
                Err(error) => return Err(error),
            };
            &fresh.insert((prepared, std::time::Instant::now())).0
        }
    };
    let enqueued = conn
        .fire_unless_fired(record, firing, tick, workflow_id)
        .await?;
    // Recorded by whichever executor enqueued the tick, rather than by every executor that woke
    // for it: the write is a no-op for all but one of them, and they would all queue on the
    // schedule's row lock to find that out.
    //
    // The tick, not the clock: it is what automatic backfill resumes from, and a backfill from the
    // clock would skip whatever fired between the tick and the write.
    if enqueued {
        conn.sysdb()
            .update_schedule_last_fired_at(
                &record.schedule_id,
                Timestamp::from_epoch_ms(tick.as_millisecond()),
            )
            .await
            .map_err(crate::Error::SystemDatabase)?;
    }
    Ok(Attempt::Fired)
}

/// A delay of up to a tenth of `until`, capped at [`MAX_JITTER`], so a fleet does not stampede.
///
/// Every executor of an application wakes for the same tick, and they race to enqueue one row.
/// Spreading them out costs a tick a few seconds of latency at most. Python's formula; TypeScript
/// and Go cap it the same way.
///
/// Drawn the way the dequeue loop's jitter is, rather than from a dependency added for it.
fn jitter(until: Duration) -> Duration {
    (until / 10).min(MAX_JITTER).mul_f64(random_unit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_tick_is_given_up_only_when_a_future_tick_would_overtake_it() {
        let wait = Duration::from_secs(4);
        let in_ = |secs: i64| Some(Instant::now() + jiff::SignedDuration::from_secs(secs));
        // Live: the next tick is far enough away to try again, then too close.
        assert!(!overtaken(in_(3600), wait));
        assert!(overtaken(in_(2), wait));
        // Catching up: the next tick is already due, so the backlog waits for this one.
        assert!(!overtaken(in_(-60), wait));
        // The last tick of a pattern that stops firing.
        assert!(!overtaken(None, wait));
    }

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
