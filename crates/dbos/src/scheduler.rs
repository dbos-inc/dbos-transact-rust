//! The loop that fires schedules: one task per active schedule, reconciled against the table.
//!
//! [`poll_schedules`] lists this application's schedules every polling interval, and
//! [`reconcile_schedules`] keeps one task per active schedule: starting one for a new schedule,
//! stopping it for a paused or deleted one, and restarting it when the definition changes. Each
//! task sleeps to its schedule's next tick and enqueues the run.
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

/// How long after launch the schedules are polled fast.
///
/// A schedule created during startup — by the application itself, just after `launch` — would
/// otherwise wait out a whole polling interval before its first tick.
const STARTUP_FAST_POLL_DURATION: Duration = Duration::from_secs(60);

/// How often the schedules are polled during [`STARTUP_FAST_POLL_DURATION`].
const STARTUP_FAST_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The ceiling on how long a tick is delayed to spread a fleet out.
const MAX_JITTER: Duration = Duration::from_secs(10);

/// Starts polling the schedules. Called once, by `launch`.
pub(crate) fn spawn(executor: Arc<Executor>, polling_interval: Duration) {
    spawn_tracked(
        &executor,
        poll_schedules(Arc::clone(&executor), polling_interval)
            .instrument(tracing::info_span!("scheduler")),
    );
}

/// Whether two rows define the same schedule to fire. A change to any of these restarts its task.
///
/// The definition, and nothing the loop itself writes. `status` is not compared because a paused
/// schedule has no task to compare; `last_fired_at` because the task writes it every tick;
/// `automatic_backfill` because it only matters when a task starts; the id and the owner because
/// neither changes what is fired.
///
/// Compared field by field, by reference, rather than through a copy or a hash of the fields: it
/// runs for every active schedule on every poll, and the context can be large.
fn same_definition(a: &ScheduleRecord, b: &ScheduleRecord) -> bool {
    a.workflow_name == b.workflow_name
        && a.workflow_class_name == b.workflow_class_name
        && a.schedule == b.schedule
        && a.context == b.context
        && a.cron_timezone == b.cron_timezone
        && a.queue_name == b.queue_name
}

/// A schedule's task, and the row it was started for.
struct Running {
    /// Shared with the task, which fires from it.
    record: Arc<ScheduleRecord>,
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

/// Lists the schedules every polling interval and hands each listing to [`reconcile_schedules`],
/// forever.
///
/// Ended by shutdown aborting it, which aborts the per-schedule tasks with it: every one is
/// spawned through [`spawn_tracked`].
async fn poll_schedules(executor: Arc<Executor>, polling_interval: Duration) {
    let fast_until = std::time::Instant::now() + STARTUP_FAST_POLL_DURATION;
    let mut running: HashMap<String, Running> = HashMap::new();
    loop {
        // This application's schedules and the unclaimed ones: the default scope, and the set
        // this application is responsible for firing.
        match executor
            .sysdb()
            .list_schedules(&ScheduleFilter::default(), None)
            .await
        {
            Ok(schedules) => reconcile_schedules(&executor, &mut running, schedules).await,
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

/// Brings the running tasks into line with one listing of the schedules.
async fn reconcile_schedules(
    executor: &Arc<Executor>,
    running: &mut HashMap<String, Running>,
    schedules: Vec<ScheduleRecord>,
) {
    // Keyed by id rather than name: a schedule deleted and created again under the same name
    // between two polls is a new schedule.
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
        let catch_up = match running.get_mut(&record.schedule_id) {
            Some(current) if same_definition(&current.record, &record) => {
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
                    // **Parked like an unfireable schedule.** Whatever panicked will most likely
                    // panic again, and restarting on every poll would loop on it.
                    Err(error) if error.is_panic() => {
                        tracing::error!(
                            "the task firing schedule `{}` panicked; it is stopped until the \
                             schedule's definition changes",
                            record.schedule_name
                        );
                        current.task = Task::Unfireable;
                        continue;
                    }
                    // It found its row replaced or redefined before this poll saw it, and the row
                    // is back as it was: restarted like any start.
                    Ok(Ended::Superseded) | Err(_) => true,
                }
            }
            // A definition change restarts the task without catching up: the ticks since the last
            // firing belonged to the old definition.
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
        let span = tracing::info_span!("schedule", name = record.schedule_name);
        let record = Arc::new(record);
        let task = spawn_tracked(
            executor,
            run_schedule(Arc::clone(executor), Arc::clone(&record), catch_up).instrument(span),
        );
        running.insert(
            record.schedule_id.clone(),
            Running {
                record,
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
/// a stalled runtime, a slow enqueue — fires the ticks it overslept rather than skipping them.
///
/// **Catching up is the same walk, started earlier.** With `catch_up` and the schedule's
/// [`automatic_backfill`](ScheduleRecord::automatic_backfill), the walk starts from `last_fired_at`
/// rather than now: the missed ticks are all due, so they fire back to back, and the walk carries
/// on into the live ticks with nothing between them. A backfill that ended at its own "now" and a
/// loop that started from a later one would drop whatever fell between the two.
async fn run_schedule(
    executor: Arc<Executor>,
    record: Arc<ScheduleRecord>,
    catch_up: bool,
) -> Ended {
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
    // Whether the last attempt found the schedule paused, so the next one that finds it active
    // knows it has been resumed.
    let mut paused = false;
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
        // Settled once, when the tick is reached: a live tick whose retries run past the next one
        // has been overtaken, not turned into a backlog.
        let catching_up = next_tick.is_some_and(|next| next <= Instant::now());
        let mut tick_state = TickState {
            paused,
            enqueued: false,
        };
        let mut wait = Duration::from_secs(1);
        loop {
            match attempt_tick(
                &executor,
                &record,
                &mut fresh,
                &mut tick_state,
                cursor,
                &workflow_id,
            )
            .await
            {
                Ok(Attempt::Fired) => {
                    paused = false;
                    break;
                }
                Ok(Attempt::Paused) => {
                    paused = true;
                    break;
                }
                Ok(Attempt::Resumed {
                    last_fired_at,
                    automatic_backfill,
                }) => {
                    paused = false;
                    // Walked again from where resuming says to start, which includes this tick.
                    cursor = resume_cursor(automatic_backfill, last_fired_at, cursor);
                    break;
                }
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
                    if give_up(next_tick, catching_up, Instant::now(), wait) {
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

/// Whether to stop retrying a tick that failed.
///
/// **Retried rather than dropped**, because a database away for longer than the system database's
/// own retries would otherwise cost a daily schedule a day. A live tick is retried until the next
/// tick would be due before the next attempt, and given up once the next tick has arrived: past
/// that, this one has been overtaken, and two ticks' runs racing each other is worse than one
/// missing.
///
/// **No bound while catching up** — `catching_up`, settled when the tick was reached, says the next
/// tick was already due then. Giving up there would drop the tick for good: the ticks after it move
/// `last_fired_at` past it, so no later catch-up comes back for it. Nothing races a backlog, which
/// is walked one tick at a time. The last tick of a pattern that stops firing has no next tick, and
/// is retried for the same reason.
fn give_up(next_tick: Option<Instant>, catching_up: bool, now: Instant, wait: Duration) -> bool {
    let Some(next_tick) = next_tick else {
        return false;
    };
    if catching_up {
        return false;
    }
    match Duration::try_from(next_tick.duration_since(now)) {
        Ok(remaining) => remaining <= wait,
        // The next tick arrived while this one was being retried.
        Err(_) => true,
    }
}

/// Where to walk from once a paused schedule is found active again, given the tick it was found
/// active at.
///
/// With automatic backfill, from when the schedule last fired, so the ticks skipped while it was
/// paused run — the same catch-up a resume gets when the reconciler restarts the task. Without it,
/// from just before the current tick, so that tick runs and the paused ones do not.
fn resume_cursor(
    automatic_backfill: bool,
    last_fired_at: Option<Timestamp>,
    tick: Instant,
) -> Instant {
    let just_before = tick - jiff::SignedDuration::from_nanos(1);
    match last_fired_at {
        Some(last) if automatic_backfill => Instant::from_millisecond(last.as_epoch_ms())
            .map_or(just_before, |last| last.min(just_before)),
        _ => just_before,
    }
}

/// What the attempts at one tick have learned, kept across retries.
struct TickState {
    /// The previous tick found the schedule paused, so finding it active means it was resumed.
    paused: bool,
    /// This executor enqueued the tick's run. Kept across retries so that a retry after a failed
    /// `last_fired_at` write — which finds the run already there — still records it.
    enqueued: bool,
}

/// What one attempt at a tick came to.
enum Attempt {
    /// Enqueued, by this executor or a peer.
    Fired,
    /// The schedule is paused, so the tick is skipped. The task keeps going rather than ending, so
    /// a resume before the reconciler's next poll loses nothing; a pause that lasts is ended by
    /// the reconciler.
    Paused,
    /// The schedule was paused at the previous tick and is active again. Nothing was fired: the
    /// task walks again from [`resume_cursor`], which includes this tick.
    Resumed {
        /// When the schedule last fired, as the row says now.
        last_fired_at: Option<Timestamp>,
        /// Whether to catch up on the paused ticks, as the row says now: the flag is not part of
        /// the definition, so it can have changed since the task started.
        automatic_backfill: bool,
    },
    /// The row under the name is gone, a different schedule, or redefined: whatever this task was
    /// started for, it is not that any more.
    Superseded,
    /// The schedule can never be fired as it stands. Carries why.
    Unfireable(String),
}

/// Tries once to enqueue a tick and record it as the schedule's last, unless re-reading the
/// schedule says it is paused, resumed, changed or deleted.
async fn attempt_tick(
    executor: &Executor,
    record: &ScheduleRecord,
    fresh: &mut Option<(Firing, std::time::Instant)>,
    state: &mut TickState,
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
                    current.schedule_id == record.schedule_id && same_definition(current, record)
                })
            else {
                return Ok(Attempt::Superseded);
            };
            if current.status != ScheduleStatus::Active {
                // Not cached: the next tick reads again, to see whether it has been resumed.
                *fresh = None;
                return Ok(Attempt::Paused);
            }
            if state.paused {
                return Ok(Attempt::Resumed {
                    last_fired_at: current.last_fired_at,
                    automatic_backfill: current.automatic_backfill,
                });
            }
            // From the row as it is now: the definition is the same, but the owner may have
            // claimed an unclaimed schedule since.
            let prepared = match conn.prepare_firing(&current).await {
                Ok(prepared) => prepared,
                // A context that cannot be decoded will not become readable by trying again.
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
    if conn
        .fire_unless_fired(record, firing, tick, workflow_id)
        .await?
    {
        state.enqueued = true;
    }
    // Recorded by whichever executor enqueued the tick, rather than by every executor that woke
    // for it. They would all write the same tick, queuing on the schedule's row lock to do it —
    // and the write is last-writer-wins, so every extra writer is another chance for a straggler
    // to move the value backwards.
    //
    // The tick, not the clock: it is what automatic backfill resumes from, and a backfill from the
    // clock would skip whatever fired between the tick and the write.
    if state.enqueued {
        conn.sysdb()
            .update_schedule_last_fired_at(
                &record.schedule_name,
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
/// Spreading them out costs a tick a few seconds of latency at most.
///
/// Drawn the way the dequeue loop's jitter is, rather than from a dependency added for it.
fn jitter(until: Duration) -> Duration {
    (until / 10).min(MAX_JITTER).mul_f64(random_unit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> Instant {
        rfc3339.parse().unwrap()
    }

    #[test]
    fn a_live_tick_is_retried_until_the_next_tick_would_overtake_it() {
        let now = at("2026-10-05T12:00:00Z");
        let wait = Duration::from_secs(4);
        // The next tick is far enough away to try again, then too close.
        assert!(!give_up(Some(at("2026-10-05T13:00:00Z")), false, now, wait));
        assert!(give_up(Some(at("2026-10-05T12:00:02Z")), false, now, wait));
        // The next tick arrived while this one was being retried: overtaken, not a backlog.
        assert!(give_up(Some(at("2026-10-05T11:59:00Z")), false, now, wait));
    }

    #[test]
    fn a_tick_of_a_backlog_or_the_last_tick_is_never_given_up() {
        let now = at("2026-10-05T12:00:00Z");
        let wait = Duration::from_secs(4);
        assert!(!give_up(Some(at("2026-10-05T11:00:00Z")), true, now, wait));
        assert!(!give_up(Some(at("2026-10-05T12:00:02Z")), true, now, wait));
        assert!(!give_up(None, false, now, wait));
    }

    #[test]
    fn a_resume_with_automatic_backfill_walks_from_the_last_firing() {
        let tick = at("2026-10-05T12:00:20Z");
        let last = Timestamp::from_epoch_ms(at("2026-10-05T12:00:05Z").as_millisecond());
        assert_eq!(
            resume_cursor(true, Some(last), tick),
            at("2026-10-05T12:00:05Z")
        );
        // Never past the tick it was found active at, which must still run.
        let later = Timestamp::from_epoch_ms(at("2026-10-05T12:01:00Z").as_millisecond());
        assert!(resume_cursor(true, Some(later), tick) < tick);
        // Nothing to walk back to.
        assert!(resume_cursor(true, None, tick) < tick);
    }

    #[test]
    fn a_resume_without_automatic_backfill_runs_only_the_current_tick() {
        let tick = at("2026-10-05T12:00:20Z");
        let last = Timestamp::from_epoch_ms(at("2026-10-05T12:00:05Z").as_millisecond());
        let cursor = resume_cursor(false, Some(last), tick);
        assert!(cursor < tick);
        assert!(cursor > at("2026-10-05T12:00:19Z"));
        let cron = CronSchedule::parse("* * * * * *", None).unwrap();
        assert_eq!(cron.next_after(cursor).unwrap().timestamp(), tick);
    }

    #[test]
    fn jitter_is_at_most_a_tenth_and_at_most_ten_seconds() {
        for _ in 0..1000 {
            assert!(jitter(Duration::from_secs(5)) <= Duration::from_millis(500));
            assert!(jitter(Duration::from_secs(3600)) <= MAX_JITTER);
        }
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    }

    /// A row as the scheduler reads it, for the comparisons below.
    fn row() -> ScheduleRecord {
        ScheduleRecord {
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
        }
    }

    #[test]
    fn what_the_loop_writes_is_not_part_of_the_definition() {
        let record = row();
        let same = ScheduleRecord {
            schedule_id: "other-id".into(),
            schedule_name: "renamed".into(),
            status: ScheduleStatus::Paused,
            last_fired_at: Some(Timestamp::from_epoch_ms(1)),
            automatic_backfill: true,
            application_name: Some("app".into()),
            ..row()
        };
        assert!(same_definition(&record, &same));
        assert!(
            same_definition(&record, &record),
            "a row is its own definition"
        );
    }

    #[test]
    fn every_field_of_the_definition_restarts_the_task() {
        let record = row();
        for (field, changed) in [
            (
                "workflow_name",
                ScheduleRecord {
                    workflow_name: "w2".into(),
                    ..row()
                },
            ),
            (
                "workflow_class_name",
                ScheduleRecord {
                    workflow_class_name: Some("C".into()),
                    ..row()
                },
            ),
            (
                "schedule",
                ScheduleRecord {
                    schedule: "0 * * * *".into(),
                    ..row()
                },
            ),
            (
                "context",
                ScheduleRecord {
                    context: "1".into(),
                    ..row()
                },
            ),
            (
                "cron_timezone",
                ScheduleRecord {
                    cron_timezone: Some("UTC".into()),
                    ..row()
                },
            ),
            (
                "queue_name",
                ScheduleRecord {
                    queue_name: Some("q".into()),
                    ..row()
                },
            ),
        ] {
            assert!(!same_definition(&record, &changed), "{field} was ignored");
            assert!(
                !same_definition(&changed, &record),
                "{field} was ignored the other way"
            );
        }
    }
}
