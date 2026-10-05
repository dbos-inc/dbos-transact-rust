//! Schedules, against real databases: managing them, and the scheduler firing them.
//!
//! Ported from Python's `tests/test_scheduler.py`. The firing tests use the six-field every-second
//! expression and rely on the scheduler polling every second for the first minute after launch,
//! as Python's do, so a schedule created just after `launch` starts within a second or two.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{
    Change, ScheduleFilter, ScheduleStatus, Timestamp, WorkflowFilter, WorkflowStatus,
};
use dbos::{
    Client, ClientConfig, Config, DBOS, EngineOnly, Error, QueueConflict, QueueOptions,
    ScheduleChange, ScheduleSpec, ScheduledWorkflowInput, WorkflowKey, WorkflowRef,
};

use dbos_test_support::{TestDatabase, test_database};

/// Every second: the firing tests' expression.
const EVERY_SECOND: &str = "* * * * * *";

/// Daily at a time that is not soon, so a schedule created with it never fires during a test.
///
/// Python's `daily_cron_far_from_now`: twelve hours from now, on the hour.
fn daily_far_from_now() -> String {
    let hour = (jiff::Timestamp::now().as_second() / 3600 + 12) % 24;
    format!("0 0 {hour} * * *")
}

/// Derived from the application name, as `management.rs` does, so two instances sharing a
/// database do not collide on a version row.
fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(format!("{app_name}-1.0.0")),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

async fn client(app_name: Option<&str>, db: &TestDatabase) -> Client {
    Client::connect(ClientConfig {
        app_name: app_name.map(str::to_owned),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed")
}

/// What a recording workflow saw: each run's context and scheduled time, by workflow id.
type Seen = Arc<Mutex<Vec<(String, String, SystemTime)>>>;

/// Registers a scheduled workflow that records what it was called with.
fn recorder(dbos: &DBOS, name: &str) -> (WorkflowRef<ScheduledWorkflowInput<String>, ()>, Seen) {
    let seen: Seen = Arc::default();
    let workflow = dbos
        .register_workflow(name, {
            let seen = Arc::clone(&seen);
            move |input: ScheduledWorkflowInput<String>| {
                let seen = Arc::clone(&seen);
                async move {
                    let id = dbos::workflow_id().unwrap_or_default();
                    seen.lock()
                        .unwrap()
                        .push((id, input.context, input.scheduled_time));
                    Ok::<(), Error>(())
                }
            }
        })
        .expect("registration failed");
    (workflow, seen)
}

/// Waits up to fifteen seconds for `condition`, and fails the test if it never holds.
async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..150 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn contexts(seen: &Seen) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|(_, context, _)| context.clone())
        .collect()
}

fn at(rfc3339: &str) -> SystemTime {
    SystemTime::from(rfc3339.parse::<jiff::Timestamp>().unwrap())
}

#[tokio::test]
async fn schedules_are_created_read_listed_and_deleted() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-crud-app", &db));
    let (workflow, _) = recorder(&dbos, "crud-workflow");
    let (other, _) = recorder(&dbos, "other-workflow");
    dbos.launch().await.expect("launch failed");

    let mut spec = ScheduleSpec::new(
        "crud-schedule",
        &workflow,
        daily_far_from_now(),
        &"ctx".to_owned(),
    )
    .unwrap();
    spec.cron_timezone = Some("America/New_York".to_owned());
    dbos.create_schedule(&spec).await.expect("create failed");

    let record = dbos
        .get_schedule("crud-schedule")
        .await
        .unwrap()
        .expect("the schedule is missing");
    assert_eq!(record.workflow_name, "crud-workflow");
    assert_eq!(record.workflow_class_name, None);
    assert_eq!(record.schedule, spec.schedule);
    assert_eq!(record.status, ScheduleStatus::Active);
    assert_eq!(record.decode_context::<String>().unwrap(), "ctx");
    assert_eq!(record.cron_timezone.as_deref(), Some("America/New_York"));
    assert_eq!(record.last_fired_at, None);
    assert!(!record.automatic_backfill);
    assert_eq!(record.queue_name, None);
    assert_eq!(record.application_name.as_deref(), Some("sched-crud-app"));

    assert!(
        dbos.get_schedule("no-such-schedule")
            .await
            .unwrap()
            .is_none()
    );

    // A duplicate name, a bad expression, a bad timezone and an unregistered workflow are each
    // refused, and none of them writes a row.
    let duplicate = dbos.create_schedule(&spec).await.unwrap_err();
    assert!(
        matches!(
            duplicate,
            Error::SystemDatabase(dbos::sysdb::Error::AlreadyRegistered { .. })
        ),
        "{duplicate}"
    );
    let bad_cron = ScheduleSpec::new("bad-cron", &workflow, "not a cron", &String::new()).unwrap();
    let error = dbos.create_schedule(&bad_cron).await.unwrap_err();
    assert!(
        error.to_string().contains("invalid cron schedule"),
        "{error}"
    );
    let mut bad_zone =
        ScheduleSpec::new("bad-zone", &workflow, EVERY_SECOND, &String::new()).unwrap();
    bad_zone.cron_timezone = Some("Not/A_Zone".to_owned());
    let error = dbos.create_schedule(&bad_zone).await.unwrap_err();
    assert!(error.to_string().contains("invalid timezone"), "{error}");
    let unregistered = ScheduleSpec::for_workflow(
        "unregistered",
        WorkflowKey::new("nobody-registered-this"),
        EVERY_SECOND,
        &(),
    )
    .unwrap();
    let error = dbos.create_schedule(&unregistered).await.unwrap_err();
    assert!(
        error.to_string().contains("no workflow is registered"),
        "{error}"
    );

    // Filters, alone and together.
    let other_spec = ScheduleSpec::new(
        "other-schedule",
        &other,
        daily_far_from_now(),
        &String::new(),
    )
    .unwrap();
    dbos.create_schedule(&other_spec).await.unwrap();
    dbos.pause_schedule("other-schedule").await.unwrap();
    let names = |records: Vec<dbos::sysdb::types::ScheduleRecord>| -> Vec<String> {
        records.into_iter().map(|r| r.schedule_name).collect()
    };
    assert_eq!(
        names(
            dbos.list_schedules(&ScheduleFilter::default())
                .await
                .unwrap()
        ),
        ["crud-schedule", "other-schedule"]
    );
    assert_eq!(
        names(
            dbos.list_schedules(&ScheduleFilter {
                statuses: vec![ScheduleStatus::Paused],
                ..ScheduleFilter::default()
            })
            .await
            .unwrap()
        ),
        ["other-schedule"]
    );
    assert_eq!(
        names(
            dbos.list_schedules(&ScheduleFilter {
                workflow_names: vec!["crud-workflow"],
                ..ScheduleFilter::default()
            })
            .await
            .unwrap()
        ),
        ["crud-schedule"]
    );
    assert_eq!(
        names(
            dbos.list_schedules(&ScheduleFilter {
                schedule_name_prefixes: vec!["crud"],
                statuses: vec![ScheduleStatus::Paused],
                ..ScheduleFilter::default()
            })
            .await
            .unwrap()
        ),
        Vec::<String>::new()
    );

    dbos.delete_schedule("crud-schedule").await.unwrap();
    assert!(dbos.get_schedule("crud-schedule").await.unwrap().is_none());
    // Deleting what is not there is not an error.
    dbos.delete_schedule("crud-schedule").await.unwrap();

    dbos.shutdown().await;
}

#[tokio::test]
async fn pausing_and_resuming_toggle_the_status_and_name_a_missing_schedule() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-pause-app", &db));
    let (workflow, _) = recorder(&dbos, "pause-workflow");
    dbos.launch().await.expect("launch failed");

    let spec =
        ScheduleSpec::new("pausable", &workflow, daily_far_from_now(), &String::new()).unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    let status = || async { dbos.get_schedule("pausable").await.unwrap().unwrap().status };

    dbos.pause_schedule("pausable").await.unwrap();
    assert_eq!(status().await, ScheduleStatus::Paused);
    dbos.resume_schedule("pausable").await.unwrap();
    assert_eq!(status().await, ScheduleStatus::Active);

    let error = dbos.pause_schedule("missing").await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::SystemDatabase(dbos::sysdb::Error::NotRegistered { .. })
        ),
        "{error}"
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn updating_a_schedule_changes_only_what_it_names() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-update-app", &db));
    let (workflow, _) = recorder(&dbos, "update-workflow");
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "update-queue",
        QueueOptions::default(),
        QueueConflict::AlwaysUpdate,
    )
    .await
    .unwrap();

    let spec = ScheduleSpec::new(
        "updatable",
        &workflow,
        daily_far_from_now(),
        &"v1".to_owned(),
    )
    .unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    let before = dbos.get_schedule("updatable").await.unwrap().unwrap();
    dbos.pause_schedule("updatable").await.unwrap();

    let change = ScheduleChange {
        schedule: Change::Set("0 30 1 * * *"),
        context: Change::Set(serde_json::json!("v2")),
        cron_timezone: Change::Set(Some("Asia/Tokyo")),
        queue_name: Change::Set(Some("update-queue")),
        ..ScheduleChange::default()
    };
    dbos.update_schedule("updatable", &change).await.unwrap();
    let after = dbos.get_schedule("updatable").await.unwrap().unwrap();
    assert_eq!(after.schedule, "0 30 1 * * *");
    assert_eq!(after.decode_context::<String>().unwrap(), "v2");
    assert_eq!(after.cron_timezone.as_deref(), Some("Asia/Tokyo"));
    assert_eq!(after.queue_name.as_deref(), Some("update-queue"));
    // What the change did not name, and what is not a change's to make.
    assert_eq!(after.schedule_id, before.schedule_id);
    assert_eq!(after.workflow_name, before.workflow_name);
    assert_eq!(after.status, ScheduleStatus::Paused);
    assert!(!after.automatic_backfill);

    let error = dbos
        .update_schedule(
            "updatable",
            &ScheduleChange {
                schedule: Change::Set("nonsense"),
                ..ScheduleChange::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("invalid cron schedule"),
        "{error}"
    );

    // A queue nothing has registered is refused, as it is on create: its ticks would never run.
    let error = dbos
        .update_schedule(
            "updatable",
            &ScheduleChange {
                queue_name: Change::Set(Some("never-registered")),
                ..ScheduleChange::default()
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("is not registered"), "{error}");
    assert_eq!(
        dbos.get_schedule("updatable")
            .await
            .unwrap()
            .unwrap()
            .queue_name
            .as_deref(),
        Some("update-queue")
    );

    let error = dbos
        .update_schedule("missing", &ScheduleChange::default())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::SystemDatabase(dbos::sysdb::Error::NotRegistered { .. })
        ),
        "{error}"
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn applying_schedules_creates_and_replaces_and_keeps_runtime_state() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-apply-app", &db));
    let (workflow, _) = recorder(&dbos, "apply-workflow");
    dbos.launch().await.expect("launch failed");

    let far = daily_far_from_now();
    let first = ScheduleSpec::new("apply-a", &workflow, &far, &"a1".to_owned()).unwrap();
    let second = ScheduleSpec::new("apply-b", &workflow, &far, &"b1".to_owned()).unwrap();
    dbos.apply_schedules(&[first, second]).await.unwrap();
    let a = dbos.get_schedule("apply-a").await.unwrap().unwrap();

    dbos.pause_schedule("apply-a").await.unwrap();
    reader(&db)
        .await
        .update_schedule_last_fired_at("apply-a", Timestamp::from_epoch_ms(1_000))
        .await
        .unwrap();

    let mut replaced = ScheduleSpec::new("apply-a", &workflow, &far, &"a2".to_owned()).unwrap();
    replaced.automatic_backfill = true;
    replaced.cron_timezone = Some("Europe/Paris".to_owned());
    let added = ScheduleSpec::new("apply-c", &workflow, &far, &"c1".to_owned()).unwrap();
    dbos.apply_schedules(&[replaced, added]).await.unwrap();

    let a2 = dbos.get_schedule("apply-a").await.unwrap().unwrap();
    assert_eq!(a2.decode_context::<String>().unwrap(), "a2");
    assert!(a2.automatic_backfill);
    assert_eq!(a2.cron_timezone.as_deref(), Some("Europe/Paris"));
    assert_eq!(a2.schedule_id, a.schedule_id, "re-applying keeps the id");
    assert_eq!(a2.status, ScheduleStatus::Paused, "and the status");
    assert_eq!(
        a2.last_fired_at,
        Some(Timestamp::from_epoch_ms(1_000)),
        "and the last tick"
    );
    assert!(dbos.get_schedule("apply-c").await.unwrap().is_some());

    // One bad spec refuses the batch before anything is written.
    let good = ScheduleSpec::new("apply-d", &workflow, &far, &String::new()).unwrap();
    let bad = ScheduleSpec::new("apply-e", &workflow, "bad", &String::new()).unwrap();
    let error = dbos.apply_schedules(&[good, bad]).await.unwrap_err();
    assert!(
        error.to_string().contains("invalid cron schedule"),
        "{error}"
    );
    assert!(dbos.get_schedule("apply-d").await.unwrap().is_none());

    dbos.shutdown().await;
}

#[tokio::test]
async fn schedule_calls_from_inside_a_workflow_are_steps_and_batches_are_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-steps-app", &db));
    let (target, _) = recorder(&dbos, "step-target");
    let spec = ScheduleSpec::new(
        "from-workflow",
        &target,
        daily_far_from_now(),
        &String::new(),
    )
    .unwrap();
    let operator = dbos
        .register_workflow("schedule-operator", {
            let dbos = dbos.clone();
            let spec = spec.clone();
            move |()| {
                let dbos = dbos.clone();
                let spec = spec.clone();
                async move {
                    dbos.create_schedule(&spec).await?;
                    let listed = dbos.list_schedules(&ScheduleFilter::default()).await?;
                    let found = dbos.get_schedule("from-workflow").await?;
                    dbos.pause_schedule("from-workflow").await?;
                    dbos.resume_schedule("from-workflow").await?;
                    dbos.update_schedule("from-workflow", &ScheduleChange::default())
                        .await?;
                    dbos.delete_schedule("from-workflow").await?;
                    let gone = dbos.get_schedule("from-workflow").await?;

                    // The three batch writes are refused rather than recorded.
                    let refused = [
                        dbos.apply_schedules(std::slice::from_ref(&spec))
                            .await
                            .is_err_and(|e| matches!(e, Error::InsideWorkflow { .. })),
                        dbos.trigger_schedule::<(), EngineOnly>("from-workflow")
                            .await
                            .is_err_and(|e| matches!(e, Error::InsideWorkflow { .. })),
                        dbos.backfill_schedule::<(), EngineOnly>(
                            "from-workflow",
                            SystemTime::UNIX_EPOCH,
                            SystemTime::UNIX_EPOCH,
                        )
                        .await
                        .is_err_and(|e| matches!(e, Error::InsideWorkflow { .. })),
                    ];
                    Ok::<_, Error>((listed.len(), found.is_some(), gone.is_none(), refused))
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let (listed, found, gone, refused) = operator
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some("schedule-operator-run"),
                ..Default::default()
            },
        )
        .await
        .expect("the operator failed");
    assert_eq!((listed, found, gone), (1, true, true));
    assert_eq!(refused, [true, true, true]);

    let steps: Vec<String> = reader(&db)
        .await
        .list_workflow_steps("schedule-operator-run", true, None, None, None)
        .await
        .unwrap()
        .into_iter()
        .map(|step| step.step_name)
        .collect();
    assert_eq!(
        steps,
        [
            "DBOS.createSchedule",
            "DBOS.listSchedules",
            "DBOS.getSchedule",
            "DBOS.pauseSchedule",
            "DBOS.resumeSchedule",
            "DBOS.updateSchedule",
            "DBOS.deleteSchedule",
            "DBOS.getSchedule",
        ]
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_schedule_fires_on_its_timezone_and_records_each_tick() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-fire-app", &db));
    let (workflow, seen) = recorder(&dbos, "fire-workflow");
    dbos.launch().await.expect("launch failed");

    let utc = ScheduleSpec::new("fire-utc", &workflow, EVERY_SECOND, &"utc".to_owned()).unwrap();
    let mut ny = ScheduleSpec::new("fire-ny", &workflow, EVERY_SECOND, &"ny".to_owned()).unwrap();
    ny.cron_timezone = Some("America/New_York".to_owned());
    dbos.create_schedule(&utc).await.unwrap();
    dbos.create_schedule(&ny).await.unwrap();

    eventually("both schedules to fire twice", || {
        let seen = contexts(&seen);
        seen.iter().filter(|c| *c == "utc").count() >= 2
            && seen.iter().filter(|c| *c == "ny").count() >= 2
    })
    .await;

    // Each run's id is the schedule and its tick, RFC 3339 to the second on the schedule's own
    // wall clock, and the tick is what the workflow was called with.
    for (id, context, scheduled_time) in seen.lock().unwrap().clone() {
        let zone = if context == "utc" {
            jiff::tz::TimeZone::UTC
        } else {
            jiff::tz::TimeZone::get("America/New_York").unwrap()
        };
        let tick = jiff::Timestamp::try_from(scheduled_time)
            .unwrap()
            .to_zoned(zone);
        let mut expected = tick.strftime("%Y-%m-%dT%H:%M:%S%:z").to_string();
        if let Some(local) = expected.strip_suffix("+00:00") {
            expected = format!("{local}Z");
        }
        assert_eq!(id, format!("sched-fire-{context}-{expected}"));
        assert_eq!(
            tick.timestamp().subsec_nanosecond(),
            0,
            "ticks are whole seconds"
        );
    }

    let reader = reader(&db).await;
    let runs = reader
        .list_workflows(
            &WorkflowFilter {
                schedule_names: vec!["fire-utc"],
                ..WorkflowFilter::default()
            },
            None,
        )
        .await
        .unwrap();
    assert!(runs.len() >= 2);
    for run in &runs {
        assert_eq!(run.schedule_name.as_deref(), Some("fire-utc"));
        assert_eq!(run.application_name.as_deref(), Some("sched-fire-app"));
        assert_eq!(
            run.application_version.as_deref(),
            Some("sched-fire-app-1.0.0")
        );
    }
    let record = dbos.get_schedule("fire-utc").await.unwrap().unwrap();
    assert!(record.last_fired_at.is_some(), "the loop records each tick");

    dbos.shutdown().await;
}

#[tokio::test]
async fn pausing_or_deleting_a_schedule_stops_it_and_resuming_restarts_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-stop-app", &db));
    let (workflow, seen) = recorder(&dbos, "stop-workflow");
    dbos.launch().await.expect("launch failed");

    let spec = ScheduleSpec::new("stoppable", &workflow, EVERY_SECOND, &"x".to_owned()).unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    eventually("the first runs", || contexts(&seen).len() >= 2).await;

    dbos.pause_schedule("stoppable").await.unwrap();
    // A poll to notice, and a tick already in flight to land.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let paused_at = contexts(&seen).len();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(contexts(&seen).len(), paused_at, "a paused schedule fired");

    dbos.resume_schedule("stoppable").await.unwrap();
    eventually("runs after resuming", || {
        contexts(&seen).len() >= paused_at + 2
    })
    .await;

    dbos.delete_schedule("stoppable").await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let deleted_at = contexts(&seen).len();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        contexts(&seen).len(),
        deleted_at,
        "a deleted schedule fired"
    );

    dbos.shutdown().await;
}

#[tokio::test]
async fn changing_a_schedules_definition_restarts_it_with_the_new_one() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-restart-app", &db));
    let (workflow, seen) = recorder(&dbos, "restart-workflow");
    dbos.launch().await.expect("launch failed");

    // Starts on an expression that does not fire during the test.
    let spec = ScheduleSpec::new(
        "restartable",
        &workflow,
        daily_far_from_now(),
        &"v1".to_owned(),
    )
    .unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(contexts(&seen).is_empty());

    dbos.update_schedule(
        "restartable",
        &ScheduleChange {
            schedule: Change::Set(EVERY_SECOND),
            context: Change::Set(serde_json::json!("v2")),
            ..ScheduleChange::default()
        },
    )
    .await
    .unwrap();
    eventually("runs with the new context", || {
        contexts(&seen).iter().filter(|c| *c == "v2").count() >= 2
    })
    .await;
    assert!(!contexts(&seen).contains(&"v1".to_owned()));

    dbos.shutdown().await;
}

#[tokio::test]
async fn a_backfill_enqueues_each_tick_once_on_the_schedules_wall_clock() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-backfill-app", &db));
    let (workflow, seen) = recorder(&dbos, "backfill-workflow");
    dbos.launch().await.expect("launch failed");

    let hourly = ScheduleSpec::new("hourly", &workflow, "0 0 * * * *", &"h".to_owned()).unwrap();
    dbos.create_schedule(&hourly).await.unwrap();

    let start = at("2025-01-01T00:30:00Z");
    let end = at("2025-01-01T03:30:00Z");
    let handles = dbos
        .backfill_schedule::<(), EngineOnly>("hourly", start, end)
        .await
        .unwrap();
    let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
    assert_eq!(
        ids,
        [
            "sched-hourly-2025-01-01T01:00:00Z",
            "sched-hourly-2025-01-01T02:00:00Z",
            "sched-hourly-2025-01-01T03:00:00Z",
        ]
    );
    for handle in handles {
        handle.result().await.expect("a backfilled run failed");
    }

    // Again over the same window: the same handles, and no new runs.
    let again = dbos
        .backfill_schedule::<(), EngineOnly>("hourly", start, end)
        .await
        .unwrap();
    assert_eq!(again.len(), 3);
    let runs = reader(&db)
        .await
        .list_workflows(
            &WorkflowFilter {
                schedule_names: vec!["hourly"],
                ..WorkflowFilter::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(runs.len(), 3, "a backfill ran a tick twice");
    let mut times: Vec<SystemTime> = seen.lock().unwrap().iter().map(|s| s.2).collect();
    times.sort();
    assert_eq!(
        times,
        [
            at("2025-01-01T01:00:00Z"),
            at("2025-01-01T02:00:00Z"),
            at("2025-01-01T03:00:00Z")
        ]
    );

    // Midnight in New York, in winter, is five in the morning UTC.
    let mut ny =
        ScheduleSpec::new("ny-midnight", &workflow, "0 0 * * *", &"ny".to_owned()).unwrap();
    ny.cron_timezone = Some("America/New_York".to_owned());
    dbos.create_schedule(&ny).await.unwrap();
    let handles = dbos
        .backfill_schedule::<(), EngineOnly>(
            "ny-midnight",
            at("2024-12-31T23:00:00Z"),
            at("2025-01-03T00:00:00Z"),
        )
        .await
        .unwrap();
    let ids: Vec<&str> = handles.iter().map(|h| h.workflow_id()).collect();
    assert_eq!(
        ids,
        [
            "sched-ny-midnight-2025-01-01T00:00:00-05:00",
            "sched-ny-midnight-2025-01-02T00:00:00-05:00",
        ]
    );

    let error = dbos
        .backfill_schedule::<(), EngineOnly>("missing", start, end)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::SystemDatabase(dbos::sysdb::Error::NotRegistered { .. })
        ),
        "{error}"
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_trigger_runs_the_workflow_now() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-trigger-app", &db));
    let (workflow, seen) = recorder(&dbos, "trigger-workflow");
    dbos.launch().await.expect("launch failed");

    let spec = ScheduleSpec::new(
        "triggered",
        &workflow,
        daily_far_from_now(),
        &"t".to_owned(),
    )
    .unwrap();
    dbos.create_schedule(&spec).await.unwrap();

    let before = SystemTime::now();
    let handle = dbos
        .trigger_schedule::<(), EngineOnly>("triggered")
        .await
        .unwrap();
    let after = SystemTime::now();
    assert!(handle.workflow_id().starts_with("sched-triggered-trigger-"));
    handle.result().await.expect("the triggered run failed");

    let (_, context, scheduled_time) = seen.lock().unwrap()[0].clone();
    assert_eq!(context, "t");
    assert!(before <= scheduled_time && scheduled_time <= after);

    let error = dbos
        .trigger_schedule::<(), EngineOnly>("missing")
        .await
        .unwrap_err();
    assert!(matches!(error, Error::SystemDatabase(_)), "{error}");
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_restart_catches_up_on_missed_ticks_when_asked() {
    let db = test_database().await;

    // Created from a client before the application launches, last fired three and a half hours
    // ago.
    let client = client(Some("sched-catchup-app"), &db).await;
    let mut spec = ScheduleSpec::for_workflow(
        "catch-up",
        WorkflowKey::new("catchup-workflow"),
        "0 0 * * * *",
        &"c".to_owned(),
    )
    .unwrap();
    spec.automatic_backfill = true;
    client.create_schedule(&spec).await.unwrap();
    let three_and_a_half_hours_ago = SystemTime::now() - Duration::from_secs(3 * 3600 + 1800);
    reader(&db)
        .await
        .update_schedule_last_fired_at(
            "catch-up",
            Timestamp::from_system_time(three_and_a_half_hours_ago).unwrap(),
        )
        .await
        .unwrap();

    let dbos = DBOS::new(config("sched-catchup-app", &db));
    let (_, seen) = recorder(&dbos, "catchup-workflow");
    dbos.launch().await.expect("launch failed");

    eventually("the missed ticks to run", || contexts(&seen).len() >= 3).await;
    let now = SystemTime::now();
    for (id, _, scheduled_time) in seen.lock().unwrap().iter() {
        assert!(*scheduled_time > three_and_a_half_hours_ago && *scheduled_time <= now);
        let tick = jiff::Timestamp::try_from(*scheduled_time).unwrap();
        assert_eq!(tick.as_second() % 3600, 0, "{id} is not on the hour");
    }
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_schedule_on_a_named_queue_needs_the_queue_and_runs_on_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-queue-app", &db));
    let (workflow, _) = recorder(&dbos, "queued-workflow");
    dbos.launch().await.expect("launch failed");

    let mut spec =
        ScheduleSpec::new("queued", &workflow, daily_far_from_now(), &String::new()).unwrap();
    spec.queue_name = Some("schedule-queue".to_owned());
    let error = dbos.create_schedule(&spec).await.unwrap_err();
    assert!(error.to_string().contains("is not registered"), "{error}");

    dbos.register_queue(
        "schedule-queue",
        QueueOptions::default(),
        QueueConflict::AlwaysUpdate,
    )
    .await
    .unwrap();
    dbos.create_schedule(&spec).await.unwrap();

    let handle = dbos
        .trigger_schedule::<(), EngineOnly>("queued")
        .await
        .unwrap();
    let workflow_id = handle.workflow_id().to_owned();
    handle.result().await.expect("the run failed");
    let row = reader(&db)
        .await
        .get_workflow(&workflow_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.queue_name.as_deref(), Some("schedule-queue"));
    assert_eq!(row.status, WorkflowStatus::Success);
    dbos.shutdown().await;
}

#[tokio::test]
async fn an_application_fires_its_own_schedules_and_unclaimed_ones_only() {
    let db = test_database().await;

    // Another application's schedule, and an unclaimed one, both on the same workflow name.
    let peer = client(Some("sched-peer-app"), &db).await;
    let nameless = client(None, &db).await;
    let theirs = ScheduleSpec::for_workflow(
        "theirs",
        WorkflowKey::new("scoped-workflow"),
        EVERY_SECOND,
        &"theirs".to_owned(),
    )
    .unwrap();
    let unclaimed = ScheduleSpec::for_workflow(
        "unclaimed",
        WorkflowKey::new("scoped-workflow"),
        EVERY_SECOND,
        &"unclaimed".to_owned(),
    )
    .unwrap();
    peer.create_schedule(&theirs).await.unwrap();
    nameless.create_schedule(&unclaimed).await.unwrap();

    let dbos = DBOS::new(config("sched-scope-app", &db));
    let (workflow, seen) = recorder(&dbos, "scoped-workflow");
    dbos.launch().await.expect("launch failed");
    let ours = ScheduleSpec::new("ours", &workflow, EVERY_SECOND, &"ours".to_owned()).unwrap();
    dbos.create_schedule(&ours).await.unwrap();

    eventually("our schedule and the unclaimed one to fire", || {
        let seen = contexts(&seen);
        seen.contains(&"ours".to_owned()) && seen.contains(&"unclaimed".to_owned())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !contexts(&seen).contains(&"theirs".to_owned()),
        "fired another application's schedule"
    );

    // The unclaimed schedule's runs are this application's, as the executor that fired them.
    let counts: HashMap<Option<String>, usize> = reader(&db)
        .await
        .list_workflows(
            &WorkflowFilter {
                schedule_names: vec!["unclaimed"],
                ..WorkflowFilter::default()
            },
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .fold(HashMap::new(), |mut counts, run| {
            *counts.entry(run.application_name).or_default() += 1;
            counts
        });
    assert_eq!(
        counts.keys().collect::<Vec<_>>(),
        [&Some("sched-scope-app".to_owned())]
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_run_is_stamped_with_the_owners_latest_version() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-version-app", &db));
    let (workflow, _) = recorder(&dbos, "version-workflow");
    dbos.launch().await.expect("launch failed");

    let spec =
        ScheduleSpec::new("versioned", &workflow, daily_far_from_now(), &String::new()).unwrap();
    dbos.create_schedule(&spec).await.unwrap();
    // A newer deployment of the same application has registered since this one launched.
    let reader = reader(&db).await;
    reader
        .create_application_version("sched-version-app-2.0.0", Some("sched-version-app"))
        .await
        .unwrap();

    let handle = dbos
        .trigger_schedule::<(), EngineOnly>("versioned")
        .await
        .unwrap();
    let row = reader
        .get_workflow(handle.workflow_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.application_version.as_deref(),
        Some("sched-version-app-2.0.0")
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_client_manages_backfills_and_triggers_schedules() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sched-client-app", &db));
    let (_, seen) = recorder(&dbos, "client-workflow");
    dbos.launch().await.expect("launch failed");

    let client = client(Some("sched-client-app"), &db).await;
    let spec = ScheduleSpec::for_workflow(
        "client-schedule",
        WorkflowKey::new("client-workflow"),
        "0 0 * * * *",
        &"from-client".to_owned(),
    )
    .unwrap();
    client.create_schedule(&spec).await.unwrap();
    let mut bad = spec.clone();
    bad.schedule = "bad".to_owned();
    let error = client.create_schedule(&bad).await.unwrap_err();
    assert!(
        error.to_string().contains("invalid cron schedule"),
        "{error}"
    );

    let record = client
        .get_schedule("client-schedule")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.application_name.as_deref(), Some("sched-client-app"));
    assert_eq!(
        client
            .list_schedules(&ScheduleFilter::default())
            .await
            .unwrap()
            .len(),
        1
    );

    client.pause_schedule("client-schedule").await.unwrap();
    assert_eq!(
        client
            .get_schedule("client-schedule")
            .await
            .unwrap()
            .unwrap()
            .status,
        ScheduleStatus::Paused
    );
    client.resume_schedule("client-schedule").await.unwrap();

    let handles = client
        .backfill_schedule::<(), EngineOnly>(
            "client-schedule",
            at("2025-01-01T00:30:00Z"),
            at("2025-01-01T03:30:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(handles.len(), 3);
    for handle in handles {
        handle.result().await.expect("a backfilled run failed");
    }
    let triggered = client
        .trigger_schedule::<(), EngineOnly>("client-schedule")
        .await
        .unwrap();
    assert!(
        triggered
            .workflow_id()
            .starts_with("sched-client-schedule-trigger-")
    );
    triggered.result().await.expect("the triggered run failed");
    assert_eq!(contexts(&seen), ["from-client"; 4]);

    client.delete_schedule("client-schedule").await.unwrap();
    assert!(
        client
            .get_schedule("client-schedule")
            .await
            .unwrap()
            .is_none()
    );
    dbos.shutdown().await;
}

#[tokio::test]
async fn a_schedule_no_executor_can_fire_does_not_stop_the_others() {
    let db = test_database().await;

    // Stored without the checks `create_schedule` makes, as a peer SDK with a laxer parser might.
    reader(&db)
        .await
        .create_schedule(
            &dbos::sysdb::types::NewSchedule::new("unparseable", "healthy-workflow", "0 0 L-1 * *"),
            None,
        )
        .await
        .unwrap();

    // And one whose context is not JSON — another SDK's encoding — which no attempt will read.
    reader(&db)
        .await
        .create_schedule(
            &dbos::sysdb::types::NewSchedule {
                context: "gASVBQAAAAAAAACMAW+ULg==",
                ..dbos::sysdb::types::NewSchedule::new(
                    "undecodable",
                    "healthy-workflow",
                    EVERY_SECOND,
                )
            },
            None,
        )
        .await
        .unwrap();

    let dbos = DBOS::new(config("sched-unfireable-app", &db));
    let (workflow, seen) = recorder(&dbos, "healthy-workflow");
    dbos.launch().await.expect("launch failed");

    // Several polls over the unfireable schedule, so the reconciler has to look at its ended task
    // more than once.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let healthy = ScheduleSpec::new("healthy", &workflow, EVERY_SECOND, &"ok".to_owned()).unwrap();
    dbos.create_schedule(&healthy).await.unwrap();
    eventually("a schedule created afterwards to fire", || {
        contexts(&seen).len() >= 2
    })
    .await;
    let undecodable = reader(&db)
        .await
        .list_workflows(
            &WorkflowFilter {
                schedule_names: vec!["undecodable"],
                ..WorkflowFilter::default()
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        undecodable.is_empty(),
        "a run was enqueued for an undecodable context"
    );
    dbos.shutdown().await;
}
