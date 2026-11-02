//! Durable waits against a database whose clock is not this process's.
//!
//! A sleep's wake time and a receive's or a read's deadline are recorded once and replayed by
//! whichever executor recovers the workflow, which may be another host with another clock. So the
//! instant is set on the database's clock, and every wait for it is measured from a reading of
//! that clock. These tests shift the database's clock an hour from this process's and replay a
//! wake time recorded on the database's clock: an executor waiting against its own wall clock
//! would sleep an hour too long, or not at all.

// Runs off tokio too, through `#[dbos_test_support::test]`, so spawning and timers go through
// `dbos::__rt`: a direct tokio call would panic there. `clippy.toml` lists what that rules out.
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::time::{Duration, Instant};

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{GetEventCaller, NewWorkflow, WorkflowIdReusePolicy};
use dbos::{Config, DBOS, EngineOnly, Error, RunOptions};

use dbos_test_support::{TestDatabase, owner_token, skewed_database, step_caller};

/// How long each replayed wait is set to last.
const WAIT: Duration = Duration::from_secs(3);

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: true,
        app_version: Some("1.0.0".to_owned()),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// Re-records the wake time at `step` as [`WAIT`] from the database's now, as an executor on the
/// database's clock would have recorded it.
async fn rerecord_wake_time(db: &TestDatabase, workflow_id: &str, step: i32) {
    let mut conn = db.admin_connection().await;
    let updated = sqlx::query(
        "UPDATE dbos.operation_outputs \
         SET output = ((EXTRACT(epoch FROM clock_timestamp()) * 1000)::bigint + $3)::text \
         WHERE workflow_uuid = $1 AND function_id = $2 AND function_name = 'DBOS.sleep'",
    )
    .bind(workflow_id)
    .bind(step)
    .bind(i64::try_from(WAIT.as_millis()).unwrap())
    .execute(&mut conn)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1, "no wake time at step {step}");
}

/// The step id of the one wake time `workflow_id` recorded.
async fn wake_time_step(db: &TestDatabase, workflow_id: &str) -> i32 {
    let steps = reader(db)
        .await
        .list_workflow_steps(workflow_id, true, None, None, None)
        .await
        .unwrap();
    let sleeps: Vec<i32> = steps
        .iter()
        .filter(|step| step.step_name == "DBOS.sleep")
        .map(|step| step.step_id)
        .collect();
    assert_eq!(sleeps.len(), 1, "{steps:?}");
    sleeps[0]
}

/// Forks `workflow_id` from `from`, replaying everything before it, and waits for the result,
/// asserting it took about [`WAIT`].
async fn replay_waits_on_the_database_clock<R>(dbos: &DBOS, workflow_id: &str, from: i32) -> R
where
    R: serde::de::DeserializeOwned + Send + 'static,
{
    waits_on_the_database_clock(async {
        dbos.fork::<R, EngineOnly>(workflow_id, from)
            .await
            .expect("fork failed")
            .result()
            .await
            .expect("the replay failed")
    })
    .await
}

/// **A replayed sleep ends at its wake time on the database's clock.**
async fn a_replayed_sleep_ends_on_time_off_by(seconds: i64) {
    let Some(db) = skewed_database(seconds).await else {
        return;
    };
    let dbos = DBOS::new(config("clock-sleep-app", &db));
    let workflow = dbos
        .register_workflow("naps", |()| async move {
            dbos::sleep(Duration::from_millis(10)).await?;
            Ok::<u32, Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "clock-sleeper";
    workflow
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");
    let step = wake_time_step(&db, id).await;
    rerecord_wake_time(&db, id, step).await;

    let replayed: u32 = replay_waits_on_the_database_clock(&dbos, id, step + 1).await;
    assert_eq!(replayed, 1);

    dbos.shutdown().await;
}

#[dbos_test_support::test]
async fn a_replayed_sleep_ends_on_time_when_the_database_clock_runs_ahead() {
    a_replayed_sleep_ends_on_time_off_by(3600).await;
}

#[dbos_test_support::test]
async fn a_replayed_sleep_ends_on_time_when_the_database_clock_runs_behind() {
    a_replayed_sleep_ends_on_time_off_by(-3600).await;
}

/// A system database over a migrated database whose clock is `seconds` off this process's, with
/// one workflow, `workflow_id`, owned by an execution; `None` on CockroachDB.
async fn skewed_sysdb(
    seconds: i64,
    workflow_id: &str,
) -> Option<(PostgresSystemDatabase, TestDatabase, String)> {
    let db = skewed_database(seconds).await?;
    dbos::sysdb::migrations::runner::run(&db.pool().await, dbos::sysdb::DEFAULT_SCHEMA, true)
        .await
        .expect("migrating the skewed database failed");
    let sys = reader(&db).await;
    sys.init_workflow(
        &NewWorkflow {
            name: Some("waits"),
            executor_id: Some("local"),
            ..NewWorkflow::new(workflow_id)
        },
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .unwrap();
    let owner = owner_token(&sys, workflow_id).await;
    Some((sys, db, owner))
}

/// Records a deadline at `step`, then re-records it [`WAIT`] from the database's now, as an
/// executor on the database's clock would have recorded it before a recovery.
async fn record_deadline(sys: &PostgresSystemDatabase, db: &TestDatabase, id: &str, owner: &str) {
    sys.record_sleep(
        step_caller(id, DEADLINE_STEP, owner),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    rerecord_wake_time(db, id, DEADLINE_STEP).await;
}

/// Where the tests below record a blocking read's deadline: the step after the read's own, which
/// is where a workflow's receive and read of an event put it.
const DEADLINE_STEP: i32 = 1;

/// Runs `wait`, asserting it took about [`WAIT`], and returns what it answered. A bound well short
/// of an hour keeps a wait timed on this process's clock from hanging the test.
async fn waits_on_the_database_clock<T>(wait: impl Future<Output = T>) -> T {
    let started = Instant::now();
    let answer = dbos::__rt::timeout(Duration::from_secs(30), wait)
        .await
        .expect("the wait went by this process's clock, an hour from the database's");
    let waited = started.elapsed();
    assert!(
        waited >= WAIT - Duration::from_millis(500),
        "the wait ended after {waited:?}, before the deadline on the database's clock"
    );
    answer
}

/// **A receive replaying its deadline times out at it on the database's clock**, neither
/// recording its empty result early nor waiting an hour past it.
async fn a_replayed_recv_times_out_on_time_off_by(seconds: i64) {
    let id = "clock-receiver";
    let Some((sys, db, owner)) = skewed_sysdb(seconds, id).await else {
        return;
    };
    record_deadline(&sys, &db, id, &owner).await;

    let received = waits_on_the_database_clock(sys.recv(
        id,
        0,
        DEADLINE_STEP,
        Some("nothing-sent"),
        Duration::from_millis(10),
        &owner,
    ))
    .await
    .unwrap();
    assert_eq!(received, None);
}

#[dbos_test_support::test]
async fn a_replayed_recv_times_out_on_time_when_the_database_clock_runs_ahead() {
    a_replayed_recv_times_out_on_time_off_by(3600).await;
}

#[dbos_test_support::test]
async fn a_replayed_recv_times_out_on_time_when_the_database_clock_runs_behind() {
    a_replayed_recv_times_out_on_time_off_by(-3600).await;
}

/// **A read of an event replaying its deadline times out at it on the database's clock**, as a
/// receive does.
async fn a_replayed_get_event_times_out_on_time_off_by(seconds: i64) {
    let id = "clock-reader";
    let Some((sys, db, owner)) = skewed_sysdb(seconds, id).await else {
        return;
    };
    record_deadline(&sys, &db, id, &owner).await;

    let read = waits_on_the_database_clock(sys.get_event(
        "nobody",
        "unset",
        Duration::from_millis(10),
        Some(GetEventCaller {
            workflow_id: id,
            step_id: 0,
            timeout_step_id: DEADLINE_STEP,
            owner_xid: &owner,
        }),
    ))
    .await
    .unwrap();
    assert_eq!(read, None);
}

#[dbos_test_support::test]
async fn a_replayed_get_event_times_out_on_time_when_the_database_clock_runs_ahead() {
    a_replayed_get_event_times_out_on_time_off_by(3600).await;
}

#[dbos_test_support::test]
async fn a_replayed_get_event_times_out_on_time_when_the_database_clock_runs_behind() {
    a_replayed_get_event_times_out_on_time_off_by(-3600).await;
}
