//! Durable sleep, against real databases.

use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::{Config, DBOS};

use dbos_test_support::{TestDatabase, test_database};

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// A sleep waits, and is checkpointed as a step of its own.
#[tokio::test]
async fn a_sleep_waits_and_is_checkpointed() {
    let db = test_database().await;
    let dbos = DBOS::new(config("sleep-app", &db));
    let workflow = dbos
        .register_workflow("naps", |()| async move {
            dbos::sleep(Duration::from_millis(150)).await?;
            Ok::<u32, dbos::Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let before = std::time::Instant::now();
    workflow.run(()).await.expect("the workflow failed");
    assert!(
        before.elapsed() >= Duration::from_millis(140),
        "the sleep did not wait: {:?}",
        before.elapsed()
    );

    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let steps = reader
        .list_workflow_steps(&rows[0].workflow_id, true, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "the sleep is a step");
    assert_eq!(steps[0].step_name, "DBOS.sleep");

    dbos.shutdown().await;
}

/// The point of the feature: a replay resumes at the *original* wake time, not a fresh one.
#[tokio::test]
async fn a_replayed_sleep_does_not_start_its_clock_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("replay-sleep-app", &db));
    // Long enough that restarting the clock would be unmistakable.
    let workflow = dbos
        .register_workflow("naps", |()| async move {
            dbos::sleep(Duration::from_secs(3)).await?;
            Ok::<u32, dbos::Error>(1)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "sleeps-once";
    let options = dbos::StartOptions {
        workflow_id: Some(id),
        ..Default::default()
    };
    // First run: records the wake time three seconds out, and waits it.
    workflow
        .run_with((), options.clone())
        .await
        .expect("the workflow failed");

    // A second execution under the same id replays the recorded wake time, which is now in the
    // past — so it must not wait another three seconds.
    let before = std::time::Instant::now();
    workflow
        .run_with((), options)
        .await
        .expect("the replay failed");
    assert!(
        before.elapsed() < Duration::from_secs(1),
        "the replay restarted the clock: {:?}",
        before.elapsed()
    );

    dbos.shutdown().await;
}

/// Outside a workflow it is a plain sleep, so a function built from steps and sleeps stays callable.
#[tokio::test]
async fn a_sleep_outside_a_workflow_waits_plainly() {
    let before = std::time::Instant::now();
    dbos::sleep::<dbos::Error>(Duration::from_millis(50))
        .await
        .expect("the sleep failed");
    assert!(before.elapsed() >= Duration::from_millis(45));
}
