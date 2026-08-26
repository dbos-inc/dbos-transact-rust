//! Resuming and forking, against real databases.
//!
//! Both put a workflow on a queue and leave it there, so both tests end by waiting on a polling
//! handle — which is the dequeue loop doing the actual work.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{Config, DBOS, EngineOnly, Enqueue, Error, ForkFrom, ForkOptions, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

/// The version an instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// **A resumed workflow is dequeued and run, which is what makes `resume` more than a status
/// change.**
///
/// The workflow starts on a queue nothing polls, so it sits `ENQUEUED` and untouched. Resuming
/// moves it to the internal queue, which this executor does poll — and it runs.
#[tokio::test]
async fn resuming_puts_a_workflow_on_a_queue_that_runs_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("resume-app", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("resumable", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(3)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "stalled";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("a-queue-with-no-row")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a queue with no row was polled"
    );

    let handle = dbos
        .resume::<u32, EngineOnly>(id)
        .await
        .expect("resume failed");
    assert_eq!(handle.workflow_id(), id, "resuming changed the id");
    assert_eq!(handle.result().await.expect("the workflow failed"), 3);
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    dbos.shutdown().await;
}

/// Resuming an id with no row says so, rather than silently doing nothing.
#[tokio::test]
async fn resuming_a_workflow_that_does_not_exist_is_an_error() {
    let db = test_database().await;
    let dbos = DBOS::new(config("resume-missing-app", &db));
    dbos.launch().await.expect("launch failed");

    let error = dbos
        .resume::<u32, EngineOnly>("never-existed")
        .await
        .expect_err("a missing workflow was resumed");
    assert!(
        matches!(&error, Error::SystemDatabase(inner) if format!("{inner}").contains("never-existed")),
        "expected a non-existent-workflow refusal, got {error:?}"
    );

    dbos.shutdown().await;
}

/// **A fork is a new workflow that replays what came before the fork point.**
///
/// The source records two steps and then fails. Forking from the beginning re-runs both; the fork
/// gets its own id, and the source is left where it was.
#[tokio::test]
async fn forking_from_the_beginning_runs_the_workflow_again_under_a_new_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-app", &db));
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("the first attempt fails")]
    struct FirstAttempt;

    let attempts = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("forkable", {
            let attempts = Arc::clone(&attempts);
            move |()| {
                let attempts = Arc::clone(&attempts);
                async move {
                    // Fails the first time, succeeds for the fork.
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(FirstAttempt)?;
                    }
                    Ok::<u32, dbos::Error<FirstAttempt>>(8)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "failed-once";
    let first = workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await;
    assert!(first.is_err(), "the first attempt was supposed to fail");

    let forked = dbos
        .fork::<u32, FirstAttempt>(id, ForkFrom::Beginning)
        .await
        .expect("fork failed");
    assert_ne!(forked.workflow_id(), id, "the fork reused the source's id");
    assert_eq!(forked.result().await.expect("the fork failed"), 8);

    // The source keeps its own outcome.
    assert_eq!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Error,
        "forking changed the source's outcome"
    );

    dbos.shutdown().await;
}

/// A fork can be given its own id, and put on a queue of the caller's choosing.
#[tokio::test]
async fn a_fork_takes_the_id_and_queue_it_is_given() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-options-app", &db));
    let workflow = dbos
        .register_workflow("placed", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "forks",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    let id = "the-source";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the source failed");

    let forked = dbos
        .fork_with::<u32, EngineOnly>(
            id,
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("the-fork"),
                queue: Some("forks"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(forked.workflow_id(), "the-fork");
    assert_eq!(forked.result().await.expect("the fork failed"), 1);

    assert_eq!(
        reader(&db)
            .await
            .get_workflow("the-fork")
            .await
            .expect("read failed")
            .expect("the row is missing")
            .queue_name
            .as_deref(),
        Some("forks"),
        "the fork was not enqueued where it was asked to be"
    );

    dbos.shutdown().await;
}

/// Neither operation is available before launch: both are writes.
#[tokio::test]
async fn resuming_and_forking_need_a_launched_instance() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-unlaunched-app", &db));

    assert!(
        matches!(
            dbos.resume::<u32, EngineOnly>("anything").await,
            Err(Error::NotLaunched { .. })
        ),
        "an unlaunched instance resumed a workflow"
    );
    assert!(
        matches!(
            dbos.fork::<u32, EngineOnly>("anything", ForkFrom::Beginning)
                .await,
            Err(Error::NotLaunched { .. })
        ),
        "an unlaunched instance forked a workflow"
    );
}
