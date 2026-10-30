//! Shutting down while workflows are running, against real databases.
//!
//! A shutdown with a workflow-completion timeout stops the dequeue loop and the scheduler, waits up
//! to the timeout for the workflows already running, and cancels what is left. The zero timeout —
//! cancel at once — is covered where the rest of the abandonment tests live, in `workflows` and
//! `recovery`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowRecord, WorkflowStatus};
use dbos::{
    Config, DBOS, Enqueue, Error, QueueConflict, QueueOptions, RunOptions, ShutdownOptions,
    StartOptions,
};

use dbos_test_support::{TestDatabase, test_database};

/// Everything here waits on real databases and background tasks; nothing legitimate takes this
/// long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails — and sharing it is what lets a relaunch recover what the previous launch left.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

/// A handle for reading rows behind the instance's back.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// Polls until the workflow reaches `status`, returning its row.
async fn await_status(
    reader: &PostgresSystemDatabase,
    workflow_id: &str,
    status: WorkflowStatus,
) -> WorkflowRecord {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(workflow_id)
                .await
                .expect("read failed")
                .expect("the row exists");
            if row.status == status {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{workflow_id} never reached {status:?}"))
}

fn draining(timeout: Duration) -> ShutdownOptions {
    ShutdownOptions {
        workflow_completion_timeout: timeout,
    }
}

/// A workflow mid-flight when shutdown begins is let finish: it checkpoints its next step, starts
/// a child, and records its outcome, all before the system database closes.
///
/// The instance stays launched while it waits, and shutdown does not return until the workflow
/// has finished.
#[tokio::test]
async fn a_draining_shutdown_lets_a_running_workflow_finish() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("drain-app", &db));
    let child = dbos
        .register_workflow("child", |n: u32| async move { Ok::<u32, Error>(n * 2) })
        .unwrap();
    let parent = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("parent", move |()| {
            let child = child.clone();
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::step("before", || async { Ok(1u32) }).await?;
                reached.notify_one();
                release.notified().await;
                // After shutdown began: the checkpoint and the child's start both still need the
                // system database, and the child is started here rather than dequeued.
                let after = dbos::step("after", || async { Ok(2u32) }).await?;
                let doubled = child.run(after).await?;
                Ok::<u32, Error>(doubled)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let id = "drained";
    let running = tokio::spawn(async move {
        parent
            .run_with(
                (),
                RunOptions {
                    workflow_id: Some(id),
                    ..RunOptions::default()
                },
            )
            .await
    });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    let started = Instant::now();
    let mut shutdown = tokio::spawn({
        let dbos = dbos.clone();
        async move { dbos.shutdown_with(draining(Duration::from_secs(30))).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut shutdown)
            .await
            .is_err(),
        "shutdown returned while a workflow was still running"
    );
    assert!(
        dbos.is_launched(),
        "the instance stays launched while it waits"
    );

    release_gate.notify_one();
    tokio::time::timeout(DEADLINE, shutdown)
        .await
        .expect("shutdown never returned")
        .expect("shutdown panicked");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "shutdown waited out its timeout rather than returning when the workflow finished"
    );
    assert!(!dbos.is_launched());

    let output = running
        .await
        .expect("the task panicked")
        .expect("the drained workflow failed");
    assert_eq!(output, 4);

    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(row.status, WorkflowStatus::Success);
    assert_eq!(row.output.as_deref(), Some("4"));
    let child_row = reader
        .get_workflow(&format!("{id}-2"))
        .await
        .expect("read failed")
        .expect("the child's row exists");
    assert_eq!(child_row.status, WorkflowStatus::Success);
}

/// Once shutdown has begun, a workflow put on a queue stays there: the dequeue loop has stopped,
/// though the instance is still launched and the enqueue itself succeeds.
///
/// The queue's worker is running before shutdown begins, so both halves of the loop are stopped
/// here: the supervisor, which would spawn a worker, and the worker, which would claim the row.
#[tokio::test]
async fn nothing_is_dequeued_once_shutdown_begins() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());
    let ran = Arc::new(AtomicU32::new(0));

    let db = test_database().await;
    let dbos = DBOS::new(config("drain-queue-app", &db));
    let holder = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("holder", move |()| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>(())
            }
        })
        .unwrap()
    };
    let queued = {
        let ran = Arc::clone(&ran);
        dbos.register_workflow("queued", move |()| {
            let ran = Arc::clone(&ran);
            async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "drain-queue",
        QueueOptions::default(),
        QueueConflict::AlwaysUpdate,
    )
    .await
    .expect("queue registration failed");

    // One run through the queue, so its worker exists by the time shutdown begins.
    queued
        .start_with(
            (),
            StartOptions {
                queue: Some(Enqueue::new("drain-queue")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the warm-up enqueue failed")
        .result()
        .await
        .expect("the warm-up run failed");
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    // Something for the shutdown to wait on, so it is still draining when the enqueue lands.
    let holding = tokio::spawn(async move { holder.run(()).await });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the holder never reached its gate");

    let shutdown = tokio::spawn({
        let dbos = dbos.clone();
        async move { dbos.shutdown_with(draining(Duration::from_secs(30))).await }
    });
    // Long enough for a dequeue already in flight when shutdown began to have finished.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let id = "enqueued-while-draining";
    queued
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("drain-queue")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the enqueue failed");
    // Several of the queue's polling intervals: a dequeue loop still running would have taken it.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(
        row.status,
        WorkflowStatus::Enqueued,
        "dequeued during shutdown"
    );

    release_gate.notify_one();
    tokio::time::timeout(DEADLINE, shutdown)
        .await
        .expect("shutdown never returned")
        .expect("shutdown panicked");
    holding
        .await
        .expect("the task panicked")
        .expect("the holder failed");

    assert_eq!(ran.load(Ordering::SeqCst), 1, "only the warm-up ran");
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(
        row.status,
        WorkflowStatus::Enqueued,
        "dequeued during shutdown"
    );
}

/// A workflow still running at the deadline is cancelled there and left `PENDING`, and a relaunch
/// recovers it from its last checkpoint.
#[tokio::test]
async fn a_workflow_that_outlasts_the_timeout_is_cancelled_and_recovered() {
    let steps_run = Arc::new(AtomicU32::new(0));
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("drain-deadline-app", &db));
    let slow = {
        let steps_run = Arc::clone(&steps_run);
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("slow", move |()| {
            let steps_run = Arc::clone(&steps_run);
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::step("counted", || {
                    let steps_run = Arc::clone(&steps_run);
                    async move {
                        steps_run.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await?;
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>("finished after recovery".to_owned())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let id = "outlasts-the-timeout";
    let running = tokio::spawn(async move {
        slow.run_with(
            (),
            RunOptions {
                workflow_id: Some(id),
                ..RunOptions::default()
            },
        )
        .await
    });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    let timeout = Duration::from_millis(500);
    let started = Instant::now();
    tokio::time::timeout(DEADLINE, dbos.shutdown_with(draining(timeout)))
        .await
        .expect("shutdown never returned");
    assert!(
        started.elapsed() >= timeout,
        "shutdown cancelled the workflow before its deadline"
    );

    let err = running.await.expect("the task panicked").unwrap_err();
    assert!(matches!(err, Error::Interrupted { .. }), "{err}");
    let reader = reader(&db).await;
    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(row.status, WorkflowStatus::Pending, "cancelled, not marked");

    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered workflow never reached its gate again");
    release_gate.notify_one();
    let row = await_status(&reader, id, WorkflowStatus::Success).await;
    assert_eq!(row.output.as_deref(), Some("\"finished after recovery\""));
    assert_eq!(
        steps_run.load(Ordering::SeqCst),
        1,
        "the recovery resumed after the step that had checkpointed"
    );

    dbos.shutdown().await;
}

/// A caller that stops waiting for shutdown does not stop the shutdown: the drain goes on, the
/// workflow finishes, and the instance ends up shut down rather than half-stopped.
#[tokio::test]
async fn dropping_the_shutdown_future_does_not_cut_the_shutdown_short() {
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("drain-dropped-app", &db));
    let held = {
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("held", move |()| {
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                reached.notify_one();
                release.notified().await;
                Ok::<_, Error>(7u32)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    let running = tokio::spawn(async move { held.run(()).await });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");

    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            dbos.shutdown_with(draining(Duration::from_secs(30)))
        )
        .await
        .is_err(),
        "shutdown returned while a workflow was still running"
    );

    release_gate.notify_one();
    let output = running
        .await
        .expect("the task panicked")
        .expect("the workflow was cut short with the caller's wait");
    assert_eq!(output, 7);
    tokio::time::timeout(DEADLINE, async {
        while dbos.is_launched() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the shutdown stopped when its caller did");

    // Shut down for real, so a relaunch works.
    dbos.launch().await.expect("relaunch failed");
    dbos.shutdown().await;
}
