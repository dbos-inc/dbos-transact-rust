//! Step timeouts: what happens when a body hangs, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::{Config, DBOS, Error, StepOptions};

use dbos_test_support::{TestDatabase, test_database};

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

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// A body that outlives its timeout fails the step, and the failure is recorded.
#[tokio::test]
async fn a_step_that_hangs_is_stopped_at_its_timeout() {
    let db = test_database().await;
    let dbos = DBOS::new(config("timeout-app", &db));
    let workflow = dbos
        .register_workflow("hangs", |()| async move {
            let options = StepOptions {
                timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            };
            dbos::step_with("sleeps", options, || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<u32, dbos::Error>(1)
            })
            .await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let error = workflow.run(()).await.expect_err("the workflow succeeded");
    let Error::StepTimeout { step, timeout } = &error else {
        panic!("expected StepTimeout, got {error:?}");
    };
    assert_eq!(step, "sleeps");
    assert_eq!(*timeout, Duration::from_millis(50));

    // Recorded as the step's outcome, so a replay fails the same way rather than hanging again.
    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed");
    let steps = reader
        .list_workflow_steps(&rows[0].workflow_id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1);
    assert!(steps[0].error.is_some(), "the timeout is checkpointed");

    dbos.shutdown().await;
}

/// The future is dropped, so the body stops rather than running on in the background.
///
/// This is the half TypeScript cannot do: it abandons a timed-out attempt and lets it settle
/// unobserved. A dropped Rust future stops at its next suspension point.
#[tokio::test]
async fn a_timed_out_body_stops_rather_than_continuing() {
    let db = test_database().await;
    let dbos = DBOS::new(config("dropped-app", &db));
    let finished = Arc::new(AtomicBool::new(false));
    let dbos_finished = Arc::clone(&finished);
    let workflow = dbos
        .register_workflow("hangs", move |()| {
            let finished = Arc::clone(&dbos_finished);
            async move {
                let options = StepOptions {
                    timeout: Some(Duration::from_millis(50)),
                    ..Default::default()
                };
                dbos::step_with("sleeps", options, || {
                    let finished = Arc::clone(&finished);
                    async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        // Never reached: the future is dropped long before this line.
                        finished.store(true, Ordering::SeqCst);
                        Ok::<u32, dbos::Error>(1)
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect_err("the workflow succeeded");
    // Well past when the body would have finished had it kept running.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !finished.load(Ordering::SeqCst),
        "the timed-out body kept running after its future was dropped"
    );

    dbos.shutdown().await;
}

/// The token fires before the future is dropped, so detached work can be told.
#[tokio::test]
async fn the_cancellation_token_fires_before_the_body_is_dropped() {
    let db = test_database().await;
    let dbos = DBOS::new(config("token-app", &db));
    let observed = Arc::new(AtomicBool::new(false));
    let dbos_observed = Arc::clone(&observed);
    let workflow = dbos
        .register_workflow("watches", move |()| {
            let observed = Arc::clone(&dbos_observed);
            async move {
                let options = StepOptions {
                    timeout: Some(Duration::from_millis(50)),
                    ..Default::default()
                };
                dbos::step_with("watches", options, || {
                    let observed = Arc::clone(&observed);
                    async move {
                        let token = dbos::Ctx::current().unwrap().cancellation();
                        // Work the runtime could not stop by dropping this future: it lives on a
                        // task of its own and only the token can reach it.
                        let watcher = tokio::spawn(async move {
                            token.cancelled().await;
                            observed.store(true, Ordering::SeqCst);
                        });
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        drop(watcher);
                        Ok::<u32, dbos::Error>(1)
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect_err("the workflow succeeded");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        observed.load(Ordering::SeqCst),
        "detached work was never told the step had timed out"
    );

    dbos.shutdown().await;
}

/// A timeout is an ordinary retryable failure, and each attempt gets a fresh one.
#[tokio::test]
async fn a_timed_out_attempt_is_retried_with_a_fresh_timeout() {
    let db = test_database().await;
    let dbos = DBOS::new(config("retry-timeout-app", &db));
    let calls = Arc::new(AtomicU32::new(0));
    let dbos_calls = Arc::clone(&calls);
    let workflow = dbos
        .register_workflow("slow_then_fast", move |()| {
            let calls = Arc::clone(&dbos_calls);
            async move {
                let options = StepOptions {
                    max_attempts: 3,
                    interval: Duration::from_millis(1),
                    max_interval: Duration::from_millis(2),
                    timeout: Some(Duration::from_millis(50)),
                    ..Default::default()
                };
                dbos::step_with("slow", options, || {
                    let calls = Arc::clone(&calls);
                    async move {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        // The first two attempts hang; the third returns at once. A shared budget
                        // rather than a per-attempt one would never reach the third.
                        if attempt < 3 {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                        }
                        Ok::<u32, dbos::Error>(attempt)
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(workflow.run(()).await.expect("the workflow failed"), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    dbos.shutdown().await;
}

/// Exhausting the policy on timeouts reports one per attempt.
#[tokio::test]
async fn every_attempt_timing_out_reports_each_timeout() {
    let db = test_database().await;
    let dbos = DBOS::new(config("all-timeout-app", &db));
    let workflow = dbos
        .register_workflow("always_hangs", |()| async move {
            let options = StepOptions {
                max_attempts: 2,
                interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(2),
                timeout: Some(Duration::from_millis(30)),
                ..Default::default()
            };
            dbos::step_with("hangs", options, || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<u32, dbos::Error>(1)
            })
            .await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let error = workflow.run(()).await.expect_err("the workflow succeeded");
    let Error::MaxStepRetriesExceeded {
        attempts, errors, ..
    } = &error
    else {
        panic!("expected MaxStepRetriesExceeded, got {error:?}");
    };
    assert_eq!(*attempts, 2);
    assert!(
        errors
            .iter()
            .all(|e| matches!(e, Error::StepTimeout { .. })),
        "every attempt should have timed out: {errors:?}"
    );

    dbos.shutdown().await;
}

/// A step that finishes inside its timeout is unaffected by the option existing.
#[tokio::test]
async fn a_step_within_its_timeout_is_unaffected() {
    let db = test_database().await;
    let dbos = DBOS::new(config("within-app", &db));
    let workflow = dbos
        .register_workflow("quick", |()| async move {
            let options = StepOptions {
                timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            };
            dbos::step_with("quick", options, || async { Ok::<u32, dbos::Error>(7) }).await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(workflow.run(()).await.expect("the workflow failed"), 7);

    dbos.shutdown().await;
}

/// A preemptible step stops when the workflow is cancelled by someone else, and records nothing.
///
/// The cancellation is written straight to the database, standing in for Conductor, a client, or
/// another SDK sharing the system database — none of which this process hears from directly.
#[tokio::test]
async fn a_preemptible_step_stops_when_the_workflow_is_cancelled_elsewhere() {
    let db = test_database().await;
    let dbos = DBOS::new(Config {
        migrate: false,
        // Poll briskly, so the test is not waiting out the one-second default.
        outcome_poll_interval: Some(Duration::from_millis(20)),
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new("preempt-app", db.url())
    });
    let entered = Arc::new(tokio::sync::Notify::new());
    let dbos_entered = Arc::clone(&entered);
    let workflow = dbos
        .register_workflow("long_step", move |()| {
            let entered = Arc::clone(&dbos_entered);
            async move {
                let options = StepOptions {
                    preemptible: true,
                    ..Default::default()
                };
                dbos::step_with("slow", options, || {
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.notify_one();
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Ok::<u32, dbos::Error>(1)
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "preempt-me";
    let handle = workflow
        .start_with(
            (),
            dbos::StartOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect("start failed");

    // Wait until the body is actually running before cancelling it.
    entered.notified().await;

    let reader = reader(&db).await;
    reader
        .cancel_workflows(&[id], false, None)
        .await
        .expect("cancel failed");

    let error = handle.result().await.expect_err("the workflow succeeded");
    assert!(
        matches!(error, Error::WorkflowCancelled { .. }),
        "expected a cancellation, got {error:?}"
    );

    // Nothing checkpointed: a preempted step was interrupted, not wrong, so a resume runs it again.
    let steps = reader
        .list_workflow_steps(id, true, None, None, None)
        .await
        .expect("read failed");
    assert!(
        steps.is_empty(),
        "a preempted step must record no outcome, found {steps:?}"
    );

    dbos.shutdown().await;
}

/// Preemption is off by default, so an ordinary step is not watched.
#[tokio::test]
async fn a_plain_step_is_not_preemptible() {
    let db = test_database().await;
    let dbos = DBOS::new(config("not-preempt-app", &db));
    let workflow = dbos
        .register_workflow("quick", |()| async move {
            dbos::step("quick", || async { Ok::<u32, dbos::Error>(7) }).await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(workflow.run(()).await.expect("the workflow failed"), 7);
    assert!(!StepOptions::<dbos::Error>::default().preemptible);

    dbos.shutdown().await;
}

/// **A step abandoned mid-body fires its cancellation token**, so work the runtime cannot stop by
/// dropping the future — a blocking thread, a task the body spawned — learns that its step is over.
///
/// The timeout and preemption paths cancel the token themselves, but a step can be abandoned in
/// other ways: a caller dropping it, or a combinator dropping it as a losing branch. Those left the
/// token silent, and a step with neither watchdog had no token to fire at all. Here the step is
/// raced against a signal it sends itself, so it is dropped while parked inside its own body.
#[tokio::test]
async fn a_dropped_step_fires_its_cancellation_token() {
    let db = test_database().await;
    let dbos = DBOS::new(config("dropped-token-app", &db));
    let released = Arc::new(tokio::sync::Notify::new());
    let workflow = {
        let released = Arc::clone(&released);
        dbos.register_workflow("workflow", move |()| {
            let released = Arc::clone(&released);
            async move {
                let (started, started_rx) = tokio::sync::oneshot::channel::<()>();
                let mut started = Some(started);
                // Biased, so the step is polled far enough to enter its body and register its
                // watcher before the signal it sends is seen and the step is dropped.
                tokio::select! {
                    biased;
                    _ = started_rx => {}
                    _ = dbos::step("parked", move || {
                        let released = Arc::clone(&released);
                        let started = started.take();
                        async move {
                            let token = dbos::Ctx::current().expect("in a step").cancellation();
                            tokio::spawn(async move {
                                token.cancelled().await;
                                released.notify_one();
                            });
                            if let Some(started) = started {
                                let _ = started.send(());
                            }
                            std::future::pending::<()>().await;
                            Ok::<u32, Error>(1)
                        }
                    }) => {}
                }
                Ok::<u32, Error>(0)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect("the workflow failed");
    tokio::time::timeout(Duration::from_secs(5), released.notified())
        .await
        .expect("the dropped step's token never fired");

    dbos.shutdown().await;
}

/// **A step that completes leaves its token alone.** The token says the attempt was *abandoned*,
/// not that the step is over: a body that reached an outcome had its chance to clean up, and work
/// it deliberately left running is not the engine's to stop. Both return paths are checked — a
/// plain step, and one carrying a watchdog it finished well inside.
#[tokio::test]
async fn a_completed_step_leaves_its_cancellation_token_alone() {
    let db = test_database().await;
    let dbos = DBOS::new(config("completed-token-app", &db));
    let plain = Arc::new(std::sync::Mutex::new(None));
    let watched = Arc::new(std::sync::Mutex::new(None));
    let workflow = {
        let (plain, watched) = (Arc::clone(&plain), Arc::clone(&watched));
        dbos.register_workflow("workflow", move |()| {
            let (plain, watched) = (Arc::clone(&plain), Arc::clone(&watched));
            async move {
                let remember = |slot: Arc<std::sync::Mutex<Option<_>>>| {
                    move || {
                        let slot = Arc::clone(&slot);
                        async move {
                            *slot.lock().unwrap() =
                                Some(dbos::Ctx::current().expect("in a step").cancellation());
                            Ok::<u32, Error>(1)
                        }
                    }
                };
                dbos::step("plain", remember(Arc::clone(&plain))).await?;
                dbos::step_with(
                    "watched",
                    StepOptions {
                        timeout: Some(Duration::from_secs(30)),
                        ..Default::default()
                    },
                    remember(Arc::clone(&watched)),
                )
                .await?;
                // Read once each step has returned. The guard disarms on the way out, so a token a
                // completed body handed to its own background work stays quiet.
                let fired =
                    |slot: &std::sync::Mutex<Option<tokio_util::sync::CancellationToken>>| {
                        slot.lock()
                            .unwrap()
                            .as_ref()
                            .expect("the body ran")
                            .is_cancelled()
                    };
                Ok::<(bool, bool), Error>((fired(&plain), fired(&watched)))
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    assert_eq!(
        workflow.run(()).await.expect("the workflow failed"),
        (false, false),
        "a step that completed cancelled its own token"
    );

    dbos.shutdown().await;
}
