//! Step retries: what happens when a body fails, against real databases.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::{Config, DBOS, Error, StepOptions};

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

/// Retry options that do not make the suite wait: the policy is under test, not the clock.
fn fast(max_attempts: u32) -> StepOptions {
    StepOptions {
        max_attempts,
        interval: Duration::from_millis(1),
        max_interval: Duration::from_millis(5),
        ..Default::default()
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("flaky failed on attempt {attempt}")]
struct Flaky {
    attempt: u32,
}

/// The point of the feature: a body that fails twice and then succeeds is one successful step.
#[tokio::test]
async fn a_step_that_fails_twice_succeeds_on_the_third_attempt() {
    let calls = Arc::new(AtomicU32::new(0));
    let dbos_calls = Arc::clone(&calls);

    let db = test_database().await;
    let dbos = DBOS::new(config("retry-app", &db));
    let workflow = dbos
        .register_workflow("flaky", move |()| {
            let calls = Arc::clone(&dbos_calls);
            async move {
                dbos::step_with("charge", fast(3), || {
                    let calls = Arc::clone(&calls);
                    async move {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt < 3 {
                            return Err(Flaky { attempt }.into());
                        }
                        Ok::<_, Error<Flaky>>(attempt)
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    assert_eq!(workflow.run(()).await.expect("the workflow failed"), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3, "the body ran three times");

    // One checkpoint for the whole sequence, holding the attempt that succeeded.
    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let steps = reader
        .list_workflow_steps(&rows[0].workflow_id, true, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "the attempts share one step id");
    assert_eq!(steps[0].step_id, 0);
    assert_eq!(steps[0].output.as_deref(), Some("3"));
    assert!(steps[0].error.is_none());

    dbos.shutdown().await;
}

/// Exhausting the policy raises one error carrying every attempt, and records it.
#[tokio::test]
async fn exhausted_retries_carry_every_attempts_failure() {
    let db = test_database().await;
    let dbos = DBOS::new(config("exhausted-app", &db));
    let calls = Arc::new(AtomicU32::new(0));
    let dbos_calls = Arc::clone(&calls);
    let workflow = dbos
        .register_workflow("always_fails", move |()| {
            let calls = Arc::clone(&dbos_calls);
            async move {
                dbos::step_with("charge", fast(3), || {
                    let calls = Arc::clone(&calls);
                    async move {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        Err::<u32, _>(Error::from(Flaky { attempt }))
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let error = workflow.run(()).await.expect_err("the workflow succeeded");
    assert_eq!(calls.load(Ordering::SeqCst), 3, "every attempt ran");
    let Error::MaxStepRetriesExceeded {
        step,
        attempts,
        errors,
    } = error
    else {
        panic!("expected MaxStepRetriesExceeded, got {error:?}");
    };
    assert_eq!(step, "charge");
    assert_eq!(attempts, 3);
    // Oldest first, and each one is its own payload rather than a description of one.
    let seen: Vec<u32> = errors
        .iter()
        .map(|e| match e {
            Error::Application(flaky) => flaky.attempt,
            other => panic!("expected an application error, got {other:?}"),
        })
        .collect();
    assert_eq!(seen, [1, 2, 3]);

    dbos.shutdown().await;
}

/// The default is no retrying, and a lone failure is recorded unwrapped.
#[tokio::test]
async fn the_default_does_not_retry_and_does_not_wrap() {
    let db = test_database().await;
    let dbos = DBOS::new(config("no-retry-app", &db));
    let calls = Arc::new(AtomicU32::new(0));
    let dbos_calls = Arc::clone(&calls);
    let workflow = dbos
        .register_workflow("plain", move |()| {
            let calls = Arc::clone(&dbos_calls);
            async move {
                dbos::step("charge", || {
                    let calls = Arc::clone(&calls);
                    async move {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        Err::<u32, _>(Error::from(Flaky { attempt }))
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let error = workflow.run(()).await.expect_err("the workflow succeeded");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the body ran once");
    assert!(
        matches!(error, Error::Application(Flaky { attempt: 1 })),
        "a step with no retry policy reports its own error, unwrapped: {error:?}"
    );

    dbos.shutdown().await;
}

/// A replay does not re-enter the body, however many attempts the original run took.
#[tokio::test]
async fn a_retried_step_replays_from_its_single_checkpoint() {
    let db = test_database().await;
    let calls = Arc::new(AtomicU32::new(0));

    // First run: fails once, succeeds on the second attempt, and the workflow then fails so the
    // row is left for a second execution under the same id.
    let dbos = DBOS::new(config("replay-app", &db));
    let dbos_calls = Arc::clone(&calls);
    let workflow = dbos
        .register_workflow("flaky_then_stop", move |stop: bool| {
            let calls = Arc::clone(&dbos_calls);
            async move {
                let charged = dbos::step_with("charge", fast(3), || {
                    let calls = Arc::clone(&calls);
                    async move {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt < 2 {
                            return Err(Flaky { attempt }.into());
                        }
                        Ok::<_, Error<Flaky>>(attempt)
                    }
                })
                .await?;
                if stop {
                    return Err(Flaky { attempt: 99 }.into());
                }
                Ok(charged)
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "replay-me";
    let options = dbos::StartOptions {
        workflow_id: Some(id),
    };
    workflow
        .run_with(true, options.clone())
        .await
        .expect_err("the first run should fail after the step");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "two attempts on the first run"
    );

    // The step is recorded, so a second execution under the same id must not enter the body again.
    let reader = reader(&db).await;
    let recorded = reader
        .list_workflow_steps(id, true, None, None)
        .await
        .expect("read failed");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].output.as_deref(), Some("2"));

    dbos.shutdown().await;
}
