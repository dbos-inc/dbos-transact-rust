//! What a workflow can ask about itself, against real databases.
//!
//! The accessors are free functions rather than methods on a context object, so these tests are
//! also the check that the ambient context reaches them: nothing here passes an id anywhere, and
//! every answer comes from the task-local the engine set.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use dbos::{Config, DBOS, StepOptions, StepStatus};
use std::time::Duration;

use dbos_test_support::{TestDatabase, test_database};

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("failed on purpose")]
struct Flaky;

/// Reports the id of the workflow it is running as.
async fn reads_id(_: ()) -> dbos::Result<String> {
    Ok(dbos::workflow_id().expect("inside a workflow"))
}

/// The step id seen at five points: before, inside, between, inside, after.
type StepIdTrace = (
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
);

async fn reads_step_ids(_: ()) -> dbos::Result<StepIdTrace> {
    // In the workflow body proper: inside a workflow, but not inside a step.
    let before = dbos::step_id();
    let first = dbos::step("first", || async { Ok(dbos::step_id()) }).await?;
    let between = dbos::step_id();
    let second = dbos::step("second", || async { Ok(dbos::step_id()) }).await?;
    let after = dbos::step_id();
    Ok((before, first, between, second, after))
}

/// Reports both accessors from inside one step body.
async fn reads_both(_: ()) -> dbos::Result<(Option<String>, Option<i32>)> {
    dbos::step("inner", || async {
        Ok((dbos::workflow_id(), dbos::step_id()))
    })
    .await
}

/// A workflow reads its own id, and it is the id the caller addresses it by.
///
/// The whole point of the accessor: the id a caller uses as an idempotency key is the one the
/// workflow publishes for that caller to pay against, and neither side passed it to the other.
#[tokio::test]
async fn a_workflow_reads_its_own_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("own-id-app", &db));
    let workflow = dbos.register_workflow("reads_id", reads_id).unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = workflow.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    let reported = handle.result().await.expect("the workflow failed");

    assert_eq!(reported, id, "a workflow's own id is the caller's id");
}

/// Outside a workflow there is no id, and that is an answer rather than a failure.
#[tokio::test]
async fn there_is_no_workflow_id_outside_a_workflow() {
    assert_eq!(dbos::workflow_id(), None);
    assert_eq!(dbos::step_id(), None);
}

/// A step sees its own id; the workflow body between steps sees none.
///
/// **The gap is the assertion worth making.** A step id belongs to a step, so a workflow that has
/// finished one and not started the next is inside neither — which is where Python answers `None`
/// and TypeScript `undefined`, and where a naive implementation that stored the last id handed out
/// would answer with a stale one.
#[tokio::test]
async fn step_ids_are_visible_inside_steps_and_nowhere_else() {
    let db = test_database().await;
    let dbos = DBOS::new(config("step-id-app", &db));
    let workflow = dbos
        .register_workflow("reads_step_ids", reads_step_ids)
        .unwrap();

    dbos.launch().await.expect("launch failed");

    let (before, first, between, second, after) =
        workflow.run(()).await.expect("the workflow failed");

    assert_eq!(before, None, "the workflow body is not inside a step");
    assert_eq!(first, Some(0), "step ids count from zero");
    assert_eq!(between, None, "between two steps is inside neither");
    assert_eq!(second, Some(1), "the next step takes the next id");
    assert_eq!(after, None, "the last step's id does not leak out of it");
}

/// A step reads the workflow it belongs to, not nothing.
///
/// A step is part of its workflow, so the two accessors answer different questions in the same
/// place: `step_id` is `Some` and so is `workflow_id`.
#[tokio::test]
async fn a_step_can_read_the_workflow_it_belongs_to() {
    let db = test_database().await;
    let dbos = DBOS::new(config("step-workflow-id-app", &db));
    let workflow = dbos.register_workflow("reads_both", reads_both).unwrap();

    dbos.launch().await.expect("launch failed");

    let handle = workflow.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    let (seen_workflow, seen_step) = handle.result().await.expect("the workflow failed");

    assert_eq!(seen_workflow.as_deref(), Some(id.as_str()));
    assert_eq!(seen_step, Some(0));
}

/// Every attempt of a retried step reports that step's id.
///
/// A retry is another attempt at the *same* step, not a new one, so the id must not move — it
/// addresses the checkpoint row both attempts are competing to write.
#[tokio::test]
async fn every_attempt_of_a_step_sees_the_same_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("retry-step-id-app", &db));
    let seen: Arc<std::sync::Mutex<Vec<Option<i32>>>> = Arc::default();
    let attempts = Arc::new(AtomicU32::new(0));
    let workflow = {
        let seen = Arc::clone(&seen);
        let attempts = Arc::clone(&attempts);
        dbos.register_workflow("retries", move |()| {
            let seen = Arc::clone(&seen);
            let attempts = Arc::clone(&attempts);
            async move {
                // A step ahead of it, so the retried step's id is 1 rather than 0: an id that
                // never moved would be indistinguishable from one that was always zero.
                dbos::step("first", || async { dbos::Result::<(), Flaky>::Ok(()) }).await?;
                // Fast retries: the id is under test, not the clock.
                let options = StepOptions {
                    max_attempts: 3,
                    interval: Duration::from_millis(1),
                    max_interval: Duration::from_millis(5),
                    ..Default::default()
                };
                dbos::step_with("flaky", options, || {
                    let seen = Arc::clone(&seen);
                    let attempts = Arc::clone(&attempts);
                    async move {
                        seen.lock().unwrap().push(dbos::step_id());
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            Err(Flaky)?
                        }
                        Ok(())
                    }
                })
                .await?;
                dbos::Result::<(), Flaky>::Ok(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect("the workflow failed");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "the step should have taken three attempts");
    assert!(
        seen.iter().all(|id| *id == Some(1)),
        "every attempt reports the same step id, got {seen:?}"
    );
}

/// A plain step reports attempt 1 of 1, where Python and TypeScript both report nothing.
///
/// The divergence is deliberate: every step here runs the same retry loop with `max_attempts`
/// defaulting to 1, so there is an honest answer to give, and "does this retry?" is
/// `max_attempts > 1` rather than an `Option` to unwrap inside an `Option`.
#[tokio::test]
async fn a_plain_step_reports_one_attempt_of_one() {
    let db = test_database().await;
    let dbos = DBOS::new(config("plain-status-app", &db));
    // Captured rather than returned: `StepStatus` is deliberately not serializable, because it
    // describes the attempt rather than its outcome and has no business in a checkpoint row.
    let seen: Arc<std::sync::Mutex<Option<StepStatus>>> = Arc::default();
    let workflow = {
        let seen = Arc::clone(&seen);
        dbos.register_workflow("plain", move |()| {
            let seen = Arc::clone(&seen);
            async move {
                dbos::step("only", || async {
                    *seen.lock().unwrap() = dbos::step_status();
                    dbos::Result::<(), Flaky>::Ok(())
                })
                .await
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect("the workflow failed");
    let status = seen.lock().unwrap().expect("inside a step");
    assert_eq!(
        (
            status.step_id(),
            status.current_attempt(),
            status.max_attempts()
        ),
        (0, 1, 1)
    );
}

/// **The attempt number moves and the id does not**, which is the whole point of the status
/// carrying both. One-based, so the last attempt of three reports 3 rather than 2.
#[tokio::test]
async fn the_attempt_number_counts_from_one_and_the_id_holds_still() {
    let db = test_database().await;
    let dbos = DBOS::new(config("status-retry-app", &db));
    let seen: Arc<std::sync::Mutex<Vec<StepStatus>>> = Arc::default();
    let attempts = Arc::new(AtomicU32::new(0));
    let workflow = {
        let seen = Arc::clone(&seen);
        let attempts = Arc::clone(&attempts);
        dbos.register_workflow("retries", move |()| {
            let seen = Arc::clone(&seen);
            let attempts = Arc::clone(&attempts);
            async move {
                // A step ahead of it, so a step id that never moved would be indistinguishable
                // from one that was always zero.
                dbos::step("first", || async { dbos::Result::<(), Flaky>::Ok(()) }).await?;
                let options = StepOptions {
                    max_attempts: 3,
                    interval: Duration::from_millis(1),
                    max_interval: Duration::from_millis(5),
                    ..Default::default()
                };
                dbos::step_with("flaky", options, || {
                    let seen = Arc::clone(&seen);
                    let attempts = Arc::clone(&attempts);
                    async move {
                        seen.lock()
                            .unwrap()
                            .push(dbos::step_status().expect("inside a step"));
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            Err(Flaky)?
                        }
                        Ok(())
                    }
                })
                .await?;
                dbos::Result::<(), Flaky>::Ok(())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    workflow.run(()).await.expect("the workflow failed");

    let seen: Vec<(i32, u32, u32)> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| (s.step_id(), s.current_attempt(), s.max_attempts()))
        .collect();
    assert_eq!(
        seen,
        vec![(1, 1, 3), (1, 2, 3), (1, 3, 3)],
        "the attempt should count 1..=3 while the id stays put"
    );
}

/// A status belongs to a step, so the workflow body between two of them has none — the same place
/// `step_id` answers `None`, and for the same reason.
#[tokio::test]
async fn there_is_no_step_status_outside_a_step() {
    assert_eq!(dbos::step_status(), None);

    let db = test_database().await;
    let dbos = DBOS::new(config("status-gap-app", &db));
    let workflow = dbos
        .register_workflow("gap", |()| async move {
            let before = dbos::step_status().is_none();
            dbos::step("only", || async { dbos::Result::<(), Flaky>::Ok(()) }).await?;
            dbos::Result::<_, Flaky>::Ok((before, dbos::step_status().is_none()))
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let (before, after) = workflow.run(()).await.expect("the workflow failed");
    assert!(before, "the workflow body is not inside a step");
    assert!(after, "a finished step's status does not leak out of it");
}
