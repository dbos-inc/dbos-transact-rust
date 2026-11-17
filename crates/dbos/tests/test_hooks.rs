//! The test hooks themselves: named points in production code where a test can run something or
//! make a call fail, and the lost acknowledgements they make possible to test.
//!
//! A *lost acknowledgement* is a write that committed and a caller that was told it did not. The
//! retry has to find its own work already there. Failing at a point after the commit with a
//! retryable error is how to produce one on purpose.

#![cfg(feature = "test-hooks")]

use std::sync::{Arc, Mutex};

use dbos::__test_hooks::{fail_once, on, points};
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{NewWorkflow, Outcome, WorkflowIdReusePolicy, WorkflowStatus};
use dbos::sysdb::{BackendError, BackendErrorKind, Error, PORTABLE_JSON, SystemDatabase};
use dbos_test_support::{TestDatabase, owner_token, test_database};

fn workflow(id: &str) -> NewWorkflow<'_> {
    NewWorkflow {
        name: Some("checkout"),
        input: Some(r#"{"positionalArgs":[1],"namedArgs":{}}"#),
        serialization: Some(PORTABLE_JSON),
        executor_id: Some("local"),
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    }
}

async fn sysdb() -> (PostgresSystemDatabase, TestDatabase) {
    let db = test_database().await;
    let sys = PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default());
    (sys, db)
}

/// What a dropped connection after a commit looks like: retryable, and the write already landed.
fn lost_acknowledgement() -> Error {
    Error::Backend(BackendError {
        message: "the connection was lost after the commit".to_owned(),
        sqlstate: None,
        kind: BackendErrorKind::Connection,
    })
}

/// **A start whose insert committed but whose acknowledgement was lost is not refused.**
///
/// Under `Reject` a second insert of the same id is an error, so a retry that did not recognise
/// its own row would report the id as taken by someone else. It presents the creator token it made
/// the first time, finds the row it wrote, and is told it owns it.
#[tokio::test]
async fn a_lost_init_acknowledgement_is_retried_by_the_same_creator() {
    let (sys, _db) = sysdb().await;
    let lost = fail_once(
        points::INIT_WORKFLOW_COMMIT,
        "wf-lost-init-ack",
        lost_acknowledgement(),
    );

    let init = sys
        .init_workflow(
            &workflow("wf-lost-init-ack"),
            None,
            WorkflowIdReusePolicy::Reject,
        )
        .await
        .expect("the retry was refused its own row");

    assert!(init.should_execute, "the retry did not recognise its row");
    assert_eq!(
        lost.fired(),
        2,
        "the point is reached by the call and again by its retry"
    );
    assert_eq!(
        sys.get_workflow("wf-lost-init-ack")
            .await
            .unwrap()
            .expect("the row is missing")
            .status,
        WorkflowStatus::Pending
    );
}

/// **A step whose record committed but whose acknowledgement was lost is recorded once.**
///
/// The retry writes the same step again and has to find it identical, not call the second write
/// nondeterminism.
#[tokio::test]
async fn a_lost_step_acknowledgement_leaves_one_recorded_step() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(
        &workflow("wf-lost-step-ack"),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .unwrap();
    let owner = owner_token(&sys, "wf-lost-step-ack").await;
    let lost = fail_once(
        points::STEP_COMMIT,
        "wf-lost-step-ack",
        lost_acknowledgement(),
    );

    sys.record_step(
        "wf-lost-step-ack",
        0,
        "charge_card",
        Outcome::Output(Some("1")),
        None,
        None,
        &owner,
    )
    .await
    .expect("the retry did not recognise its own step");

    assert_eq!(lost.fired(), 2);
    let steps = sys
        .list_workflow_steps("wf-lost-step-ack", true, None, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1, "the step was recorded more than once");
    assert_eq!(steps[0].step_name, "charge_card");
}

/// **A hook runs at the point, after the commit, and sees what the commit left.**
///
/// What a test does inside a window — a management call, a read, a wait — is run by the hook, and
/// what it finds is what is true at that moment.
#[tokio::test]
async fn a_hook_runs_after_the_commit_and_sees_the_row() {
    let (sys, db) = sysdb().await;
    let reader = Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    let seen: Arc<Mutex<Option<WorkflowStatus>>> = Arc::new(Mutex::new(None));
    let hook = on(points::INIT_WORKFLOW_COMMIT, "wf-sees-commit", {
        let (reader, seen) = (Arc::clone(&reader), Arc::clone(&seen));
        move || {
            let (reader, seen) = (Arc::clone(&reader), Arc::clone(&seen));
            async move {
                let row = reader.get_workflow("wf-sees-commit").await?;
                *seen.lock().unwrap() = row.map(|row| row.status);
                Ok(())
            }
        }
    });

    sys.init_workflow(
        &workflow("wf-sees-commit"),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .unwrap();

    assert_eq!(hook.fired(), 1);
    assert_eq!(*seen.lock().unwrap(), Some(WorkflowStatus::Pending));
}

/// **A hook is for its scope only, and goes when its guard does.**
///
/// Tests in one binary run side by side and name their own workflows, so a hook on one id does not
/// touch another's; and one left set by a test that has finished does not fail the next.
#[tokio::test]
async fn a_hook_is_scoped_and_cleared_by_its_guard() {
    let (sys, _db) = sysdb().await;
    let lost = fail_once(
        points::INIT_WORKFLOW_COMMIT,
        "wf-scoped",
        lost_acknowledgement(),
    );

    sys.init_workflow(
        &workflow("wf-bystander"),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .unwrap();
    assert_eq!(lost.fired(), 0, "another workflow reached this hook");

    drop(lost);
    sys.init_workflow(
        &workflow("wf-scoped"),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .expect("a hook outlived its guard");
}

/// Two hooks on one point and scope are two tests sharing a name, and that is refused.
#[test]
#[should_panic(expected = "a hook is already set")]
fn two_hooks_on_one_point_and_scope_are_refused() {
    let _first = on(points::STEP_COMMIT, "wf-doubled", || async { Ok(()) });
    let _second = on(points::STEP_COMMIT, "wf-doubled", || async { Ok(()) });
}
