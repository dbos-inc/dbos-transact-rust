//! The test hooks themselves: named points in production code where a test can run something or
//! make a call fail, and the lost acknowledgements they make possible to test.
//!
//! A *lost acknowledgement* is a write that committed and a caller that was told it did not. The
//! retry has to find its own work already there. Failing at a point after the commit with a
//! retryable error is how to produce one on purpose.

use std::sync::{Arc, Mutex};

use dbos::__test_hooks::{fail_once, on, points};
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{
    NewWorkflow, Outcome, StepTiming, Timestamp, WorkflowIdReusePolicy, WorkflowStatus,
};
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

/// A scope no other test uses. The registry is per process and every test has a database of its
/// own, so a hook on `wf-1` would fire in every test that records a step for `wf-1`.
fn unique(name: &str) -> String {
    format!("{name}-{}", uuid::Uuid::new_v4())
}

/// A failure the engine retries, after a commit that already landed: what a lost acknowledgement
/// is. A conflict stands in for a dropped connection because it is replayed after 50 ms, where a
/// connection failure waits a second or more, and the retry is the same code for both.
fn lost_acknowledgement() -> Error {
    backend_error(BackendErrorKind::Conflict)
}

fn backend_error(kind: BackendErrorKind) -> Error {
    Error::Backend(BackendError {
        message: "injected by a test hook".to_owned(),
        sqlstate: None,
        kind,
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
    let id = unique("wf-lost-init-ack");
    let lost = fail_once(points::INIT_WORKFLOW_COMMIT, &id, lost_acknowledgement());

    let init = sys
        .init_workflow(&workflow(&id), None, WorkflowIdReusePolicy::Reject)
        .await
        .expect("the retry was refused its own row");

    assert!(init.should_execute, "the retry did not recognise its row");
    assert_eq!(
        lost.fired(),
        2,
        "the point is reached by the call and again by its retry"
    );
    assert_eq!(
        sys.get_workflow(&id)
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
    let id = unique("wf-lost-step-ack");
    sys.init_workflow(&workflow(&id), None, WorkflowIdReusePolicy::ReturnExisting)
        .await
        .unwrap();
    let owner = owner_token(&sys, &id).await;
    let lost = fail_once(points::STEP_COMMIT, &id, lost_acknowledgement());
    // The timing is what tells the retry its own write from someone else's: on a duplicate insert
    // the stored `completed_at` is compared with this one, and without a timing any duplicate
    // would pass for the caller's own. It is held across the retry, as the engine holds it.
    let timing = StepTiming {
        started_at: Timestamp::from_epoch_ms(1_000),
        completed_at: Timestamp::from_epoch_ms(2_000),
    };

    sys.record_step(
        &id,
        0,
        "charge_card",
        Outcome::Output(Some("1")),
        None,
        Some(timing),
        &owner,
    )
    .await
    .expect("the retry did not recognise its own step");

    assert_eq!(lost.fired(), 2);
    let steps = sys
        .list_workflow_steps(&id, true, None, None, None)
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
    let id = unique("wf-sees-commit");
    let hook = on(points::INIT_WORKFLOW_COMMIT, &id, {
        let (reader, seen, id) = (Arc::clone(&reader), Arc::clone(&seen), id.clone());
        move || {
            let (reader, seen, id) = (Arc::clone(&reader), Arc::clone(&seen), id.clone());
            async move {
                let row = reader.get_workflow(&id).await?;
                *seen.lock().unwrap() = row.map(|row| row.status);
                Ok(())
            }
        }
    });

    sys.init_workflow(&workflow(&id), None, WorkflowIdReusePolicy::ReturnExisting)
        .await
        .unwrap();

    assert_eq!(hook.fired(), 1);
    assert_eq!(*seen.lock().unwrap(), Some(WorkflowStatus::Pending));
}

/// **A hook is for its scope only, and goes when its guard does.**
///
/// A hook on one id does not touch another's, and one left set by a test that has finished does
/// not fail the next. The hook here fails with an error the engine does not retry, so one that
/// outlived its guard would surface as a refused start instead of being retried away.
#[tokio::test]
async fn a_hook_is_scoped_and_cleared_by_its_guard() {
    let (sys, _db) = sysdb().await;
    let (scoped, bystander) = (unique("wf-scoped"), unique("wf-bystander"));
    let refused = fail_once(
        points::INIT_WORKFLOW_COMMIT,
        &scoped,
        backend_error(BackendErrorKind::Permanent),
    );

    sys.init_workflow(
        &workflow(&bystander),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .expect("a hook on another scope reached this workflow");
    assert_eq!(refused.fired(), 0, "another workflow reached this hook");

    drop(refused);
    sys.init_workflow(
        &workflow(&scoped),
        None,
        WorkflowIdReusePolicy::ReturnExisting,
    )
    .await
    .expect("a hook outlived its guard");
}

/// **The same hook does fail its own scope, which is what makes the test above mean something.**
#[tokio::test]
async fn a_permanent_failure_at_a_point_reaches_the_caller_once() {
    let (sys, _db) = sysdb().await;
    let id = unique("wf-permanent");
    let refused = fail_once(
        points::INIT_WORKFLOW_COMMIT,
        &id,
        backend_error(BackendErrorKind::Permanent),
    );

    let first = sys
        .init_workflow(&workflow(&id), None, WorkflowIdReusePolicy::ReturnExisting)
        .await;
    assert!(first.is_err(), "the failure did not reach the caller");
    assert_eq!(refused.fired(), 1, "a permanent failure was retried");

    // The commit landed, and the hook is spent: the next call finds the row and carries on.
    sys.init_workflow(&workflow(&id), None, WorkflowIdReusePolicy::ReturnExisting)
        .await
        .expect("the spent hook failed a second call");
}

/// Two hooks on one point and scope are two tests sharing a name, and that is refused.
#[test]
#[should_panic(expected = "a hook is already set")]
fn two_hooks_on_one_point_and_scope_are_refused() {
    let scope = unique("wf-doubled");
    let _first = on(points::STEP_COMMIT, &scope, || async { Ok(()) });
    let _second = on(points::STEP_COMMIT, &scope, || async { Ok(()) });
}

/// **Dropping a guard whose hook owns another guard does not deadlock.**
///
/// Removing a hook drops its closure, and a closure that captured another hook's guard drops that
/// guard too, which takes the registry's lock again. The drop has to happen after the lock is
/// released. A deadlock would hang the test, so the drop runs on a thread the test waits on.
#[test]
fn dropping_a_guard_whose_hook_owns_another_guard_does_not_hang() {
    let inner = on(points::STEP_COMMIT, unique("wf-inner"), || async { Ok(()) });
    let outer = on(points::STEP_COMMIT, unique("wf-outer"), move || {
        let _owned = &inner;
        async { Ok(()) }
    });

    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        drop(outer);
        let _ = done.send(());
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("dropping the outer guard deadlocked on the registry lock");
}
