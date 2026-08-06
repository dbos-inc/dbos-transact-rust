//! The system database surface, against real databases.

mod support;

use dbos::sysdb::postgres::PostgresSystemDatabase;
use dbos::sysdb::types::{Timestamp, WorkflowRecord, WorkflowStatus};
use dbos::sysdb::{DEFAULT_SCHEMA, Error, InitWorkflowStatus, OutcomeWrite, SystemDatabase};

use support::test_database;

fn record(id: &str) -> WorkflowRecord {
    WorkflowRecord {
        workflow_id: id.to_owned(),
        status: WorkflowStatus::Pending,
        name: Some("checkout".to_owned()),
        class_name: None,
        config_name: None,
        input: Some(r#"{"positionalArgs":[1],"namedArgs":{}}"#.to_owned()),
        output: None,
        error: None,
        serialization: Some("portable_json".to_owned()),
        executor_id: Some("local".to_owned()),
        application_version: Some("v1".to_owned()),
        recovery_attempts: 0,
        queue_name: None,
        created_at: Timestamp::from_epoch_ms(1_700_000_000_000),
        updated_at: Timestamp::from_epoch_ms(1_700_000_000_000),
        started_at: None,
        completed_at: None,
        forked_from: None,
        parent_workflow_id: None,
    }
}

async fn sysdb() -> (PostgresSystemDatabase, support::TestDatabase) {
    let db = test_database().await;
    let pool = db.pool().await;
    (PostgresSystemDatabase::from_pool(pool, DEFAULT_SCHEMA), db)
}

/// A workflow round-trips through the database unchanged.
#[tokio::test]
async fn a_workflow_round_trips() {
    let (sys, _db) = sysdb().await;
    let written = sys
        .init_workflow_status(InitWorkflowStatus::new(&record("wf-1")))
        .await
        .expect("insert failed");
    assert_eq!(written.status, WorkflowStatus::Pending);
    assert_eq!(
        written.recovery_attempts, 1,
        "starting a workflow counts as an attempt; enqueueing would not",
    );
    assert!(written.should_execute);

    let read = sys
        .get_workflow_status("wf-1")
        .await
        .expect("read failed")
        .expect("the workflow should exist");
    assert_eq!(read.status, WorkflowStatus::Pending);
    assert_eq!(read.name.as_deref(), Some("checkout"));
    assert_eq!(read.serialization.as_deref(), Some("portable_json"));
}

/// Payloads come back exactly as they went in.
///
/// This layer does not encode, decode, or validate them — that is what lets one workflow's
/// `rust_serde` bytes and another's `portable_json` share a table, and what a caller in another
/// language depends on.
#[tokio::test]
async fn payloads_are_stored_verbatim() {
    let (sys, _db) = sysdb().await;
    let mut r = record("wf-opaque");
    // Deliberately not valid JSON, to show nothing here parses it.
    r.input = Some("not json at all \u{1F600} '\"; --".to_owned());
    sys.init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .expect("insert failed");

    let read = sys.get_workflow_status("wf-opaque").await.unwrap().unwrap();
    assert_eq!(read.input, r.input);
}

/// An unknown id reads as absent rather than erroring.
#[tokio::test]
async fn a_missing_workflow_reads_as_none() {
    let (sys, _db) = sysdb().await;
    assert!(
        sys.get_workflow_status("nobody-here")
            .await
            .unwrap()
            .is_none()
    );
}

/// Inserting the same id twice leaves the first row alone.
///
/// A retried enqueue must not reset a workflow that is already running or finished, so the
/// second insert reports what is actually stored rather than what it tried to store.
#[tokio::test]
async fn resubmitting_reconciles_and_a_different_function_is_rejected() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow_status(InitWorkflowStatus::new(&record("wf-dup")))
        .await
        .unwrap();

    // The same id running a different function is a programming error, not a race.
    let mut different = record("wf-dup");
    different.name = Some("a-different-function".to_owned());
    let err = sys
        .init_workflow_status(InitWorkflowStatus::new(&different))
        .await
        .expect_err("a different function under the same id should be rejected");
    assert!(
        matches!(err, Error::ConflictingWorkflow { .. }),
        "expected a conflict, got {err:?}",
    );

    // Re-submitting the same workflow is fine, and the original row stands.
    let again = sys
        .init_workflow_status(InitWorkflowStatus::new(&record("wf-dup")))
        .await
        .expect("re-submitting the same workflow should succeed");
    assert_eq!(again.status, WorkflowStatus::Pending);
    let read = sys.get_workflow_status("wf-dup").await.unwrap().unwrap();
    assert_eq!(read.application_version.as_deref(), Some("v1"));
}

/// Recovery attempts count, but only for the attempts that should count.
///
/// A fresh start records one. A recovery or dequeue adds another. A plain re-submission does
/// not, or a client retrying its own enqueue would burn through the dead-letter budget.
#[tokio::test]
async fn only_recoveries_and_dequeues_count_as_attempts() {
    let (sys, _db) = sysdb().await;
    let r = record("wf-attempts");

    let first = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert_eq!(first.recovery_attempts, 1);

    let plain = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert_eq!(
        plain.recovery_attempts, 1,
        "a re-submission is not an attempt"
    );

    let recovered = sys
        .init_workflow_status(InitWorkflowStatus {
            is_recovery: true,
            ..InitWorkflowStatus::new(&r)
        })
        .await
        .unwrap();
    assert_eq!(recovered.recovery_attempts, 2, "a recovery is an attempt");
}

/// A queued workflow does not accrue attempts, because it is not running.
#[tokio::test]
async fn queued_workflows_do_not_accrue_attempts() {
    let (sys, _db) = sysdb().await;
    let mut r = record("wf-queued");
    r.status = WorkflowStatus::Enqueued;
    r.queue_name = Some("orders".to_owned());

    let first = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert_eq!(first.recovery_attempts, 0, "enqueueing is not an attempt");

    let again = sys
        .init_workflow_status(InitWorkflowStatus {
            is_recovery: true,
            ..InitWorkflowStatus::new(&r)
        })
        .await
        .unwrap();
    assert_eq!(
        again.recovery_attempts, 0,
        "a queued row stays at zero even when forced",
    );
}

/// Passing the recovery limit parks the workflow instead of running it again.
///
/// The limit is what stops a workflow that crashes its executor from taking down every
/// executor in turn. Parking is deliberate rather than an error state: the row stays, and can
/// be resumed once whatever made it fail is fixed.
#[tokio::test]
async fn exceeding_the_recovery_limit_parks_the_workflow() {
    let (sys, _db) = sysdb().await;
    let r = record("wf-dlq");
    sys.init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();

    // Each recovery is a different attempt, so each carries its own owner.
    let mut last = Ok(());
    for attempt in 0..5 {
        let owner = format!("owner-{attempt}");
        last = sys
            .init_workflow_status(InitWorkflowStatus {
                max_recovery_attempts: Some(2),
                owner_xid: Some(&owner),
                is_recovery: true,
                ..InitWorkflowStatus::new(&r)
            })
            .await
            .map(|_| ());
        if last.is_err() {
            break;
        }
    }

    let err = last.expect_err("the workflow should have been parked");
    assert!(
        matches!(err, Error::MaxRecoveryAttemptsExceeded { limit: 2, .. }),
        "expected a dead-letter error, got {err:?}",
    );

    let read = sys.get_workflow_status("wf-dlq").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::MaxRecoveryAttemptsExceeded);
    assert_eq!(read.queue_name, None, "parking clears the queue assignment");
}

/// A second owner must not run a workflow someone else holds.
///
/// `executor_id` cannot decide this — it defaults to `"local"` and so collides between
/// processes on one machine. `owner_xid` is per-attempt, which is the point of migration 7.
#[tokio::test]
async fn a_second_owner_records_but_does_not_execute() {
    let (sys, _db) = sysdb().await;
    let r = record("wf-owned");

    let first = sys
        .init_workflow_status(InitWorkflowStatus {
            owner_xid: Some("owner-a"),
            ..InitWorkflowStatus::new(&r)
        })
        .await
        .unwrap();
    assert!(first.should_execute);

    let second = sys
        .init_workflow_status(InitWorkflowStatus {
            owner_xid: Some("owner-b"),
            ..InitWorkflowStatus::new(&r)
        })
        .await
        .unwrap();
    assert!(
        !second.should_execute,
        "another owner holds this workflow, so running it would be a second execution",
    );

    // Recovery is exactly the case where taking it over is correct.
    let recovering = sys
        .init_workflow_status(InitWorkflowStatus {
            owner_xid: Some("owner-b"),
            is_recovery: true,
            ..InitWorkflowStatus::new(&r)
        })
        .await
        .unwrap();
    assert!(recovering.should_execute, "a recovery may claim the row");
}

/// The first writer decides the payload format, and later attempts are told what it is.
#[tokio::test]
async fn the_stored_serialization_is_reported_back() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow_status(InitWorkflowStatus::new(&record("wf-fmt")))
        .await
        .unwrap();

    let mut later = record("wf-fmt");
    later.serialization = Some("rust_serde".to_owned());
    let outcome = sys
        .init_workflow_status(InitWorkflowStatus::new(&later))
        .await
        .unwrap();

    assert_eq!(
        outcome.serialization.as_deref(),
        Some("portable_json"),
        "the stored format wins; the payloads already there are in it",
    );
}

/// A caller that supplies no owner still gets one, and still owns what it created.
///
/// The identity has to exist for the guard to mean anything — without one, every attempt would
/// look like it belonged to nobody, and a second executor would happily run a workflow the
/// first is already running.
#[tokio::test]
async fn an_owner_is_generated_when_none_is_supplied() {
    let (sys, _db) = sysdb().await;
    let r = record("wf-auto-owner");

    let first = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert!(first.should_execute, "the creator owns what it created");

    // A second attempt generates a different identity, so it must not execute.
    let second = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert!(
        !second.should_execute,
        "a fresh attempt with a new identity must not claim a workflow someone else holds",
    );
}

/// The first outcome wins; a later one is reported as lost rather than applied.
///
/// Two executors can believe they own the same workflow, one having recovered it from the
/// other. Whichever finishes second must not overwrite the first result.
#[tokio::test]
async fn only_the_first_outcome_is_recorded() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow_status(InitWorkflowStatus::new(&record("wf-race")))
        .await
        .unwrap();

    let first = sys
        .update_workflow_outcome("wf-race", WorkflowStatus::Success, Some("\"winner\""), None)
        .await
        .expect("the first write should succeed");
    assert_eq!(first, OutcomeWrite::Recorded);

    let second = sys
        .update_workflow_outcome("wf-race", WorkflowStatus::Error, None, Some("\"loser\""))
        .await
        .expect("the second write should not error");
    assert_eq!(
        second,
        OutcomeWrite::AlreadyFinished,
        "the loser should learn it lost",
    );

    let read = sys.get_workflow_status("wf-race").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Success);
    assert_eq!(read.output.as_deref(), Some("\"winner\""));
    assert_eq!(read.error, None, "the loser's error must not have landed");
    assert!(read.completed_at.is_some(), "completion should be stamped");
}

/// Recording an outcome for a workflow that does not exist reports the loss, not an error.
#[tokio::test]
async fn recording_an_outcome_for_a_missing_workflow_is_not_an_error() {
    let (sys, _db) = sysdb().await;
    let result = sys
        .update_workflow_outcome("never-existed", WorkflowStatus::Success, None, None)
        .await
        .expect("a missing row should not be an error");
    assert_eq!(result, OutcomeWrite::AlreadyFinished);
}

/// The trait is usable behind a pointer, which is what keeps a second backend droppable in.
#[tokio::test]
async fn the_trait_is_object_safe() {
    let (sys, _db) = sysdb().await;
    let dynamic: &dyn SystemDatabase = &sys;
    dynamic
        .init_workflow_status(InitWorkflowStatus::new(&record("wf-dyn")))
        .await
        .unwrap();
    assert!(
        dynamic
            .get_workflow_status("wf-dyn")
            .await
            .unwrap()
            .is_some()
    );
}
