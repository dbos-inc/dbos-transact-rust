//! The system database surface, against real databases.

use dbos::sysdb::postgres::{Config, PostgresSystemDatabase, Settings};
use dbos::sysdb::retry::RetryPolicy;
use dbos::sysdb::types::{
    Applications, AwaitedOutcome, BlockingCaller, Change, Debounce, DebounceRequest, EncodedValue,
    Fork, ForkOptions, ForkPoint, Message, NewQueue, NewSchedule, NewWorkflow, OnExistingQueue,
    Outcome, OutcomeWrite, QueueRecord, QueueUpdate, RateLimit, RenameBatching, RenameFrom,
    ScheduleFilter, ScheduleStatus, ScheduleUpdate, StepTiming, Submission, Timestamp,
    WorkflowDelay, WorkflowFilter, WorkflowRecord, WorkflowStatus, WrittenBy,
};
use dbos::sysdb::{BackendErrorKind, Error, INTERNAL_QUEUE, SystemDatabase};

use dbos_test_support as support;
use dbos_test_support::{Backend, test_database};

fn workflow(id: &str) -> NewWorkflow<'_> {
    NewWorkflow {
        name: Some("checkout"),
        input: Some(r#"{"positionalArgs":[1],"namedArgs":{}}"#),
        serialization: Some("portable_json"),
        executor_id: Some("local"),
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    }
}

async fn sysdb() -> (PostgresSystemDatabase, support::TestDatabase) {
    let db = test_database().await;
    let pool = db.pool().await;
    (
        PostgresSystemDatabase::from_pool(pool, &Settings::default()),
        db,
    )
}

/// A workflow round-trips through the database unchanged.
#[tokio::test]
async fn a_workflow_round_trips() {
    let (sys, _db) = sysdb().await;
    let written = sys
        .init_workflow(&workflow("wf-1"), None, Submission::Fresh)
        .await
        .expect("insert failed");
    assert_eq!(written.status, WorkflowStatus::Pending);
    assert_eq!(
        written.recovery_attempts, 1,
        "starting a workflow counts as an attempt; enqueueing would not",
    );
    assert!(written.should_execute);

    let read = sys
        .get_workflow("wf-1")
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
    let mut r = workflow("wf-opaque");
    // Deliberately not valid JSON, to show nothing here parses it.
    r.input = Some("not json at all \u{1F600} '\"; --");
    sys.init_workflow(&r, None, Submission::Fresh)
        .await
        .expect("insert failed");

    let read = sys.get_workflow("wf-opaque").await.unwrap().unwrap();
    assert_eq!(read.input.as_deref(), r.input);
}

/// An unknown id reads as absent rather than erroring.
#[tokio::test]
async fn a_missing_workflow_reads_as_none() {
    let (sys, _db) = sysdb().await;
    assert!(sys.get_workflow("nobody-here").await.unwrap().is_none());
}

/// Inserting the same id twice leaves the first row alone.
///
/// A retried enqueue must not reset a workflow that is already running or finished, so the
/// second insert reports what is actually stored rather than what it tried to store.
#[tokio::test]
async fn resubmitting_reconciles_and_a_different_function_is_rejected() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-dup"), None, Submission::Fresh)
        .await
        .unwrap();

    // The same id running a different function is a programming error, not a race.
    let mut different = workflow("wf-dup");
    different.name = Some("a-different-function");
    let err = sys
        .init_workflow(&different, None, Submission::Fresh)
        .await
        .expect_err("a different function under the same id should be rejected");
    assert!(
        matches!(err, Error::ConflictingWorkflow { .. }),
        "expected a conflict, got {err:?}",
    );

    // Re-submitting the same workflow is fine, and the original row stands.
    let again = sys
        .init_workflow(&workflow("wf-dup"), None, Submission::Fresh)
        .await
        .expect("re-submitting the same workflow should succeed");
    assert_eq!(again.status, WorkflowStatus::Pending);
    let read = sys.get_workflow("wf-dup").await.unwrap().unwrap();
    assert_eq!(read.application_version.as_deref(), Some("v1"));
}

/// Recovery attempts count, but only for the attempts that should count.
///
/// A fresh start records one. A recovery or dequeue adds another. A plain re-submission does
/// not, or a client retrying its own enqueue would burn through the dead-letter budget.
#[tokio::test]
async fn only_recoveries_and_dequeues_count_as_attempts() {
    let (sys, _db) = sysdb().await;
    let r = workflow("wf-attempts");

    let first = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert_eq!(first.recovery_attempts, 1);

    let plain = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert_eq!(
        plain.recovery_attempts, 1,
        "a re-submission is not an attempt"
    );

    let recovered = sys
        .init_workflow(&r, None, Submission::Recovery)
        .await
        .unwrap();
    assert_eq!(recovered.recovery_attempts, 2, "a recovery is an attempt");
}

/// The initial status follows from the queue and the delay, and is not the caller's to choose.
///
/// Java derives it the same way. A caller that could name the status could enqueue a workflow
/// as `SUCCESS` and have it never run.
#[tokio::test]
async fn the_initial_status_is_derived_from_the_queue_and_delay() {
    let (sys, _db) = sysdb().await;
    let cases = [
        (None, None, WorkflowStatus::Pending),
        (Some("orders"), None, WorkflowStatus::Enqueued),
        (
            Some("orders"),
            Some(std::time::Duration::from_secs(5)),
            WorkflowStatus::Delayed,
        ),
        // A delay without a queue is still PENDING: nothing dequeues it, so nothing waits.
        (
            None,
            Some(std::time::Duration::from_secs(5)),
            WorkflowStatus::Pending,
        ),
    ];
    for (index, (queue, delay, expected)) in cases.into_iter().enumerate() {
        // The id has to outlive the borrow now that `NewWorkflow` holds `&str`.
        let id = format!("wf-status-{index}");
        let wf = NewWorkflow {
            queue_name: queue,
            delay,
            ..workflow(&id)
        };
        assert_eq!(
            wf.initial_status(),
            expected,
            "case {index} derived wrongly"
        );
        let init = sys
            .init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
        assert_eq!(init.status, expected, "case {index} stored wrongly");
    }
}

/// A queued workflow does not accrue attempts, because it is not running.
#[tokio::test]
async fn queued_workflows_do_not_accrue_attempts() {
    let (sys, _db) = sysdb().await;
    let mut r = workflow("wf-queued");
    r.queue_name = Some("orders");

    let first = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert_eq!(first.recovery_attempts, 0, "enqueueing is not an attempt");

    let again = sys
        .init_workflow(&r, None, Submission::Recovery)
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
    let r = workflow("wf-dlq");
    sys.init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();

    // Each recovery is a separate attempt, and so gets its own owner identity.
    let mut last = Ok(());
    for _ in 0..5 {
        last = sys
            .init_workflow(&r, Some(2), Submission::Recovery)
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

    let read = sys.get_workflow("wf-dlq").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::MaxRecoveryAttemptsExceeded);
    assert_eq!(read.queue_name, None, "parking clears the queue assignment");
}

/// A second owner must not run a workflow someone else holds.
///
/// `executor_id` cannot decide this — it defaults to `"local"` and so collides between
/// processes on one machine. `owner_xid` is per-attempt, which is the point of migration 7.
/// Each call generates its own, so two calls here are two owners without saying so.
#[tokio::test]
async fn a_second_owner_records_but_does_not_execute() {
    let (sys, _db) = sysdb().await;
    let r = workflow("wf-owned");

    let first = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert!(first.should_execute);

    let second = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert!(
        !second.should_execute,
        "another owner holds this workflow, so running it would be a second execution",
    );

    // Recovery is exactly the case where taking it over is correct.
    let recovering = sys
        .init_workflow(&r, None, Submission::Recovery)
        .await
        .unwrap();
    assert!(recovering.should_execute, "a recovery may claim the row");
}

/// The first writer decides the payload format, and later attempts are told what it is.
#[tokio::test]
async fn the_stored_serialization_is_reported_back() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-fmt"), None, Submission::Fresh)
        .await
        .unwrap();

    let mut later = workflow("wf-fmt");
    later.serialization = Some("rust_serde");
    let outcome = sys
        .init_workflow(&later, None, Submission::Fresh)
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
    let r = workflow("wf-auto-owner");

    let first = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert!(first.should_execute, "the creator owns what it created");

    // A second attempt generates a different identity, so it must not execute.
    let second = sys
        .init_workflow(&r, None, Submission::Fresh)
        .await
        .unwrap();
    assert!(
        !second.should_execute,
        "a fresh attempt with a new identity must not claim a workflow someone else holds",
    );
}

/// Every field a caller can set is written, and comes back on the row.
///
/// [`NewWorkflow`] has 24 fields and most tests set a handful. Without this, a field could be
/// dropped from the `INSERT` and nothing would notice — which is exactly what happened while
/// this test was being written.
#[tokio::test]
async fn every_settable_field_round_trips() {
    let (sys, _db) = sysdb().await;
    let written = NewWorkflow {
        class_name: Some("Checkout"),
        config_name: Some("primary"),
        queue_name: Some("orders"),
        deduplication_id: Some("dedup-key"),
        priority: 7,
        queue_partition_key: Some("eu-west"),
        delay: Some(std::time::Duration::from_secs(60)),
        is_debounced: true,
        debounce_deadline: Some(Timestamp::from_epoch_ms(1_700_000_020_000)),
        timeout: Some(std::time::Duration::from_secs(30)),
        deadline: Some(Timestamp::from_epoch_ms(1_700_000_030_000)),
        application_id: Some("app-1"),
        authenticated_user: Some("alice"),
        authenticated_roles: vec!["admin", "auditor"],
        assumed_role: Some("admin"),
        parent_workflow_id: Some("wf-parent"),
        schedule_name: Some("nightly"),
        attributes: Some(r#"{"tenant": "acme"}"#),
        ..workflow("wf-all-fields")
    };
    let before = Timestamp::now();
    let init = sys
        .init_workflow(&written, None, Submission::Fresh)
        .await
        .expect("insert failed");

    // A queue plus a delay is DELAYED — derived here, not offered by the caller.
    assert_eq!(init.status, WorkflowStatus::Delayed);
    assert_eq!(init.deadline, written.deadline);

    let read = sys.get_workflow("wf-all-fields").await.unwrap().unwrap();

    assert_eq!(read.workflow_id, "wf-all-fields");
    assert_eq!(read.name.as_deref(), written.name);
    assert_eq!(read.class_name.as_deref(), written.class_name);
    assert_eq!(read.config_name.as_deref(), written.config_name);
    assert_eq!(read.input.as_deref(), written.input);
    assert_eq!(read.serialization.as_deref(), written.serialization);
    assert_eq!(read.queue_name.as_deref(), written.queue_name);
    assert_eq!(read.deduplication_id.as_deref(), written.deduplication_id);
    assert_eq!(read.priority, written.priority);
    assert_eq!(
        read.queue_partition_key.as_deref(),
        written.queue_partition_key
    );
    assert!(read.is_debounced);
    assert_eq!(read.debounce_deadline, written.debounce_deadline);
    assert_eq!(read.timeout, written.timeout);
    assert_eq!(read.deadline, written.deadline);
    assert_eq!(read.executor_id.as_deref(), written.executor_id);
    assert_eq!(
        read.application_version.as_deref(),
        written.application_version
    );
    assert_eq!(read.application_id.as_deref(), written.application_id);
    assert_eq!(
        read.authenticated_user.as_deref(),
        written.authenticated_user
    );
    // The roles round-trip as a list: the JSON encoding is this layer's, not the caller's.
    assert_eq!(read.authenticated_roles, written.authenticated_roles);
    assert_eq!(read.assumed_role.as_deref(), written.assumed_role);
    assert_eq!(
        read.parent_workflow_id.as_deref(),
        written.parent_workflow_id
    );
    assert_eq!(read.schedule_name.as_deref(), written.schedule_name);
    // `jsonb` normalises whitespace, so compare the parsed shape rather than the text.
    assert!(
        read.attributes
            .as_deref()
            .unwrap_or_default()
            .contains("acme"),
        "attributes should survive, got {:?}",
        read.attributes,
    );

    // The delay is a duration in, an instant out, stamped against the database layer's clock.
    let delay_until = read.delay_until.expect("a delay should have been stamped");
    let offset = delay_until.as_epoch_ms() - before.as_epoch_ms();
    assert!(
        (60_000..70_000).contains(&offset),
        "delay_until should be ~60s from now, was {offset}ms",
    );

    // Columns the caller cannot set, and which creation must therefore leave alone.
    assert_eq!(read.output, None);
    assert_eq!(read.error, None);
    assert_eq!(read.started_at, None);
    assert_eq!(read.completed_at, None);
    assert_eq!(read.forked_from, None);
    assert!(!read.was_forked_from);
    assert!(!read.rate_limited);
    assert!(
        read.owner_xid.is_some(),
        "an owner should have been stamped"
    );
    assert!(read.created_at.as_epoch_ms() >= before.as_epoch_ms());
    assert_eq!(read.created_at, read.updated_at);
}

/// The first outcome wins; a later one is reported as lost rather than applied.
///
/// Two executors can believe they own the same workflow, one having recovered it from the
/// other. Whichever finishes second must not overwrite the first result.
#[tokio::test]
async fn only_the_first_outcome_is_recorded() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-race"), None, Submission::Fresh)
        .await
        .unwrap();

    let first = sys
        .record_workflow_outcome("wf-race", Outcome::Output(Some("\"winner\"")))
        .await
        .expect("the first write should succeed");
    assert_eq!(first, OutcomeWrite::Recorded);

    let second = sys
        .record_workflow_outcome("wf-race", Outcome::Error("\"loser\""))
        .await
        .expect("the second write should not error");
    assert_eq!(
        second,
        OutcomeWrite::AlreadyFinished,
        "the loser should learn it lost",
    );

    let read = sys.get_workflow("wf-race").await.unwrap().unwrap();
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
        .record_workflow_outcome("never-existed", Outcome::Output(None))
        .await
        .expect("a missing row should not be an error");
    assert_eq!(result, OutcomeWrite::AlreadyFinished);
}

// ==================== await_workflow_result ====================

/// A brisk poll: these tests are about what a wait returns, not how often it looks.
const BRISK_POLL: std::time::Duration = std::time::Duration::from_millis(50);
/// Long enough to prove a wait is waiting, short enough not to pad the suite.
const BRIEFLY: std::time::Duration = std::time::Duration::from_millis(300);

/// Both outcomes a run can record come back as values, the failure included.
#[tokio::test]
async fn an_await_returns_a_recorded_outcome() {
    let (sys, _db) = sysdb().await;
    for id in ["wf-ok", "wf-bad"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    sys.record_workflow_outcome("wf-ok", Outcome::Output(Some("\"done\"")))
        .await
        .unwrap();
    assert_eq!(
        sys.await_workflow_result("wf-ok", BRISK_POLL)
            .await
            .unwrap(),
        AwaitedOutcome::Succeeded {
            output: Some("\"done\"".to_owned()),
            // Stamped by `init_workflow` from the workflow's own input encoding.
            serialization: Some("portable_json".to_owned()),
        }
    );

    // Not raised: this layer does not deserialize payloads, so a failure is data like any other.
    sys.record_workflow_outcome("wf-bad", Outcome::Error("\"boom\""))
        .await
        .unwrap();
    assert_eq!(
        sys.await_workflow_result("wf-bad", BRISK_POLL)
            .await
            .unwrap(),
        AwaitedOutcome::Failed {
            error: "\"boom\"".to_owned(),
            serialization: Some("portable_json".to_owned()),
        }
    );
}

/// A void return is a success, not an absent result.
#[tokio::test]
async fn an_await_reports_a_void_return_as_a_success() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-void"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_workflow_outcome("wf-void", Outcome::Output(None))
        .await
        .unwrap();

    assert!(matches!(
        sys.await_workflow_result("wf-void", BRISK_POLL)
            .await
            .unwrap(),
        AwaitedOutcome::Succeeded { output: None, .. }
    ));
}

/// A workflow still running is waited on, and the wait ends when the outcome lands.
#[tokio::test]
async fn an_await_waits_for_a_workflow_that_has_not_finished() {
    let (sys, db) = sysdb().await;
    sys.init_workflow(&workflow("wf-slow"), None, Submission::Fresh)
        .await
        .unwrap();

    // Recorded by another handle, as the awaiting process never is the running one.
    let finisher = PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default());
    let finishing = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        finisher
            .record_workflow_outcome("wf-slow", Outcome::Output(Some("\"late\"")))
            .await
            .unwrap();
    });

    let started = std::time::Instant::now();
    let settled = sys
        .await_workflow_result("wf-slow", BRISK_POLL)
        .await
        .unwrap();
    let waited = started.elapsed();
    finishing.await.unwrap();

    assert_eq!(
        settled,
        AwaitedOutcome::Succeeded {
            output: Some("\"late\"".to_owned()),
            serialization: Some("portable_json".to_owned()),
        }
    );
    assert!(
        waited >= BRIEFLY,
        "it cannot have read an outcome that had not been recorded"
    );
    assert!(
        waited < std::time::Duration::from_secs(5),
        "waited {waited:?}"
    );
}

/// A cap of one must not starve the waiters it is not currently admitting.
///
/// The whole design of the polling cap rests on a permit covering the *query* and never the wait.
/// Held across the interval sleep instead, one waiter on a workflow that never finishes would hold
/// the only permit for the life of the process, and every other waiter would block behind it — not
/// slowly, but forever.
///
/// The never-finishing waiter is the point. An earlier version of this test finished every
/// workflow, which a permit held across the wait survives comfortably: the waiters merely serialise,
/// each releasing as its own answer arrives. Verified by mutation — hoisting the permit out of the
/// retried region must fail this test, and against that earlier version it did not.
#[tokio::test]
async fn a_waiter_that_never_finishes_does_not_starve_the_others() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            polling_concurrency: Some(1),
            ..Settings::default()
        },
    ));

    const OTHERS: usize = 4;
    let ids: Vec<String> = (0..OTHERS).map(|i| format!("wf-capped-{i}")).collect();
    for id in std::iter::once(&"wf-never".to_owned()).chain(ids.iter()) {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    // First, and given a head start, so it is the one holding the permit if the permit is held.
    let forever = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move { sys.await_workflow_result("wf-never", BRISK_POLL).await })
    };
    tokio::time::sleep(BRIEFLY).await;

    let waiting: Vec<_> = ids
        .iter()
        .map(|id| {
            let (sys, id) = (std::sync::Arc::clone(&sys), id.clone());
            tokio::spawn(async move { sys.await_workflow_result(&id, BRISK_POLL).await })
        })
        .collect();
    tokio::time::sleep(BRIEFLY).await;

    let finisher = PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default());
    for id in &ids {
        finisher
            .record_workflow_outcome(id, Outcome::Output(Some("\"done\"")))
            .await
            .unwrap();
    }

    for (id, handle) in ids.iter().zip(waiting) {
        let settled = tokio::time::timeout(std::time::Duration::from_secs(20), handle)
            .await
            .unwrap_or_else(|_| panic!("{id} starved: a permit is being held across a wait"))
            .unwrap()
            .unwrap();
        assert_eq!(
            settled,
            AwaitedOutcome::Succeeded {
                output: Some("\"done\"".to_owned()),
                serialization: Some("portable_json".to_owned()),
            }
        );
    }

    // Still waiting, as it should be, and dropped rather than awaited.
    assert!(!forever.is_finished());
    forever.abort();
}

/// A wait on a closed handle reports rather than hanging, including one parked on a polling permit.
///
/// Run at a cap of one with the permit held by the waiter itself, so this covers the case that once
/// argued for closing the limiter alongside the pool: closing the pool is enough on its own, since
/// every in-flight poll then fails permanently and releases its permit. Verified by mutation — the
/// limiter close was deleted and no test failed, which is why it is not there.
#[tokio::test]
async fn a_wait_on_a_closed_handle_reports_rather_than_hanging() {
    let db = test_database().await;
    // One permit, and it is held by the waiter below, so the second wait can only be ended by the
    // close itself.
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            polling_concurrency: Some(1),
            ..Settings::default()
        },
    ));
    sys.init_workflow(&workflow("wf-closing"), None, Submission::Fresh)
        .await
        .unwrap();

    let waiting = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.await_workflow_result("wf-closing", BRISK_POLL)
                .await
                .map(|_| ())
        })
    };
    // Parked rather than not yet scheduled.
    tokio::time::sleep(BRIEFLY).await;

    sys.close().await;
    let ended = tokio::time::timeout(std::time::Duration::from_secs(20), waiting)
        .await
        .expect("closing the handle left a waiter parked")
        .unwrap();
    assert!(
        ended.is_err(),
        "a wait on a closed handle must report rather than succeed"
    );
}

/// Cancellation is a terminal state no run reports, and it is a value rather than an error.
///
/// Reporting it as `Error::WorkflowCancelled` would say the *waiter* had been cancelled, which is
/// the distinction Python keeps by raising a separate error for the awaited workflow.
#[tokio::test]
async fn an_await_reports_a_cancelled_workflow_as_cancelled() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-doomed"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.cancel_workflows(&["wf-doomed"], false).await.unwrap();

    assert_eq!(
        sys.await_workflow_result("wf-doomed", BRISK_POLL)
            .await
            .unwrap(),
        AwaitedOutcome::Cancelled
    );
}

/// A parked workflow ends the wait too, carrying the attempt count.
///
/// It would otherwise be waited on forever: `MAX_RECOVERY_ATTEMPTS_EXCEEDED` is not terminal — the
/// workflow can still be resumed — but nothing is running to produce an outcome.
#[tokio::test]
async fn an_await_reports_a_parked_workflow_rather_than_waiting_for_it() {
    let (sys, _db) = sysdb().await;
    let parked = workflow("wf-parked");
    sys.init_workflow(&parked, None, Submission::Fresh)
        .await
        .unwrap();
    for _ in 0..5 {
        if sys
            .init_workflow(&parked, Some(2), Submission::Recovery)
            .await
            .is_err()
        {
            break;
        }
    }

    let settled = sys
        .await_workflow_result("wf-parked", BRISK_POLL)
        .await
        .unwrap();
    let AwaitedOutcome::Parked { recovery_attempts } = settled else {
        panic!("expected a parked workflow, got {settled:?}");
    };
    assert!(
        recovery_attempts > 2,
        "the count that passed the limit, got {recovery_attempts}"
    );
}

/// An absent row is reported rather than waited through, and a caller that wants to wait can.
///
/// The references take a `fail_if_missing` flag here and default it to waiting. This does not, so
/// the deleted-mid-wait hang is unreachable for every caller rather than for the one that opts out;
/// the cost is that holding an id from outside this process becomes a loop, which is what the second
/// half of this test is.
#[tokio::test]
async fn an_absent_row_is_reported_and_a_caller_may_still_wait_for_one() {
    let (sys, db) = sysdb().await;

    let err = sys
        .await_workflow_result("never-existed", BRISK_POLL)
        .await
        .expect_err("an id that names nothing has no outcome to wait for");
    assert!(
        matches!(err, Error::NonExistentWorkflow { ref workflow_ids } if workflow_ids == &["never-existed"]),
        "got {err:?}"
    );

    // Enqueued by another process, after this one has already started waiting for it.
    let inserting = tokio::spawn({
        let sys = PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default());
        async move {
            tokio::time::sleep(BRIEFLY).await;
            sys.init_workflow(&workflow("wf-later"), None, Submission::Fresh)
                .await
                .unwrap();
            sys.record_workflow_outcome("wf-later", Outcome::Output(Some("\"arrived\"")))
                .await
                .unwrap();
        }
    });

    // The loop the flag used to be. Readable here because the absence is a value to match on, which
    // is the half of this that is a language difference: in Python the same loop is a `try`/`except`
    // around a poll, and pushing it down into the system database is the more attractive option.
    let settled = loop {
        match sys.await_workflow_result("wf-later", BRISK_POLL).await {
            Err(Error::NonExistentWorkflow { .. }) => tokio::time::sleep(BRISK_POLL).await,
            other => break other.unwrap(),
        }
    };
    inserting.await.unwrap();

    assert_eq!(
        settled,
        AwaitedOutcome::Succeeded {
            output: Some("\"arrived\"".to_owned()),
            serialization: Some("portable_json".to_owned()),
        }
    );
}

/// The trait is usable behind a pointer, which is what keeps a second backend droppable in.
#[tokio::test]
async fn the_trait_is_object_safe() {
    let (sys, _db) = sysdb().await;
    let dynamic: &dyn SystemDatabase = &sys;
    dynamic
        .init_workflow(&workflow("wf-dyn"), None, Submission::Fresh)
        .await
        .unwrap();
    assert!(dynamic.get_workflow("wf-dyn").await.unwrap().is_some());
}

/// A rejected statement comes back at once, with the SQLSTATE the database gave.
///
/// The timeout is the assertion that matters. `42P01 undefined_table` classified as anything
/// but permanent would be retried for ever, and this test would hang rather than fail — so it
/// is given a deadline far shorter than the one-second first backoff.
#[tokio::test]
async fn a_rejected_statement_is_not_retried() {
    let db = test_database().await;
    let pool = db.pool().await;
    // Nothing has migrated this schema, so the table genuinely is not there.
    let sys = PostgresSystemDatabase::from_pool(
        pool,
        &Settings {
            schema: "no_such_schema",
            ..Settings::default()
        },
    );

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sys.get_workflow("wf-1"),
    )
    .await
    .expect("a permanent failure was retried instead of being returned");

    match result {
        Err(Error::Backend(e)) => {
            assert_eq!(e.kind, BackendErrorKind::Permanent);
            assert_eq!(
                e.sqlstate.as_deref(),
                Some("42P01"),
                "the SQLSTATE should survive onto the error, got {e:?}",
            );
        }
        other => panic!("expected a permanent backend error, got {other:?}"),
    }
}

/// A failure to reach the database is a connection failure, and opting out surfaces it.
///
/// With the default policy this call would block until the database came back, which is the
/// trade the retry layer exists to make. The opt-out is what makes the classification testable
/// without waiting for it.
///
/// A real unreachable address rather than a closed pool: the two look alike but are not alike. A
/// database on a port that stops answering may answer again, and waiting is the right thing; a
/// closed pool never reopens, so waiting is forever. See
/// [`operations_after_close_fail_rather_than_hang`].
#[tokio::test]
async fn an_unreachable_database_is_a_connection_failure() {
    let db = test_database().await;
    // Port 1 is reserved and nothing listens there.
    let unreachable = sqlx::pool::PoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(1))
        .connect_lazy_with(db.options().port(1));

    let sys = PostgresSystemDatabase::from_pool(
        unreachable,
        &Settings {
            retry: RetryPolicy {
                retry_connection_errors: false,
                ..RetryPolicy::default()
            },
            ..Settings::default()
        },
    );

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), sys.get_workflow("wf-1"))
        .await
        .expect("the opt-out should have returned rather than retried");

    match result {
        Err(Error::Backend(e)) => assert_eq!(
            e.kind,
            BackendErrorKind::Connection,
            "an unreachable database may yet answer, so it is worth waiting for, got {e:?}",
        ),
        other => panic!("expected a connection error, got {other:?}"),
    }
}

/// A connection killed underneath a live pool is waited out, and the operation still succeeds.
///
/// This is the case the whole layer exists for, and the only one that exercises it end to end:
/// classification, the backoff, and sqlx replacing the dead connection on the second attempt.
/// Java tests the same thing with `ChaosTest.causeChaos`, which is the same `pg_terminate_backend`
/// used here.
///
/// `test_before_acquire(false)` is what makes it deterministic. sqlx otherwise validates a
/// pooled connection before handing it out, so a killed one is replaced silently and the retry
/// never engages — the test would pass while proving nothing.
#[tokio::test]
async fn a_killed_connection_is_waited_out() {
    const TAG: &str = "chaos-waited-out";
    let db = test_database().await;
    let pool = db
        .pool_options()
        .test_before_acquire(false)
        .connect_with(db.options().application_name(TAG))
        .await
        .expect("failed to connect");
    let sys = PostgresSystemDatabase::from_pool(
        pool,
        &Settings {
            retry: RetryPolicy {
                // Capped, not just shortened. With the default 60s ceiling the delays double
                // past the deadline below — 50ms, 100ms, … 12.8s, 25.6s — which passes alone and
                // fails under the parallel Cockroach run, where more attempts miss before the
                // pool settles. Backoff *growth* is unit-tested in `retry`; this test is about
                // recovering at all.
                initial_backoff: std::time::Duration::from_millis(50),
                max_backoff: std::time::Duration::from_millis(200),
                ..RetryPolicy::default()
            },
            ..Settings::default()
        },
    );

    let wf = workflow("wf-chaos");
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .expect("insert failed");

    // Every pooled connection is now dead, and the pool does not know it.
    db.kill_connections(TAG).await;

    let read = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        sys.get_workflow("wf-chaos"),
    )
    .await
    .expect("the retry never recovered")
    .expect("the read should have succeeded on a later attempt");

    assert_eq!(
        read.expect("the workflow should still exist").workflow_id,
        "wf-chaos",
        "the row is durable; only the connection was lost",
    );
}

/// The same kill, with retrying opted out, surfaces the failure instead of waiting.
///
/// This is the guard on the test above. If `kill_connections` stopped landing — a driver change,
/// a Cockroach syntax drift — that test would still pass, having quietly proven nothing. This
/// one fails, because it asserts the error is really there to be retried.
#[tokio::test]
async fn the_opt_out_surfaces_a_killed_connection() {
    const TAG: &str = "chaos-opt-out";
    let db = test_database().await;
    let pool = db
        .pool_options()
        .test_before_acquire(false)
        .connect_with(db.options().application_name(TAG))
        .await
        .expect("failed to connect");
    let sys = PostgresSystemDatabase::from_pool(
        pool,
        &Settings {
            retry: RetryPolicy {
                retry_connection_errors: false,
                ..RetryPolicy::default()
            },
            ..Settings::default()
        },
    );

    let wf = workflow("wf-chaos-optout");
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .expect("insert failed");

    db.kill_connections(TAG).await;

    match sys.get_workflow("wf-chaos-optout").await {
        Err(Error::Backend(e)) => assert_eq!(
            e.kind,
            BackendErrorKind::Connection,
            "a terminated backend should classify as a connection failure, got {e:?}",
        ),
        other => panic!("the connection kill did not land; got {other:?}"),
    }
}

/// Seeds a fixed set of workflows for the filter tests, and returns the database holding them.
///
/// Five workflows chosen so that every filter has both a match and a non-match. Some columns are
/// then set with raw SQL: `forked_from`, `was_forked_from`, `completed_at`, and
/// `started_at_epoch_ms` are not settable at creation in any implementation, but the filters on
/// them are real and are worth proving before fork and execution land.
async fn seeded() -> (PostgresSystemDatabase, support::TestDatabase) {
    let (sys, db) = sysdb().await;

    let seeds = [
        NewWorkflow {
            name: Some("checkout"),
            class_name: Some("Checkout"),
            config_name: Some("primary"),
            application_version: Some("v1"),
            executor_id: Some("alpha"),
            authenticated_user: Some("alice"),
            attributes: Some(r#"{"tenant": "acme", "tier": "gold"}"#),
            ..NewWorkflow::new("wf-a")
        },
        NewWorkflow {
            name: Some("refund"),
            application_version: Some("v2"),
            executor_id: Some("beta"),
            authenticated_user: Some("bob"),
            queue_name: Some("orders"),
            deduplication_id: Some("dedup-b"),
            schedule_name: Some("nightly"),
            parent_workflow_id: Some("wf-a"),
            attributes: Some(r#"{"tenant": "globex"}"#),
            ..NewWorkflow::new("wf-b")
        },
        NewWorkflow {
            name: Some("checkout"),
            queue_name: Some("orders"),
            delay: Some(std::time::Duration::from_secs(300)),
            is_debounced: true,
            deduplication_id: Some("dedup-c"),
            ..NewWorkflow::new("wf-c")
        },
        NewWorkflow {
            name: Some("audit"),
            queue_name: Some("reports"),
            parent_workflow_id: Some("wf-a"),
            ..NewWorkflow::new("wf-d")
        },
        // A `%` in the id, to prove a prefix filter treats it as text and not a wildcard.
        NewWorkflow {
            name: Some("audit"),
            ..NewWorkflow::new("other-100%-done")
        },
    ];
    for wf in &seeds {
        sys.init_workflow(wf, None, Submission::Fresh)
            .await
            .expect("seed insert failed");
    }

    // Columns no caller can set at creation.
    let mut conn = db.admin_connection().await;
    for (sql, id) in [
        (
            "UPDATE dbos.workflow_status SET forked_from = 'wf-a', was_forked_from = false WHERE workflow_uuid = $1",
            "wf-c",
        ),
        (
            "UPDATE dbos.workflow_status SET was_forked_from = true WHERE workflow_uuid = $1",
            "wf-a",
        ),
        (
            "UPDATE dbos.workflow_status SET completed_at = 5000, started_at_epoch_ms = 4000 WHERE workflow_uuid = $1",
            "wf-b",
        ),
        (
            "UPDATE dbos.workflow_status SET completed_at = 9000, started_at_epoch_ms = 8000 WHERE workflow_uuid = $1",
            "wf-d",
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .execute(&mut conn)
            .await
            .expect("seed update failed");
    }

    (sys, db)
}

/// Ids the filter selects, in the order the query returned them.
async fn ids(
    sys: &PostgresSystemDatabase,
    filter: &dbos::sysdb::types::WorkflowFilter<'_>,
) -> Vec<String> {
    sys.list_workflows(filter)
        .await
        .expect("list failed")
        .into_iter()
        .map(|r| r.workflow_id)
        .collect()
}

/// Every filter narrows to what it claims to.
///
/// One test rather than twenty-odd, because the value here is coverage of the *set*: the filters
/// are a union across four implementations that do not agree on it, and one missing `WHERE`
/// clause is exactly the kind of thing a per-filter test suite tends not to be written for.
#[tokio::test]
async fn every_filter_narrows() {
    use dbos::sysdb::types::WorkflowFilter as F;
    let (sys, _db) = seeded().await;

    let cases: Vec<(&str, F, &[&str])> = vec![
        (
            "no filter",
            F::default(),
            &["wf-a", "wf-b", "wf-c", "wf-d", "other-100%-done"],
        ),
        (
            "workflow_ids",
            F {
                workflow_ids: vec!["wf-a", "wf-d"],
                ..F::default()
            },
            &["wf-a", "wf-d"],
        ),
        (
            "workflow_id_prefixes",
            F {
                workflow_id_prefixes: vec!["wf-"],
                ..F::default()
            },
            &["wf-a", "wf-b", "wf-c", "wf-d"],
        ),
        // The `%` is data, not a wildcard: a naive LIKE would match every id here.
        (
            "prefix with a wildcard character",
            F {
                workflow_id_prefixes: vec!["other-100%"],
                ..F::default()
            },
            &["other-100%-done"],
        ),
        (
            "names",
            F {
                names: vec!["checkout"],
                ..F::default()
            },
            &["wf-a", "wf-c"],
        ),
        (
            "class_names",
            F {
                class_names: vec!["Checkout"],
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "config_names",
            F {
                config_names: vec!["primary"],
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "status",
            F {
                status: vec![WorkflowStatus::Delayed],
                ..F::default()
            },
            &["wf-c"],
        ),
        (
            "application_versions",
            F {
                application_versions: vec!["v2"],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "executor_ids",
            F {
                executor_ids: vec!["alpha"],
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "authenticated_users",
            F {
                authenticated_users: vec!["bob"],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "queue_names",
            F {
                queue_names: vec!["orders"],
                ..F::default()
            },
            &["wf-b", "wf-c"],
        ),
        (
            "queues_only",
            F {
                queues_only: true,
                ..F::default()
            },
            &["wf-b", "wf-c", "wf-d"],
        ),
        (
            "schedule_names",
            F {
                schedule_names: vec!["nightly"],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "deduplication_ids",
            F {
                deduplication_ids: vec!["dedup-c"],
                ..F::default()
            },
            &["wf-c"],
        ),
        (
            "is_debounced",
            F {
                is_debounced: Some(true),
                ..F::default()
            },
            &["wf-c"],
        ),
        (
            "parent_workflow_ids",
            F {
                parent_workflow_ids: vec!["wf-a"],
                ..F::default()
            },
            &["wf-b", "wf-d"],
        ),
        (
            "has_parent",
            F {
                has_parent: Some(true),
                ..F::default()
            },
            &["wf-b", "wf-d"],
        ),
        (
            "has_parent = false",
            F {
                has_parent: Some(false),
                ..F::default()
            },
            &["wf-a", "wf-c", "other-100%-done"],
        ),
        (
            "forked_from",
            F {
                forked_from: vec!["wf-a"],
                ..F::default()
            },
            &["wf-c"],
        ),
        (
            "was_forked_from",
            F {
                was_forked_from: Some(true),
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "completed_after",
            F {
                completed_after: Some(Timestamp::from_epoch_ms(6000)),
                ..F::default()
            },
            &["wf-d"],
        ),
        (
            "completed_before",
            F {
                completed_before: Some(Timestamp::from_epoch_ms(6000)),
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "started_after",
            F {
                started_after: Some(Timestamp::from_epoch_ms(6000)),
                ..F::default()
            },
            &["wf-d"],
        ),
        (
            "started_before",
            F {
                started_before: Some(Timestamp::from_epoch_ms(6000)),
                ..F::default()
            },
            &["wf-b"],
        ),
        // Containment, not equality: `wf-a` has a second key beyond the one asked for.
        (
            "attributes",
            F {
                attributes: Some(r#"{"tenant": "acme"}"#),
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "combined filters are ANDed",
            F {
                names: vec!["checkout"],
                queues_only: true,
                ..F::default()
            },
            &["wf-c"],
        ),
    ];

    for (label, filter, expected) in cases {
        let mut got = ids(&sys, &filter).await;
        got.sort();
        let mut want: Vec<String> = expected.iter().map(|s| (*s).to_owned()).collect();
        want.sort();
        assert_eq!(got, want, "filter `{label}` selected the wrong workflows");
    }
}

/// Ordering, limit, and offset page through the results.
#[tokio::test]
async fn results_are_ordered_and_pageable() {
    use dbos::sysdb::types::WorkflowFilter as F;
    let (sys, _db) = seeded().await;
    let only_wf = F {
        workflow_id_prefixes: vec!["wf-"],
        ..F::default()
    };

    // Seeded in order, and `created_at` is stamped at insert, so oldest-first is insertion order.
    let ascending = ids(&sys, &only_wf).await;
    assert_eq!(ascending, ["wf-a", "wf-b", "wf-c", "wf-d"]);

    let descending = ids(
        &sys,
        &F {
            sort_desc: true,
            ..only_wf.clone()
        },
    )
    .await;
    assert_eq!(descending, ["wf-d", "wf-c", "wf-b", "wf-a"]);

    let page = ids(
        &sys,
        &F {
            limit: Some(2),
            offset: Some(1),
            ..only_wf.clone()
        },
    )
    .await;
    assert_eq!(
        page,
        ["wf-b", "wf-c"],
        "limit and offset should page in sort order"
    );

    // Offset without limit is legal, and is how a caller skips a known prefix.
    let rest = ids(
        &sys,
        &F {
            offset: Some(3),
            ..only_wf
        },
    )
    .await;
    assert_eq!(rest, ["wf-d"]);
}

/// Declining a payload leaves it absent rather than changing which rows come back.
#[tokio::test]
async fn payloads_can_be_left_unloaded() {
    use dbos::sysdb::types::WorkflowFilter as F;
    // `_db` rather than `db`: the lease has to outlive every query below. Dropping it early
    // returns the database to the pool while this test is still reading, so another test leases it
    // and `reset()`s it — deleting these rows underneath the assertions. That is what this test did
    // until 2026-08-18, and it hung CI for six hours.
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        input: Some(r#"{"positionalArgs":[1]}"#),
        ..NewWorkflow::new("wf-payload")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_workflow_outcome("wf-payload", Outcome::Output(Some("42")))
        .await
        .unwrap();

    let loaded = &sys.list_workflows(&F::default()).await.unwrap()[0];
    assert_eq!(loaded.input.as_deref(), Some(r#"{"positionalArgs":[1]}"#));
    assert_eq!(loaded.output.as_deref(), Some("42"));

    let bare = &sys
        .list_workflows(&F {
            load_input: false,
            load_output: false,
            ..F::default()
        })
        .await
        .unwrap()[0];
    assert_eq!(bare.workflow_id, "wf-payload", "the row is still returned");
    assert_eq!(bare.input, None, "input was not asked for");
    assert_eq!(bare.output, None, "output was not asked for");
    assert_eq!(
        bare.status,
        WorkflowStatus::Success,
        "declining payloads must not affect any other column",
    );
}

/// Cancelling stops a workflow and clears what would let it be picked up again.
#[tokio::test]
async fn cancelling_clears_the_queue_and_the_deduplication_key() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("dedup-1"),
        ..NewWorkflow::new("wf-cancel")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();

    let cancelled = sys.cancel_workflows(&["wf-cancel"], false).await.unwrap();
    assert_eq!(cancelled, ["wf-cancel"]);

    let read = sys.get_workflow("wf-cancel").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Cancelled);
    assert_eq!(
        read.queue_name, None,
        "a cancelled workflow must not dequeue"
    );
    assert_eq!(
        read.deduplication_id, None,
        "the key must be released, or it blocks a later workflow forever",
    );
    assert!(read.completed_at.is_some());

    // The key is genuinely free again, and not merely absent from the row this test read.
    let holders = sys
        .list_workflows(&dbos::sysdb::types::WorkflowFilter {
            deduplication_ids: vec!["dedup-1"],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        holders.is_empty(),
        "the deduplication key should be held by nobody",
    );
}

/// Cancelling an already-cancelled workflow changes nothing and reports nothing.
///
/// Go excludes `CANCELLED` from the guard; Java, Python and TypeScript exclude only `SUCCESS`
/// and `ERROR`, so a second call there rewrites `completed_at` and moves the moment of
/// cancellation. That matters more here than in any of them: this is the only implementation
/// whose return value is *the ids that actually moved*, so re-reporting one would be a lie.
#[tokio::test]
async fn cancelling_twice_does_not_move_the_cancellation() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-twice-cancelled"), None, Submission::Fresh)
        .await
        .unwrap();

    let first = sys
        .cancel_workflows(&["wf-twice-cancelled"], false)
        .await
        .unwrap();
    assert_eq!(first, ["wf-twice-cancelled"]);
    let after_first = sys
        .get_workflow("wf-twice-cancelled")
        .await
        .unwrap()
        .unwrap();

    let second = sys
        .cancel_workflows(&["wf-twice-cancelled"], false)
        .await
        .unwrap();
    assert!(
        second.is_empty(),
        "nothing moved the second time, and the caller is told so",
    );

    let after_second = sys
        .get_workflow("wf-twice-cancelled")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_second.completed_at, after_first.completed_at,
        "the moment of cancellation must not drift on a repeat call",
    );
    assert_eq!(after_second.updated_at, after_first.updated_at);
    assert_eq!(after_second.status, WorkflowStatus::Cancelled);
}

/// A finished workflow keeps its result; cancelling it is a no-op rather than an error.
#[tokio::test]
async fn cancelling_a_finished_workflow_does_not_overwrite_it() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-done"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_workflow_outcome("wf-done", Outcome::Output(Some("42")))
        .await
        .unwrap();

    let cancelled = sys.cancel_workflows(&["wf-done"], false).await.unwrap();
    assert!(
        cancelled.is_empty(),
        "nothing moved, and the caller is told so",
    );

    let read = sys.get_workflow("wf-done").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Success);
    assert_eq!(read.output.as_deref(), Some("42"), "the result survives");
}

/// The cascade reaches grandchildren, not just direct children.
///
/// A three-generation tree, because a two-generation one passes under any of the three
/// implementations' strategies and so proves nothing about depth.
#[tokio::test]
async fn cancelling_children_descends_the_whole_tree() {
    let (sys, _db) = sysdb().await;
    for (id, parent) in [
        ("wf-root", None),
        ("wf-child", Some("wf-root")),
        ("wf-grandchild", Some("wf-child")),
        ("wf-unrelated", None),
    ] {
        let wf = NewWorkflow {
            parent_workflow_id: parent,
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }

    let mut cancelled = sys.cancel_workflows(&["wf-root"], true).await.unwrap();
    cancelled.sort();
    assert_eq!(cancelled, ["wf-child", "wf-grandchild", "wf-root"]);

    let untouched = sys.get_workflow("wf-unrelated").await.unwrap().unwrap();
    assert_eq!(
        untouched.status,
        WorkflowStatus::Pending,
        "the cascade must follow parentage, not cancel everything",
    );

    // Without the flag, only the root moves.
    sys.init_workflow(&workflow("wf-root2"), None, Submission::Fresh)
        .await
        .unwrap();
    let child = NewWorkflow {
        parent_workflow_id: Some("wf-root2"),
        ..NewWorkflow::new("wf-child2")
    };
    sys.init_workflow(&child, None, Submission::Fresh)
        .await
        .unwrap();
    let shallow = sys.cancel_workflows(&["wf-root2"], false).await.unwrap();
    assert_eq!(shallow, ["wf-root2"]);
}

/// Resuming re-enqueues and clears the counters that would sink the next attempt.
#[tokio::test]
async fn resuming_clears_the_attempt_count_and_the_deadline() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        deadline: Some(Timestamp::from_epoch_ms(1_000)),
        ..workflow("wf-resume")
    };
    // Two recoveries, so there is a count to clear.
    for _ in 0..2 {
        sys.init_workflow(&wf, None, Submission::Recovery)
            .await
            .unwrap();
    }

    let resumed = sys.resume_workflows(&["wf-resume"], None).await.unwrap();
    assert_eq!(resumed, ["wf-resume"]);

    let read = sys.get_workflow("wf-resume").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Enqueued);
    assert_eq!(
        read.queue_name.as_deref(),
        Some(dbos::sysdb::INTERNAL_QUEUE),
        "no queue named means the internal one",
    );
    assert_eq!(read.recovery_attempts, 0);
    assert_eq!(
        read.deadline, None,
        "a deadline set before parking would fail the fresh attempt at once",
    );
    assert_eq!(read.completed_at, None);

    // A named queue is honoured.
    sys.init_workflow(&workflow("wf-resume2"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.resume_workflows(&["wf-resume2"], Some("orders"))
        .await
        .unwrap();
    let read = sys.get_workflow("wf-resume2").await.unwrap().unwrap();
    assert_eq!(read.queue_name.as_deref(), Some("orders"));
}

/// Resuming an id that does not exist says so; cancelling one does not.
///
/// The asymmetry is deliberate and is Python's. A zero-row update cannot tell "already finished"
/// from "never existed", and only one of the two operations cares about the difference.
#[tokio::test]
async fn resuming_a_missing_workflow_is_an_error_but_cancelling_one_is_not() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-real"), None, Submission::Fresh)
        .await
        .unwrap();

    match sys.resume_workflows(&["wf-real", "wf-ghost"], None).await {
        Err(Error::NonExistentWorkflow { workflow_ids }) => {
            assert_eq!(workflow_ids, ["wf-ghost"], "only the missing id is named");
        }
        other => panic!("expected a non-existent-workflow error, got {other:?}"),
    }
    let read = sys.get_workflow("wf-real").await.unwrap().unwrap();
    assert_eq!(
        read.status,
        WorkflowStatus::Pending,
        "the batch is rejected before anything moves",
    );

    let cancelled = sys
        .cancel_workflows(&["wf-ghost"], false)
        .await
        .expect("cancelling a missing workflow is a no-op, not an error");
    assert!(cancelled.is_empty());
}

/// Attributes are replaced wholesale, and `None` clears them.
#[tokio::test]
async fn attributes_are_replaced_not_merged() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        attributes: Some(r#"{"tenant": "acme", "tier": "gold"}"#),
        ..NewWorkflow::new("wf-attrs")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();

    sys.update_workflow_attributes("wf-attrs", Some(r#"{"tier": "silver"}"#))
        .await
        .unwrap();
    let read = sys.get_workflow("wf-attrs").await.unwrap().unwrap();
    let attributes = read.attributes.unwrap();
    assert!(attributes.contains("silver"));
    assert!(
        !attributes.contains("acme"),
        "a replacement drops keys the new value omits, got {attributes}",
    );

    sys.update_workflow_attributes("wf-attrs", None)
        .await
        .unwrap();
    let read = sys.get_workflow("wf-attrs").await.unwrap().unwrap();
    assert_eq!(read.attributes, None);
}

/// Values the database would accept but no caller meant are rejected before they are written.
///
/// Java validates the same list in its constructor. The cases here are the ones that would
/// otherwise create a row nobody can act on: a workflow with no id, a queue named `""`, or a
/// timeout that expires before the workflow starts.
#[tokio::test]
async fn empty_and_zero_values_are_rejected() {
    let (sys, _db) = sysdb().await;
    let cases: Vec<(&str, NewWorkflow)> = vec![
        ("workflow_id", NewWorkflow::new("")),
        (
            "queue_name",
            NewWorkflow {
                queue_name: Some(""),
                ..NewWorkflow::new("wf-bad")
            },
        ),
        (
            "name",
            NewWorkflow {
                name: Some(""),
                ..NewWorkflow::new("wf-bad")
            },
        ),
        (
            "deduplication_id",
            NewWorkflow {
                deduplication_id: Some(""),
                ..NewWorkflow::new("wf-bad")
            },
        ),
        (
            "delay",
            NewWorkflow {
                delay: Some(std::time::Duration::ZERO),
                ..NewWorkflow::new("wf-bad")
            },
        ),
        (
            "timeout",
            NewWorkflow {
                timeout: Some(std::time::Duration::ZERO),
                ..NewWorkflow::new("wf-bad")
            },
        ),
    ];

    for (expected_field, wf) in cases {
        match sys.init_workflow(&wf, None, Submission::Fresh).await {
            Err(Error::InvalidInput { field, .. }) => assert_eq!(
                field, expected_field,
                "the wrong field was blamed for {expected_field}",
            ),
            other => panic!("expected {expected_field} to be rejected, got {other:?}"),
        }
    }

    // Nothing was written on the way to any of those errors.
    let all = sys
        .list_workflows(&dbos::sysdb::types::WorkflowFilter::default())
        .await
        .unwrap();
    assert!(all.is_empty(), "a rejected workflow must leave no row");
}

/// An absent auth context is stored as NULL however the caller spelled it.
///
/// TypeScript and Go send `""` rather than null. Storing both spellings would make an
/// `authenticated_user` filter miss rows another SDK wrote — so the empty string is normalised
/// rather than rejected, which is the one place empty is not an error.
#[tokio::test]
async fn empty_auth_fields_are_normalised_to_null() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        authenticated_user: Some(""),
        assumed_role: Some(""),
        ..NewWorkflow::new("wf-auth")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .expect("empty auth fields are normalised, not rejected");

    let read = sys.get_workflow("wf-auth").await.unwrap().unwrap();
    assert_eq!(read.authenticated_user, None);
    assert_eq!(read.assumed_role, None);
}

/// A step's result survives and is found again by position — the whole of durable execution.
#[tokio::test]
async fn a_step_result_round_trips() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-steps"), None, Submission::Fresh)
        .await
        .unwrap();

    assert_eq!(
        sys.check_step("wf-steps", 0, "charge").await.unwrap(),
        None,
        "a step that has not run has no result",
    );

    let timing = StepTiming {
        started_at: Timestamp::from_epoch_ms(1_000),
        completed_at: Timestamp::from_epoch_ms(2_000),
    };
    sys.record_step(
        "wf-steps",
        0,
        "charge",
        Outcome::Output(Some(r#"{"ok":true}"#)),
        Some("portable_json"),
        Some(timing),
    )
    .await
    .unwrap();

    let read = sys
        .check_step("wf-steps", 0, "charge")
        .await
        .unwrap()
        .expect("the step should be recorded");
    assert_eq!(read.output.as_deref(), Some(r#"{"ok":true}"#));
    assert_eq!(read.error, None);
    assert_eq!(read.serialization.as_deref(), Some("portable_json"));
    assert_eq!(read.started_at, Some(Timestamp::from_epoch_ms(1_000)));
    assert_eq!(read.completed_at, Some(Timestamp::from_epoch_ms(2_000)));

    // A position with no row of its own reads as absent even though the workflow has steps.
    // The join is against `function_id`, so it must not fall back to any recorded step.
    assert_eq!(sys.check_step("wf-steps", 1, "refund").await.unwrap(), None,);
}

/// Re-recording with the same completion is this caller's own retry; a different one is a rival.
///
/// This is the reason `completed_at` is a parameter rather than read inside the call. Python
/// says so at its call site: "Outside the retry: the conflict check compares the stored
/// completion to ours."
#[tokio::test]
async fn a_step_recorded_twice_distinguishes_a_retry_from_a_rival() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-twice"), None, Submission::Fresh)
        .await
        .unwrap();

    // Held in a variable, which is exactly what makes the retry below idempotent.
    let timing = StepTiming {
        started_at: Timestamp::from_epoch_ms(4_000),
        completed_at: Timestamp::from_epoch_ms(5_000),
    };
    let first = Outcome::Output(Some("first"));
    sys.record_step("wf-twice", 0, "charge", first, None, Some(timing))
        .await
        .unwrap();

    // The same timing again: an acknowledgement was lost and the caller asked again.
    sys.record_step("wf-twice", 0, "charge", first, None, Some(timing))
        .await
        .expect("a caller's own retry must succeed");

    // A different completion is a second execution, which must not take the step.
    let rival_timing = StepTiming {
        started_at: Timestamp::from_epoch_ms(4_000),
        completed_at: Timestamp::from_epoch_ms(6_000),
    };
    match sys
        .record_step(
            "wf-twice",
            0,
            "charge",
            Outcome::Output(Some("second")),
            None,
            Some(rival_timing),
        )
        .await
    {
        Err(Error::StepAlreadyRecorded { step_id, .. }) => assert_eq!(step_id, 0),
        other => panic!("expected the rival to be rejected, got {other:?}"),
    }

    // The first result stands.
    let read = sys
        .check_step("wf-twice", 0, "charge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.output.as_deref(), Some("first"));
}

/// A replay whose code changed finds the wrong step at a position, and says so.
#[tokio::test]
async fn a_step_recorded_under_another_name_is_rejected() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-drift"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_step("wf-drift", 0, "charge", Outcome::Output(None), None, None)
        .await
        .unwrap();

    match sys.check_step("wf-drift", 0, "refund").await {
        Err(Error::UnexpectedStep {
            expected, recorded, ..
        }) => {
            assert_eq!(expected, "refund");
            assert_eq!(recorded, "charge");
        }
        other => panic!("expected a step-drift error, got {other:?}"),
    }
}

/// A cancelled workflow stops at its next step boundary rather than replaying.
#[tokio::test]
async fn a_cancelled_workflow_refuses_to_replay_steps() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stopped"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.cancel_workflows(&["wf-stopped"], false).await.unwrap();

    match sys.check_step("wf-stopped", 0, "charge").await {
        Err(Error::WorkflowCancelled { workflow_id }) => assert_eq!(workflow_id, "wf-stopped"),
        other => panic!("expected a cancellation error, got {other:?}"),
    }

    // A workflow that never existed is a different error, not the same one.
    match sys.check_step("wf-ghost", 0, "charge").await {
        Err(Error::NonExistentWorkflow { workflow_ids }) => assert_eq!(workflow_ids, ["wf-ghost"]),
        other => panic!("expected a non-existent-workflow error, got {other:?}"),
    }
}

/// Steps come back in execution order, and payloads can be declined.
#[tokio::test]
async fn steps_are_listed_in_execution_order() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-list"), None, Submission::Fresh)
        .await
        .unwrap();
    // Recorded out of order, to prove the ordering comes from `step_id` and not insertion.
    for id in [2, 0, 1] {
        // Both have to outlive the borrow now that `Outcome` holds `&str`.
        let (name, output) = (format!("step-{id}"), format!("out-{id}"));
        sys.record_step(
            "wf-list",
            id,
            &name,
            Outcome::Output(Some(&output)),
            None,
            None,
        )
        .await
        .unwrap();
    }

    let steps = sys
        .list_workflow_steps("wf-list", true, None, None)
        .await
        .unwrap();
    let ids: Vec<i32> = steps.iter().map(|s| s.step_id).collect();
    assert_eq!(ids, [0, 1, 2], "ordered by position, not by when recorded");
    assert_eq!(steps[1].step_name, "step-1");
    assert_eq!(steps[1].output.as_deref(), Some("out-1"));

    let bare = sys
        .list_workflow_steps("wf-list", false, None, None)
        .await
        .unwrap();
    assert_eq!(bare.len(), 3, "declining payloads returns the same rows");
    assert_eq!(bare[1].output, None);
    assert_eq!(bare[1].step_name, "step-1", "other columns still load");

    let page = sys
        .list_workflow_steps("wf-list", true, Some(1), Some(1))
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].step_id, 1);
}

/// A child workflow is found by the position that started it, even before it finishes.
#[tokio::test]
async fn a_child_workflow_is_recorded_against_its_step() {
    let (sys, _db) = sysdb().await;
    for id in ["wf-parent", "wf-kid"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    assert_eq!(
        sys.check_step("wf-parent", 0, "run_child").await.unwrap(),
        None,
        "nothing has started a child at this position",
    );
    sys.record_child_workflow("wf-parent", "wf-kid", 0, "run_child", None)
        .await
        .unwrap();
    // Read back through the ordinary replay check, which is the path Python uses: the child id
    // is a field of the recorded step, not a separate lookup.
    let step = sys
        .check_step("wf-parent", 0, "run_child")
        .await
        .unwrap()
        .expect("the launch is recorded as a step");
    assert_eq!(step.child_workflow_id.as_deref(), Some("wf-kid"));
    assert_eq!(step.output, None);
    assert_eq!(
        step.error, None,
        "the launch carries no result; the child's outcome lives on the child's row",
    );

    // Recording the same child again is a retry, and must succeed however much later it is —
    // the completion time will differ, and the child id is what decides.
    sys.record_child_workflow("wf-parent", "wf-kid", 0, "run_child", None)
        .await
        .expect("re-recording the same child is idempotent");

    // A different child at the same position is nondeterminism in the parent.
    match sys
        .record_child_workflow("wf-parent", "wf-other", 0, "run_child", None)
        .await
    {
        Err(Error::StepAlreadyRecorded { step_id, .. }) => assert_eq!(step_id, 0),
        other => panic!("expected a conflicting child to be rejected, got {other:?}"),
    }

    // An empty child id would wedge the parent on replay, so it is refused.
    match sys
        .record_child_workflow("wf-parent", "", 2, "run_child", None)
        .await
    {
        Err(Error::InvalidInput { field, .. }) => assert_eq!(field, "child_workflow_id"),
        other => panic!("expected an empty child id to be rejected, got {other:?}"),
    }

    // A plain step at another position started no workflow.
    sys.record_step("wf-parent", 1, "charge", Outcome::Output(None), None, None)
        .await
        .unwrap();
    let step = sys
        .check_step("wf-parent", 1, "charge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(step.child_workflow_id, None);
}

/// Recording a step claims the workflow for the executor that ran it.
#[tokio::test]
async fn recording_a_step_restamps_the_executor() {
    let db = test_database().await;
    let pool = db.pool().await;
    let original = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());
    let wf = NewWorkflow {
        executor_id: Some("executor-a"),
        ..workflow("wf-takeover")
    };
    original
        .init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();

    // A second process recovers the workflow and runs a step.
    let recovering = PostgresSystemDatabase::from_pool(
        pool,
        &Settings {
            executor_id: Some("executor-b"),
            ..Settings::default()
        },
    );
    recovering
        .record_step(
            "wf-takeover",
            0,
            "charge",
            Outcome::Output(None),
            None,
            None,
        )
        .await
        .unwrap();

    let read = recovering
        .get_workflow("wf-takeover")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read.executor_id.as_deref(),
        Some("executor-b"),
        "running a step is what makes an executor the owner",
    );
}
/// A step may be recorded with no timings at all.
///
/// A host calling across an FFI boundary may have no timings to offer, and inventing them here
/// would record a duration this layer never measured. Half a pair needs no test: `timing` is an
/// `Option<StepTiming>`, so a start without a finish does not compile.
#[tokio::test]
async fn a_step_may_be_recorded_without_timings() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-timing"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.record_step("wf-timing", 0, "charge", Outcome::Output(None), None, None)
        .await
        .expect("a step without timings is legal");

    let read = sys
        .check_step("wf-timing", 0, "charge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.started_at, None);
    assert_eq!(read.completed_at, None);

    // And a timed step keeps both halves.
    let timing = StepTiming {
        started_at: Timestamp::from_epoch_ms(1_000),
        completed_at: Timestamp::from_epoch_ms(2_000),
    };
    sys.record_step(
        "wf-timing",
        1,
        "refund",
        Outcome::Output(None),
        None,
        Some(timing),
    )
    .await
    .unwrap();
    let read = sys
        .check_step("wf-timing", 1, "refund")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.started_at, Some(Timestamp::from_epoch_ms(1_000)));
    assert_eq!(read.completed_at, Some(Timestamp::from_epoch_ms(2_000)));
}

/// Without a completion time there is nothing to compare, so a duplicate is accepted.
///
/// This is the cost of recording a step with no timings, and it is worth pinning rather than
/// discovering: the retry-versus-rival detection is *bought* with the timestamp, not free.
#[tokio::test]
async fn an_untimed_step_cannot_detect_a_rival() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-untimed"), None, Submission::Fresh)
        .await
        .unwrap();

    let first = Outcome::Output(Some("first"));
    sys.record_step("wf-untimed", 0, "charge", first, None, None)
        .await
        .unwrap();

    let rival = Outcome::Output(Some("second"));
    sys.record_step("wf-untimed", 0, "charge", rival, None, None)
        .await
        .expect("with no completion time there is nothing to compare against");

    // The first result still stands: the insert conflicted and changed nothing.
    let read = sys
        .check_step("wf-untimed", 0, "charge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read.output.as_deref(),
        Some("first"),
        "the duplicate is ignored, not applied",
    );
}

/// A step that raised is recorded as an error, and a void one as an empty success.
///
/// The two are different `StepOutcome` variants and land in different columns, so a replay can
/// tell "returned nothing" from "threw" — which a single nullable payload could not.
#[tokio::test]
async fn a_step_records_either_an_output_or_an_error() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-outcome"), None, Submission::Fresh)
        .await
        .unwrap();

    let failed = Outcome::Error(r#"{"type":"ValueError"}"#);
    sys.record_step("wf-outcome", 0, "charge", failed, None, None)
        .await
        .unwrap();
    let read = sys
        .check_step("wf-outcome", 0, "charge")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.output, None);
    assert_eq!(read.error.as_deref(), Some(r#"{"type":"ValueError"}"#));

    // The default outcome is a void success, which is not the same as a failure.
    sys.record_step("wf-outcome", 1, "notify", Outcome::Output(None), None, None)
        .await
        .unwrap();
    let read = sys
        .check_step("wf-outcome", 1, "notify")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.output, None);
    assert_eq!(read.error, None, "a void return is a success, not an error");
}

/// An executor that loses the checkpoint does not claim the workflow.
///
/// Winning the step is what proves an executor is advancing the workflow. Claiming regardless
/// would leave the row attributed to a process that is not running it — which is what an
/// unconditional re-stamp does, and why the claim follows the insert rather than preceding it.
#[tokio::test]
async fn losing_the_checkpoint_does_not_claim_the_workflow() {
    let db = test_database().await;
    let pool = db.pool().await;
    let winner = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            executor_id: Some("executor-a"),
            ..Settings::default()
        },
    );
    let loser = PostgresSystemDatabase::from_pool(
        pool,
        &Settings {
            executor_id: Some("executor-b"),
            ..Settings::default()
        },
    );

    winner
        .init_workflow(&workflow("wf-race-step"), None, Submission::Fresh)
        .await
        .unwrap();

    let winning = StepTiming {
        started_at: Timestamp::from_epoch_ms(1_000),
        completed_at: Timestamp::from_epoch_ms(2_000),
    };
    winner
        .record_step(
            "wf-race-step",
            0,
            "charge",
            Outcome::Output(Some("first")),
            None,
            Some(winning),
        )
        .await
        .unwrap();
    let read = winner.get_workflow("wf-race-step").await.unwrap().unwrap();
    assert_eq!(read.executor_id.as_deref(), Some("executor-a"));

    // A second executor records the same position with its own completion time, and loses.
    let losing = StepTiming {
        started_at: Timestamp::from_epoch_ms(1_000),
        completed_at: Timestamp::from_epoch_ms(3_000),
    };
    let result = loser
        .record_step(
            "wf-race-step",
            0,
            "charge",
            Outcome::Output(Some("second")),
            None,
            Some(losing),
        )
        .await;
    assert!(
        matches!(result, Err(Error::StepAlreadyRecorded { .. })),
        "expected the loser to be rejected, got {result:?}",
    );

    let read = winner.get_workflow("wf-race-step").await.unwrap().unwrap();
    assert_eq!(
        read.executor_id.as_deref(),
        Some("executor-a"),
        "the loser must not take ownership of a workflow it is not advancing",
    );
}

/// Descendants come back at every depth, and the root is not one of them.
#[tokio::test]
async fn workflow_children_reach_the_whole_tree() {
    let (sys, _db) = sysdb().await;
    for (id, parent) in [
        ("wf-root", None),
        ("wf-child", Some("wf-root")),
        ("wf-grandchild", Some("wf-child")),
        ("wf-stranger", None),
    ] {
        let wf = NewWorkflow {
            parent_workflow_id: parent,
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }

    let mut children = sys.get_workflow_children("wf-root").await.unwrap();
    children.sort();
    assert_eq!(children, ["wf-child", "wf-grandchild"]);
    assert!(
        sys.get_workflow_children("wf-stranger")
            .await
            .unwrap()
            .is_empty(),
    );
}

/// Deleting a workflow takes its steps with it, and optionally its descendants.
#[tokio::test]
async fn deleting_a_workflow_cascades_to_its_rows() {
    let (sys, _db) = sysdb().await;
    for (id, parent) in [("wf-gone", None), ("wf-gone-kid", Some("wf-gone"))] {
        let wf = NewWorkflow {
            parent_workflow_id: parent,
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }
    sys.record_step("wf-gone", 0, "charge", Outcome::Output(None), None, None)
        .await
        .unwrap();

    // Without the flag the child survives, so the cascade is opt-in rather than implied.
    let deleted = sys.delete_workflows(&["wf-gone"], false).await.unwrap();
    assert_eq!(deleted, 1);
    assert!(sys.get_workflow("wf-gone").await.unwrap().is_none());
    assert!(sys.get_workflow("wf-gone-kid").await.unwrap().is_some());

    // The step went with the row: the foreign key cascades, so no second delete is needed.
    let steps = sys
        .list_workflow_steps("wf-gone", true, None, None)
        .await
        .unwrap();
    assert!(steps.is_empty(), "operation_outputs should cascade");

    // With the flag, descendants go too.
    for (id, parent) in [
        ("wf-p", None),
        ("wf-c", Some("wf-p")),
        ("wf-g", Some("wf-c")),
    ] {
        let wf = NewWorkflow {
            parent_workflow_id: parent,
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }
    let deleted = sys.delete_workflows(&["wf-p"], true).await.unwrap();
    assert_eq!(deleted, 3, "the whole tree, at every depth");
}

/// Recovery finds this executor's abandoned work, scoped by application version.
#[tokio::test]
async fn pending_workflows_are_scoped_by_executor_and_version() {
    let (sys, _db) = sysdb().await;
    for (id, executor, version) in [
        ("wf-mine", "alpha", "v1"),
        ("wf-theirs", "beta", "v1"),
        ("wf-old-code", "alpha", "v0"),
    ] {
        let wf = NewWorkflow {
            executor_id: Some(executor),
            application_version: Some(version),
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }
    // A finished workflow is not pending, whoever ran it.
    let done = NewWorkflow {
        executor_id: Some("alpha"),
        application_version: Some("v1"),
        ..NewWorkflow::new("wf-finished")
    };
    sys.init_workflow(&done, None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_workflow_outcome("wf-finished", Outcome::Output(None))
        .await
        .unwrap();

    let pending = sys.get_pending_workflows("alpha", "v1").await.unwrap();
    assert_eq!(
        pending,
        ["wf-mine"],
        "another executor's work, another version's work, and finished work are all excluded",
    );
}

/// A delay can be moved while the workflow is held, and not after it is released.
#[tokio::test]
async fn a_delay_can_be_moved_only_while_the_workflow_is_delayed() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        queue_name: Some("orders"),
        delay: Some(std::time::Duration::from_secs(3600)),
        ..NewWorkflow::new("wf-delayed")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
    assert_eq!(
        sys.get_workflow("wf-delayed")
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkflowStatus::Delayed,
    );

    sys.set_workflow_delay(
        "wf-delayed",
        WorkflowDelay::Until(Timestamp::from_epoch_ms(9_000_000)),
    )
    .await
    .unwrap();
    let read = sys.get_workflow("wf-delayed").await.unwrap().unwrap();
    assert_eq!(read.delay_until, Some(Timestamp::from_epoch_ms(9_000_000)));

    // A relative delay resolves against the database layer's clock.
    let before = Timestamp::now();
    sys.set_workflow_delay(
        "wf-delayed",
        WorkflowDelay::For(std::time::Duration::from_secs(60)),
    )
    .await
    .unwrap();
    let read = sys.get_workflow("wf-delayed").await.unwrap().unwrap();
    let offset = read.delay_until.unwrap().as_epoch_ms() - before.as_epoch_ms();
    assert!(
        (60_000..70_000).contains(&offset),
        "expected ~60s from now, got {offset}ms",
    );

    // A workflow that is not DELAYED is left alone: pushing its delay out cannot recall it.
    sys.init_workflow(&workflow("wf-running"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.set_workflow_delay(
        "wf-running",
        WorkflowDelay::Until(Timestamp::from_epoch_ms(9_000_000)),
    )
    .await
    .unwrap();
    let read = sys.get_workflow("wf-running").await.unwrap().unwrap();
    assert_eq!(read.delay_until, None, "a PENDING workflow is untouched");
    assert_eq!(read.status, WorkflowStatus::Pending);
}

/// Releasing a delayed workflow clears its debounce key, and only its debounce key.
///
/// The key is held only while the workflow is DELAYED. Once released the workflow is committed
/// to running, so a later debounce with the same key must start a fresh workflow rather than
/// bounce this one. Python does this in the same statement and explains it; Java does not.
#[tokio::test]
async fn releasing_a_delayed_workflow_clears_only_the_debounce_key() {
    let (sys, db) = sysdb().await;
    for (id, debounced) in [("wf-debounced", true), ("wf-plain-dedup", false)] {
        let key = format!("key-{id}");
        let wf = NewWorkflow {
            queue_name: Some("orders"),
            delay: Some(std::time::Duration::from_secs(3600)),
            deduplication_id: Some(&key),
            is_debounced: debounced,
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
    }
    // A workflow whose delay has not expired must not move.
    let held = NewWorkflow {
        queue_name: Some("orders"),
        delay: Some(std::time::Duration::from_secs(3600)),
        ..NewWorkflow::new("wf-still-held")
    };
    sys.init_workflow(&held, None, Submission::Fresh)
        .await
        .unwrap();

    // Bring the first two due without waiting an hour.
    let mut conn = db.admin_connection().await;
    sqlx::query(sqlx::AssertSqlSafe(
        "UPDATE dbos.workflow_status SET delay_until_epoch_ms = 1 \
         WHERE workflow_uuid IN ('wf-debounced', 'wf-plain-dedup')",
    ))
    .execute(&mut conn)
    .await
    .unwrap();

    let moved = sys.transition_delayed_workflows().await.unwrap();
    assert_eq!(moved, 2, "only the two whose delay expired");

    let debounced = sys.get_workflow("wf-debounced").await.unwrap().unwrap();
    assert_eq!(debounced.status, WorkflowStatus::Enqueued);
    assert_eq!(
        debounced.deduplication_id, None,
        "a debounce key is released with the workflow",
    );

    let plain = sys.get_workflow("wf-plain-dedup").await.unwrap().unwrap();
    assert_eq!(plain.status, WorkflowStatus::Enqueued);
    assert_eq!(
        plain.deduplication_id.as_deref(),
        Some("key-wf-plain-dedup"),
        "an ordinary deduplication id is not a debounce key and must survive",
    );

    let held = sys.get_workflow("wf-still-held").await.unwrap().unwrap();
    assert_eq!(held.status, WorkflowStatus::Delayed);
}

/// A claimed workflow can be handed back to its queue, but only if it came from one.
#[tokio::test]
async fn a_queued_workflow_can_be_returned_to_its_queue() {
    let (sys, db) = sysdb().await;
    let queued = NewWorkflow {
        queue_name: Some("orders"),
        ..NewWorkflow::new("wf-claimed")
    };
    sys.init_workflow(&queued, None, Submission::Fresh)
        .await
        .unwrap();

    // Claim it as a dequeue would. Done in SQL because the ENQUEUED -> PENDING transition
    // belongs to `start_queued_workflows`, which is task 3.6 and does not exist yet —
    // `init_workflow` derives the status from the queue and so leaves it ENQUEUED.
    let mut conn = db.admin_connection().await;
    sqlx::query(sqlx::AssertSqlSafe(
        "UPDATE dbos.workflow_status SET status = 'PENDING', started_at_epoch_ms = 1000 \
         WHERE workflow_uuid = 'wf-claimed'",
    ))
    .execute(&mut conn)
    .await
    .unwrap();

    assert!(sys.clear_queue_assignment("wf-claimed").await.unwrap());
    let read = sys.get_workflow("wf-claimed").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Enqueued);
    assert_eq!(read.started_at, None, "the claim's start time is cleared");

    // A workflow that never came from a queue has none to go back to.
    sys.init_workflow(&workflow("wf-direct"), None, Submission::Fresh)
        .await
        .unwrap();
    assert!(!sys.clear_queue_assignment("wf-direct").await.unwrap());
}

/// A second workflow cannot take a deduplication key another already holds on the same queue.
///
/// Java raises `DBOSQueueDuplicatedException` here and Python `DBOSQueueDeduplicatedError`; both
/// translate SQLSTATE 23505 rather than letting it surface as a generic database failure. The
/// only unique index this insert can violate is migration 27's partial one on
/// `(queue_name, deduplication_id)`, since `ON CONFLICT (workflow_uuid)` absorbs the other.
#[tokio::test]
async fn a_held_deduplication_key_is_reported_as_such() {
    let (sys, _db) = sysdb().await;
    let holder = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("only-once"),
        ..NewWorkflow::new("wf-holder")
    };
    sys.init_workflow(&holder, None, Submission::Fresh)
        .await
        .unwrap();

    let rival = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("only-once"),
        ..NewWorkflow::new("wf-rival")
    };
    match sys.init_workflow(&rival, None, Submission::Fresh).await {
        Err(Error::QueueDeduplicated {
            workflow_id,
            queue_name,
            deduplication_id,
        }) => {
            assert_eq!(workflow_id, "wf-rival");
            assert_eq!(queue_name, "orders");
            assert_eq!(deduplication_id, "only-once");
        }
        other => panic!("expected a deduplication error, got {other:?}"),
    }

    // The same key on a different queue is a different key: the index is on the pair.
    let elsewhere = NewWorkflow {
        queue_name: Some("reports"),
        deduplication_id: Some("only-once"),
        ..NewWorkflow::new("wf-elsewhere")
    };
    sys.init_workflow(&elsewhere, None, Submission::Fresh)
        .await
        .expect("a deduplication key is scoped to its queue");

    // Re-submitting the *holder* is not a collision — `ON CONFLICT (workflow_uuid)` absorbs it.
    sys.init_workflow(&holder, None, Submission::Fresh)
        .await
        .expect("the holder may re-submit itself");
}

/// Roles cross as a list and are stored as JSON, which is this layer's encoding rather than the
/// caller's.
///
/// Unlike `input` and the outcome payloads — opaque bytes whose format the caller chooses — the
/// column is always a JSON array of strings in every implementation, so the sysdb owns it.
#[tokio::test]
async fn authenticated_roles_are_encoded_by_this_layer() {
    let (sys, db) = sysdb().await;
    let wf = NewWorkflow {
        authenticated_roles: vec!["admin", "auditor \"quoted\""],
        ..NewWorkflow::new("wf-roles")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();

    let read = sys.get_workflow("wf-roles").await.unwrap().unwrap();
    assert_eq!(read.authenticated_roles, ["admin", "auditor \"quoted\""]);

    // The stored form is JSON, so another SDK reading this column sees what it expects.
    let mut conn = db.admin_connection().await;
    let stored: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "SELECT authenticated_roles FROM dbos.workflow_status WHERE workflow_uuid = 'wf-roles'",
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        stored.as_deref(),
        Some(r#"["admin","auditor \"quoted\""]"#),
        "quotes must be escaped, which is why this is not string concatenation",
    );

    // No roles is NULL, not `[]`, so "none" has one representation in the column.
    sys.init_workflow(&NewWorkflow::new("wf-no-roles"), None, Submission::Fresh)
        .await
        .unwrap();
    let stored: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "SELECT authenticated_roles FROM dbos.workflow_status WHERE workflow_uuid = 'wf-no-roles'",
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(stored, None);
    assert!(
        sys.get_workflow("wf-no-roles")
            .await
            .unwrap()
            .unwrap()
            .authenticated_roles
            .is_empty(),
        "a NULL column reads back as no roles",
    );
}

/// A duplicate submission does not take the executor stamp from the executor that owns the work.
///
/// The upsert would otherwise hand `executor_id` to whoever submitted last. That misdirects
/// recovery: `get_pending_workflows` keys on `executor_id`, so the owner's sweep would stop
/// finding the workflow and the submitter's would start.
///
/// A deliberate divergence — Java and TypeScript reach this outcome by rolling their transaction
/// back, Python and Go leave the re-stamp in place.
#[tokio::test]
async fn a_duplicate_submission_does_not_steal_the_executor_stamp() {
    let (sys, _db) = sysdb().await;
    let running = NewWorkflow {
        executor_id: Some("executor-a"),
        ..workflow("wf-owned-elsewhere")
    };
    sys.init_workflow(&running, None, Submission::Fresh)
        .await
        .unwrap();

    // A second process submits the same id. It does not own the workflow, and is told so.
    let duplicate = NewWorkflow {
        executor_id: Some("executor-b"),
        ..workflow("wf-owned-elsewhere")
    };
    let result = sys
        .init_workflow(&duplicate, None, Submission::Fresh)
        .await
        .unwrap();
    assert!(
        !result.should_execute,
        "another owner holds this workflow, so running it would be a second execution",
    );

    let read = sys
        .get_workflow("wf-owned-elsewhere")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read.executor_id.as_deref(),
        Some("executor-a"),
        "the stamp must stay with the executor that is actually running it",
    );
    // Which is what keeps recovery pointed at the right executor.
    assert_eq!(
        sys.get_pending_workflows("executor-a", "v1").await.unwrap(),
        ["wf-owned-elsewhere"],
    );
    assert!(
        sys.get_pending_workflows("executor-b", "v1")
            .await
            .unwrap()
            .is_empty(),
    );

    // Recovery and dequeue *are* being told they own it, so they claim the stamp.
    let recovering = NewWorkflow {
        executor_id: Some("executor-b"),
        ..workflow("wf-owned-elsewhere")
    };
    let result = sys
        .init_workflow(&recovering, None, Submission::Recovery)
        .await
        .unwrap();
    assert!(result.should_execute);
    let read = sys
        .get_workflow("wf-owned-elsewhere")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.executor_id.as_deref(), Some("executor-b"));
}

/// Attributes must be a JSON object, on both the paths that can set them.
///
/// Every other implementation takes a map, and TypeScript rejects arrays explicitly. Since this
/// layer takes the encoded form, the check has to be made rather than inherited from a type —
/// and it is not cosmetic: `attributes @> …` is containment against an object, so a stored array
/// or scalar would silently never match any filter.
#[tokio::test]
async fn attributes_must_be_a_json_object() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-attr-ok"), None, Submission::Fresh)
        .await
        .unwrap();

    for bad in [r#"[1,2,3]"#, "42", r#""a string""#, "not json at all"] {
        // At creation.
        let wf = NewWorkflow {
            attributes: Some(bad),
            ..NewWorkflow::new("wf-attr-bad")
        };
        match sys.init_workflow(&wf, None, Submission::Fresh).await {
            Err(Error::InvalidInput { field, .. }) => assert_eq!(field, "attributes"),
            other => panic!("expected {bad} to be rejected at creation, got {other:?}"),
        }

        // And on update.
        match sys
            .update_workflow_attributes("wf-attr-ok", Some(bad))
            .await
        {
            Err(Error::InvalidInput { field, .. }) => assert_eq!(field, "attributes"),
            other => panic!("expected {bad} to be rejected on update, got {other:?}"),
        }
    }

    // An object is fine, and so is clearing.
    sys.update_workflow_attributes("wf-attr-ok", Some(r#"{"tenant":"acme"}"#))
        .await
        .unwrap();
    sys.update_workflow_attributes("wf-attr-ok", None)
        .await
        .unwrap();

    // Nothing was written on the way to any of those errors.
    assert!(sys.get_workflow("wf-attr-bad").await.unwrap().is_none());
}

/// The version registry is idempotent on the name and ordered by timestamp, not by creation.
#[tokio::test]
async fn application_versions_are_registered_once_and_ordered_by_timestamp() {
    let (sys, _db) = sysdb().await;
    assert_eq!(
        sys.get_latest_application_version(None).await.unwrap(),
        None,
        "an empty registry is what a fresh database looks like, not an error",
    );

    sys.create_application_version("v1", None).await.unwrap();
    sys.create_application_version("v2", None).await.unwrap();

    // Registering the same name again is a no-op, not a second row.
    sys.create_application_version("v1", None).await.unwrap();
    let all = sys.list_application_versions().await.unwrap();
    assert_eq!(all.len(), 2);

    let latest = sys
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version_name, "v2", "highest timestamp wins");

    // Promoting v1 makes it current even though v2 was created later — which is the whole point
    // of ordering on `version_timestamp` rather than `created_at`.
    let promoted = Timestamp::from_epoch_ms(latest.version_timestamp.as_epoch_ms() + 60_000);
    sys.update_application_version_timestamp("v1", promoted, None)
        .await
        .unwrap();

    let latest = sys
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version_name, "v1");
    assert_eq!(latest.version_timestamp, promoted);

    let all = sys.list_application_versions().await.unwrap();
    assert_eq!(
        all.iter()
            .map(|v| v.version_name.as_str())
            .collect::<Vec<_>>(),
        ["v1", "v2"],
        "the list is latest-first, on the same ordering",
    );

    // The generated id is distinct from the name, and stable across a repeat registration.
    let v1 = &all[0];
    assert_ne!(v1.version_id, v1.version_name);
    sys.create_application_version("v1", None).await.unwrap();
    let again = sys
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        again.version_id, v1.version_id,
        "re-registering must not mint a new id or reset the promotion",
    );
    assert_eq!(again.version_timestamp, promoted);
}

/// The bulk readers report everything a workflow was sent, published, and streamed.
///
/// Written with raw SQL because nothing yet *writes* these tables — `send`, `set_event`, and
/// `write_stream` are the rest of 3.5 — so this pins the read shape ahead of them rather than
/// waiting.
#[tokio::test]
async fn the_bulk_readers_return_notifications_events_and_streams() {
    let (sys, db) = sysdb().await;
    sys.init_workflow(&workflow("wf-bulk"), None, Submission::Fresh)
        .await
        .unwrap();
    let mut conn = db.admin_connection().await;

    for sql in [
        "INSERT INTO dbos.notifications (message_uuid, destination_uuid, topic, message, \
         serialization, created_at_epoch_ms, consumed) \
         VALUES ('msg-2', 'wf-bulk', 'orders', '\"second\"', 'portable_json', 2000, false), \
                ('msg-1', 'wf-bulk', NULL, '\"first\"', NULL, 1000, true)",
        "INSERT INTO dbos.workflow_events (workflow_uuid, key, value, serialization) \
         VALUES ('wf-bulk', 'progress', '50', 'portable_json'), ('wf-bulk', 'answer', '42', NULL)",
        "INSERT INTO dbos.streams (workflow_uuid, key, \"offset\", value, serialization, \
         function_id) \
         VALUES ('wf-bulk', 'log', 1, '\"b\"', NULL, 7), \
                ('wf-bulk', 'log', 0, '\"a\"', 'portable_json', 3), \
                ('wf-bulk', 'audit', 0, '\"x\"', NULL, 3)",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Oldest first, and a consumed message is still reported.
    let notifications = sys.get_all_notifications("wf-bulk").await.unwrap();
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].message_uuid, "msg-1");
    assert_eq!(notifications[0].message, "\"first\"");
    assert_eq!(notifications[0].topic, None, "the default topic is NULL");
    assert!(
        notifications[0].consumed,
        "receiving marks rather than deletes, so this must still be visible",
    );
    assert_eq!(notifications[1].topic.as_deref(), Some("orders"));
    assert_eq!(
        notifications[1].serialization.as_deref(),
        Some("portable_json")
    );

    let events = sys.get_all_events("wf-bulk").await.unwrap();
    assert_eq!(
        events.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
        ["answer", "progress"],
    );
    assert_eq!(events[0].value, "42");
    assert_eq!(events[0].serialization, None);

    // Grouped by key, then in stream order — which the insertion order deliberately is not.
    let entries = sys.get_all_stream_entries("wf-bulk").await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|e| (e.key.as_str(), e.offset))
            .collect::<Vec<_>>(),
        [("audit", 0), ("log", 0), ("log", 1)],
    );
    assert_eq!(entries[1].value, "\"a\"");
    // The step that wrote each entry, which a fork uses to decide what to carry forward.
    assert_eq!(
        entries.iter().map(|e| e.step_id).collect::<Vec<_>>(),
        [3, 3, 7],
    );

    // A workflow with none of any reads as empty rather than failing.
    sys.init_workflow(&workflow("wf-quiet"), None, Submission::Fresh)
        .await
        .unwrap();
    assert!(
        sys.get_all_notifications("wf-quiet")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(sys.get_all_events("wf-quiet").await.unwrap().is_empty());
    assert!(
        sys.get_all_stream_entries("wf-quiet")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Publishing a key writes the current value, a history row, and the step — or none of them.
#[tokio::test]
async fn set_event_publishes_a_value_and_its_history() {
    let (sys, db) = sysdb().await;
    sys.init_workflow(&workflow("wf-publisher"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.set_event("wf-publisher", 0, "progress", "50", Some("portable_json"))
        .await
        .unwrap();

    let events = sys.get_all_events("wf-publisher").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, "progress");
    assert_eq!(events[0].value, "50");
    assert_eq!(events[0].serialization.as_deref(), Some("portable_json"));

    // The step is recorded under the name every implementation uses, so a workflow replayed by
    // another SDK finds what it expects rather than an `UnexpectedStep`.
    let step = sys
        .check_step("wf-publisher", 0, "DBOS.setEvent")
        .await
        .unwrap()
        .expect("setting an event is a step");
    assert_eq!(step.step_name, "DBOS.setEvent");

    // Setting the same key again from a later step replaces the value and adds history.
    sys.set_event("wf-publisher", 1, "progress", "100", None)
        .await
        .unwrap();
    let events = sys.get_all_events("wf-publisher").await.unwrap();
    assert_eq!(events.len(), 1, "the current value is one row per key");
    assert_eq!(events[0].value, "100");

    let mut conn = db.admin_connection().await;
    let history: Vec<(i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(
        "SELECT function_id, value FROM dbos.workflow_events_history \
         WHERE workflow_uuid = 'wf-publisher' AND key = 'progress' ORDER BY function_id",
    ))
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        history,
        [(0, "50".to_owned()), (1, "100".to_owned())],
        "history keeps a row per step, which is what a fork copies forward",
    );
}

/// A replayed publish is skipped, not repeated and not reported as a failure.
#[tokio::test]
async fn replaying_set_event_does_not_republish() {
    let (sys, db) = sysdb().await;
    sys.init_workflow(&workflow("wf-replay"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.set_event("wf-replay", 0, "answer", "42", None)
        .await
        .unwrap();

    // The same step id again — what a replay does. The recorded step short-circuits it, so the
    // second value never lands.
    sys.set_event("wf-replay", 0, "answer", "99", None)
        .await
        .expect("a replay is skipped, not an error");

    let events = sys.get_all_events("wf-replay").await.unwrap();
    assert_eq!(events[0].value, "42", "the replay must not overwrite");

    // And the step check and the write are one commit: nothing partial is left behind.
    let mut conn = db.admin_connection().await;
    let rows: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(
        "SELECT count(*) FROM dbos.workflow_events_history WHERE workflow_uuid = 'wf-replay'",
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows.0, 1, "the replay added no history row either");
}

/// How long the blocking reads wait between looks. Nothing pushes yet, so it is also how long a
/// value takes to arrive.
const RECHECK: std::time::Duration = std::time::Duration::from_secs(1);

/// Publisher and reader, both initialised, sharing one handle.
async fn publisher_and_reader(sys: &PostgresSystemDatabase, publisher: &str, reader: &str) {
    for id in [publisher, reader] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }
}

/// A value already published comes back without a wait.
#[tokio::test]
async fn an_event_already_published_returns_at_once() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-publisher"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.set_event("wf-publisher", 0, "progress", "50", Some("portable_json"))
        .await
        .unwrap();

    let began = Timestamp::now();
    let found = sys
        .get_event("wf-publisher", "progress", RECHECK * 30, None)
        .await
        .unwrap();

    assert_eq!(
        found,
        Some(EncodedValue {
            value: "50".to_owned(),
            serialization: Some("portable_json".to_owned()),
        })
    );
    // The first look answers it, so no interval is spent. Generous, because the assertion is
    // "did not wait for the loop" rather than a latency budget.
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK,
        "a value already there should not cost an interval",
    );
}

/// A value published *after* the wait began still arrives — which is the whole feature.
///
/// **Nothing pushes.** No listener exists yet and no trigger fires into this process, so what
/// delivers here is the re-query, exactly as it does on CockroachDB in every SDK. If this passes
/// only once a notification transport lands, the transport has become load-bearing and the design
/// has gone wrong.
#[tokio::test]
async fn a_value_published_during_the_wait_still_arrives() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;

    let publishing = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            // Long enough that the reader's first look has already found nothing, so the value can
            // only be delivered by a later pass of the loop.
            tokio::time::sleep(BRIEFLY).await;
            sys.set_event(
                "wf-publisher",
                0,
                "progress",
                "\"done\"",
                Some("portable_json"),
            )
            .await
            .unwrap();
        })
    };

    // The read is given ten minutes, so its deadline cannot be what ends the wait — only the loop
    // finding the value can be. Bounded at ten intervals: an interval sized for a transport that
    // does not exist yet would still deliver, eventually, and "eventually" is the regression.
    let began = Timestamp::now();
    let found = tokio::time::timeout(
        RECHECK * 10,
        sys.get_event("wf-publisher", "progress", RECHECK * 600, None),
    )
    .await
    .expect("the wait loop never delivered a value that was published during it")
    .unwrap();
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK * 5,
        "delivered, but far slower than the interval it is supposed to run at",
    );

    publishing.await.unwrap();
    assert_eq!(
        found,
        Some(EncodedValue {
            value: "\"done\"".to_owned(),
            serialization: Some("portable_json".to_owned()),
        })
    );
}

/// Nothing published by the deadline is a value, not an error.
///
/// Python and TypeScript return the same. Go raises a timeout instead, but in its engine — and an
/// error is the one shape a caller cannot synthesise from the other.
#[tokio::test]
async fn a_read_that_finds_nothing_reports_absence() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-publisher"), None, Submission::Fresh)
        .await
        .unwrap();

    let found = sys
        .get_event("wf-publisher", "never-set", BRIEFLY, None)
        .await
        .unwrap();
    assert_eq!(found, None);

    // A workflow that does not exist reads the same way: this waits for a key, not for a workflow.
    let found = sys
        .get_event("wf-nobody", "progress", BRIEFLY, None)
        .await
        .unwrap();
    assert_eq!(found, None);
}

/// A read inside a workflow records the publisher's own payload, in the publisher's own format.
///
/// Not a wrapper around the pair: the column holds what was published, so the row stays legible to
/// an operator and to every other implementation, all four of which record exactly this.
#[tokio::test]
async fn a_read_records_the_publishers_own_payload() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.set_event("wf-publisher", 0, "progress", "50", Some("pickle"))
        .await
        .unwrap();

    let caller = BlockingCaller {
        workflow_id: "wf-reader",
        step_id: 0,
        timeout_step_id: 1,
    };
    sys.get_event("wf-publisher", "progress", RECHECK * 30, Some(caller))
        .await
        .unwrap();

    let step = sys
        .check_step("wf-reader", 0, "DBOS.getEvent")
        .await
        .unwrap()
        .expect("reading an event inside a workflow is a step");
    assert_eq!(step.output.as_deref(), Some("50"));
    assert_eq!(
        step.serialization.as_deref(),
        Some("pickle"),
        "the publisher's format travels with the value, not this layer's",
    );
}

/// A replay returns what the first run saw, whatever has been published since.
#[tokio::test]
async fn a_replayed_read_returns_what_the_first_run_saw() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.set_event("wf-publisher", 0, "progress", "50", None)
        .await
        .unwrap();

    let caller = BlockingCaller {
        workflow_id: "wf-reader",
        step_id: 0,
        timeout_step_id: 1,
    };
    let first = sys
        .get_event("wf-publisher", "progress", RECHECK * 30, Some(caller))
        .await
        .unwrap();
    assert_eq!(first.as_ref().map(|v| v.value.as_str()), Some("50"));

    sys.set_event("wf-publisher", 1, "progress", "100", None)
        .await
        .unwrap();
    let replayed = sys
        .get_event("wf-publisher", "progress", RECHECK * 30, Some(caller))
        .await
        .unwrap();
    assert_eq!(
        replayed, first,
        "a replay must not see a value that arrived after the decision it fed",
    );
}

/// A timeout is a result too, so a replay of one does not wait again — or find a late value.
#[tokio::test]
async fn a_replayed_timeout_stays_a_timeout() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;

    let caller = BlockingCaller {
        workflow_id: "wf-reader",
        step_id: 0,
        timeout_step_id: 1,
    };
    assert_eq!(
        sys.get_event("wf-publisher", "progress", BRIEFLY, Some(caller))
            .await
            .unwrap(),
        None,
    );

    // The replay is asked for thirty intervals, with still nothing published — so an
    // implementation that looked again rather than replaying would sit here rather than answer,
    // and the bound is what catches it. Nothing is published yet on purpose: with a value in the
    // table the first look ends the wait, and waiting-when-it-should-not becomes invisible.
    let began = Timestamp::now();
    assert_eq!(
        tokio::time::timeout(
            RECHECK * 5,
            sys.get_event("wf-publisher", "progress", RECHECK * 30, Some(caller)),
        )
        .await
        .expect("a replay waited instead of returning the answer it had already recorded")
        .unwrap(),
        None,
    );
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK,
        "a replay must not wait at all",
    );

    // And a value arriving after the fact does not change it.
    sys.set_event("wf-publisher", 0, "progress", "50", None)
        .await
        .unwrap();
    assert_eq!(
        sys.get_event("wf-publisher", "progress", RECHECK * 30, Some(caller))
            .await
            .unwrap(),
        None,
        "the recorded absence is the answer, not a fresh look",
    );
}

/// A replay does not go looking at the publisher's table at all.
///
/// The recorded answer is the answer, so a replay must survive the row it was read from being
/// gone — deleted with its workflow, or garbage-collected. An implementation that replayed by
/// looking again would find nothing and sit here for the ten minutes it was given.
#[tokio::test]
async fn a_replayed_read_does_not_depend_on_the_row_still_existing() {
    let (sys, db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.set_event("wf-publisher", 0, "progress", "50", None)
        .await
        .unwrap();

    let caller = BlockingCaller {
        workflow_id: "wf-reader",
        step_id: 0,
        timeout_step_id: 1,
    };
    let timeout = std::time::Duration::from_secs(600);
    let first = sys
        .get_event("wf-publisher", "progress", timeout, Some(caller))
        .await
        .unwrap();
    assert_eq!(first.as_ref().map(|v| v.value.as_str()), Some("50"));

    // The published value goes away, while the deadline the first run checkpointed has ten minutes
    // left — so nothing but the recorded step can end this wait.
    let mut conn = db.admin_connection().await;
    sqlx::query(sqlx::AssertSqlSafe(
        "DELETE FROM dbos.workflow_events WHERE workflow_uuid = 'wf-publisher'",
    ))
    .execute(&mut conn)
    .await
    .unwrap();

    let replayed = tokio::time::timeout(
        RECHECK * 10,
        sys.get_event("wf-publisher", "progress", timeout, Some(caller)),
    )
    .await
    .expect("a replay went looking for a row it had already read")
    .unwrap();
    assert_eq!(replayed, first);
}

/// A rival execution that records the read while this one is waiting wins, and this one adopts it.
///
/// The check that opens the read cannot cover this: the rival's step lands *during* the wait. So
/// the final read checks again inside the transaction that records, which is what makes the two
/// executions agree rather than making the loser fail.
#[tokio::test]
async fn a_read_adopts_a_rivals_answer_rather_than_failing() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;

    let reading = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.get_event(
                "wf-publisher",
                "progress",
                RECHECK * 30,
                Some(BlockingCaller {
                    workflow_id: "wf-reader",
                    step_id: 0,
                    timeout_step_id: 1,
                }),
            )
            .await
        })
    };

    // While it waits: another execution of wf-reader records the step, and only then does the value
    // appear. So the wait ends on a value this call must not return.
    tokio::time::sleep(BRIEFLY).await;
    sys.record_step(
        "wf-reader",
        0,
        "DBOS.getEvent",
        Outcome::Output(Some("\"rival\"")),
        Some("portable_json"),
        None,
    )
    .await
    .unwrap();
    sys.set_event("wf-publisher", 0, "progress", "\"mine\"", None)
        .await
        .unwrap();

    let found = tokio::time::timeout(RECHECK * 20, reading)
        .await
        .expect("the read never finished")
        .unwrap()
        .expect("losing the race is not a failure");
    assert_eq!(
        found,
        Some(EncodedValue {
            value: "\"rival\"".to_owned(),
            serialization: Some("portable_json".to_owned()),
        }),
        "the recorded step is the workflow's answer, whoever wrote it",
    );
}

/// The deadline is checkpointed as a step that completes now, not one that completes at the
/// deadline.
///
/// A read that answers in milliseconds under a long timeout must not be recorded as having taken
/// the whole timeout, or every step aggregate and every Conductor timeline reports it that way.
/// The sleep tests above cover the other stamping, which is what a real sleep gets.
#[tokio::test]
async fn a_read_records_a_deadline_it_may_abandon_rather_than_a_sleep() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.set_event("wf-publisher", 0, "progress", "50", None)
        .await
        .unwrap();

    let timeout = std::time::Duration::from_secs(600);
    sys.get_event(
        "wf-publisher",
        "progress",
        timeout,
        Some(BlockingCaller {
            workflow_id: "wf-reader",
            step_id: 0,
            timeout_step_id: 1,
        }),
    )
    .await
    .unwrap();

    let sleep = sys
        .check_step("wf-reader", 1, "DBOS.sleep")
        .await
        .unwrap()
        .expect("the deadline is checkpointed even when the value is already there");
    let started = sleep.started_at.unwrap();
    let completed = sleep.completed_at.unwrap();
    let deadline: i64 = sleep.output.unwrap().parse().unwrap();

    assert!(
        deadline - started.as_epoch_ms() >= timeout.as_millis() as i64,
        "the recorded deadline is still ten minutes out; only the completion differs",
    );
    assert!(
        completed.duration_since(started).unwrap() < RECHECK,
        "a deadline is stamped complete now, not at the deadline: \
         started {started:?}, completed {completed:?}",
    );
}

/// A recovered read resumes the original deadline instead of restarting the timeout.
///
/// Set up the way a crash leaves it: the deadline step is already recorded and has already passed,
/// while the read itself never got as far as recording anything. A ten-minute timeout must then
/// expire at once rather than ten minutes from now.
#[tokio::test]
async fn a_recovered_read_resumes_the_original_deadline() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.record_sleep("wf-reader", 1, std::time::Duration::ZERO)
        .await
        .unwrap();

    let began = Timestamp::now();
    let found = tokio::time::timeout(
        RECHECK * 10,
        sys.get_event(
            "wf-publisher",
            "progress",
            std::time::Duration::from_secs(600),
            Some(BlockingCaller {
                workflow_id: "wf-reader",
                step_id: 0,
                timeout_step_id: 1,
            }),
        ),
    )
    .await
    .expect("the recovered deadline had already passed; the timeout was restarted instead")
    .unwrap();

    assert_eq!(found, None);
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK * 5,
        "the recovered deadline had already passed; the timeout was restarted instead",
    );
}

/// A read inside a cancelled workflow stops at its next step boundary rather than waiting out its
/// timeout.
///
/// Free, rather than a per-interval status check: the replay check that opens every read is a join
/// against `workflow_status`, so a cancelled caller is reported by the query already being made.
#[tokio::test]
async fn a_cancelled_reader_stops_rather_than_waiting() {
    let (sys, _db) = sysdb().await;
    publisher_and_reader(&sys, "wf-publisher", "wf-reader").await;
    sys.cancel_workflows(&["wf-reader"], false).await.unwrap();

    let err = tokio::time::timeout(
        RECHECK * 10,
        sys.get_event(
            "wf-publisher",
            "progress",
            std::time::Duration::from_secs(600),
            Some(BlockingCaller {
                workflow_id: "wf-reader",
                step_id: 0,
                timeout_step_id: 1,
            }),
        ),
    )
    .await
    .expect("a cancelled reader waited out its timeout instead of stopping")
    .expect_err("a cancelled workflow finds out at its next step boundary");
    assert!(
        matches!(err, Error::WorkflowCancelled { ref workflow_id } if workflow_id == "wf-reader"),
        "unexpected error: {err:?}",
    );
}

/// A read waiting at a cap of one does not hold its permit across the wait.
///
/// The failure this rules out is a deadlock built from safe parts: cap the concurrent *waiters*
/// rather than the concurrent queries and one pool's worth of blocked reads blocks every later one
/// until they time out. The first reader here waits for something nobody will ever publish, so if
/// it holds the permit it holds it for the full timeout.
#[tokio::test]
async fn a_capped_read_does_not_hold_its_permit_across_the_wait() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            polling_concurrency: Some(1),
            ..Settings::default()
        },
    ));
    sys.init_workflow(&workflow("wf-publisher"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.set_event("wf-publisher", 0, "published", "50", None)
        .await
        .unwrap();

    // First, and given a head start, so it is the one holding the permit if the permit is held.
    let forever = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.get_event("wf-publisher", "never-published", RECHECK * 60, None)
                .await
        })
    };
    tokio::time::sleep(BRIEFLY).await;

    let found = tokio::time::timeout(
        RECHECK * 10,
        sys.get_event("wf-publisher", "published", RECHECK * 30, None),
    )
    .await
    .expect("starved: a polling permit is being held across a wait")
    .unwrap();
    assert_eq!(found.map(|v| v.value), Some("50".to_owned()));

    // Still waiting, as it should be, and dropped rather than awaited.
    assert!(!forever.is_finished());
    forever.abort();
}

/// One message to `wf-receiver`, from a sender outside a workflow.
async fn send_to_receiver(sys: &PostgresSystemDatabase, topic: Option<&str>, message: &str) {
    sys.send_messages(
        &[Message {
            destination_id: "wf-receiver",
            topic,
            message,
            idempotency_key: None,
        }],
        Some("portable_json"),
        None,
        false,
    )
    .await
    .unwrap();
}

/// A message already waiting is taken without a wait, and taken means consumed.
#[tokio::test]
async fn a_message_already_waiting_is_taken_at_once() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    send_to_receiver(&sys, Some("orders"), "\"one\"").await;

    let began = Timestamp::now();
    let taken = sys
        .recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
        .await
        .unwrap();

    assert_eq!(
        taken,
        Some(EncodedValue {
            value: "\"one\"".to_owned(),
            serialization: Some("portable_json".to_owned()),
        })
    );
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK,
        "a message already there should not cost an interval",
    );

    // Marked rather than deleted, so what a workflow was sent stays visible to export and audit.
    let notifications = sys.get_all_notifications("wf-receiver").await.unwrap();
    assert_eq!(notifications.len(), 1);
    assert!(notifications[0].consumed, "receiving consumes the message");

    let step = sys
        .check_step("wf-receiver", 0, "DBOS.recv")
        .await
        .unwrap()
        .expect("receiving is a step");
    assert_eq!(step.output.as_deref(), Some("\"one\""));
    assert_eq!(
        step.serialization.as_deref(),
        Some("portable_json"),
        "the sender's format travels with the message",
    );
}

/// A message sent *after* the wait began still arrives, with nothing pushing.
#[tokio::test]
async fn a_message_sent_during_the_wait_still_arrives() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();

    let sending = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            // Long enough that the receiver's first look has already found nothing.
            tokio::time::sleep(BRIEFLY).await;
            send_to_receiver(&sys, None, "\"late\"").await;
        })
    };

    // Ten minutes to receive in, so the deadline cannot be what ends the wait — only the loop
    // finding the message can be — and ten intervals to do it in.
    let began = Timestamp::now();
    let taken = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-receiver", 0, 1, None, RECHECK * 600),
    )
    .await
    .expect("the wait loop never delivered a message that was sent during it")
    .unwrap();
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK * 5,
        "delivered, but far slower than the interval it is supposed to run at",
    );

    sending.await.unwrap();
    assert_eq!(taken.map(|m| m.value), Some("\"late\"".to_owned()));
}

/// Nothing sent by the deadline is a value, not an error — and it is recorded as one.
#[tokio::test]
async fn a_recv_that_finds_nothing_reports_absence() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();

    assert_eq!(
        sys.recv("wf-receiver", 0, 1, Some("orders"), BRIEFLY)
            .await
            .unwrap(),
        None,
    );
    let step = sys
        .check_step("wf-receiver", 0, "DBOS.recv")
        .await
        .unwrap()
        .expect("a timeout is a result, and results are recorded");
    assert_eq!(step.output, None, "nothing received is a NULL output");

    // A message on another topic is not this receiver's, and the sentinel topic is its own topic
    // rather than a wildcard.
    send_to_receiver(&sys, None, "\"untopicked\"").await;
    assert_eq!(
        sys.recv("wf-receiver", 2, 3, Some("orders"), BRIEFLY)
            .await
            .unwrap(),
        None,
        "a message on the default topic is not a message on `orders`",
    );
}

/// Messages are taken oldest first, and each exactly once.
#[tokio::test]
async fn messages_are_taken_oldest_first() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    for message in ["\"one\"", "\"two\"", "\"three\""] {
        send_to_receiver(&sys, Some("orders"), message).await;
    }

    let mut taken = Vec::new();
    for step_id in [0, 2, 4] {
        taken.push(
            sys.recv(
                "wf-receiver",
                step_id,
                step_id + 1,
                Some("orders"),
                RECHECK * 600,
            )
            .await
            .unwrap()
            .map(|m| m.value),
        );
    }
    assert_eq!(
        taken,
        [
            Some("\"one\"".to_owned()),
            Some("\"two\"".to_owned()),
            Some("\"three\"".to_owned()),
        ],
        "FIFO, and no message twice",
    );
}

/// A replay returns the message the first run took, and does not take another.
#[tokio::test]
async fn a_replayed_recv_does_not_take_a_second_message() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    send_to_receiver(&sys, Some("orders"), "\"one\"").await;
    send_to_receiver(&sys, Some("orders"), "\"two\"").await;

    let first = sys
        .recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
        .await
        .unwrap();
    let replayed = sys
        .recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
        .await
        .unwrap();

    assert_eq!(replayed, first, "a replay returns, it does not receive");
    let consumed = sys
        .get_all_notifications("wf-receiver")
        .await
        .unwrap()
        .iter()
        .filter(|n| n.consumed)
        .count();
    assert_eq!(consumed, 1, "the replay must not have taken the second one");
}

/// A replay does not go looking for a message it has already been given.
///
/// The recorded answer is the answer, so a replay must return at once even when the queue is empty
/// — an implementation that looked again would sit here for the ten minutes it was given.
#[tokio::test]
async fn a_replayed_recv_does_not_wait_for_a_message_it_already_has() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    send_to_receiver(&sys, Some("orders"), "\"one\"").await;

    let timeout = RECHECK * 600;
    let first = sys
        .recv("wf-receiver", 0, 1, Some("orders"), timeout)
        .await
        .unwrap();
    assert_eq!(first.as_ref().map(|m| m.value.as_str()), Some("\"one\""));

    // Nothing is waiting now, and the deadline the first run checkpointed has ten minutes left —
    // so only the recorded step can end this call.
    let began = Timestamp::now();
    let replayed = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-receiver", 0, 1, Some("orders"), timeout),
    )
    .await
    .expect("a replay waited for a message it had already received")
    .unwrap();
    assert_eq!(replayed, first);
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK,
        "a replay must not wait at all",
    );
}

/// A rival execution that records the receive mid-wait wins, and this one takes no message.
///
/// The check that opens the call cannot cover this: the rival's step lands *during* the wait. So
/// the taking checks again inside the transaction that records, which is what keeps the message on
/// the queue rather than consuming it for a step nobody will record.
#[tokio::test]
async fn a_recv_defers_to_a_rival_that_recorded_first() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();

    let receiving = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
                .await
        })
    };

    // While it waits: another execution of wf-receiver records the step, and only then does a
    // message appear. So the wait ends on a message this call must not take.
    tokio::time::sleep(BRIEFLY).await;
    sys.record_step(
        "wf-receiver",
        0,
        "DBOS.recv",
        Outcome::Output(Some("\"rival\"")),
        Some("portable_json"),
        None,
    )
    .await
    .unwrap();
    send_to_receiver(&sys, Some("orders"), "\"mine\"").await;

    let taken = tokio::time::timeout(RECHECK * 20, receiving)
        .await
        .expect("the receive never finished")
        .unwrap()
        .expect("losing to a rival is not a failure");
    assert_eq!(
        taken.map(|m| m.value),
        Some("\"rival\"".to_owned()),
        "the recorded step is the workflow's answer, whoever wrote it",
    );

    let notifications = sys.get_all_notifications("wf-receiver").await.unwrap();
    assert!(
        notifications.iter().all(|n| !n.consumed),
        "the message must still be on the queue: nothing recorded having taken it",
    );
}

/// A second receiver on one (workflow, topic) is refused rather than left to time out.
///
/// One message goes to one of them, so the loser would wait out its whole timeout and report that
/// nothing arrived — which the sender cannot tell apart from not having sent. Python and Go refuse
/// it too; TypeScript and Java allow it.
#[tokio::test]
async fn a_second_receiver_on_one_topic_is_refused() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();

    let waiting = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
                .await
        })
    };
    tokio::time::sleep(BRIEFLY).await;

    // Bounded, because the regression is that it *waits*: a second receiver that is admitted
    // rather than refused sits out its whole ten minutes for a message it was never going to get,
    // which is exactly the behaviour being ruled out.
    let err = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-receiver", 2, 3, Some("orders"), RECHECK * 600),
    )
    .await
    .expect("a second receiver was admitted and left to wait rather than refused")
    .expect_err("a second receiver on one topic is a bug in the caller, and is told so");
    assert!(
        matches!(
            err,
            Error::ConcurrentRecv { ref workflow_id, ref topic }
                if workflow_id == "wf-receiver" && topic.as_deref() == Some("orders")
        ),
        "unexpected error: {err:?}",
    );

    // Another topic on the same workflow is a different wait, and is not refused.
    send_to_receiver(&sys, Some("refunds"), "\"other\"").await;
    assert!(
        sys.recv("wf-receiver", 4, 5, Some("refunds"), RECHECK * 600)
            .await
            .unwrap()
            .is_some(),
    );

    waiting.abort();
}

/// Two receivers racing at the database consume one message between them, not two.
///
/// **The cross-process case, which the in-process guard cannot see** — two handles have two
/// registries, so both get past `subscribe_exclusive` exactly as two executors recovering one
/// workflow would. What arbitrates here is the database: the consuming `UPDATE` re-evaluates
/// `consumed = FALSE` against the winner's committed row, and the losing transaction rolls back
/// whatever it took when its step write conflicts. Two messages are queued so that a loser going
/// back for a *different* one would be visible as a second consumed row.
///
/// **What this does not pin:** the `consumed = FALSE` predicate itself. Removing it and re-running
/// this test passes, because the transaction covers the same case — verified by mutation. The
/// predicate stays for the reasons given where it is written, but no test here can tell it apart,
/// and claiming otherwise would be worse than saying so.
#[tokio::test]
async fn two_receivers_cannot_take_the_same_message() {
    let db = test_database().await;
    // Two handles over one pool: separate registries, so the in-process exclusivity does not apply
    // and both reach the database — which is the situation this test is about.
    let first = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    let second = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings::default(),
    ));
    first
        .init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    send_to_receiver(&first, Some("orders"), "\"one\"").await;
    send_to_receiver(&first, Some("orders"), "\"two\"").await;

    let racing: Vec<_> = [first.clone(), second.clone()]
        .into_iter()
        .map(|sys| {
            tokio::spawn(async move {
                sys.recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600)
                    .await
            })
        })
        .collect();

    let mut answers = Vec::new();
    for handle in racing {
        answers.push(
            tokio::time::timeout(RECHECK * 20, handle)
                .await
                .expect("a racing receiver never finished")
                .unwrap()
                .expect("losing the race is not a failure"),
        );
    }

    assert_eq!(
        answers[0], answers[1],
        "both executions of one workflow must report the message its one recorded step holds",
    );
    let notifications = first.get_all_notifications("wf-receiver").await.unwrap();
    assert_eq!(
        notifications.iter().filter(|n| n.consumed).count(),
        1,
        "one message was received, so exactly one may be consumed: {notifications:?}",
    );
}

/// A receive inside a cancelled workflow stops rather than waiting out its timeout.
#[tokio::test]
async fn a_cancelled_receiver_stops_rather_than_waiting() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.cancel_workflows(&["wf-receiver"], false).await.unwrap();

    let err = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600),
    )
    .await
    .expect("a cancelled receiver waited out its timeout instead of stopping")
    .expect_err("a cancelled workflow finds out at its next step boundary");
    assert!(
        matches!(err, Error::WorkflowCancelled { ref workflow_id } if workflow_id == "wf-receiver"),
        "unexpected error: {err:?}",
    );
}

/// A recovered receive resumes the original deadline instead of restarting the timeout.
#[tokio::test]
async fn a_recovered_recv_resumes_the_original_deadline() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-receiver"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_sleep("wf-receiver", 1, std::time::Duration::ZERO)
        .await
        .unwrap();

    let began = Timestamp::now();
    let taken = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-receiver", 0, 1, Some("orders"), RECHECK * 600),
    )
    .await
    .expect("the recovered deadline had already passed; the timeout was restarted instead")
    .unwrap();

    assert_eq!(taken, None);
    assert!(
        Timestamp::now().duration_since(began).unwrap() < RECHECK * 5,
        "the recovered deadline had already passed; the timeout was restarted instead",
    );
}

/// A receiver waiting at a cap of one does not hold its permit across the wait.
#[tokio::test]
async fn a_capped_recv_does_not_hold_its_permit_across_the_wait() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            polling_concurrency: Some(1),
            ..Settings::default()
        },
    ));
    for id in ["wf-receiver", "wf-other"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }
    sys.send_messages(
        &[Message {
            destination_id: "wf-other",
            topic: None,
            message: "\"here\"",
            idempotency_key: None,
        }],
        Some("portable_json"),
        None,
        false,
    )
    .await
    .unwrap();

    // First, and given a head start, so it is the one holding the permit if the permit is held.
    let forever = {
        let sys = std::sync::Arc::clone(&sys);
        tokio::spawn(async move {
            sys.recv("wf-receiver", 0, 1, Some("never-sent"), RECHECK * 600)
                .await
        })
    };
    tokio::time::sleep(BRIEFLY).await;

    let taken = tokio::time::timeout(
        RECHECK * 10,
        sys.recv("wf-other", 0, 1, None, RECHECK * 600),
    )
    .await
    .expect("starved: a polling permit is being held across a wait")
    .unwrap();
    assert_eq!(taken.map(|m| m.value), Some("\"here\"".to_owned()));

    assert!(!forever.is_finished());
    forever.abort();
}

/// A replayed sleep wakes at the original instant, not a fresh one.
///
/// This is the whole point of checkpointing it: a workflow that slept an hour and crashed fifty
/// minutes in has ten minutes left, not sixty.
#[tokio::test]
async fn a_replayed_sleep_keeps_its_original_wake_time() {
    let (sys, db) = sysdb().await;
    sys.init_workflow(&workflow("wf-sleeper"), None, Submission::Fresh)
        .await
        .unwrap();

    let before = Timestamp::now();
    let wake = sys
        .record_sleep("wf-sleeper", 0, std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    let offset = wake.as_epoch_ms() - before.as_epoch_ms();
    assert!(
        (3_600_000..3_610_000).contains(&offset),
        "expected ~1h from now, got {offset}ms",
    );

    // The replay asks for the same duration again and must get the same instant back.
    let replayed = sys
        .record_sleep("wf-sleeper", 0, std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(replayed, wake, "a replay must not restart the clock");

    // Stored as epoch milliseconds in portable JSON, so anything can read it.
    let mut conn = db.admin_connection().await;
    let (output, serialization): (Option<String>, Option<String>) =
        sqlx::query_as(sqlx::AssertSqlSafe(
            "SELECT output, serialization FROM dbos.operation_outputs \
             WHERE workflow_uuid = 'wf-sleeper' AND function_id = 0",
        ))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        output.as_deref(),
        Some(wake.as_epoch_ms().to_string().as_str())
    );
    assert_eq!(serialization.as_deref(), Some("portable_json"));
}

/// A sleep's step is stamped complete at the wake time, so its recorded duration is the sleep.
///
/// That timestamp is in the future when the row is written. Deliberate: nothing in execution or
/// recovery reads it, and a timeline should show an hour's sleep as an hour. Java does the same.
#[tokio::test]
async fn a_sleep_records_its_duration_as_the_sleep() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-waiter"), None, Submission::Fresh)
        .await
        .unwrap();

    let before = Timestamp::now();
    let wake = sys
        .record_sleep("wf-waiter", 0, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let step = sys
        .check_step("wf-waiter", 0, "DBOS.sleep")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(step.completed_at, Some(wake));
    assert!(
        step.completed_at.unwrap().as_epoch_ms() > before.as_epoch_ms(),
        "the completion is stamped ahead of now, which is the point",
    );
    let started = step.started_at.expect("a sleep records when it began");
    assert!(started.as_epoch_ms() >= before.as_epoch_ms());
    assert_eq!(
        wake.as_epoch_ms() - started.as_epoch_ms(),
        60_000,
        "the recorded span is exactly the sleep",
    );
}

/// An operation issued after the handle is closed reports a failure instead of waiting for one.
///
/// A closed pool looks like every other connection failure, and the retry layer is built to wait
/// connection failures out — deliberately, since a database that is briefly unreachable comes
/// back. A closed pool never does. Classifying it as merely unreachable makes anything issued
/// after shutdown hang forever, which is the one outcome the retry layer must never produce.
#[tokio::test]
async fn operations_after_close_fail_rather_than_hang() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-closed"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.close().await;

    let answered = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sys.get_workflow("wf-closed"),
    )
    .await
    .expect("a closed pool must answer, not block");

    match answered {
        Err(Error::Backend(e)) => assert_eq!(
            e.kind,
            BackendErrorKind::Permanent,
            "a closed pool cannot reopen, so waiting for it is waiting forever"
        ),
        other => panic!("expected a backend failure, got {other:?}"),
    }
}

/// Connecting without migrating checks that someone else did.
///
/// Opting out of migration says the database is someone else's to prepare. Trusting that blindly
/// turns a deployment mistake into a missing column on whichever statement happens to touch it
/// first — long after launch reported success, and with nothing pointing at the cause.
#[tokio::test]
async fn connecting_without_migrating_refuses_a_schema_that_is_not_ready() {
    let db = test_database().await;

    // A schema nothing has ever migrated.
    let config = Config {
        url: db.url(),
        migrate: false,
        settings: Settings {
            schema: "never_migrated",
            ..Settings::default()
        },
        ..Config::new(db.url())
    };
    let message = match PostgresSystemDatabase::connect(&config).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("an unmigrated schema should not connect"),
    };
    // Not the version number itself, which moves with every migration added — only that the
    // message names where the database is and what to do about it.
    assert!(
        message.contains("migration 0") && message.contains("Migrate it"),
        "the message should say how far the database got and what to do, got: {message}"
    );

    // And the schema the harness did migrate is accepted without being touched.
    let config = Config {
        migrate: false,
        ..Config::new(db.url())
    };
    assert!(
        PostgresSystemDatabase::connect(&config).await.is_ok(),
        "a migrated schema should connect without migrating"
    );
}

/// A fork inherits its source's identity, replays the steps below its start step, and is enqueued.
#[tokio::test]
async fn a_fork_carries_the_steps_below_its_start_step() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-src"), None, Submission::Fresh)
        .await
        .unwrap();
    for step_id in 0..4 {
        sys.record_step(
            "wf-src",
            step_id,
            &format!("step_{step_id}"),
            Outcome::Output(Some(&format!("\"out{step_id}\""))),
            None,
            None,
        )
        .await
        .unwrap();
    }

    let forks = [Fork {
        source_id: "wf-src",
        forked_id: Some("wf-fork"),
        start_step: 2,
    }];
    let ids = sys
        .fork_workflows(&forks, &ForkOptions::default())
        .await
        .unwrap();
    assert_eq!(ids, ["wf-fork"]);

    // Enqueued rather than started, on the internal queue, pointing back at its source.
    let fork = sys.get_workflow("wf-fork").await.unwrap().unwrap();
    assert_eq!(fork.status, WorkflowStatus::Enqueued);
    assert_eq!(fork.queue_name.as_deref(), Some(INTERNAL_QUEUE));
    assert_eq!(fork.forked_from.as_deref(), Some("wf-src"));
    // Identity is inherited, so the fork runs the same function on the same input.
    let source = sys.get_workflow("wf-src").await.unwrap().unwrap();
    assert_eq!(fork.name, source.name);
    assert_eq!(fork.input, source.input);
    assert_eq!(fork.application_version, source.application_version);
    assert!(
        source.was_forked_from,
        "the source is marked as forked from"
    );

    // Steps 0 and 1 came across; 2 and 3 did not — those are the ones the fork will run.
    let steps = sys
        .list_workflow_steps("wf-fork", true, None, None)
        .await
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str(), s.output.as_deref()))
            .collect::<Vec<_>>(),
        [
            (0, "step_0", Some("\"out0\"")),
            (1, "step_1", Some("\"out1\"")),
        ],
    );
}

/// A fork's events are rebuilt from the history, so it does not see a value set after its start.
///
/// The source's `workflow_events` row holds whatever was published *last*. Copying that would
/// hand the fork a value from its own future — the point it is being forked to redo.
#[tokio::test]
async fn a_fork_sees_the_event_values_as_of_its_start_step() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-events"), None, Submission::Fresh)
        .await
        .unwrap();
    // The same key set three times, at steps 0, 1 and 2.
    for (step_id, value) in [(0, "\"first\""), (1, "\"second\""), (2, "\"third\"")] {
        sys.set_event("wf-events", step_id, "progress", value, None)
            .await
            .unwrap();
    }

    let forks = [Fork {
        source_id: "wf-events",
        forked_id: Some("wf-events-fork"),
        start_step: 2,
    }];
    sys.fork_workflows(&forks, &ForkOptions::default())
        .await
        .unwrap();

    let events = sys.get_all_events("wf-events-fork").await.unwrap();
    assert_eq!(events.len(), 1, "one key, at its value as of step 2");
    assert_eq!(events[0].key, "progress");
    assert_eq!(
        events[0].value, "\"second\"",
        "the value set at step 2 is the fork's future, not its past"
    );

    // The source keeps the value it actually reached.
    let source_events = sys.get_all_events("wf-events").await.unwrap();
    assert_eq!(source_events[0].value, "\"third\"");
}

/// Forking is all-or-nothing: one missing source leaves nothing written.
#[tokio::test]
async fn forking_a_missing_workflow_writes_nothing() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-present"), None, Submission::Fresh)
        .await
        .unwrap();

    let forks = [
        Fork {
            source_id: "wf-present",
            forked_id: Some("wf-would-be"),
            start_step: 1,
        },
        Fork {
            source_id: "wf-absent",
            forked_id: Some("wf-never"),
            start_step: 1,
        },
    ];
    match sys.fork_workflows(&forks, &ForkOptions::default()).await {
        Err(Error::NonExistentWorkflow { workflow_ids }) => {
            assert_eq!(workflow_ids, ["wf-absent"], "names which one is missing")
        }
        other => panic!("expected a missing-workflow error, got {other:?}"),
    }
    assert!(
        sys.get_workflow("wf-would-be").await.unwrap().is_none(),
        "the fork that could have been made must not have been"
    );
}

/// A fork with no id supplied gets one, and the options land on the row.
#[tokio::test]
async fn a_fork_can_have_its_id_generated_and_its_placement_chosen() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-opts"), None, Submission::Fresh)
        .await
        .unwrap();

    let ids = sys
        .fork_workflows(
            &[Fork::new("wf-opts")],
            &ForkOptions {
                application_version: Some("v2"),
                queue_name: Some("reprocess"),
                queue_partition_key: Some("eu"),
                timeout: Some(std::time::Duration::from_secs(30)),
                replacement_children: &[],
            },
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert!(!ids[0].is_empty(), "an id should have been generated");

    let fork = sys.get_workflow(&ids[0]).await.unwrap().unwrap();
    assert_eq!(fork.application_version.as_deref(), Some("v2"));
    assert_eq!(fork.queue_name.as_deref(), Some("reprocess"));
    assert_eq!(fork.queue_partition_key.as_deref(), Some("eu"));
    assert_eq!(fork.timeout, Some(std::time::Duration::from_secs(30)));
}

/// An empty option is refused rather than overwriting what it was meant to leave alone.
///
/// `application_version` is `COALESCE`d against the source's, so an empty string would *win* over
/// the inheritance it was supposed to trigger — stamping the fork with no version and hiding it
/// from version-scoped recovery.
#[tokio::test]
async fn a_fork_option_that_is_empty_rather_than_absent_is_refused() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-blank"), None, Submission::Fresh)
        .await
        .unwrap();

    for (field, options) in [
        (
            "application_version",
            ForkOptions {
                application_version: Some(""),
                ..ForkOptions::default()
            },
        ),
        (
            "queue_name",
            ForkOptions {
                queue_name: Some(""),
                ..ForkOptions::default()
            },
        ),
        (
            "timeout",
            ForkOptions {
                timeout: Some(std::time::Duration::ZERO),
                ..ForkOptions::default()
            },
        ),
    ] {
        let result = sys.fork_workflows(&[Fork::new("wf-blank")], &options).await;
        match result {
            Err(Error::InvalidInput { field: f, .. }) => assert_eq!(f, field),
            other => panic!("expected {field} to be refused, got {other:?}"),
        }
    }

    // An empty id is refused too: it is one nothing could look up.
    let result = sys
        .fork_workflows(
            &[Fork {
                source_id: "wf-blank",
                forked_id: Some(""),
                start_step: 0,
            }],
            &ForkOptions::default(),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::InvalidInput {
            field: "forked_id",
            ..
        })
    ));
}

/// One child replaced twice is refused, rather than duplicating the step that names it.
///
/// The replacements are applied by joining against them, so a child named twice matches a copied
/// step twice. Without this the primary key on `(workflow_uuid, function_id)` would report it as
/// a constraint violation, which says nothing about the map that caused it.
#[tokio::test]
async fn replacing_one_child_twice_is_refused() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-dup"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_child_workflow("wf-dup", "child", 0, "spawn", None)
        .await
        .unwrap();

    let result = sys
        .fork_workflows(
            &[Fork {
                source_id: "wf-dup",
                forked_id: Some("wf-dup-fork"),
                start_step: 1,
            }],
            &ForkOptions {
                replacement_children: &[("child", "fork-a"), ("child", "fork-b")],
                ..ForkOptions::default()
            },
        )
        .await;
    match result {
        Err(Error::InvalidInput { field, detail }) => {
            assert_eq!(field, "replacement_children");
            assert!(detail.contains("child"), "should name it, got: {detail}");
        }
        other => panic!("expected the duplicate to be refused, got {other:?}"),
    }
    assert!(sys.get_workflow("wf-dup-fork").await.unwrap().is_none());
}

/// A timeout too large to store is reported rather than quietly becoming a different one.
#[tokio::test]
async fn a_fork_timeout_that_cannot_be_stored_is_refused() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-huge"), None, Submission::Fresh)
        .await
        .unwrap();

    let result = sys
        .fork_workflows(
            &[Fork::new("wf-huge")],
            &ForkOptions {
                timeout: Some(std::time::Duration::MAX),
                ..ForkOptions::default()
            },
        )
        .await;
    assert!(
        matches!(
            result,
            Err(Error::InvalidInput {
                field: "timeout",
                ..
            })
        ),
        "expected the timeout to be refused, got {result:?}"
    );
}

/// Forking a parent rewrites the children it recorded, so it adopts the forked ones.
///
/// Also the guard for the `start_step > 0` boundary: forking from step 1 must carry step 0. It is
/// where TypeScript and Python disagree — Python's `step > 1` filter drops it — so the assertion
/// that one step came across is load-bearing.
#[tokio::test]
async fn a_fork_can_rewrite_the_children_it_replays() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-parent"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_child_workflow("wf-parent", "child-original", 0, "spawn", None)
        .await
        .unwrap();
    // An ordinary step beside it: with a non-empty map, `r.original = NULL` matches nothing, so
    // this must come across with its `child_workflow_id` still null rather than picking one up.
    sys.record_step(
        "wf-parent",
        1,
        "charge",
        Outcome::Output(Some("\"ok\"")),
        None,
        None,
    )
    .await
    .unwrap();

    sys.fork_workflows(
        &[Fork {
            source_id: "wf-parent",
            forked_id: Some("wf-parent-fork"),
            start_step: 2,
        }],
        &ForkOptions {
            replacement_children: &[("child-original", "child-forked")],
            ..ForkOptions::default()
        },
    )
    .await
    .unwrap();

    let steps = sys
        .list_workflow_steps("wf-parent-fork", true, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 2, "both steps came across");
    assert_eq!(
        steps[0].child_workflow_id.as_deref(),
        Some("child-forked"),
        "the replayed step should point at the fork's own child"
    );
    assert_eq!(
        steps[1].child_workflow_id, None,
        "a step that spawned nothing must not acquire a child from the map"
    );
    assert_eq!(steps[1].output.as_deref(), Some("\"ok\""));
}

/// Sets up a workflow whose step 1 failed and whose step 2 succeeded afterwards.
async fn workflow_with_a_failure(sys: &PostgresSystemDatabase, id: &str) {
    sys.init_workflow(&workflow(id), None, Submission::Fresh)
        .await
        .unwrap();
    sys.record_step(
        id,
        0,
        "validate",
        Outcome::Output(Some("\"ok\"")),
        None,
        None,
    )
    .await
    .unwrap();
    sys.record_step(id, 1, "charge", Outcome::Error("card declined"), None, None)
        .await
        .unwrap();
    sys.record_step(
        id,
        2,
        "notify",
        Outcome::Output(Some("\"sent\"")),
        None,
        None,
    )
    .await
    .unwrap();
}

/// Forking from the failure restarts at the step that failed, not the last one recorded.
#[tokio::test]
async fn forking_from_the_failure_restarts_at_the_failed_step() {
    let (sys, _db) = sysdb().await;
    workflow_with_a_failure(&sys, "wf-failed").await;

    let ids = sys
        .fork_from(
            &["wf-failed"],
            ForkPoint::LastFailure,
            &ForkOptions::default(),
        )
        .await
        .unwrap();

    // Step 1 failed, so the fork starts there: only step 0 comes across, and the fork will run
    // the failing step again.
    let steps = sys
        .list_workflow_steps(&ids[0], true, None, None)
        .await
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect::<Vec<_>>(),
        [(0, "validate")],
    );
}

/// With no failure recorded, forking from the failure falls back to the last step.
///
/// The fallback is what makes it useful on a workflow killed mid-step: nothing recorded an error,
/// but there is still a place to resume from.
#[tokio::test]
async fn forking_from_the_failure_of_a_workflow_that_never_failed_uses_its_last_step() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-clean"), None, Submission::Fresh)
        .await
        .unwrap();
    for (step_id, name) in [(0, "one"), (1, "two")] {
        sys.record_step(
            "wf-clean",
            step_id,
            name,
            Outcome::Output(Some("\"ok\"")),
            None,
            None,
        )
        .await
        .unwrap();
    }

    let ids = sys
        .fork_from(
            &["wf-clean"],
            ForkPoint::LastFailure,
            &ForkOptions::default(),
        )
        .await
        .unwrap();
    let steps = sys
        .list_workflow_steps(&ids[0], true, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1, "resumes at step 1, so only step 0 replays");
    assert_eq!(steps[0].step_name, "one");
}

/// Each fork point picks a different step, and a named step is found by name.
#[tokio::test]
async fn each_fork_point_resolves_to_its_own_step() {
    let (sys, _db) = sysdb().await;
    workflow_with_a_failure(&sys, "wf-points").await;

    for (point, expected_replayed) in [
        (ForkPoint::LastStep, 2),            // step 2 re-runs, 0 and 1 replay
        (ForkPoint::StepNamed("charge"), 1), // step 1 re-runs, 0 replays
        (ForkPoint::Step(0), 0),             // nothing replays
    ] {
        let ids = sys
            .fork_from(&["wf-points"], point, &ForkOptions::default())
            .await
            .unwrap();
        let steps = sys
            .list_workflow_steps(&ids[0], true, None, None)
            .await
            .unwrap();
        assert_eq!(
            steps.len(),
            expected_replayed,
            "{point:?} should leave {expected_replayed} steps to replay"
        );
    }
}

/// A workflow with nothing at the requested point is reported, and nothing is forked.
#[tokio::test]
async fn forking_from_a_point_that_does_not_exist_is_refused() {
    let (sys, _db) = sysdb().await;
    workflow_with_a_failure(&sys, "wf-has-steps").await;
    sys.init_workflow(&workflow("wf-no-steps"), None, Submission::Fresh)
        .await
        .unwrap();

    // No steps at all.
    match sys
        .fork_from(
            &["wf-has-steps", "wf-no-steps"],
            ForkPoint::LastStep,
            &ForkOptions::default(),
        )
        .await
    {
        Err(Error::NoForkPoint {
            workflow_ids,
            step_name,
        }) => {
            assert_eq!(workflow_ids, ["wf-no-steps"]);
            assert_eq!(step_name, None);
        }
        other => panic!("expected a missing fork point, got {other:?}"),
    }

    // A step name nothing matches, which reports the name so the caller can see the typo.
    match sys
        .fork_from(
            &["wf-has-steps"],
            ForkPoint::StepNamed("refund"),
            &ForkOptions::default(),
        )
        .await
    {
        Err(Error::NoForkPoint {
            workflow_ids,
            step_name,
        }) => {
            assert_eq!(workflow_ids, ["wf-has-steps"]);
            assert_eq!(step_name.as_deref(), Some("refund"));
        }
        other => panic!("expected a missing step name, got {other:?}"),
    }

    // The workflow that did have a fork point was not forked either.
    let forks = sys
        .list_workflows(&dbos::sysdb::types::WorkflowFilter {
            forked_from: vec!["wf-has-steps"],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(forks.is_empty(), "a refused batch forks nothing");
}

/// A batch resolves each workflow against its own history, and answers in the caller's order.
///
/// The reason to fork a batch at all: two workflows that failed at different steps must each
/// resume at their own. `GROUP BY` does not preserve the order the ids were given in, so the
/// returned ids lining up with the inputs is a property of the mapping, not of the query.
#[tokio::test]
async fn a_batch_resolves_each_workflow_separately_and_keeps_the_order() {
    let (sys, _db) = sysdb().await;

    // Fails at step 1 of 3.
    sys.init_workflow(&workflow("wf-alpha"), None, Submission::Fresh)
        .await
        .unwrap();
    for (step_id, outcome) in [
        (0, Outcome::Output(Some("\"ok\""))),
        (1, Outcome::Error("boom")),
        (2, Outcome::Output(Some("\"ok\""))),
    ] {
        sys.record_step("wf-alpha", step_id, "s", outcome, None, None)
            .await
            .unwrap();
    }

    // Fails at step 3 of 4, so it must resolve to a different step than wf-alpha.
    sys.init_workflow(&workflow("wf-beta"), None, Submission::Fresh)
        .await
        .unwrap();
    for (step_id, outcome) in [
        (0, Outcome::Output(Some("\"ok\""))),
        (1, Outcome::Output(Some("\"ok\""))),
        (2, Outcome::Output(Some("\"ok\""))),
        (3, Outcome::Error("boom")),
    ] {
        sys.record_step("wf-beta", step_id, "s", outcome, None, None)
            .await
            .unwrap();
    }

    let ids = sys
        .fork_from(
            &["wf-alpha", "wf-beta"],
            ForkPoint::LastFailure,
            &ForkOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 2);

    // Position 0 answers for wf-alpha, position 1 for wf-beta. Were the mapping to transpose
    // them, the step counts below would swap too — which is what makes this an assertion and
    // not a restatement of the call.
    let alpha = sys.get_workflow(&ids[0]).await.unwrap().unwrap();
    let beta = sys.get_workflow(&ids[1]).await.unwrap().unwrap();
    assert_eq!(alpha.forked_from.as_deref(), Some("wf-alpha"));
    assert_eq!(beta.forked_from.as_deref(), Some("wf-beta"));

    // wf-alpha failed at step 1, so only step 0 replays; wf-beta failed at 3, so 0..=2 do.
    let alpha_steps = sys
        .list_workflow_steps(&ids[0], true, None, None)
        .await
        .unwrap();
    let beta_steps = sys
        .list_workflow_steps(&ids[1], true, None, None)
        .await
        .unwrap();
    assert_eq!(alpha_steps.len(), 1, "wf-alpha resumes at its own step 1");
    assert_eq!(beta_steps.len(), 3, "wf-beta resumes at its own step 3");
}

/// A message reaches its destination, under the sentinel topic when none is given.
#[tokio::test]
async fn a_message_is_delivered_to_its_destination() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-dest"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.send_messages(
        &[
            Message {
                destination_id: "wf-dest",
                topic: Some("approvals"),
                message: "\"yes\"",
                idempotency_key: None,
            },
            Message {
                destination_id: "wf-dest",
                topic: None,
                message: "\"untopicked\"",
                idempotency_key: None,
            },
        ],
        Some("portable_json"),
        None,
        false,
    )
    .await
    .unwrap();

    let sent = sys.get_all_notifications("wf-dest").await.unwrap();
    assert_eq!(sent.len(), 2);
    let topic_of = |message: &str| {
        sent.iter()
            .find(|n| n.message == message)
            .unwrap_or_else(|| panic!("{message} was not delivered"))
            .topic
            .clone()
    };
    assert_eq!(topic_of("\"yes\"").as_deref(), Some("approvals"));
    // The untopicked message is filed under the sentinel, not NULL — a receiver selects on
    // equality, and nothing equals NULL.
    assert_eq!(
        topic_of("\"untopicked\"").as_deref(),
        Some(dbos::sysdb::NULL_TOPIC)
    );
}

/// An idempotency key makes a repeated send a no-op.
#[tokio::test]
async fn a_keyed_message_is_delivered_once_however_often_it_is_sent() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-once"), None, Submission::Fresh)
        .await
        .unwrap();

    let message = Message {
        destination_id: "wf-once",
        topic: Some("t"),
        message: "\"payload\"",
        idempotency_key: Some("order-42"),
    };
    for _ in 0..3 {
        sys.send_messages(&[message], None, None, false)
            .await
            .unwrap();
    }

    assert_eq!(sys.get_all_notifications("wf-once").await.unwrap().len(), 1);
}

/// One key used by two messages is refused rather than silently dropping one.
#[tokio::test]
async fn two_messages_under_one_key_are_refused() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-clash"), None, Submission::Fresh)
        .await
        .unwrap();

    let result = sys
        .send_messages(
            &[
                Message {
                    destination_id: "wf-clash",
                    topic: None,
                    message: "\"a\"",
                    idempotency_key: Some("same"),
                },
                Message {
                    destination_id: "wf-clash",
                    topic: None,
                    message: "\"b\"",
                    idempotency_key: Some("same"),
                },
            ],
            None,
            None,
            false,
        )
        .await;
    match result {
        Err(Error::InvalidInput { field, detail }) => {
            assert_eq!(field, "idempotency_key");
            assert!(detail.contains("same"), "should name it, got: {detail}");
        }
        other => panic!("expected the clash to be refused, got {other:?}"),
    }
    assert!(
        sys.get_all_notifications("wf-clash")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Sending from a workflow records a step, and a replay sends nothing more.
#[tokio::test]
async fn a_replayed_send_does_not_send_again() {
    let (sys, _db) = sysdb().await;
    for id in ["wf-sender", "wf-receiver"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    let caller = ("wf-sender", 0);
    let message = Message {
        destination_id: "wf-receiver",
        topic: None,
        // No idempotency key: the step is the only thing preventing a second delivery, which is
        // what this test is about.
        message: "\"hello\"",
        idempotency_key: None,
    };

    sys.send_messages(&[message], None, Some(caller), false)
        .await
        .unwrap();
    sys.send_messages(&[message], None, Some(caller), false)
        .await
        .unwrap();

    assert_eq!(
        sys.get_all_notifications("wf-receiver")
            .await
            .unwrap()
            .len(),
        1,
        "the replay should have found the step recorded and sent nothing"
    );
    let steps = sys
        .list_workflow_steps("wf-sender", true, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1);
    // The name every implementation records for a single send, so a workflow replayed by another
    // finds what it expects.
    assert_eq!(steps[0].step_name, "DBOS.send");
}

/// Sending to a workflow that does not exist is refused, and delivers nothing.
#[tokio::test]
async fn sending_to_a_missing_workflow_is_refused() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-real"), None, Submission::Fresh)
        .await
        .unwrap();

    let result = sys
        .send_messages(
            &[
                Message {
                    destination_id: "wf-real",
                    topic: None,
                    message: "\"a\"",
                    idempotency_key: None,
                },
                Message {
                    destination_id: "wf-ghost",
                    topic: None,
                    message: "\"b\"",
                    idempotency_key: None,
                },
            ],
            None,
            None,
            false,
        )
        .await;
    assert!(matches!(result, Err(Error::NonExistentWorkflow { .. })));
    assert!(
        sys.get_all_notifications("wf-real")
            .await
            .unwrap()
            .is_empty(),
        "the deliverable message must not have been delivered either"
    );
}

/// With `send_to_forks`, a message reaches every workflow forked from its destination.
#[tokio::test]
async fn a_message_can_follow_a_workflow_to_its_forks() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-root"), None, Submission::Fresh)
        .await
        .unwrap();

    // A fork, and a fork of that fork: the walk must reach both.
    let child = sys
        .fork_workflows(&[Fork::new("wf-root")], &ForkOptions::default())
        .await
        .unwrap()
        .remove(0);
    let grandchild = sys
        .fork_workflows(&[Fork::new(&child)], &ForkOptions::default())
        .await
        .unwrap()
        .remove(0);

    sys.send_messages(
        &[Message {
            destination_id: "wf-root",
            topic: Some("t"),
            message: "\"broadcast\"",
            idempotency_key: Some("key"),
        }],
        None,
        None,
        true,
    )
    .await
    .unwrap();

    for id in ["wf-root", &child, &grandchild] {
        assert_eq!(
            sys.get_all_notifications(id).await.unwrap().len(),
            1,
            "{id} should have received the broadcast"
        );
    }

    // Without the flag, only the destination hears it.
    sys.send_messages(
        &[Message {
            destination_id: "wf-root",
            topic: Some("t"),
            message: "\"direct\"",
            idempotency_key: Some("key2"),
        }],
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(sys.get_all_notifications("wf-root").await.unwrap().len(), 2);
    assert_eq!(sys.get_all_notifications(&child).await.unwrap().len(), 1);
}

/// An empty batch still records its step, so a replay stays a replay.
///
/// The workflow spent a step id on the call. Leaving it unoccupied would make the replay re-run
/// the send — and a message list that came out empty once and non-empty the next time would then
/// be delivered for real, which is the one thing the step is there to prevent.
#[tokio::test]
async fn sending_no_messages_still_records_the_step() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-empty"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.send_messages(&[], None, Some(("wf-empty", 0)), false)
        .await
        .unwrap();

    let steps = sys
        .list_workflow_steps("wf-empty", true, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1, "the step id must not be left unoccupied");
    assert_eq!(steps[0].step_id, 0);
    // A single send carries exactly one message, so an empty batch came from the bulk API.
    assert_eq!(steps[0].step_name, "DBOS.sendBulk");

    // And it replays: a second call finds the step and does not record a second one.
    sys.send_messages(&[], None, Some(("wf-empty", 0)), false)
        .await
        .unwrap();
    assert_eq!(
        sys.list_workflow_steps("wf-empty", true, None, None)
            .await
            .unwrap()
            .len(),
        1
    );

    // With no step to record either, there is nothing to do and nothing is written.
    sys.send_messages(&[], None, None, false).await.unwrap();
    assert_eq!(
        sys.list_workflow_steps("wf-empty", true, None, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A batch records the bulk step name, a single message the plain one.
///
/// One system-database method serves both API surfaces, so the count is what distinguishes them.
/// `DBOS.send` is unanimous across the references; the bulk name follows Java's spelling, since
/// Python's `DBOS.send_bulk` would be the only snake_case name in a camelCase family.
#[tokio::test]
async fn the_recorded_step_name_follows_the_batch_size() {
    let (sys, _db) = sysdb().await;
    for id in ["wf-namer", "wf-a", "wf-b"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }
    let to = |destination_id| Message {
        destination_id,
        topic: None,
        message: "\"m\"",
        idempotency_key: None,
    };

    sys.send_messages(&[to("wf-a")], None, Some(("wf-namer", 0)), false)
        .await
        .unwrap();
    sys.send_messages(
        &[to("wf-a"), to("wf-b")],
        None,
        Some(("wf-namer", 1)),
        false,
    )
    .await
    .unwrap();

    let steps = sys
        .list_workflow_steps("wf-namer", true, None, None)
        .await
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect::<Vec<_>>(),
        [(0, "DBOS.send"), (1, "DBOS.sendBulk")],
    );
}

/// Outside a workflow with no key, nothing makes a repeated send idempotent — and that is the
/// point of the other two mechanisms.
///
/// The contrast worth pinning: the same call that delivers once inside a workflow, or once with
/// an idempotency key, delivers every time without either.
#[tokio::test]
async fn an_unprotected_send_outside_a_workflow_delivers_every_time() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-unprotected"), None, Submission::Fresh)
        .await
        .unwrap();

    let message = Message {
        destination_id: "wf-unprotected",
        topic: None,
        message: "\"again\"",
        idempotency_key: None,
    };
    for _ in 0..3 {
        sys.send_messages(&[message], None, None, false)
            .await
            .unwrap();
    }

    assert_eq!(
        sys.get_all_notifications("wf-unprotected")
            .await
            .unwrap()
            .len(),
        3,
        "no step and no key means no idempotency"
    );
}

/// A replay whose batch size changed is caught, rather than sending a second time.
///
/// The step name is derived from the message count, so a workflow that sent one message and then
/// replays sending two is asking for a step that does not match what it recorded. That is
/// nondeterminism in the workflow, and being told about it is better than the alternative — a
/// replay that silently delivers again because it looked like a different step.
#[tokio::test]
async fn a_replay_that_changed_its_batch_size_is_refused() {
    let (sys, _db) = sysdb().await;
    for id in ["wf-varying", "wf-x", "wf-y"] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }
    let to = |destination_id| Message {
        destination_id,
        topic: None,
        message: "\"m\"",
        idempotency_key: None,
    };

    // The original run sends one message, recording `DBOS.send`.
    sys.send_messages(&[to("wf-x")], None, Some(("wf-varying", 0)), false)
        .await
        .unwrap();

    // The replay sends two, which would record `DBOS.sendBulk` at the same step id.
    let result = sys
        .send_messages(
            &[to("wf-x"), to("wf-y")],
            None,
            Some(("wf-varying", 0)),
            false,
        )
        .await;
    match result {
        Err(Error::UnexpectedStep {
            step_id,
            expected,
            recorded,
            ..
        }) => {
            assert_eq!(step_id, 0);
            assert_eq!(expected, "DBOS.sendBulk");
            assert_eq!(recorded, "DBOS.send");
        }
        other => panic!("expected the changed batch to be caught, got {other:?}"),
    }

    // And nothing extra was delivered.
    assert_eq!(sys.get_all_notifications("wf-x").await.unwrap().len(), 1);
    assert!(sys.get_all_notifications("wf-y").await.unwrap().is_empty());
}

/// One offset reads back with the producer's status, from one snapshot.
#[tokio::test]
async fn a_stream_offset_reads_back_with_its_producers_status() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.write_stream(
        "wf-stream",
        0,
        "progress",
        "\"a\"",
        Some("portable_json"),
        WrittenBy::Workflow,
    )
    .await
    .unwrap();

    let read = sys
        .read_stream_value("wf-stream", "progress", 0)
        .await
        .unwrap();
    assert_eq!(read.status, WorkflowStatus::Pending);
    assert_eq!(
        read.value,
        Some(EncodedValue {
            value: "\"a\"".to_owned(),
            serialization: Some("portable_json".to_owned()),
        })
    );
}

/// Nothing at the offset still reports the status, which is what a reader waits on.
///
/// The `LEFT JOIN` is what makes this possible: a reader that got no row at all could not tell
/// "not written yet" from "no such workflow", and those are the two answers it has to act on
/// differently.
#[tokio::test]
async fn an_empty_offset_still_reports_whether_the_producer_is_running() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();

    // Nothing written at all, and no such key either — both are simply empty offsets.
    for (key, offset) in [("progress", 0), ("never-written", 0), ("progress", 7)] {
        let read = sys
            .read_stream_value("wf-stream", key, offset)
            .await
            .unwrap();
        assert_eq!(read.value, None, "key {key} offset {offset}");
        assert_eq!(read.status, WorkflowStatus::Pending);
    }

    // And the status is the *current* one, so a reader sees the producer stop.
    sys.record_workflow_outcome("wf-stream", Outcome::Output(Some("\"done\"")))
        .await
        .unwrap();
    let read = sys
        .read_stream_value("wf-stream", "progress", 0)
        .await
        .unwrap();
    assert_eq!(read.status, WorkflowStatus::Success);
    assert_eq!(read.value, None);
}

/// A stream nobody can write to is an error, not an empty stream.
#[tokio::test]
async fn reading_a_stream_of_a_missing_workflow_is_refused() {
    let (sys, _db) = sysdb().await;
    let err = sys
        .read_stream_value("wf-nobody", "progress", 0)
        .await
        .expect_err("there is no workflow to wait on");
    assert!(
        matches!(err, Error::NonExistentWorkflow { ref workflow_ids }
            if workflow_ids == &["wf-nobody".to_owned()]),
        "unexpected error: {err:?}",
    );
}

/// The closing sentinel comes back as a value; recognising it is the loop's job, not this layer's.
#[tokio::test]
async fn a_closed_stream_reports_its_sentinel_like_any_other_value() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.write_stream(
        "wf-stream",
        0,
        "progress",
        "\"a\"",
        Some("portable_json"),
        WrittenBy::Workflow,
    )
    .await
    .unwrap();
    sys.close_stream("wf-stream", 1, "progress").await.unwrap();

    assert_eq!(
        sys.read_stream_value("wf-stream", "progress", 1)
            .await
            .unwrap()
            .value
            .map(|v| v.value),
        Some(dbos::sysdb::STREAM_CLOSED.to_owned()),
        "the sentinel is a value at an offset like any other",
    );
    // And it is the last one: nothing follows a close.
    assert_eq!(
        sys.read_stream_value("wf-stream", "progress", 2)
            .await
            .unwrap()
            .value,
        None,
    );
}

/// A reader draining a stream sees every offset in order, and stops at the first empty one.
///
/// This is the loop the engine will own, written out by hand — the point being that everything it
/// needs comes from this one call: the value, whether there is one, and whether the producer is
/// still going.
#[tokio::test]
async fn the_offsets_of_a_stream_read_back_as_the_stream() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();
    for (step_id, value) in [(0, "\"a\""), (1, "\"b\""), (2, "\"c\"")] {
        sys.write_stream(
            "wf-stream",
            step_id,
            "progress",
            value,
            Some("portable_json"),
            WrittenBy::Workflow,
        )
        .await
        .unwrap();
    }
    sys.close_stream("wf-stream", 3, "progress").await.unwrap();

    let mut read = Vec::new();
    for offset in 0..10 {
        let at = sys
            .read_stream_value("wf-stream", "progress", offset)
            .await
            .unwrap();
        match at.value {
            Some(v) if v.value == dbos::sysdb::STREAM_CLOSED => break,
            Some(v) => read.push(v.value),
            None => break,
        }
    }
    assert_eq!(read, ["\"a\"", "\"b\"", "\"c\""]);
}

/// A read waiting at a cap of one does not hold its permit across the calls around it.
///
/// The loop is above this layer, so unlike `recv` and `get_event` the permit is taken and released
/// inside a single call — but the property that matters is the same: a reader parked between
/// offsets must not be holding one.
#[tokio::test]
async fn capped_stream_reads_do_not_block_each_other() {
    let db = test_database().await;
    let sys = std::sync::Arc::new(PostgresSystemDatabase::from_pool(
        db.pool().await,
        &Settings {
            polling_concurrency: Some(1),
            ..Settings::default()
        },
    ));
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.write_stream(
        "wf-stream",
        0,
        "progress",
        "\"a\"",
        None,
        WrittenBy::Workflow,
    )
    .await
    .unwrap();

    let readers: Vec<_> = (0..8)
        .map(|_| {
            let sys = std::sync::Arc::clone(&sys);
            tokio::spawn(async move { sys.read_stream_value("wf-stream", "progress", 0).await })
        })
        .collect();
    for reader in readers {
        let read = tokio::time::timeout(RECHECK * 20, reader)
            .await
            .expect("a reader starved behind the polling cap")
            .unwrap()
            .unwrap();
        assert_eq!(read.value.map(|v| v.value), Some("\"a\"".to_owned()));
    }
}

/// Stream entries land at consecutive offsets, in the order they were written.
#[tokio::test]
async fn stream_writes_are_appended_in_order() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-stream"), None, Submission::Fresh)
        .await
        .unwrap();

    for (step_id, value) in [(0, "\"a\""), (1, "\"b\""), (2, "\"c\"")] {
        sys.write_stream(
            "wf-stream",
            step_id,
            "progress",
            value,
            Some("portable_json"),
            WrittenBy::Workflow,
        )
        .await
        .unwrap();
    }

    let entries = sys.get_all_stream_entries("wf-stream").await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|e| (e.offset, e.value.as_str()))
            .collect::<Vec<_>>(),
        [(0, "\"a\""), (1, "\"b\""), (2, "\"c\"")],
    );
    // Two keys are independent streams, each numbered from zero.
    sys.write_stream("wf-stream", 3, "other", "\"z\"", None, WrittenBy::Workflow)
        .await
        .unwrap();
    let other = sys.get_all_stream_entries("wf-stream").await.unwrap();
    let zero_offsets = other.iter().filter(|e| e.offset == 0).count();
    assert_eq!(zero_offsets, 2, "each key numbers its own entries");
}

/// A workflow-level write is a step, so a replay appends nothing.
#[tokio::test]
async fn a_replayed_stream_write_does_not_append_again() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-replay"), None, Submission::Fresh)
        .await
        .unwrap();

    for _ in 0..3 {
        sys.write_stream("wf-replay", 0, "k", "\"once\"", None, WrittenBy::Workflow)
            .await
            .unwrap();
    }

    assert_eq!(
        sys.get_all_stream_entries("wf-replay").await.unwrap().len(),
        1
    );
    let steps = sys
        .list_workflow_steps("wf-replay", true, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "DBOS.writeStream");
}

/// A write from inside a step records no step of its own, so it appends every time.
///
/// That is the difference between the two writers, and it is deliberate: the enclosing step is
/// the durable unit, so a step that reruns rewrites its entries rather than replaying them.
#[tokio::test]
async fn a_write_from_inside_a_step_records_nothing_of_its_own() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-instep"), None, Submission::Fresh)
        .await
        .unwrap();

    for _ in 0..3 {
        sys.write_stream("wf-instep", 0, "k", "\"each\"", None, WrittenBy::Step)
            .await
            .unwrap();
    }

    assert_eq!(
        sys.get_all_stream_entries("wf-instep").await.unwrap().len(),
        3
    );
    assert!(
        sys.list_workflow_steps("wf-instep", true, None, None)
            .await
            .unwrap()
            .is_empty(),
        "the enclosing step is the durable unit, not this write"
    );
}

/// Closing appends the sentinel, and records itself as a close rather than a write.
#[tokio::test]
async fn closing_a_stream_appends_the_sentinel() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-close"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.write_stream("wf-close", 0, "k", "\"value\"", None, WrittenBy::Workflow)
        .await
        .unwrap();
    sys.close_stream("wf-close", 1, "k").await.unwrap();

    let entries = sys.get_all_stream_entries("wf-close").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].offset, 1);
    assert_eq!(
        entries[1].value,
        dbos::sysdb::STREAM_CLOSED,
        "closing is an ordinary append of the sentinel"
    );

    // Recorded as a close, which is what tells a replay it was closing rather than writing.
    let steps = sys
        .list_workflow_steps("wf-close", true, None, None)
        .await
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| s.step_name.as_str())
            .collect::<Vec<_>>(),
        ["DBOS.writeStream", "DBOS.closeStream"],
    );

    // And closing twice is a replay, not a second sentinel.
    sys.close_stream("wf-close", 1, "k").await.unwrap();
    assert_eq!(
        sys.get_all_stream_entries("wf-close").await.unwrap().len(),
        2
    );
}

/// Writing to a workflow that does not exist is refused.
#[tokio::test]
async fn writing_a_stream_for_a_missing_workflow_is_refused() {
    let (sys, _db) = sysdb().await;
    let result = sys
        .write_stream("wf-nowhere", 0, "k", "\"v\"", None, WrittenBy::Step)
        .await;
    assert!(matches!(result, Err(Error::NonExistentWorkflow { .. })));
}

/// A handle that names its application stamps that name on everything it writes.
///
/// The column is what makes a row *owned*, and nothing else can supply it: a row's owner is
/// decided by whoever wrote it, not by a later caller. Every table in the shared series that
/// this layer writes is checked, because a table left unstamped is a table an ownership
/// predicate silently excludes.
#[tokio::test]
async fn a_named_handle_stamps_its_application_on_what_it_writes() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    for (sys, id) in [(&alpha, "wf-alpha"), (&anonymous, "wf-nobody")] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
        sys.record_step(
            id,
            0,
            "a_step",
            Outcome::Output(Some("\"out\"")),
            None,
            None,
        )
        .await
        .unwrap();
        sys.record_child_workflow(id, &format!("{id}-child"), 1, "a_child", None)
            .await
            .unwrap();
    }

    for (id, expected) in [("wf-alpha", Some("alpha")), ("wf-nobody", None)] {
        let owner: Option<String> = sqlx::query_scalar(
            r#"SELECT "application_name" FROM "dbos"."workflow_status" WHERE "workflow_uuid" = $1"#,
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner.as_deref(), expected, "{id}: workflow owner");

        // Both steps, in one query: a step and a child launch take different insert paths.
        let owners: Vec<Option<String>> = sqlx::query_scalar(
            r#"SELECT "application_name" FROM "dbos"."operation_outputs"
               WHERE "workflow_uuid" = $1 ORDER BY "function_id""#,
        )
        .bind(id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            owners.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
            [expected, expected],
            "{id}: step owners",
        );
    }
}

/// A second submission of a running workflow does not take it over.
///
/// The insert is the only place ownership is decided, so `application_name` is deliberately
/// absent from the conflict update — the same reasoning that keeps `executor_id` from being
/// handed to whoever submitted last. A workflow that changed hands mid-run would have its
/// remaining steps recorded under one application and its earlier ones under another.
#[tokio::test]
async fn a_resubmission_does_not_re_own_a_claimed_workflow() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );

    alpha
        .init_workflow(&workflow("wf-contested"), None, Submission::Fresh)
        .await
        .unwrap();
    beta.init_workflow(&workflow("wf-contested"), None, Submission::Fresh)
        .await
        .unwrap();

    let owner: Option<String> = sqlx::query_scalar(
        r#"SELECT "application_name" FROM "dbos"."workflow_status" WHERE "workflow_uuid" = $1"#,
    )
    .bind("wf-contested")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner.as_deref(),
        Some("alpha"),
        "the first writer keeps the workflow",
    );
}

/// A fork runs on its source's application, and claims a source nobody owns.
///
/// It has to: a fork replays the steps its source recorded, so the application that recorded
/// them is the one that can interpret them. Claiming an unclaimed source is what a dequeue would
/// do anyway, and leaving it unclaimed would let every application coalesce onto the one fork.
#[tokio::test]
async fn a_fork_inherits_its_sources_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    // One source owned by alpha, one owned by nobody, each with a step to copy across.
    for (sys, id) in [(&alpha, "wf-owned"), (&anonymous, "wf-unowned")] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
        sys.record_step(
            id,
            0,
            "a_step",
            Outcome::Output(Some("\"out\"")),
            None,
            None,
        )
        .await
        .unwrap();
    }

    // Beta does the forking in both cases, and gets a different answer each time.
    beta.fork_workflows(
        &[
            Fork {
                source_id: "wf-owned",
                forked_id: Some("fork-of-owned"),
                start_step: 1,
            },
            Fork {
                source_id: "wf-unowned",
                forked_id: Some("fork-of-unowned"),
                start_step: 1,
            },
        ],
        &ForkOptions::default(),
    )
    .await
    .unwrap();

    for (fork_id, expected) in [
        ("fork-of-owned", "alpha"),  // the source's owner wins
        ("fork-of-unowned", "beta"), // nobody owned it, so the forker claims it
    ] {
        let owner: Option<String> = sqlx::query_scalar(
            r#"SELECT "application_name" FROM "dbos"."workflow_status" WHERE "workflow_uuid" = $1"#,
        )
        .bind(fork_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            owner.as_deref(),
            Some(expected),
            "{fork_id}: workflow owner"
        );

        // The copied steps take the same owner as the status row — one owner per fork.
        let step_owner: Option<String> = sqlx::query_scalar(
            r#"SELECT "application_name" FROM "dbos"."operation_outputs"
               WHERE "workflow_uuid" = $1"#,
        )
        .bind(fork_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            step_owner.as_deref(),
            Some(expected),
            "{fork_id}: step owner"
        );
    }
}

/// Recovery finds only this application's workflows, and the unclaimed ones.
///
/// The sharpest case in the feature, and it is correctness rather than filtering: `executor_id`
/// defaults to `"local"`, so two applications on one machine present the same executor to this
/// query. Unscoped, each would find the other's PENDING workflows, decide they were its own to
/// restart, and run functions it has never heard of.
#[tokio::test]
async fn recovery_does_not_reach_across_applications() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    // The same executor id and version throughout — that is the collision being guarded against.
    for (sys, id) in [
        (&alpha, "wf-alpha"),
        (&beta, "wf-beta"),
        (&anonymous, "wf-nobody"),
    ] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    let mut found = alpha.get_pending_workflows("local", "v1").await.unwrap();
    found.sort();
    assert_eq!(
        found,
        ["wf-alpha", "wf-nobody"],
        "alpha recovers its own and the unclaimed, never beta's",
    );

    // A handle with no application of its own is not scoped to anything, so it sees all three.
    let mut all = anonymous
        .get_pending_workflows("local", "v1")
        .await
        .unwrap();
    all.sort();
    assert_eq!(all, ["wf-alpha", "wf-beta", "wf-nobody"]);
}

/// Listing defaults to the caller's own application; naming an id is an identity read.
///
/// The two halves of the same field. A search that has not said whose workflows it wants means
/// its own — otherwise an unfiltered dashboard shows a peer's work. But a workflow id is a global
/// address, so a lookup by id answers about that exact workflow whoever owns it, and
/// `Applications::Any` says "every application" out loud.
#[tokio::test]
async fn listing_scopes_to_the_caller_unless_it_names_ids_or_applications() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    for (sys, id) in [
        (&alpha, "wf-alpha"),
        (&beta, "wf-beta"),
        (&anonymous, "wf-nobody"),
    ] {
        sys.init_workflow(&workflow(id), None, Submission::Fresh)
            .await
            .unwrap();
    }

    let ids = |found: Vec<WorkflowRecord>| {
        let mut ids: Vec<String> = found.into_iter().map(|w| w.workflow_id).collect();
        ids.sort();
        ids
    };

    // A search: alpha's own, plus the unclaimed.
    assert_eq!(
        ids(alpha
            .list_workflows(&WorkflowFilter::default())
            .await
            .unwrap()),
        ["wf-alpha", "wf-nobody"],
    );

    // Said out loud: everyone's.
    assert_eq!(
        ids(alpha
            .list_workflows(&WorkflowFilter {
                applications: Applications::Any,
                ..WorkflowFilter::default()
            })
            .await
            .unwrap()),
        ["wf-alpha", "wf-beta", "wf-nobody"],
    );

    // Named: that application's, plus the unclaimed.
    assert_eq!(
        ids(alpha
            .list_workflows(&WorkflowFilter {
                applications: Applications::Named(vec!["beta"]),
                ..WorkflowFilter::default()
            })
            .await
            .unwrap()),
        ["wf-beta", "wf-nobody"],
    );

    // An id is an address: alpha asks for beta's workflow by id and gets it.
    assert_eq!(
        ids(alpha
            .list_workflows(&WorkflowFilter {
                workflow_ids: vec!["wf-beta"],
                ..WorkflowFilter::default()
            })
            .await
            .unwrap()),
        ["wf-beta"],
    );

    // A prefix is a search, not an address, so it stays scoped.
    assert_eq!(
        ids(alpha
            .list_workflows(&WorkflowFilter {
                workflow_id_prefixes: vec!["wf-"],
                ..WorkflowFilter::default()
            })
            .await
            .unwrap()),
        ["wf-alpha", "wf-nobody"],
    );

    // And a nameless handle has nothing to scope to, so its default search sees everything.
    assert_eq!(
        ids(anonymous
            .list_workflows(&WorkflowFilter::default())
            .await
            .unwrap()),
        ["wf-alpha", "wf-beta", "wf-nobody"],
    );
}

/// The owner is readable through the surface, not just through the column.
#[tokio::test]
async fn a_workflow_reports_the_application_that_owns_it() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    alpha
        .init_workflow(&workflow("wf-owned"), None, Submission::Fresh)
        .await
        .unwrap();
    anonymous
        .init_workflow(&workflow("wf-unowned"), None, Submission::Fresh)
        .await
        .unwrap();

    let owned = alpha.get_workflow("wf-owned").await.unwrap().unwrap();
    assert_eq!(owned.application_name.as_deref(), Some("alpha"));
    let unowned = alpha.get_workflow("wf-unowned").await.unwrap().unwrap();
    assert_eq!(
        unowned.application_name, None,
        "unclaimed reads back as unclaimed rather than as this handle's own",
    );
}

/// A delayed workflow is released by its own application, never by a peer.
#[tokio::test]
async fn releasing_delayed_workflows_stays_within_an_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));

    // Both delayed, then both moved into the past, so only the scope decides which is released.
    for (sys, id) in [(&alpha, "wf-alpha"), (&beta, "wf-beta")] {
        let wf = NewWorkflow {
            queue_name: Some("orders"),
            delay: Some(std::time::Duration::from_secs(3600)),
            ..NewWorkflow::new(id)
        };
        sys.init_workflow(&wf, None, Submission::Fresh)
            .await
            .unwrap();
        sys.set_workflow_delay(id, WorkflowDelay::Until(Timestamp::from_epoch_ms(1)))
            .await
            .unwrap();
    }

    assert_eq!(
        alpha.transition_delayed_workflows().await.unwrap(),
        1,
        "alpha releases only its own",
    );
    assert_eq!(
        beta.get_workflow("wf-beta").await.unwrap().unwrap().status,
        WorkflowStatus::Delayed,
        "beta's workflow is untouched",
    );
    assert_eq!(beta.transition_delayed_workflows().await.unwrap(), 1);
}

/// Each application sees its own latest version, and a peer's deploy cannot demote it.
///
/// "Latest" is what a dequeue compares a workflow's recorded version against, so this is not a
/// display concern: if a peer's newer registration counted as this application's latest, every
/// workflow this application had already enqueued would stop matching and stay stranded.
#[tokio::test]
async fn a_peers_deploy_does_not_become_this_applications_latest() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));

    alpha
        .create_application_version("alpha-v1", None)
        .await
        .unwrap();
    beta.create_application_version("beta-v1", None)
        .await
        .unwrap();
    // Registered last, so an unscoped "latest" would return it to both.
    beta.create_application_version("beta-v2", None)
        .await
        .unwrap();

    let latest = alpha
        .get_latest_application_version(None)
        .await
        .unwrap()
        .expect("alpha has a version");
    assert_eq!(latest.version_name, "alpha-v1");
    assert_eq!(latest.application_name.as_deref(), Some("alpha"));

    // And the listing is scoped the same way.
    let names: Vec<String> = alpha
        .list_application_versions()
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.version_name)
        .collect();
    assert_eq!(names, ["alpha-v1"]);
}

/// A version registered by another application is refused rather than taken.
///
/// Version names address a row across every application sharing the database, so this is not the
/// library's to resolve: taking it would retime a peer's deploy, and ignoring the write would
/// leave this application pointing at a version it does not own.
#[tokio::test]
async fn registering_a_peers_version_name_is_refused() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));

    alpha
        .create_application_version("v1.0.0", None)
        .await
        .unwrap();

    let result = beta.create_application_version("v1.0.0", None).await;
    match result {
        Err(Error::RegisteredByAnother {
            kind,
            ref name,
            ref holder,
            ref claimant,
        }) => {
            assert_eq!(kind, "Application version");
            assert_eq!(name, "v1.0.0");
            assert_eq!(holder, "alpha");
            assert_eq!(claimant.as_deref(), Some("beta"));
            // The message has to carry the remedy: nothing here can be acted on from code.
            let text = result.as_ref().unwrap_err().to_string();
            assert!(text.contains("already registered"), "got {text}");
            assert!(text.contains("was renamed"), "got {text}");
        }
        other => panic!("expected a registration conflict, got {other:?}"),
    }

    // Promotion is refused for the same reason, and alpha's timestamp is untouched.
    let before = alpha
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap()
        .version_timestamp;
    let promote = beta
        .update_application_version_timestamp("v1.0.0", Timestamp::from_epoch_ms(9_000_000), None)
        .await;
    assert!(matches!(promote, Err(Error::RegisteredByAnother { .. })));
    assert_eq!(
        alpha
            .get_latest_application_version(None)
            .await
            .unwrap()
            .unwrap()
            .version_timestamp,
        before,
    );

    // Re-registering under the same name is not a conflict; it is the ordinary restart path.
    alpha
        .create_application_version("v1.0.0", None)
        .await
        .unwrap();
}

/// An unclaimed version is claimed in place, keeping the timestamp that decides which is current.
///
/// The row an older SDK left behind is the case: recreating the version would reset the timestamp
/// and silently promote it, so the claim is an `UPDATE` guarded on the row being unowned.
#[tokio::test]
async fn an_unclaimed_version_is_claimed_where_it_stands() {
    let db = test_database().await;
    let pool = db.pool().await;
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );

    // Stands in for a row written before any implementation had application names.
    anonymous
        .create_application_version("v1", None)
        .await
        .unwrap();
    anonymous
        .update_application_version_timestamp("v1", Timestamp::from_epoch_ms(5_000_000), None)
        .await
        .unwrap();
    let unclaimed = anonymous
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unclaimed.application_name, None);

    alpha.create_application_version("v1", None).await.unwrap();

    let claimed = alpha
        .get_latest_application_version(None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.application_name.as_deref(), Some("alpha"));
    assert_eq!(
        claimed.version_id, unclaimed.version_id,
        "claimed in place rather than recreated",
    );
    assert_eq!(
        claimed.version_timestamp, unclaimed.version_timestamp,
        "the timestamp that decides which version is current is untouched",
    );

    // A nameless writer leaves the owner alone rather than clearing it.
    anonymous
        .create_application_version("v1", None)
        .await
        .unwrap();
    assert_eq!(
        alpha
            .get_latest_application_version(None)
            .await
            .unwrap()
            .unwrap()
            .application_name
            .as_deref(),
        Some("alpha"),
    );
}

/// A handle can act for an application other than its own, on every method that takes a target.
///
/// This is the client's case: a tool with no application of its own — or one acting on behalf of
/// another — names the target per call rather than being configured as it. The handle's name is
/// only the default, which is what `applicationName ?? this.appName` means in both references.
#[tokio::test]
async fn a_named_target_overrides_the_handles_own_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let client = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    // A nameless client registers a version *for* beta.
    client
        .create_application_version("beta-v1", Some("beta"))
        .await
        .unwrap();
    // And alpha, which has its own name, registers one for beta too — the target wins over it.
    alpha
        .create_application_version("beta-v2", Some("beta"))
        .await
        .unwrap();

    // Neither landed on alpha: its own view is empty.
    assert!(
        alpha
            .get_latest_application_version(None)
            .await
            .unwrap()
            .is_none(),
        "alpha registered nothing for itself",
    );

    // Read back by naming beta, from a handle that is not beta.
    let latest = alpha
        .get_latest_application_version(Some("beta"))
        .await
        .unwrap()
        .expect("beta has versions");
    assert_eq!(latest.application_name.as_deref(), Some("beta"));

    // Promotion is targeted the same way: pin beta-v1 past beta-v2 from alpha's handle. The
    // timestamp has to be genuinely later than beta-v2's, which migration 13 defaults to `now()`.
    alpha
        .update_application_version_timestamp(
            "beta-v1",
            Timestamp::from_epoch_ms(99_000_000_000_000),
            Some("beta"),
        )
        .await
        .unwrap();
    assert_eq!(
        alpha
            .get_latest_application_version(Some("beta"))
            .await
            .unwrap()
            .unwrap()
            .version_name,
        "beta-v1",
        "an older version promoted past a newer one is what a rollback looks like",
    );

    // Naming a peer does not let a handle take a version another application holds.
    alpha
        .create_application_version("shared", None)
        .await
        .unwrap();
    let stolen = client
        .create_application_version("shared", Some("beta"))
        .await;
    assert!(matches!(stolen, Err(Error::RegisteredByAnother { .. })));
}

/// A workflow can be enqueued for an application other than the writing handle's.
///
/// The client enqueue: the target is on the creation input, not the handle, because only the
/// insert decides a workflow's owner and a client may write for several applications in turn.
#[tokio::test]
async fn a_workflow_can_be_enqueued_for_another_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );

    // Named on the input, so it overrides alpha.
    let mut for_beta = workflow("wf-for-beta");
    for_beta.application_name = Some("beta");
    alpha
        .init_workflow(&for_beta, None, Submission::Fresh)
        .await
        .unwrap();
    // Unnamed on the input, so it falls back to the handle.
    alpha
        .init_workflow(&workflow("wf-for-alpha"), None, Submission::Fresh)
        .await
        .unwrap();

    for (id, expected) in [("wf-for-beta", "beta"), ("wf-for-alpha", "alpha")] {
        assert_eq!(
            alpha
                .get_workflow(id)
                .await
                .unwrap()
                .unwrap()
                .application_name
                .as_deref(),
            Some(expected),
        );
    }

    // And alpha's recovery sweep leaves beta's workflow alone, even though alpha wrote it.
    let pending = alpha.get_pending_workflows("local", "v1").await.unwrap();
    assert_eq!(pending, ["wf-for-alpha"]);
}

/// Two applications racing to claim one unclaimed version: exactly one gets it.
///
/// The claim is `UPDATE … WHERE version_name = $2 AND application_name IS NULL`, and that guard is
/// the whole of the safety here — not the surrounding transaction, which at READ COMMITTED does
/// not stop the row changing under it. What does is Postgres re-evaluating the `WHERE` after the
/// blocked writer unblocks: it re-reads the committed row, finds `application_name` no longer
/// null, and matches nothing.
///
/// Drop the `IS NULL` guard and both writes land, the later one silently overwriting the earlier —
/// which every serial test would still pass. This is what notices.
#[tokio::test]
async fn two_applications_cannot_both_claim_one_version() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    // An unclaimed version, as a pre-ownership SDK would have left it.
    anonymous
        .create_application_version("contested", None)
        .await
        .unwrap();

    let (a, b) = tokio::join!(
        alpha.create_application_version("contested", None),
        beta.create_application_version("contested", None),
    );

    assert_eq!(
        [a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(),
        1,
        "exactly one application may hold a version name; got alpha={a:?} beta={b:?}",
    );

    // The loser is refused rather than silently ignored.
    let loser = if a.is_err() { &a } else { &b };
    assert!(
        matches!(loser, Err(Error::RegisteredByAnother { .. })),
        "the loser should be told why, got {loser:?}",
    );

    // And the row belongs to whichever won, not to whoever wrote last.
    let owner: Option<String> = sqlx::query_scalar(
        r#"SELECT "application_name" FROM "dbos"."application_versions" WHERE "version_name" = $1"#,
    )
    .bind("contested")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner.as_deref(),
        Some(if a.is_ok() { "alpha" } else { "beta" })
    );
}

/// Inserts a queue and a schedule directly, since 3.6 has not built their methods yet.
async fn register_queue_and_schedule(pool: &sqlx::PgPool, suffix: &str, owner: Option<&str>) {
    sqlx::query(r#"INSERT INTO "dbos"."queues" ("name", "application_name") VALUES ($1, $2)"#)
        .bind(format!("queue-{suffix}"))
        .bind(owner)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_schedules"
           ("schedule_id", "schedule_name", "workflow_name", "schedule", "context", "application_name")
           VALUES ($1, $2, 'nightly', '0 0 * * *', '{}', $3)"#,
    )
    .bind(format!("sched-id-{suffix}"))
    .bind(format!("sched-{suffix}"))
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}

async fn owner_of(pool: &sqlx::PgPool, table: &str, key_column: &str, key: &str) -> Option<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        r#"SELECT "application_name" FROM "dbos"."{table}" WHERE "{key_column}" = $1"#
    )))
    .bind(key)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// A rename re-owns every kind of row an application holds.
#[tokio::test]
async fn a_rename_moves_every_kind_of_row() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );

    register_queue_and_schedule(&pool, "a", Some("alpha")).await;
    alpha.create_application_version("v1", None).await.unwrap();

    // One workflow still in flight and one finished, so both halves of the rename are exercised.
    alpha
        .init_workflow(&workflow("wf-running"), None, Submission::Fresh)
        .await
        .unwrap();
    alpha
        .init_workflow(&workflow("wf-done"), None, Submission::Fresh)
        .await
        .unwrap();
    alpha
        .record_step(
            "wf-done",
            0,
            "a_step",
            Outcome::Output(Some("\"x\"")),
            None,
            None,
        )
        .await
        .unwrap();
    alpha
        .record_workflow_outcome("wf-done", Outcome::Output(Some("\"done\"")))
        .await
        .unwrap();

    let counts = alpha
        .rename_application(
            RenameFrom::Application("alpha"),
            "alpha-renamed",
            RenameBatching::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        counts,
        dbos::sysdb::types::ApplicationRowCounts {
            queues: 1,
            schedules: 1,
            versions: 1,
            workflows: 2, // one in-flight, one terminal
            steps: 1,
        },
    );

    for (table, key_column, key) in [
        ("queues", "name", "queue-a"),
        ("workflow_schedules", "schedule_name", "sched-a"),
        ("application_versions", "version_name", "v1"),
        ("workflow_status", "workflow_uuid", "wf-running"),
        ("workflow_status", "workflow_uuid", "wf-done"),
        ("operation_outputs", "workflow_uuid", "wf-done"),
    ] {
        assert_eq!(
            owner_of(&pool, table, key_column, key).await.as_deref(),
            Some("alpha-renamed"),
            "{table}.{key} did not move",
        );
    }
}

/// Unclaimed rows move only when the source asks for them.
///
/// The one place in this feature where `IS NULL` does not ride along with an ownership predicate:
/// an unclaimed row belongs to every application, so taking it from all of them is a decision.
#[tokio::test]
async fn a_rename_takes_unclaimed_rows_only_when_asked() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let alpha = named("alpha");
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    alpha
        .init_workflow(&workflow("wf-alpha"), None, Submission::Fresh)
        .await
        .unwrap();
    anonymous
        .init_workflow(&workflow("wf-nobody"), None, Submission::Fresh)
        .await
        .unwrap();

    // Naming the application alone leaves the unclaimed workflow where it is.
    let counts = alpha
        .rename_application(
            RenameFrom::Application("alpha"),
            "alpha-two",
            RenameBatching::Unbatched,
        )
        .await
        .unwrap();
    assert_eq!(counts.workflows, 1);
    assert_eq!(
        owner_of(&pool, "workflow_status", "workflow_uuid", "wf-nobody").await,
        None,
        "an unclaimed row is not swept up by a plain rename",
    );

    // Asking for them adopts it.
    let counts = alpha
        .rename_application(
            RenameFrom::Unclaimed,
            "alpha-two",
            RenameBatching::Unbatched,
        )
        .await
        .unwrap();
    assert_eq!(counts.workflows, 1);
    assert_eq!(
        owner_of(&pool, "workflow_status", "workflow_uuid", "wf-nobody")
            .await
            .as_deref(),
        Some("alpha-two"),
    );
}

/// Batching moves every row, including the ones a watermark would otherwise skip.
///
/// The batch size is deliberately smaller than the workflow count, so the loop runs several
/// passes and the half-open ranges have to tile the key space exactly. A gap loses rows silently.
#[tokio::test]
async fn a_batched_rename_loses_no_rows() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );

    // Seven terminal workflows, each with two steps: enough for four passes at a batch of two,
    // and enough steps per key to catch a batch that splits one workflow across two ranges.
    for i in 0..7 {
        let id = format!("wf-{i:02}");
        alpha
            .init_workflow(&workflow(&id), None, Submission::Fresh)
            .await
            .unwrap();
        for step in 0..2 {
            alpha
                .record_step(
                    &id,
                    step,
                    "a_step",
                    Outcome::Output(Some("\"x\"")),
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        alpha
            .record_workflow_outcome(&id, Outcome::Output(Some("\"done\"")))
            .await
            .unwrap();
    }

    let counts = alpha
        .rename_application(
            RenameFrom::Application("alpha"),
            "alpha-batched",
            RenameBatching::Batched(2),
        )
        .await
        .unwrap();

    assert_eq!(counts.workflows, 7, "every workflow moved");
    assert_eq!(counts.steps, 14, "every step moved");

    let stragglers: i64 = sqlx::query_scalar(
        r#"SELECT count(*) FROM "dbos"."workflow_status" WHERE "application_name" = 'alpha'"#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stragglers, 0, "no workflow was left behind by the batching");
}

/// A rename validates its target name and refuses a no-op.
#[tokio::test]
async fn a_rename_refuses_a_bad_target_name() {
    let (sys, _db) = sysdb().await;

    for bad in ["ab", "Has-Capitals", "has spaces", &"x".repeat(31)] {
        let result = sys
            .rename_application(
                RenameFrom::Application("alpha"),
                bad,
                RenameBatching::Unbatched,
            )
            .await;
        assert!(
            matches!(
                result,
                Err(Error::InvalidInput {
                    field: "new_name",
                    ..
                })
            ),
            "{bad:?} should be rejected, got {result:?}",
        );
    }

    // Renaming an application to the name it already holds is a no-op worth refusing.
    let result = sys
        .rename_application(
            RenameFrom::Application("alpha"),
            "alpha",
            RenameBatching::Unbatched,
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::InvalidInput {
            field: "new_name",
            ..
        })
    ));
}

/// A queue round-trips through the registry, including both of its fractional-second periods.
///
/// `rate_limit_period_sec` and `polling_interval_sec` are `DOUBLE PRECISION` seconds where every
/// other duration in the schema is integer milliseconds, so this checks the unit as much as the
/// storage — a value that survived as `1` rather than `1.5` would pass a coarser assertion.
#[tokio::test]
async fn a_queue_round_trips_through_the_registry() {
    let (sys, _db) = sysdb().await;

    let queue = NewQueue {
        concurrency: Some(8),
        worker_concurrency: Some(2),
        rate_limit: Some(RateLimit {
            limit: 100,
            period: std::time::Duration::from_millis(1_500),
        }),
        priority_enabled: true,
        partition_queue: true,
        polling_interval: std::time::Duration::from_millis(250),
        ..NewQueue::new("orders")
    };
    assert!(
        sys.upsert_queue(&queue, OnExistingQueue::Update)
            .await
            .unwrap(),
        "the first registration creates the row",
    );

    let read = sys.get_queue("orders").await.unwrap().expect("registered");
    assert_eq!(read.name, "orders");
    assert_eq!(read.concurrency, Some(8));
    assert_eq!(read.worker_concurrency, Some(2));
    assert_eq!(
        read.rate_limit,
        Some(RateLimit {
            limit: 100,
            period: std::time::Duration::from_millis(1_500),
        }),
    );
    assert!(read.priority_enabled);
    assert!(read.partition_queue);
    assert_eq!(read.polling_interval, std::time::Duration::from_millis(250));

    assert!(sys.get_queue("no-such-queue").await.unwrap().is_none());
}

/// Re-registering reports that the queue already existed, and honours what to do with it.
#[tokio::test]
async fn re_registering_a_queue_reports_it_existed() {
    let (sys, _db) = sysdb().await;

    let first = NewQueue {
        concurrency: Some(1),
        ..NewQueue::new("orders")
    };
    assert!(
        sys.upsert_queue(&first, OnExistingQueue::Update)
            .await
            .unwrap()
    );

    let second = NewQueue {
        concurrency: Some(9),
        ..NewQueue::new("orders")
    };
    assert!(
        !sys.upsert_queue(&second, OnExistingQueue::Update)
            .await
            .unwrap(),
        "the second registration finds the row already there",
    );
    assert_eq!(
        sys.get_queue("orders").await.unwrap().unwrap().concurrency,
        Some(9),
        "Update overwrites the stored limits",
    );

    let third = NewQueue {
        concurrency: Some(3),
        ..NewQueue::new("orders")
    };
    assert!(
        !sys.upsert_queue(&third, OnExistingQueue::Leave)
            .await
            .unwrap()
    );
    assert_eq!(
        sys.get_queue("orders").await.unwrap().unwrap().concurrency,
        Some(9),
        "Leave keeps what is stored",
    );
}

/// A queue name held by another application is refused, in either mode.
///
/// The name addresses one row across every application sharing the database, so registering over
/// it would point this application at a peer's work. Ownership moves only by rename.
#[tokio::test]
async fn a_peers_queue_name_is_refused() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));

    alpha
        .upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();

    for mode in [OnExistingQueue::Update, OnExistingQueue::Leave] {
        let result = beta.upsert_queue(&NewQueue::new("orders"), mode).await;
        match result {
            Err(Error::RegisteredByAnother {
                kind,
                ref holder,
                ref claimant,
                ..
            }) => {
                assert_eq!(kind, "Queue");
                assert_eq!(holder, "alpha");
                assert_eq!(claimant.as_deref(), Some("beta"));
            }
            other => panic!("{mode:?} should be refused, got {other:?}"),
        }
    }

    // Alpha's own row is untouched, and alpha may still re-register it.
    assert_eq!(
        sys_owner(&pool, "orders").await.as_deref(),
        Some("alpha"),
        "a refused registration changes nothing",
    );
    alpha
        .upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();

    // A nameless writer leaves the owner alone rather than clearing it.
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());
    anonymous
        .upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    assert_eq!(sys_owner(&pool, "orders").await.as_deref(), Some("alpha"));
}

async fn sys_owner(pool: &sqlx::PgPool, queue: &str) -> Option<String> {
    sqlx::query_scalar(r#"SELECT "application_name" FROM "dbos"."queues" WHERE "name" = $1"#)
        .bind(queue)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Listing scopes to the caller's application; reading one by name does not.
#[tokio::test]
async fn listing_queues_scopes_but_reading_one_does_not() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    for (sys, name) in [
        (&alpha, "alpha-queue"),
        (&beta, "beta-queue"),
        (&anonymous, "nobodys-queue"),
    ] {
        sys.upsert_queue(&NewQueue::new(name), OnExistingQueue::Update)
            .await
            .unwrap();
    }

    let names = |queues: Vec<dbos::sysdb::types::QueueRecord>| {
        queues.into_iter().map(|q| q.name).collect::<Vec<_>>()
    };

    assert_eq!(
        names(alpha.list_queues(&Applications::Unset).await.unwrap()),
        ["alpha-queue", "nobodys-queue"],
        "a search defaults to this application's own plus the unclaimed",
    );
    assert_eq!(
        names(alpha.list_queues(&Applications::Any).await.unwrap()),
        ["alpha-queue", "beta-queue", "nobodys-queue"],
    );
    assert_eq!(
        names(
            alpha
                .list_queues(&Applications::Named(vec!["beta"]))
                .await
                .unwrap()
        ),
        ["beta-queue", "nobodys-queue"],
    );

    // Addressed by name, so alpha reads beta's queue rather than being told it does not exist.
    assert_eq!(
        alpha.get_queue("beta-queue").await.unwrap().unwrap().name,
        "beta-queue",
    );

    // Deleting removes only the registration.
    alpha.delete_queue("alpha-queue").await.unwrap();
    assert!(alpha.get_queue("alpha-queue").await.unwrap().is_none());
    // Deleting one that is not there is not an error.
    alpha.delete_queue("alpha-queue").await.unwrap();
}

/// A queue carrying half a rate limit reads as having none, as TypeScript does.
///
/// The two columns are only meaningful together and no SDK can write one alone, so reaching this
/// state takes direct SQL. Matching TypeScript matters more than reporting it: a peer reading the
/// same row must not see a different queue. The safety cost — an unthrottled queue that says
/// nothing — is recorded as a `TODO` on the reader.
#[tokio::test]
async fn half_a_rate_limit_reads_as_none() {
    let (sys, db) = sysdb().await;
    let pool = db.pool().await;

    sqlx::query(
        r#"INSERT INTO "dbos"."queues" ("name", "rate_limit_max") VALUES ('half-limit', 10)"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let queue = sys.get_queue("half-limit").await.unwrap().expect("exists");
    assert_eq!(queue.rate_limit, None);
}

/// Whether to skip a test that asserts a dequeue returned every workflow enqueued.
///
/// **`SKIP LOCKED` under-delivers on CockroachDB.** It skips rows whose write intents are still
/// unresolved, which a just-enqueued workflow's are, so a dequeue comes back short and the work
/// waits for the next poll — 11 of 15 rounds in a stress run. Nothing in the test can prevent it:
/// a row is readable while still being skippable, so reading it first only narrows the window.
///
/// Skipped rather than loosened, because the assertion is the point: these tests are what caught
/// the behaviour, and Java's equivalent tests miss it precisely because they assert a limit being
/// enforced rather than a count being complete. Weakening ours the same way would lose the only
/// coverage anyone has.
///
/// Remove this once the lock mode is settled — see the `TODO` on `start_queued_workflows` and
/// `UPSTREAM.md`.
fn skip_dequeue_completeness(db: &support::TestDatabase) -> bool {
    let skipping = matches!(db.backend(), Backend::Cockroach);
    if skipping {
        eprintln!("skipped on CockroachDB: SKIP LOCKED may under-deliver; see UPSTREAM.md item 7");
    }
    skipping
}

/// Enqueues a workflow on `queue`, optionally with a priority and a timeout.
async fn enqueue(
    sys: &PostgresSystemDatabase,
    id: &str,
    queue: &str,
    priority: i32,
    timeout: Option<std::time::Duration>,
) {
    let wf = NewWorkflow {
        queue_name: Some(queue),
        priority,
        timeout,
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
}

/// A dequeue takes enqueued workflows in priority then age order, and starts them.
#[tokio::test]
async fn a_dequeue_starts_workflows_in_order() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let queue = NewQueue::new("orders");
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    // Enqueued oldest first, but the priorities invert that for two of them.
    enqueue(&sys, "wf-low", "orders", 10, None).await;
    enqueue(&sys, "wf-high", "orders", 1, None).await;
    enqueue(&sys, "wf-also-high", "orders", 1, None).await;

    let started = sys
        .start_queued_workflows(&registered, "exec-1", "v1", None, 0)
        .await
        .unwrap();
    assert_eq!(
        started,
        ["wf-high", "wf-also-high", "wf-low"],
        "priority first, then age within a priority",
    );

    let read = sys.get_workflow("wf-high").await.unwrap().unwrap();
    assert_eq!(read.status, WorkflowStatus::Pending);
    assert_eq!(read.executor_id.as_deref(), Some("exec-1"));
    assert!(read.started_at.is_some());

    // Nothing is left to take.
    assert!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A dequeue sets a deadline from the workflow's timeout, and leaves one already set alone.
#[tokio::test]
async fn a_dequeue_sets_the_deadline_from_the_timeout() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    enqueue(
        &sys,
        "wf-timed",
        "orders",
        0,
        Some(std::time::Duration::from_secs(60)),
    )
    .await;
    enqueue(&sys, "wf-untimed", "orders", 0, None).await;

    let before = Timestamp::now();
    sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
        .await
        .unwrap();

    let timed = sys.get_workflow("wf-timed").await.unwrap().unwrap();
    let deadline = timed
        .deadline
        .expect("a timeout becomes a deadline on dequeue");
    let offset = deadline.as_epoch_ms() - before.as_epoch_ms();
    assert!(
        (60_000..=65_000).contains(&offset),
        "the deadline is the start plus the timeout, got {offset}ms",
    );
    assert_eq!(
        sys.get_workflow("wf-untimed")
            .await
            .unwrap()
            .unwrap()
            .deadline,
        None,
        "a workflow with no timeout gets no deadline",
    );
}

/// Worker concurrency bounds a dequeue by what this process is already running.
#[tokio::test]
async fn worker_concurrency_bounds_a_dequeue() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let queue = NewQueue {
        worker_concurrency: Some(2),
        ..NewQueue::new("orders")
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    for i in 0..5 {
        enqueue(&sys, &format!("wf-{i}"), "orders", 0, None).await;
    }

    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .len(),
        2,
        "a free worker takes its whole allowance",
    );
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 1)
            .await
            .unwrap()
            .len(),
        1,
        "one already running leaves room for one",
    );
    assert!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 2)
            .await
            .unwrap()
            .is_empty(),
        "a full worker takes nothing",
    );
}

/// Global concurrency counts what every executor has running, not just this one.
#[tokio::test]
async fn global_concurrency_counts_across_executors() {
    let (sys, db) = sysdb().await;
    // Both assertions below are exact counts, so an under-delivering dequeue breaks them: too few
    // started, and then the second executor finds the slack the first one left.
    if skip_dequeue_completeness(&db) {
        return;
    }
    let queue = NewQueue {
        concurrency: Some(2),
        ..NewQueue::new("orders")
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    for i in 0..4 {
        enqueue(&sys, &format!("wf-{i}"), "orders", 0, None).await;
    }

    // A different executor takes the first two, and they are still PENDING.
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-other", "v1", None, 0)
            .await
            .unwrap()
            .len(),
        2,
    );
    // This one is told nothing is available, even with nothing running locally.
    assert!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .is_empty(),
        "the limit is global, so a second executor sees none free",
    );
}

/// A rate limit stops a dequeue once the window is full, and refills once it passes.
#[tokio::test]
async fn a_rate_limit_bounds_starts_per_window() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let queue = NewQueue {
        rate_limit: Some(RateLimit {
            limit: 2,
            period: std::time::Duration::from_millis(400),
        }),
        ..NewQueue::new("orders")
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    for i in 0..5 {
        enqueue(&sys, &format!("wf-{i}"), "orders", 0, None).await;
    }

    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .len(),
        2,
        "the window allows two",
    );
    assert!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .is_empty(),
        "and no more until it passes",
    );

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap()
            .len(),
        2,
        "a fresh window allows two more",
    );
}

/// A dequeue takes only this application's workflows, and claims the unclaimed ones.
///
/// This is 5.2: the claim rides on the same statement that starts the workflow, so an unclaimed
/// workflow belongs to whichever application dequeued it and nothing can take it back.
#[tokio::test]
async fn a_dequeue_claims_what_it_starts() {
    let db = test_database().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    alpha
        .upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = alpha.get_queue("orders").await.unwrap().unwrap();

    enqueue(&alpha, "wf-alpha", "orders", 0, None).await;
    enqueue(&beta, "wf-beta", "orders", 0, None).await;
    enqueue(&anonymous, "wf-nobody", "orders", 0, None).await;

    let mut started = alpha
        .start_queued_workflows(&registered, "exec-1", "v1", None, 0)
        .await
        .unwrap();
    started.sort();
    assert_eq!(
        started,
        ["wf-alpha", "wf-nobody"],
        "alpha takes its own and the unclaimed, never beta's",
    );

    assert_eq!(
        alpha
            .get_workflow("wf-nobody")
            .await
            .unwrap()
            .unwrap()
            .application_name
            .as_deref(),
        Some("alpha"),
        "starting an unclaimed workflow claims it",
    );
    assert_eq!(
        alpha.get_workflow("wf-beta").await.unwrap().unwrap().status,
        WorkflowStatus::Enqueued,
        "beta's workflow is untouched",
    );
}

/// An unversioned workflow is taken only by an executor running the latest version.
#[tokio::test]
async fn unversioned_work_goes_to_the_latest_version() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();
    sys.create_application_version("v2", None).await.unwrap();

    let unversioned = NewWorkflow {
        queue_name: Some("orders"),
        ..NewWorkflow::new("wf-unversioned")
    };
    sys.init_workflow(&unversioned, None, Submission::Fresh)
        .await
        .unwrap();

    // An executor on a superseded version leaves it alone.
    assert!(
        sys.start_queued_workflows(&registered, "exec-old", "v1", None, 0)
            .await
            .unwrap()
            .is_empty(),
        "only the latest version adopts unversioned work",
    );
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-new", "v2", None, 0)
            .await
            .unwrap(),
        ["wf-unversioned"],
    );
}

/// An empty partition key is refused rather than matching nothing.
///
/// `NewWorkflow` and `ForkOptions` both reject one on the way in, so no row can hold it. A
/// dequeue that accepted it would select nothing and look like an idle partition.
#[tokio::test]
async fn an_empty_partition_key_is_refused() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    let registered = sys.get_queue("orders").await.unwrap().unwrap();

    let result = sys
        .start_queued_workflows(&registered, "exec-1", "v1", Some(""), 0)
        .await;
    assert!(
        matches!(
            result,
            Err(Error::InvalidInput {
                field: "partition_key",
                ..
            })
        ),
        "got {result:?}",
    );

    // And absent still means every partition.
    enqueue(&sys, "wf-1", "orders", 0, None).await;
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap(),
        ["wf-1"],
    );
}

/// A period too large for a `Duration` is reported, not a panic.
///
/// `rate_limit_period_sec` and `polling_interval_sec` are `DOUBLE PRECISION`, so a row can hold a
/// value no `Duration` can represent. Reading one used to panic inside `Duration::from_secs_f64`,
/// which a library must never do on stored data.
#[tokio::test]
async fn an_unrepresentable_period_is_malformed() {
    let (sys, db) = sysdb().await;
    let pool = db.pool().await;

    sqlx::query(
        r#"INSERT INTO "dbos"."queues" ("name", "polling_interval_sec") VALUES ('huge', 1e300)"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    match sys.get_queue("huge").await {
        Err(Error::Malformed(message)) => {
            assert!(message.contains("polling_interval_sec"), "got {message}");
        }
        other => panic!("expected a malformed row, got {other:?}"),
    }
}

/// Registers a partitioned queue that a sweep will accept.
async fn partitioned_queue(sys: &PostgresSystemDatabase, name: &'static str) -> QueueRecord {
    let queue = NewQueue {
        concurrency: Some(1),
        partition_queue: true,
        ..NewQueue::new(name)
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();
    sys.get_queue(name).await.unwrap().unwrap()
}

/// Enqueues into a partition.
async fn enqueue_partitioned(sys: &PostgresSystemDatabase, id: &str, queue: &str, partition: &str) {
    let wf = NewWorkflow {
        queue_name: Some(queue),
        queue_partition_key: Some(partition),
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
}

/// The partitions of a queue are the distinct keys with work waiting, each once.
#[tokio::test]
async fn queue_partitions_are_the_keys_with_work() {
    let (sys, _db) = sysdb().await;
    partitioned_queue(&sys, "orders").await;

    enqueue_partitioned(&sys, "wf-a1", "orders", "alpha").await;
    enqueue_partitioned(&sys, "wf-a2", "orders", "alpha").await;
    enqueue_partitioned(&sys, "wf-b1", "orders", "beta").await;
    // Unpartitioned work on the same queue is not a partition.
    enqueue(&sys, "wf-none", "orders", 0, None).await;

    assert_eq!(
        sys.get_queue_partitions("orders").await.unwrap(),
        ["alpha", "beta"],
        "each key once, in order, and no null key",
    );
    assert!(sys.get_queue_partitions("other").await.unwrap().is_empty());
}

/// A sweep takes one workflow per partition, and will not take a second while the first runs.
#[tokio::test]
async fn a_sweep_takes_one_head_per_partition() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let queue = partitioned_queue(&sys, "orders").await;

    for partition in ["alpha", "beta"] {
        for n in 0..2 {
            enqueue_partitioned(&sys, &format!("wf-{partition}-{n}"), "orders", partition).await;
        }
    }

    let mut started = sys
        .start_queued_partitioned_workflows(&queue, "exec-1", "v1")
        .await
        .unwrap();
    started.sort();
    assert_eq!(
        started,
        ["wf-alpha-0", "wf-beta-0"],
        "the head of each partition, never two from one",
    );

    // The PENDING head gates its partition, so a second sweep takes nothing.
    assert!(
        sys.start_queued_partitioned_workflows(&queue, "exec-1", "v1")
            .await
            .unwrap()
            .is_empty(),
        "a partition whose head is running is not eligible",
    );

    // Once the head finishes, the next one becomes available.
    sys.record_workflow_outcome("wf-alpha-0", Outcome::Output(Some("\"done\"")))
        .await
        .unwrap();
    assert_eq!(
        sys.start_queued_partitioned_workflows(&queue, "exec-1", "v1")
            .await
            .unwrap(),
        ["wf-alpha-1"],
    );
}

/// A sweep refuses a queue whose settings make its admission control unsound.
#[tokio::test]
async fn a_sweep_refuses_an_unsuitable_queue() {
    let (sys, _db) = sysdb().await;

    let unsuitable = [
        // Not partitioned at all.
        NewQueue {
            concurrency: Some(1),
            ..NewQueue::new("plain")
        },
        // Partitioned, but admitting more than one per partition.
        NewQueue {
            concurrency: Some(2),
            partition_queue: true,
            ..NewQueue::new("wide")
        },
        // Partitioned and single, but rate limited — which needs the counting a sweep omits.
        NewQueue {
            concurrency: Some(1),
            partition_queue: true,
            rate_limit: Some(RateLimit {
                limit: 5,
                period: std::time::Duration::from_secs(1),
            }),
            ..NewQueue::new("limited")
        },
    ];

    for queue in unsuitable {
        sys.upsert_queue(&queue, OnExistingQueue::Update)
            .await
            .unwrap();
        let registered = sys.get_queue(queue.name).await.unwrap().unwrap();
        let result = sys
            .start_queued_partitioned_workflows(&registered, "exec-1", "v1")
            .await;
        assert!(
            matches!(result, Err(Error::InvalidInput { field: "queue", .. })),
            "{} should be refused, got {result:?}",
            queue.name,
        );
    }
}

/// A sweep takes only this application's partitions, and claims what it starts.
#[tokio::test]
async fn a_sweep_stays_within_an_application() {
    let db = test_database().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );
    let queue = partitioned_queue(&alpha, "orders").await;

    enqueue_partitioned(&alpha, "wf-alpha", "orders", "p1").await;
    enqueue_partitioned(&beta, "wf-beta", "orders", "p2").await;

    assert_eq!(
        alpha.get_queue_partitions("orders").await.unwrap(),
        ["p1"],
        "beta's partition is not alpha's to poll",
    );
    assert_eq!(
        alpha
            .start_queued_partitioned_workflows(&queue, "exec-1", "v1")
            .await
            .unwrap(),
        ["wf-alpha"],
    );
    assert_eq!(
        alpha.get_workflow("wf-beta").await.unwrap().unwrap().status,
        WorkflowStatus::Enqueued,
    );
}

/// An update changes the fields it names and leaves the rest, including clearing to NULL.
#[tokio::test]
async fn an_update_changes_only_what_it_names() {
    let (sys, _db) = sysdb().await;
    let queue = NewQueue {
        concurrency: Some(4),
        worker_concurrency: Some(2),
        rate_limit: Some(RateLimit {
            limit: 10,
            period: std::time::Duration::from_secs(1),
        }),
        priority_enabled: true,
        polling_interval: std::time::Duration::from_millis(500),
        ..NewQueue::new("orders")
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();

    // Set one field, clear another, leave the rest alone.
    sys.update_queue(
        "orders",
        &QueueUpdate {
            concurrency: Change::Set(Some(9)),
            worker_concurrency: Change::Set(None),
            ..QueueUpdate::default()
        },
    )
    .await
    .unwrap();

    let read = sys.get_queue("orders").await.unwrap().unwrap();
    assert_eq!(read.concurrency, Some(9), "named and set");
    assert_eq!(read.worker_concurrency, None, "named and cleared");
    assert_eq!(
        read.rate_limit,
        Some(RateLimit {
            limit: 10,
            period: std::time::Duration::from_secs(1),
        }),
        "not named, so untouched",
    );
    assert!(read.priority_enabled, "not named, so untouched");
    assert_eq!(read.polling_interval, std::time::Duration::from_millis(500));
}

/// A rate limit moves as one value: both columns or neither.
#[tokio::test]
async fn an_update_moves_a_rate_limit_whole() {
    let (sys, db) = sysdb().await;
    let pool = db.pool().await;
    let queue = NewQueue {
        rate_limit: Some(RateLimit {
            limit: 10,
            period: std::time::Duration::from_secs(1),
        }),
        ..NewQueue::new("orders")
    };
    sys.upsert_queue(&queue, OnExistingQueue::Update)
        .await
        .unwrap();

    sys.update_queue(
        "orders",
        &QueueUpdate {
            rate_limit: Change::Set(Some(RateLimit {
                limit: 3,
                period: std::time::Duration::from_millis(250),
            })),
            ..QueueUpdate::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        sys.get_queue("orders").await.unwrap().unwrap().rate_limit,
        Some(RateLimit {
            limit: 3,
            period: std::time::Duration::from_millis(250),
        }),
    );

    // Clearing takes both columns, so no half-limit can be left behind.
    sys.update_queue(
        "orders",
        &QueueUpdate {
            rate_limit: Change::Set(None),
            ..QueueUpdate::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        sys.get_queue("orders").await.unwrap().unwrap().rate_limit,
        None
    );
    let columns: (Option<i32>, Option<f64>) = sqlx::query_as(
        r#"SELECT "rate_limit_max", "rate_limit_period_sec" FROM "dbos"."queues" WHERE "name" = $1"#,
    )
    .bind("orders")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(columns, (None, None), "both columns cleared, not one");
}

/// An update naming nothing touches the row at all, including its `updated_at`.
#[tokio::test]
async fn an_empty_update_is_a_no_op() {
    let (sys, db) = sysdb().await;
    let pool = db.pool().await;
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();

    let before: i64 =
        sqlx::query_scalar(r#"SELECT "updated_at" FROM "dbos"."queues" WHERE "name" = $1"#)
            .bind("orders")
            .fetch_one(&pool)
            .await
            .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    sys.update_queue("orders", &QueueUpdate::default())
        .await
        .unwrap();

    let after: i64 =
        sqlx::query_scalar(r#"SELECT "updated_at" FROM "dbos"."queues" WHERE "name" = $1"#)
            .bind("orders")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after, "an empty update records no write");
}

/// The lookup answers who holds a key, so a losing enqueue can adopt the winner.
#[tokio::test]
async fn the_deduplication_key_holder_is_read_by_queue_and_key() {
    let (sys, _db) = sysdb().await;
    let wf = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("cart-1"),
        application_version: Some("v1"),
        ..NewWorkflow::new("wf-holder")
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();

    assert_eq!(
        sys.get_deduplication_key_holder("orders", "cart-1")
            .await
            .unwrap()
            .as_deref(),
        Some("wf-holder")
    );

    // The key is scoped to its queue, and an unheld key is `None` rather than an error — the
    // caller retries its insert instead of reporting a conflict it can no longer see.
    assert_eq!(
        sys.get_deduplication_key_holder("shipping", "cart-1")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        sys.get_deduplication_key_holder("orders", "cart-2")
            .await
            .unwrap(),
        None
    );
}

/// Finishing releases the key, so the next submission under it succeeds.
///
/// Reaching a terminal status means going through `PENDING`, and the only route there is a dequeue —
/// `init_workflow`'s conflict path does not touch `status`, and `resume` enqueues rather than starts.
/// So this inherits the dequeue's CockroachDB caveat despite being a test about neither.
#[tokio::test]
async fn finishing_releases_the_deduplication_key() {
    let (sys, db) = sysdb().await;
    if skip_dequeue_completeness(&db) {
        return;
    }
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();
    let held = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("cart-1"),
        application_version: Some("v1"),
        ..NewWorkflow::new("wf-first")
    };
    sys.init_workflow(&held, None, Submission::Fresh)
        .await
        .unwrap();

    // It has to reach a terminal status through `PENDING`, which is where the outcome lands. Both
    // steps are asserted rather than assumed: a dequeue that returned nothing leaves the workflow
    // `ENQUEUED`, the outcome write finds no `PENDING` row and reports `AlreadyFinished`, and the
    // key assertion below then fails for a reason that has nothing to do with deduplication.
    let registered = sys.get_queue("orders").await.unwrap().unwrap();
    assert_eq!(
        sys.start_queued_workflows(&registered, "exec-1", "v1", None, 0)
            .await
            .unwrap(),
        ["wf-first"],
    );
    assert_eq!(
        sys.record_workflow_outcome("wf-first", Outcome::Output(Some("\"done\"")))
            .await
            .unwrap(),
        OutcomeWrite::Recorded,
    );

    assert_eq!(
        sys.get_deduplication_key_holder("orders", "cart-1")
            .await
            .unwrap(),
        None,
        "a finished workflow no longer holds its key"
    );

    // And the key is genuinely free: the unique index spans every status, so this insert would
    // fail if the finished row still carried it.
    let next = NewWorkflow {
        queue_name: Some("orders"),
        deduplication_id: Some("cart-1"),
        application_version: Some("v1"),
        ..NewWorkflow::new("wf-next")
    };
    sys.init_workflow(&next, None, Submission::Fresh)
        .await
        .unwrap();
    assert_eq!(
        sys.get_deduplication_key_holder("orders", "cart-1")
            .await
            .unwrap()
            .as_deref(),
        Some("wf-next")
    );
}

/// The names a filter returns, which is what most of the listing assertions are about.
async fn schedule_names(sys: &PostgresSystemDatabase, filter: &ScheduleFilter<'_>) -> Vec<String> {
    sys.list_schedules(filter, None)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.schedule_name)
        .collect()
}

/// A schedule round-trips through the database unchanged.
#[tokio::test]
async fn a_schedule_round_trips() {
    let (sys, _db) = sysdb().await;
    let schedule = NewSchedule {
        schedule_id: Some("sch-1"),
        workflow_class_name: Some("Reports"),
        context: r#"{"tenant":"acme"}"#,
        last_fired_at: Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
        automatic_backfill: true,
        cron_timezone: Some("Europe/London"),
        queue_name: Some("reports"),
        ..NewSchedule::new("nightly", "generate_report", "0 0 * * *")
    };
    sys.create_schedule(&schedule, None).await.unwrap();

    let stored = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert_eq!(stored.schedule_id, "sch-1");
    assert_eq!(stored.schedule_name, "nightly");
    assert_eq!(stored.workflow_name, "generate_report");
    assert_eq!(stored.workflow_class_name.as_deref(), Some("Reports"));
    assert_eq!(stored.schedule, "0 0 * * *");
    assert_eq!(stored.status, ScheduleStatus::Active);
    assert_eq!(stored.context, r#"{"tenant":"acme"}"#);
    assert_eq!(
        stored.last_fired_at,
        Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
        "the instant survives the column's ISO-8601 encoding"
    );
    assert!(stored.automatic_backfill);
    assert_eq!(stored.cron_timezone.as_deref(), Some("Europe/London"));
    assert_eq!(stored.queue_name.as_deref(), Some("reports"));

    assert_eq!(sys.get_schedule("no-such", None).await.unwrap(), None);
}

/// A value another implementation wrote reads back as the same instant this one would write.
#[tokio::test]
async fn a_peers_last_fired_at_spelling_is_read_as_an_instant() {
    let db = test_database().await;
    let pool = db.pool().await;
    let sys = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();

    // The column is text, so each implementation writes its own spelling of the same instant,
    // and Conductor writes an explicit numeric offset rather than `Z` on purpose.
    for stored in [
        "2026-08-12T00:00:00.000Z",
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:00+00:00",
        "2026-08-12T00:00:00.000000000Z",
        "2026-08-12T01:30:00+01:30",
    ] {
        sqlx::query(
            r#"UPDATE "dbos"."workflow_schedules" SET "last_fired_at" = $1
               WHERE "schedule_name" = $2"#,
        )
        .bind(stored)
        .bind("nightly")
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            sys.get_schedule("nightly", None)
                .await
                .unwrap()
                .unwrap()
                .last_fired_at,
            Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
            "{stored:?} is the same instant"
        );
    }

    // A value that is not an instant at all is reported rather than read as never-fired.
    sqlx::query(
        r#"UPDATE "dbos"."workflow_schedules" SET "last_fired_at" = 'yesterday'
           WHERE "schedule_name" = $1"#,
    )
    .bind("nightly")
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        sys.get_schedule("nightly", None).await,
        Err(Error::Malformed(_))
    ));
}

/// An id is generated when the caller supplies none, and it survives re-registration.
#[tokio::test]
async fn a_schedule_keeps_its_identity_across_a_re_apply() {
    let (sys, _db) = sysdb().await;
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();
    let first = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert!(!first.schedule_id.is_empty(), "an id is generated");

    // The definition moves; the identity and the runtime state do not.
    sys.set_schedule_status("nightly", ScheduleStatus::Paused, None)
        .await
        .unwrap();
    sys.update_schedule_last_fired_at("nightly", Timestamp::from_epoch_ms(1_786_492_800_000))
        .await
        .unwrap();
    sys.upsert_schedule(
        &NewSchedule {
            workflow_class_name: Some("Reports"),
            ..NewSchedule::new("nightly", "generate_report", "*/5 * * * *")
        },
        None,
    )
    .await
    .unwrap();

    let after = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert_eq!(after.schedule_id, first.schedule_id, "the identity is kept");
    assert_eq!(after.schedule, "*/5 * * * *", "the definition moved");
    assert_eq!(after.workflow_class_name.as_deref(), Some("Reports"));
    assert_eq!(
        after.status,
        ScheduleStatus::Paused,
        "a redeployment does not resume a paused schedule"
    );
    assert_eq!(
        after.last_fired_at,
        Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
        "nor forget where it had got to"
    );
}

/// Registering a name this application already holds is an error, not an update.
#[tokio::test]
async fn creating_a_schedule_twice_is_refused() {
    let (sys, _db) = sysdb().await;
    let schedule = NewSchedule::new("nightly", "generate_report", "0 0 * * *");
    sys.create_schedule(&schedule, None).await.unwrap();

    assert!(matches!(
        sys.create_schedule(&schedule, None).await,
        Err(Error::AlreadyRegistered {
            kind: "Schedule",
            ref name
        }) if name == "nightly"
    ));

    // The id has its own unique index, and a collision on it is reported as itself rather than
    // as a name collision — which is what all four references report it as.
    let taken = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert!(matches!(
        sys.create_schedule(
            &NewSchedule {
                schedule_id: Some(&taken.schedule_id),
                ..NewSchedule::new("hourly", "sweep", "0 * * * *")
            },
            None
        )
        .await,
        Err(Error::AlreadyRegistered {
            kind: "Schedule id",
            ..
        })
    ));

    // The upsert is the way to re-register, and it leaves one row behind.
    sys.upsert_schedule(&schedule, None).await.unwrap();
    assert_eq!(
        sys.list_schedules(&ScheduleFilter::default(), None)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A peer's name is a collision the pre-check names, and a race is one it cannot.
#[tokio::test]
async fn a_creation_losing_to_a_peer_reports_what_it_can_see() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );

    // Beta takes the name *while alpha is mid-call*: under READ COMMITTED alpha's resolve reads a
    // snapshot from before beta committed, so the insert is the first thing that can see it. On a
    // SERIALIZABLE backend the resolve gets another look — see the assertion at the end.
    // Beta's uncommitted insert makes the interleaving deterministic — alpha blocks on the unique
    // index until beta commits, rather than racing it.
    let mut beta_tx = pool.begin().await.unwrap();
    sqlx::query(
        r#"INSERT INTO "dbos"."workflow_schedules"
           (schedule_id, schedule_name, workflow_name, schedule, status, context, application_name)
           VALUES ($1, $2, $3, $4, 'ACTIVE', 'null', 'beta')"#,
    )
    .bind("sch-beta")
    .bind("nightly")
    .bind("generate_report")
    .bind("0 0 * * *")
    .execute(&mut *beta_tx)
    .await
    .unwrap();

    let racing = tokio::spawn(async move {
        alpha
            .create_schedule(
                &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
                None,
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let started_waiting = std::time::Instant::now();
    beta_tx.commit().await.unwrap();
    assert!(
        started_waiting.elapsed() < std::time::Duration::from_millis(300),
        "beta's commit should be what releases alpha, not the other way round"
    );

    // **Which collision it reports depends on the isolation level, and both are true.**
    //
    // Under PostgreSQL's `READ COMMITTED` the resolve has already taken its snapshot, so the unique
    // index is the first thing to see beta — and an index does not know whose row it protected. So
    // the error says only that the name is taken, which is what all four references report for any
    // collision here; naming the holder would mean a second query on a fresh transaction after
    // every failure, which none of them does.
    //
    // CockroachDB is `SERIALIZABLE`: the conflict makes the statement retry, the resolve runs again
    // against a snapshot that now includes beta, and it can name the holder. Strictly the better
    // answer, and the same code produces both — so what is asserted is that the collision is
    // reported, not which sentence it uses.
    let outcome = racing.await.unwrap();
    assert!(
        matches!(
            outcome,
            Err(Error::AlreadyRegistered {
                kind: "Schedule",
                ..
            }) | Err(Error::RegisteredByAnother {
                kind: "Schedule",
                ..
            })
        ),
        "a peer that commits mid-call is a collision either way, got {outcome:?}"
    );

    // Committed before the call, the same collision is named exactly: the pre-check sees it.
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    assert!(matches!(
        alpha
            .create_schedule(
                &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
                None
            )
            .await,
        Err(Error::RegisteredByAnother {
            kind: "Schedule",
            ref holder,
            ..
        }) if holder == "beta"
    ));
}

/// A peer's schedule name is a collision this layer cannot resolve.
#[tokio::test]
async fn a_schedule_name_held_by_another_application_is_refused() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );

    let schedule = NewSchedule::new("nightly", "generate_report", "0 0 * * *");
    alpha.create_schedule(&schedule, None).await.unwrap();
    assert_eq!(
        alpha
            .get_schedule("nightly", None)
            .await
            .unwrap()
            .unwrap()
            .application_name
            .as_deref(),
        Some("alpha")
    );

    for result in [
        beta.create_schedule(&schedule, None).await,
        beta.upsert_schedule(&schedule, None).await,
    ] {
        assert!(matches!(
            result,
            Err(Error::RegisteredByAnother {
                kind: "Schedule",
                ref holder,
                ..
            }) if holder == "alpha"
        ));
    }

    // An anonymous handle claims an unclaimed row rather than colliding with it.
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());
    anonymous
        .create_schedule(&NewSchedule::new("hourly", "sweep", "0 * * * *"), None)
        .await
        .unwrap();
    beta.upsert_schedule(&NewSchedule::new("hourly", "sweep", "0 * * * *"), None)
        .await
        .unwrap();
    assert_eq!(
        beta.get_schedule("hourly", None)
            .await
            .unwrap()
            .unwrap()
            .application_name
            .as_deref(),
        Some("beta")
    );
}

/// Applying a set of schedules is one transaction, so a collision moves none of them.
#[tokio::test]
async fn applying_schedules_is_all_or_nothing() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );
    alpha
        .create_schedule(
            &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
            None,
        )
        .await
        .unwrap();

    // Beta declares two, the second of which alpha already holds.
    let result = beta
        .apply_schedules(&[
            NewSchedule::new("hourly", "sweep", "0 * * * *"),
            NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        ])
        .await;
    assert!(matches!(
        result,
        Err(Error::RegisteredByAnother { ref holder, .. }) if holder == "alpha"
    ));
    assert_eq!(
        beta.get_schedule("hourly", None).await.unwrap(),
        None,
        "the first schedule rolled back with the second"
    );

    // Without the collision, both land, and re-applying is a no-op.
    let declared = [
        NewSchedule::new("hourly", "sweep", "0 * * * *"),
        NewSchedule::new("weekly", "archive", "0 0 * * 0"),
    ];
    beta.apply_schedules(&declared).await.unwrap();
    beta.apply_schedules(&declared).await.unwrap();
    assert_eq!(
        schedule_names(&beta, &ScheduleFilter::default()).await,
        ["hourly", "weekly"]
    );
    assert!(
        beta.apply_schedules(&[]).await.is_ok(),
        "applying nothing is not an error"
    );

    // Runtime state on a declaration is taken at face value rather than refused: it seeds a fresh
    // row and the conflict clause keeps the stored value on an existing one. Deciding whether a
    // declaration should carry it at all belongs to whatever builds the declaration.
    let paused = NewSchedule {
        status: ScheduleStatus::Paused,
        last_fired_at: Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
        ..NewSchedule::new("paused-decl", "run", "0 0 * * *")
    };
    let declared_paused = [paused];
    beta.apply_schedules(&declared_paused).await.unwrap();
    let seeded = beta
        .get_schedule("paused-decl", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seeded.status, ScheduleStatus::Paused);
    assert_eq!(
        seeded.last_fired_at,
        Some(Timestamp::from_epoch_ms(1_786_492_800_000))
    );

    beta.set_schedule_status("paused-decl", ScheduleStatus::Active, None)
        .await
        .unwrap();
    beta.apply_schedules(&declared_paused).await.unwrap();
    assert_eq!(
        beta.get_schedule("paused-decl", None)
            .await
            .unwrap()
            .unwrap()
            .status,
        ScheduleStatus::Active,
        "re-applying does not pause a schedule an operator resumed"
    );
}

/// Every filter narrows, and a prefix match treats wildcards as ordinary characters.
#[tokio::test]
async fn schedules_are_listed_by_status_workflow_and_prefix() {
    let (sys, _db) = sysdb().await;
    for (name, workflow_name) in [
        ("report-nightly", "generate_report"),
        ("report-weekly", "generate_report"),
        ("sweep-hourly", "sweep"),
        ("100%-odd", "sweep"),
    ] {
        sys.create_schedule(&NewSchedule::new(name, workflow_name, "0 0 * * *"), None)
            .await
            .unwrap();
    }
    sys.set_schedule_status("report-weekly", ScheduleStatus::Paused, None)
        .await
        .unwrap();

    assert_eq!(
        schedule_names(&sys, &ScheduleFilter::default()).await,
        [
            "100%-odd",
            "report-nightly",
            "report-weekly",
            "sweep-hourly"
        ],
        "an empty filter returns the table, ordered by name"
    );
    assert_eq!(
        schedule_names(
            &sys,
            &ScheduleFilter {
                statuses: vec![ScheduleStatus::Paused],
                ..Default::default()
            }
        )
        .await,
        ["report-weekly"]
    );
    assert_eq!(
        schedule_names(
            &sys,
            &ScheduleFilter {
                workflow_names: vec!["sweep"],
                ..Default::default()
            }
        )
        .await,
        ["100%-odd", "sweep-hourly"]
    );
    assert_eq!(
        schedule_names(
            &sys,
            &ScheduleFilter {
                schedule_name_prefixes: vec!["report-", "sweep-"],
                ..Default::default()
            }
        )
        .await,
        ["report-nightly", "report-weekly", "sweep-hourly"],
        "prefixes are an OR, not an AND"
    );
    // `%` is a character in a name, not a wildcard: were it one, this would match everything.
    assert_eq!(
        schedule_names(
            &sys,
            &ScheduleFilter {
                schedule_name_prefixes: vec!["100%"],
                ..Default::default()
            }
        )
        .await,
        ["100%-odd"]
    );
    // The filters compose.
    assert_eq!(
        schedule_names(
            &sys,
            &ScheduleFilter {
                statuses: vec![ScheduleStatus::Active],
                workflow_names: vec!["generate_report"],
                ..Default::default()
            }
        )
        .await,
        ["report-nightly"]
    );
}

/// A listing is a search, so it defaults to this application's schedules plus the unclaimed.
#[tokio::test]
async fn a_schedule_listing_defaults_to_its_own_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let alpha = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("alpha"),
            ..Settings::default()
        },
    );
    let beta = PostgresSystemDatabase::from_pool(
        pool.clone(),
        &Settings {
            application_name: Some("beta"),
            ..Settings::default()
        },
    );
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    alpha
        .create_schedule(&NewSchedule::new("alpha-job", "run", "0 0 * * *"), None)
        .await
        .unwrap();
    beta.create_schedule(&NewSchedule::new("beta-job", "run", "0 0 * * *"), None)
        .await
        .unwrap();
    anonymous
        .create_schedule(&NewSchedule::new("nobody-job", "run", "0 0 * * *"), None)
        .await
        .unwrap();

    let scoped = |applications| ScheduleFilter {
        applications,
        ..Default::default()
    };
    assert_eq!(
        schedule_names(&alpha, &scoped(Applications::Unset)).await,
        ["alpha-job", "nobody-job"],
        "its own plus the unclaimed"
    );
    assert_eq!(
        schedule_names(&alpha, &scoped(Applications::Any)).await,
        ["alpha-job", "beta-job", "nobody-job"]
    );
    assert_eq!(
        schedule_names(&alpha, &scoped(Applications::Named(vec!["beta"]))).await,
        ["beta-job", "nobody-job"]
    );
    assert_eq!(
        schedule_names(&anonymous, &scoped(Applications::Unset)).await,
        ["alpha-job", "beta-job", "nobody-job"],
        "an unnamed handle has no application to narrow to"
    );

    // A schedule is addressed by name, so an id-keyed read crosses applications.
    assert!(
        alpha
            .get_schedule("beta-job", None)
            .await
            .unwrap()
            .is_some()
    );
}

/// An update moves the definition and leaves the runtime state alone.
#[tokio::test]
async fn updating_a_schedule_touches_only_its_definition() {
    let (sys, _db) = sysdb().await;
    sys.create_schedule(
        &NewSchedule {
            cron_timezone: Some("Europe/London"),
            queue_name: Some("reports"),
            ..NewSchedule::new("nightly", "generate_report", "0 0 * * *")
        },
        None,
    )
    .await
    .unwrap();
    sys.set_schedule_status("nightly", ScheduleStatus::Paused, None)
        .await
        .unwrap();
    sys.update_schedule_last_fired_at("nightly", Timestamp::from_epoch_ms(1_786_492_800_000))
        .await
        .unwrap();
    let before = sys.get_schedule("nightly", None).await.unwrap().unwrap();

    sys.update_schedule(
        "nightly",
        &ScheduleUpdate {
            schedule: Change::Set("*/5 * * * *"),
            automatic_backfill: Change::Set(true),
            // Both nullable columns clear, which is why they are `Change<Option<_>>`.
            cron_timezone: Change::Set(None),
            queue_name: Change::Set(None),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let after = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert_eq!(after.schedule, "*/5 * * * *");
    assert!(after.automatic_backfill);
    assert_eq!(after.cron_timezone, None);
    assert_eq!(after.queue_name, None);
    assert_eq!(
        after.context, before.context,
        "unnamed fields are untouched"
    );
    assert_eq!(after.schedule_id, before.schedule_id);
    assert_eq!(after.status, ScheduleStatus::Paused);
    assert_eq!(after.last_fired_at, before.last_fired_at);
}

/// A write addressed to a name nothing holds is an error rather than a silent no-op.
#[tokio::test]
async fn addressing_a_missing_schedule_is_refused() {
    let (sys, _db) = sysdb().await;
    let missing = |result: Result<(), Error>| matches!(result, Err(Error::NotRegistered { kind: "Schedule", ref name }) if name == "ghost");

    assert!(missing(
        sys.update_schedule(
            "ghost",
            &ScheduleUpdate {
                schedule: Change::Set("0 0 * * *"),
                ..Default::default()
            },
            None,
        )
        .await
    ));
    // An empty update still reports the name, or a typo would read as success.
    assert!(missing(
        sys.update_schedule("ghost", &ScheduleUpdate::default(), None)
            .await
    ));
    assert!(missing(
        sys.set_schedule_status("ghost", ScheduleStatus::Paused, None)
            .await
    ));

    // The two that race a concurrent delete stay silent, so the scheduler loop has nothing to
    // absorb when an operator removes a schedule between firing and recording it.
    sys.update_schedule_last_fired_at("ghost", Timestamp::from_epoch_ms(1_786_492_800_000))
        .await
        .unwrap();
    sys.delete_schedule("ghost", None).await.unwrap();

    // An empty update against a schedule that does exist changes nothing and succeeds.
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();
    let before = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    sys.update_schedule("nightly", &ScheduleUpdate::default(), None)
        .await
        .unwrap();
    assert_eq!(
        sys.get_schedule("nightly", None).await.unwrap().unwrap(),
        before
    );
}

/// Pausing and resuming move only the status, and deleting removes the row.
#[tokio::test]
async fn a_schedule_pauses_resumes_and_deletes() {
    let (sys, _db) = sysdb().await;
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();
    sys.update_schedule_last_fired_at("nightly", Timestamp::from_epoch_ms(1_786_492_800_000))
        .await
        .unwrap();

    sys.set_schedule_status("nightly", ScheduleStatus::Paused, None)
        .await
        .unwrap();
    let paused = sys.get_schedule("nightly", None).await.unwrap().unwrap();
    assert_eq!(paused.status, ScheduleStatus::Paused);
    assert_eq!(
        paused.last_fired_at,
        Some(Timestamp::from_epoch_ms(1_786_492_800_000)),
        "pausing does not forget where it had got to"
    );

    sys.set_schedule_status("nightly", ScheduleStatus::Active, None)
        .await
        .unwrap();
    assert_eq!(
        sys.get_schedule("nightly", None)
            .await
            .unwrap()
            .unwrap()
            .status,
        ScheduleStatus::Active
    );

    sys.delete_schedule("nightly", None).await.unwrap();
    assert_eq!(sys.get_schedule("nightly", None).await.unwrap(), None);
}

/// A schedule write driven by a workflow step is replayed from its checkpoint, not redone.
#[tokio::test]
async fn a_schedule_step_replays_rather_than_repeating() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();

    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        Some(("wf-caller", 0)),
    )
    .await
    .unwrap();
    let recorded = sys
        .check_step("wf-caller", 0, "DBOS.createSchedule")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recorded.step_name, "DBOS.createSchedule",
        "the write and its checkpoint committed together"
    );
    // A method returning nothing still records something: the JSON `null`, not a NULL column.
    // Whether a step ran is answered by the row existing, never by its output being empty.
    assert_eq!(recorded.output.as_deref(), Some("null"));
    assert_eq!(recorded.error, None);

    // Replaying returns the first run's outcome. Without the step, this second create would be
    // `AlreadyRegistered` — the name is taken, by the first run.
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        Some(("wf-caller", 0)),
    )
    .await
    .unwrap();

    // A read replays its recorded answer rather than the current row, so a workflow branching on
    // a schedule sees the same one however an operator has since changed it.
    let first = sys
        .get_schedule("nightly", Some(("wf-caller", 1)))
        .await
        .unwrap()
        .unwrap();
    sys.update_schedule(
        "nightly",
        &ScheduleUpdate {
            schedule: Change::Set("*/5 * * * *"),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        sys.get_schedule("nightly", Some(("wf-caller", 1)))
            .await
            .unwrap()
            .unwrap(),
        first,
        "the replay is the recorded schedule, not the changed one"
    );
    assert_eq!(
        sys.get_schedule("nightly", None)
            .await
            .unwrap()
            .unwrap()
            .schedule,
        "*/5 * * * *",
        "and the change did land"
    );

    // A read that found nothing records `null` too, and replays as the `None` it was — not as an
    // absent step, which is the same JSON and a different meaning.
    assert_eq!(
        sys.get_schedule("no-such", Some(("wf-caller", 4)))
            .await
            .unwrap(),
        None
    );
    sys.create_schedule(&NewSchedule::new("no-such", "run", "0 0 * * *"), None)
        .await
        .unwrap();
    assert_eq!(
        sys.get_schedule("no-such", Some(("wf-caller", 4)))
            .await
            .unwrap(),
        None,
        "the replay is the recorded absence, not the schedule that now exists"
    );
    sys.delete_schedule("no-such", None).await.unwrap();

    // A listing replays as a whole, empty included.
    let listed = sys
        .list_schedules(&ScheduleFilter::default(), Some(("wf-caller", 2)))
        .await
        .unwrap();
    sys.delete_schedule("nightly", None).await.unwrap();
    assert_eq!(
        sys.list_schedules(&ScheduleFilter::default(), Some(("wf-caller", 2)))
            .await
            .unwrap(),
        listed
    );
    assert!(
        sys.list_schedules(&ScheduleFilter::default(), Some(("wf-caller", 3)))
            .await
            .unwrap()
            .is_empty(),
        "a fresh step sees the deletion"
    );
}

/// Pausing and resuming record different steps, so one never replays as the other.
#[tokio::test]
async fn pausing_and_resuming_are_distinct_steps() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();

    sys.set_schedule_status("nightly", ScheduleStatus::Paused, Some(("wf-caller", 0)))
        .await
        .unwrap();
    sys.set_schedule_status("nightly", ScheduleStatus::Active, Some(("wf-caller", 1)))
        .await
        .unwrap();

    assert_eq!(
        sys.check_step("wf-caller", 0, "DBOS.pauseSchedule")
            .await
            .unwrap()
            .map(|s| s.step_name),
        Some("DBOS.pauseSchedule".to_owned())
    );
    assert_eq!(
        sys.check_step("wf-caller", 1, "DBOS.resumeSchedule")
            .await
            .unwrap()
            .map(|s| s.step_name),
        Some("DBOS.resumeSchedule".to_owned())
    );
}

/// The four writes the replay test above does not drive: each replays rather than repeating.
///
/// Every case changes the row out from under the recorded step first, so a replay that re-ran the
/// work would be visible — and in the update's case would fail outright, the schedule having been
/// deleted between the two calls.
#[tokio::test]
async fn every_schedule_write_replays_from_its_checkpoint() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();
    let schedule = |cron| NewSchedule::new("nightly", "generate_report", cron);
    let cron = async |sys: &PostgresSystemDatabase| {
        sys.get_schedule("nightly", None)
            .await
            .unwrap()
            .map(|s| s.schedule)
    };

    // Upsert. The replay must not put the definition back.
    sys.upsert_schedule(&schedule("0 0 * * *"), Some(("wf-caller", 0)))
        .await
        .unwrap();
    sys.update_schedule(
        "nightly",
        &ScheduleUpdate {
            schedule: Change::Set("*/5 * * * *"),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    sys.upsert_schedule(&schedule("0 0 * * *"), Some(("wf-caller", 0)))
        .await
        .unwrap();
    assert_eq!(
        cron(&sys).await.as_deref(),
        Some("*/5 * * * *"),
        "the replayed upsert wrote nothing"
    );

    // Update. Its replay succeeds against a schedule that no longer exists, which re-running
    // could not do: the row is gone, and an addressed write to a missing name is refused.
    sys.update_schedule(
        "nightly",
        &ScheduleUpdate {
            schedule: Change::Set("*/10 * * * *"),
            ..Default::default()
        },
        Some(("wf-caller", 1)),
    )
    .await
    .unwrap();
    assert_eq!(cron(&sys).await.as_deref(), Some("*/10 * * * *"));
    sys.delete_schedule("nightly", None).await.unwrap();
    sys.update_schedule(
        "nightly",
        &ScheduleUpdate {
            schedule: Change::Set("*/10 * * * *"),
            ..Default::default()
        },
        Some(("wf-caller", 1)),
    )
    .await
    .unwrap();

    // Status. The replay must not pause a schedule an operator has since resumed.
    sys.create_schedule(&schedule("0 0 * * *"), None)
        .await
        .unwrap();
    sys.set_schedule_status("nightly", ScheduleStatus::Paused, Some(("wf-caller", 2)))
        .await
        .unwrap();
    sys.set_schedule_status("nightly", ScheduleStatus::Active, None)
        .await
        .unwrap();
    sys.set_schedule_status("nightly", ScheduleStatus::Paused, Some(("wf-caller", 2)))
        .await
        .unwrap();
    assert_eq!(
        sys.get_schedule("nightly", None)
            .await
            .unwrap()
            .unwrap()
            .status,
        ScheduleStatus::Active,
        "the replayed pause left the resumed schedule alone"
    );

    // Delete. The replay must not remove the schedule registered since.
    sys.delete_schedule("nightly", Some(("wf-caller", 3)))
        .await
        .unwrap();
    sys.create_schedule(&schedule("0 0 * * *"), None)
        .await
        .unwrap();
    sys.delete_schedule("nightly", Some(("wf-caller", 3)))
        .await
        .unwrap();
    assert!(
        cron(&sys).await.is_some(),
        "the replayed delete removed nothing"
    );

    // Four steps for four ids, and nothing from the calls that passed no caller — a `None` is not
    // a step at some other position, it is no step at all.
    let steps = sys
        .list_workflow_steps("wf-caller", true, None, None)
        .await
        .unwrap();
    let names: Vec<&str> = steps.iter().map(|s| s.step_name.as_str()).collect();
    assert_eq!(
        names,
        [
            "DBOS.upsertSchedule",
            "DBOS.updateSchedule",
            "DBOS.pauseSchedule",
            "DBOS.deleteSchedule",
        ]
    );
    // All four return nothing, so all four record the JSON `null` rather than a NULL column.
    assert!(steps.iter().all(|s| s.output.as_deref() == Some("null")));
}

/// A failed step leaves no checkpoint, so a replay runs the work again.
#[tokio::test]
async fn a_failed_schedule_step_records_nothing() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        None,
    )
    .await
    .unwrap();

    // The name is taken, so this fails — and takes the step's checkpoint down with it, both being
    // on the one transaction.
    assert!(matches!(
        sys.create_schedule(
            &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
            Some(("wf-caller", 0))
        )
        .await,
        Err(Error::AlreadyRegistered { .. })
    ));
    assert_eq!(
        sys.check_step("wf-caller", 0, "DBOS.createSchedule")
            .await
            .unwrap(),
        None,
        "a write that failed is not a step a replay can adopt"
    );

    // So the work runs again, and can now succeed — which is what both references do, and the
    // reason a caller must not read a failed step as a settled answer.
    sys.delete_schedule("nightly", None).await.unwrap();
    sys.create_schedule(
        &NewSchedule::new("nightly", "generate_report", "0 0 * * *"),
        Some(("wf-caller", 0)),
    )
    .await
    .unwrap();
    assert!(sys.get_schedule("nightly", None).await.unwrap().is_some());
}

/// Enqueues a debounced workflow holding `key`, released at `delay_until`.
async fn enqueue_debounced(
    sys: &PostgresSystemDatabase,
    id: &str,
    name: &str,
    key: &str,
    delay: std::time::Duration,
    deadline: Option<Timestamp>,
) {
    let wf = NewWorkflow {
        name: Some(name),
        queue_name: Some("orders"),
        deduplication_id: Some(key),
        is_debounced: true,
        delay: Some(delay),
        debounce_deadline: deadline,
        input: Some("\"first\""),
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
}

fn bounce<'a>(name: &'a str, key: &'a str, delay_until: Timestamp) -> DebounceRequest<'a> {
    DebounceRequest {
        workflow_name: name,
        class_name: None,
        config_name: None,
        queue_name: "orders",
        deduplication_id: key,
        delay_until,
        inputs: Some("\"later\""),
        serialization: None,
        application_name: None,
    }
}

/// Enqueues a debounced workflow belonging to a configured instance of a class.
async fn enqueue_debounced_instance(
    sys: &PostgresSystemDatabase,
    id: &str,
    class_name: &str,
    config_name: &str,
    key: &str,
) {
    let wf = NewWorkflow {
        name: Some("checkout"),
        class_name: Some(class_name),
        config_name: Some(config_name),
        queue_name: Some("orders"),
        deduplication_id: Some(key),
        is_debounced: true,
        delay: Some(std::time::Duration::from_secs(3600)),
        input: Some("\"first\""),
        application_version: Some("v1"),
        ..NewWorkflow::new(id)
    };
    sys.init_workflow(&wf, None, Submission::Fresh)
        .await
        .unwrap();
}

/// The class and instance are part of the identity, so one instance never bounces another's.
#[tokio::test]
async fn a_bounce_matches_the_class_and_configured_instance() {
    let (sys, _db) = sysdb().await;
    enqueue_debounced_instance(&sys, "wf-east", "Checkout", "east", "cart-1").await;

    // Same name and class, a different instance: a collision, not a bounce.
    let west = DebounceRequest {
        class_name: Some("Checkout"),
        config_name: Some("west"),
        ..bounce("checkout", "cart-1", Timestamp::from_epoch_ms(9_000_000))
    };
    match sys.debounce_delayed_workflow(&west, None).await.unwrap() {
        Debounce::Held(holder) => {
            assert_eq!(holder.workflow_id, "wf-east");
            assert_eq!(holder.class_name.as_deref(), Some("Checkout"));
            assert_eq!(holder.config_name.as_deref(), Some("east"));
        }
        other => panic!("expected the east instance reported as the holder, got {other:?}"),
    }

    // An unclassed bounce is a third identity again, not a match for either.
    assert!(matches!(
        sys.debounce_delayed_workflow(
            &bounce("checkout", "cart-1", Timestamp::from_epoch_ms(9_000_000)),
            None
        )
        .await
        .unwrap(),
        Debounce::Held(_)
    ));

    // The east instance's own bounce lands.
    let east = DebounceRequest {
        class_name: Some("Checkout"),
        config_name: Some("east"),
        ..bounce("checkout", "cart-1", Timestamp::from_epoch_ms(9_000_000))
    };
    assert_eq!(
        sys.debounce_delayed_workflow(&east, None).await.unwrap(),
        Debounce::Bounced {
            workflow_id: "wf-east".to_owned()
        }
    );
}

/// An empty class or instance is absent rather than a value, so a bounce carrying one is refused
/// instead of matching nothing and reporting a held key unheld.
#[tokio::test]
async fn a_bounce_refuses_an_empty_class_or_instance() {
    let (sys, _db) = sysdb().await;
    let request = DebounceRequest {
        class_name: Some(""),
        ..bounce("checkout", "cart-1", Timestamp::from_epoch_ms(9_000_000))
    };
    assert!(matches!(
        sys.debounce_delayed_workflow(&request, None).await,
        Err(Error::InvalidInput {
            field: "class_name",
            ..
        })
    ));
}

/// A bounce pushes the release out and replaces the inputs.
#[tokio::test]
async fn a_bounce_extends_the_delay_and_replaces_the_inputs() {
    let (sys, _db) = sysdb().await;
    enqueue_debounced(
        &sys,
        "wf-debounced",
        "checkout",
        "cart-1",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;

    let pushed_to = Timestamp::from_epoch_ms(9_000_000_000);
    assert_eq!(
        sys.debounce_delayed_workflow(&bounce("checkout", "cart-1", pushed_to), None)
            .await
            .unwrap(),
        Debounce::Bounced {
            workflow_id: "wf-debounced".to_owned()
        },
    );

    let read = sys.get_workflow("wf-debounced").await.unwrap().unwrap();
    assert_eq!(read.delay_until, Some(pushed_to));
    assert_eq!(read.input.as_deref(), Some("\"later\""));
    assert_eq!(read.status, WorkflowStatus::Delayed);
}

/// The debounce deadline caps how far a bounce can push the release.
///
/// Without the cap a steady stream of requests postpones the workflow forever.
#[tokio::test]
async fn a_bounce_cannot_push_past_the_debounce_deadline() {
    let (sys, _db) = sysdb().await;
    let deadline = Timestamp::from_epoch_ms(5_000_000_000);
    enqueue_debounced(
        &sys,
        "wf-capped",
        "checkout",
        "cart-1",
        std::time::Duration::from_secs(60),
        Some(deadline),
    )
    .await;

    sys.debounce_delayed_workflow(
        &bounce(
            "checkout",
            "cart-1",
            Timestamp::from_epoch_ms(9_000_000_000),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        sys.get_workflow("wf-capped")
            .await
            .unwrap()
            .unwrap()
            .delay_until,
        Some(deadline),
        "the deadline caps the push, so the workflow still runs",
    );
}

/// A key held by a different workflow is reported rather than overwritten.
#[tokio::test]
async fn a_key_collision_reports_the_holder() {
    let (sys, _db) = sysdb().await;
    enqueue_debounced(
        &sys,
        "wf-held",
        "checkout",
        "cart-1",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;

    // Same key, different workflow — the `"a" + "b-c"` against `"a-b" + "c"` case.
    let result = sys
        .debounce_delayed_workflow(
            &bounce("refund", "cart-1", Timestamp::from_epoch_ms(9_000_000)),
            None,
        )
        .await
        .unwrap();
    match result {
        Debounce::Held(holder) => {
            assert_eq!(holder.workflow_id, "wf-held");
            assert!(holder.is_debounced);
            assert_eq!(holder.workflow_name.as_deref(), Some("checkout"));
        }
        other => panic!("expected the holder to be reported, got {other:?}"),
    }

    // And the holder's inputs are untouched.
    assert_eq!(
        sys.get_workflow("wf-held")
            .await
            .unwrap()
            .unwrap()
            .input
            .as_deref(),
        Some("\"first\""),
    );
}

/// An unheld key reports that nothing holds it, so the caller starts fresh.
#[tokio::test]
async fn an_unheld_key_is_reported_as_unheld() {
    let (sys, _db) = sysdb().await;
    sys.upsert_queue(&NewQueue::new("orders"), OnExistingQueue::Update)
        .await
        .unwrap();

    assert_eq!(
        sys.debounce_delayed_workflow(
            &bounce(
                "checkout",
                "never-used",
                Timestamp::from_epoch_ms(9_000_000)
            ),
            None
        )
        .await
        .unwrap(),
        Debounce::Unheld,
    );
}

/// A bounce never extends a peer's workflow, and claims an unclaimed one.
#[tokio::test]
async fn a_bounce_stays_within_an_application() {
    let db = test_database().await;
    let pool = db.pool().await;
    let named = |name: &'static str| {
        PostgresSystemDatabase::from_pool(
            pool.clone(),
            &Settings {
                application_name: Some(name),
                ..Settings::default()
            },
        )
    };
    let (alpha, beta) = (named("alpha"), named("beta"));
    let anonymous = PostgresSystemDatabase::from_pool(pool.clone(), &Settings::default());

    enqueue_debounced(
        &beta,
        "wf-beta",
        "checkout",
        "beta-key",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;
    enqueue_debounced(
        &anonymous,
        "wf-nobody",
        "checkout",
        "free-key",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;

    // Beta's is reported, not extended.
    let result = alpha
        .debounce_delayed_workflow(
            &bounce("checkout", "beta-key", Timestamp::from_epoch_ms(9_000_000)),
            None,
        )
        .await
        .unwrap();
    match result {
        Debounce::Held(holder) => {
            assert_eq!(holder.application_name.as_deref(), Some("beta"));
        }
        other => panic!("expected beta reported as the holder, got {other:?}"),
    }

    // The unclaimed one is extended and claimed in the same statement.
    assert_eq!(
        alpha
            .debounce_delayed_workflow(
                &bounce("checkout", "free-key", Timestamp::from_epoch_ms(9_000_000)),
                None
            )
            .await
            .unwrap(),
        Debounce::Bounced {
            workflow_id: "wf-nobody".to_owned()
        },
    );
    assert_eq!(
        alpha
            .get_workflow("wf-nobody")
            .await
            .unwrap()
            .unwrap()
            .application_name
            .as_deref(),
        Some("alpha"),
        "bouncing an unclaimed workflow claims it, as its dequeue would",
    );
}

/// A bounce from inside a workflow records a step, and a replay reports the first run's decision.
///
/// The step and the bounce commit together, so a crash cannot leave one without the other. On
/// replay the delay must not move again: the first run already pushed it, and the workflow it
/// pushed may since have started.
#[tokio::test]
async fn a_bounce_inside_a_workflow_is_a_step_and_replays() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();
    enqueue_debounced(
        &sys,
        "wf-debounced",
        "checkout",
        "cart-1",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;

    let pushed_to = Timestamp::from_epoch_ms(9_000_000_000);
    let first = sys
        .debounce_delayed_workflow(
            &bounce("checkout", "cart-1", pushed_to),
            Some(("wf-caller", 0)),
        )
        .await
        .unwrap();
    assert_eq!(
        first,
        Debounce::Bounced {
            workflow_id: "wf-debounced".to_owned()
        },
    );

    // Recorded as a step, so a replay has something to read.
    let steps = sys
        .list_workflow_steps("wf-caller", true, None, None)
        .await
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| s.step_name.as_str())
            .collect::<Vec<_>>(),
        ["DBOS.debounceDelayedWorkflow"],
    );

    // Move the delay out from under the replay: if it bounced again, this would be overwritten.
    sys.set_workflow_delay(
        "wf-debounced",
        WorkflowDelay::Until(Timestamp::from_epoch_ms(1)),
    )
    .await
    .unwrap();

    let replayed = sys
        .debounce_delayed_workflow(
            &bounce(
                "checkout",
                "cart-1",
                Timestamp::from_epoch_ms(7_000_000_000),
            ),
            Some(("wf-caller", 0)),
        )
        .await
        .unwrap();
    assert_eq!(replayed, first, "a replay reports the first run's decision");
    assert_eq!(
        sys.get_workflow("wf-debounced")
            .await
            .unwrap()
            .unwrap()
            .delay_until,
        Some(Timestamp::from_epoch_ms(1)),
        "and does not bounce again",
    );
}

/// A held key replays as held, with the holder it originally reported.
#[tokio::test]
async fn a_replayed_bounce_reports_the_original_holder() {
    let (sys, _db) = sysdb().await;
    sys.init_workflow(&workflow("wf-caller"), None, Submission::Fresh)
        .await
        .unwrap();
    enqueue_debounced(
        &sys,
        "wf-held",
        "checkout",
        "cart-1",
        std::time::Duration::from_secs(60),
        None,
    )
    .await;

    let first = sys
        .debounce_delayed_workflow(
            &bounce("refund", "cart-1", Timestamp::from_epoch_ms(9_000_000)),
            Some(("wf-caller", 0)),
        )
        .await
        .unwrap();
    assert!(matches!(first, Debounce::Held(_)), "got {first:?}");

    // The holder finishes, so a fresh bounce would now report the key unheld.
    sys.record_workflow_outcome("wf-held", Outcome::Output(Some("\"done\"")))
        .await
        .unwrap();

    let replayed = sys
        .debounce_delayed_workflow(
            &bounce("refund", "cart-1", Timestamp::from_epoch_ms(9_000_000)),
            Some(("wf-caller", 0)),
        )
        .await
        .unwrap();
    assert_eq!(
        replayed, first,
        "the recorded holder is reported, not one re-read after it changed",
    );
}
