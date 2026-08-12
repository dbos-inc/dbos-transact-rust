//! The system database surface, against real databases.

mod support;

use dbos::sysdb::postgres::{Config, PostgresSystemDatabase, Settings};
use dbos::sysdb::retry::RetryPolicy;
use dbos::sysdb::types::{
    Applications, Fork, ForkOptions, ForkPoint, Message, NewWorkflow, Outcome, OutcomeWrite,
    RenameBatching, RenameFrom, StepTiming, Submission, Timestamp, WorkflowDelay, WorkflowFilter,
    WorkflowRecord, WorkflowStatus, WrittenBy,
};
use dbos::sysdb::{BackendErrorKind, Error, INTERNAL_QUEUE, SystemDatabase};

use support::test_database;

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
    let (sys, db) = sysdb().await;
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
    drop(db);

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
