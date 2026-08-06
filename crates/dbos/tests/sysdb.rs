//! The system database surface, against real databases.

mod support;

use dbos::sysdb::postgres::PostgresSystemDatabase;
use dbos::sysdb::retry::RetryPolicy;
use dbos::sysdb::types::{NewWorkflow, Timestamp, WorkflowStatus};
use dbos::sysdb::{
    BackendErrorKind, DEFAULT_SCHEMA, Error, InitWorkflowStatus, OutcomeWrite, SystemDatabase,
};

use support::test_database;

fn workflow(id: &str) -> NewWorkflow {
    NewWorkflow {
        name: Some("checkout".to_owned()),
        input: Some(r#"{"positionalArgs":[1],"namedArgs":{}}"#.to_owned()),
        serialization: Some("portable_json".to_owned()),
        executor_id: Some("local".to_owned()),
        application_version: Some("v1".to_owned()),
        ..NewWorkflow::new(id)
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
        .init_workflow_status(InitWorkflowStatus::new(&workflow("wf-1")))
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
    let mut r = workflow("wf-opaque");
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
    sys.init_workflow_status(InitWorkflowStatus::new(&workflow("wf-dup")))
        .await
        .unwrap();

    // The same id running a different function is a programming error, not a race.
    let mut different = workflow("wf-dup");
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
        .init_workflow_status(InitWorkflowStatus::new(&workflow("wf-dup")))
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
    let r = workflow("wf-attempts");

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
        let wf = NewWorkflow {
            queue_name: queue.map(str::to_owned),
            delay,
            ..workflow(&format!("wf-status-{index}"))
        };
        assert_eq!(
            wf.initial_status(),
            expected,
            "case {index} derived wrongly"
        );
        let init = sys
            .init_workflow_status(InitWorkflowStatus::new(&wf))
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
    let r = workflow("wf-dlq");
    sys.init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();

    // Each recovery is a separate attempt, and so gets its own owner identity.
    let mut last = Ok(());
    for _ in 0..5 {
        last = sys
            .init_workflow_status(InitWorkflowStatus {
                max_recovery_attempts: Some(2),
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
/// Each call generates its own, so two calls here are two owners without saying so.
#[tokio::test]
async fn a_second_owner_records_but_does_not_execute() {
    let (sys, _db) = sysdb().await;
    let r = workflow("wf-owned");

    let first = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert!(first.should_execute);

    let second = sys
        .init_workflow_status(InitWorkflowStatus::new(&r))
        .await
        .unwrap();
    assert!(
        !second.should_execute,
        "another owner holds this workflow, so running it would be a second execution",
    );

    // Recovery is exactly the case where taking it over is correct.
    let recovering = sys
        .init_workflow_status(InitWorkflowStatus {
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
    sys.init_workflow_status(InitWorkflowStatus::new(&workflow("wf-fmt")))
        .await
        .unwrap();

    let mut later = workflow("wf-fmt");
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
    let r = workflow("wf-auto-owner");

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

/// Every field a caller can set is written, and comes back on the row.
///
/// [`NewWorkflow`] has 24 fields and most tests set a handful. Without this, a field could be
/// dropped from the `INSERT` and nothing would notice — which is exactly what happened while
/// this test was being written.
#[tokio::test]
async fn every_settable_field_round_trips() {
    let (sys, _db) = sysdb().await;
    let written = NewWorkflow {
        class_name: Some("Checkout".to_owned()),
        config_name: Some("primary".to_owned()),
        queue_name: Some("orders".to_owned()),
        deduplication_id: Some("dedup-key".to_owned()),
        priority: 7,
        queue_partition_key: Some("eu-west".to_owned()),
        delay: Some(std::time::Duration::from_secs(60)),
        is_debounced: true,
        debounce_deadline: Some(Timestamp::from_epoch_ms(1_700_000_020_000)),
        timeout: Some(std::time::Duration::from_secs(30)),
        deadline: Some(Timestamp::from_epoch_ms(1_700_000_030_000)),
        application_id: Some("app-1".to_owned()),
        authenticated_user: Some("alice".to_owned()),
        authenticated_roles: Some(r#"["admin"]"#.to_owned()),
        assumed_role: Some("admin".to_owned()),
        parent_workflow_id: Some("wf-parent".to_owned()),
        schedule_name: Some("nightly".to_owned()),
        attributes: Some(r#"{"tenant": "acme"}"#.to_owned()),
        ..workflow("wf-all-fields")
    };
    let before = Timestamp::now();
    let init = sys
        .init_workflow_status(InitWorkflowStatus::new(&written))
        .await
        .expect("insert failed");

    // A queue plus a delay is DELAYED — derived here, not offered by the caller.
    assert_eq!(init.status, WorkflowStatus::Delayed);
    assert_eq!(init.deadline, written.deadline);

    let read = sys
        .get_workflow_status("wf-all-fields")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.workflow_id, "wf-all-fields");
    assert_eq!(read.name, written.name);
    assert_eq!(read.class_name, written.class_name);
    assert_eq!(read.config_name, written.config_name);
    assert_eq!(read.input, written.input);
    assert_eq!(read.serialization, written.serialization);
    assert_eq!(read.queue_name, written.queue_name);
    assert_eq!(read.deduplication_id, written.deduplication_id);
    assert_eq!(read.priority, written.priority);
    assert_eq!(read.queue_partition_key, written.queue_partition_key);
    assert!(read.is_debounced);
    assert_eq!(read.debounce_deadline, written.debounce_deadline);
    assert_eq!(read.timeout, written.timeout);
    assert_eq!(read.deadline, written.deadline);
    assert_eq!(read.executor_id, written.executor_id);
    assert_eq!(read.application_version, written.application_version);
    assert_eq!(read.application_id, written.application_id);
    assert_eq!(read.authenticated_user, written.authenticated_user);
    assert_eq!(read.authenticated_roles, written.authenticated_roles);
    assert_eq!(read.assumed_role, written.assumed_role);
    assert_eq!(read.parent_workflow_id, written.parent_workflow_id);
    assert_eq!(read.schedule_name, written.schedule_name);
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
    sys.init_workflow_status(InitWorkflowStatus::new(&workflow("wf-race")))
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
        .init_workflow_status(InitWorkflowStatus::new(&workflow("wf-dyn")))
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
    let sys = PostgresSystemDatabase::from_pool(pool, "no_such_schema");

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sys.get_workflow_status("wf-1"),
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
#[tokio::test]
async fn an_unreachable_database_is_a_connection_failure() {
    let db = test_database().await;
    let pool = db.pool().await;
    let sys = PostgresSystemDatabase::from_pool(pool.clone(), DEFAULT_SCHEMA).with_retry_policy(
        RetryPolicy {
            retry_connection_errors: false,
            ..RetryPolicy::default()
        },
    );
    pool.close().await;

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sys.get_workflow_status("wf-1"),
    )
    .await
    .expect("the opt-out should have returned rather than retried");

    match result {
        Err(Error::Backend(e)) => assert_eq!(
            e.kind,
            BackendErrorKind::Connection,
            "a closed pool is a connection failure, got {e:?}",
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
    let sys =
        PostgresSystemDatabase::from_pool(pool, DEFAULT_SCHEMA).with_retry_policy(RetryPolicy {
            initial_backoff: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        });

    let wf = workflow("wf-chaos");
    sys.init_workflow_status(InitWorkflowStatus::new(&wf))
        .await
        .expect("insert failed");

    // Every pooled connection is now dead, and the pool does not know it.
    db.kill_connections(TAG).await;

    let read = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        sys.get_workflow_status("wf-chaos"),
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
    let sys =
        PostgresSystemDatabase::from_pool(pool, DEFAULT_SCHEMA).with_retry_policy(RetryPolicy {
            retry_connection_errors: false,
            ..RetryPolicy::default()
        });

    let wf = workflow("wf-chaos-optout");
    sys.init_workflow_status(InitWorkflowStatus::new(&wf))
        .await
        .expect("insert failed");

    db.kill_connections(TAG).await;

    match sys.get_workflow_status("wf-chaos-optout").await {
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
            name: Some("checkout".to_owned()),
            class_name: Some("Checkout".to_owned()),
            config_name: Some("primary".to_owned()),
            application_version: Some("v1".to_owned()),
            executor_id: Some("alpha".to_owned()),
            authenticated_user: Some("alice".to_owned()),
            attributes: Some(r#"{"tenant": "acme", "tier": "gold"}"#.to_owned()),
            ..NewWorkflow::new("wf-a")
        },
        NewWorkflow {
            name: Some("refund".to_owned()),
            application_version: Some("v2".to_owned()),
            executor_id: Some("beta".to_owned()),
            authenticated_user: Some("bob".to_owned()),
            queue_name: Some("orders".to_owned()),
            deduplication_id: Some("dedup-b".to_owned()),
            schedule_name: Some("nightly".to_owned()),
            parent_workflow_id: Some("wf-a".to_owned()),
            attributes: Some(r#"{"tenant": "globex"}"#.to_owned()),
            ..NewWorkflow::new("wf-b")
        },
        NewWorkflow {
            name: Some("checkout".to_owned()),
            queue_name: Some("orders".to_owned()),
            delay: Some(std::time::Duration::from_secs(300)),
            is_debounced: true,
            deduplication_id: Some("dedup-c".to_owned()),
            ..NewWorkflow::new("wf-c")
        },
        NewWorkflow {
            name: Some("audit".to_owned()),
            queue_name: Some("reports".to_owned()),
            parent_workflow_id: Some("wf-a".to_owned()),
            ..NewWorkflow::new("wf-d")
        },
        // A `%` in the id, to prove a prefix filter treats it as text and not a wildcard.
        NewWorkflow {
            name: Some("audit".to_owned()),
            ..NewWorkflow::new("other-100%-done")
        },
    ];
    for wf in &seeds {
        sys.init_workflow_status(InitWorkflowStatus::new(wf))
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
    filter: &dbos::sysdb::types::WorkflowFilter,
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
                workflow_ids: vec!["wf-a".into(), "wf-d".into()],
                ..F::default()
            },
            &["wf-a", "wf-d"],
        ),
        (
            "workflow_id_prefixes",
            F {
                workflow_id_prefixes: vec!["wf-".into()],
                ..F::default()
            },
            &["wf-a", "wf-b", "wf-c", "wf-d"],
        ),
        // The `%` is data, not a wildcard: a naive LIKE would match every id here.
        (
            "prefix with a wildcard character",
            F {
                workflow_id_prefixes: vec!["other-100%".into()],
                ..F::default()
            },
            &["other-100%-done"],
        ),
        (
            "names",
            F {
                names: vec!["checkout".into()],
                ..F::default()
            },
            &["wf-a", "wf-c"],
        ),
        (
            "class_names",
            F {
                class_names: vec!["Checkout".into()],
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "config_names",
            F {
                config_names: vec!["primary".into()],
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
                application_versions: vec!["v2".into()],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "executor_ids",
            F {
                executor_ids: vec!["alpha".into()],
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "authenticated_users",
            F {
                authenticated_users: vec!["bob".into()],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "queue_names",
            F {
                queue_names: vec!["orders".into()],
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
                schedule_names: vec!["nightly".into()],
                ..F::default()
            },
            &["wf-b"],
        ),
        (
            "deduplication_ids",
            F {
                deduplication_ids: vec!["dedup-c".into()],
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
                parent_workflow_ids: vec!["wf-a".into()],
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
                forked_from: vec!["wf-a".into()],
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
                attributes: Some(r#"{"tenant": "acme"}"#.into()),
                ..F::default()
            },
            &["wf-a"],
        ),
        (
            "combined filters are ANDed",
            F {
                names: vec!["checkout".into()],
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
        workflow_id_prefixes: vec!["wf-".into()],
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
        input: Some(r#"{"positionalArgs":[1]}"#.to_owned()),
        ..NewWorkflow::new("wf-payload")
    };
    sys.init_workflow_status(InitWorkflowStatus::new(&wf))
        .await
        .unwrap();
    sys.update_workflow_outcome("wf-payload", WorkflowStatus::Success, Some("42"), None)
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
