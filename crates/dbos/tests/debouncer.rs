//! Debouncing, against real databases.
//!
//! A debounced workflow is a `DELAYED` row holding `<workflow>-<key>` as its deduplication id, so
//! most of these assert on that row as well as on what runs. The timing-sensitive ones use periods
//! of a second or two and assert only lower bounds on when a workflow ran, or wait with generous
//! timeouts; a workflow meant to stay `DELAYED` is given a period of days.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dbos::sysdb::PORTABLE_JSON;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowFilter, WorkflowRecord, WorkflowStatus};
use dbos::sysdb::{Error as SysdbError, INTERNAL_QUEUE, SystemDatabase};
use dbos::{
    Client, ClientConfig, Config, DBOS, DebounceOptions, Debouncer, DebouncerClient,
    DuplicationPolicy, Enqueue, EnqueueOptions, Error, QueueConflict, QueueOptions, RunOptions,
    SerializationFormat, StartOptions, Timeout, WorkflowHandle, WorkflowKey,
};
use dbos_test_support::{TestDatabase, test_database};

/// How long a test waits for a workflow it expects to run.
const WAIT: Duration = Duration::from_secs(60);

/// A period long enough that a workflow given it stays `DELAYED` for the whole test.
const FOREVER: Duration = Duration::from_secs(10 * 24 * 3600);

/// A period short enough to wait out, long enough that the calls made within it land within it.
const PERIOD: Duration = Duration::from_secs(2);

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(format!("{app_name}-1.0.0")),
        ..Config::new(app_name, db.url())
    }
}

async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// A client for `db`, acting for `app_name`, or for no application.
async fn client(app_name: Option<&str>, db: &TestDatabase) -> Client {
    Client::connect(ClientConfig {
        app_name: app_name.map(str::to_owned),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed")
}

async fn row(reader: &PostgresSystemDatabase, workflow_id: &str) -> WorkflowRecord {
    reader
        .get_workflow(workflow_id)
        .await
        .expect("read failed")
        .unwrap_or_else(|| panic!("workflow {workflow_id} has no row"))
}

async fn rows_named(reader: &PostgresSystemDatabase, name: &str) -> Vec<WorkflowRecord> {
    reader
        .list_workflows(
            &WorkflowFilter {
                names: vec![name],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("read failed")
}

async fn result<R: serde::de::DeserializeOwned + Send + 'static>(handle: WorkflowHandle<R>) -> R {
    tokio::time::timeout(WAIT, handle.result())
        .await
        .expect("the workflow never finished")
        .expect("the workflow failed")
}

/// Per-call options pinning the fresh workflow's id.
fn pinned(workflow_id: &str) -> DebounceOptions<'_> {
    DebounceOptions {
        workflow_id: Some(workflow_id),
    }
}

fn is_conflicting(error: &Error) -> bool {
    matches!(
        error,
        Error::SystemDatabase(SysdbError::ConflictingWorkflow { .. })
    )
}

fn is_deduplicated(error: &Error) -> bool {
    matches!(
        error,
        Error::SystemDatabase(SysdbError::QueueDeduplicated { .. })
    )
}

/// Calls within the period coalesce into one run, with the last call's input; once that run is
/// released, the same key starts a fresh workflow. A pinned workflow id names a fresh workflow.
///
/// Timing-sensitive only in that the two calls of each pair must land within [`PERIOD`] of each
/// other, which back-to-back calls do.
#[tokio::test]
async fn calls_within_the_period_coalesce_into_one_run_with_the_last_input() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-coalesce-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let debouncer = Debouncer::new(&echo);

    let first = debouncer.debounce("key", PERIOD, 0).await.unwrap();
    let second = debouncer.debounce("key", PERIOD, 1).await.unwrap();
    let first_id = first.workflow_id().to_owned();
    assert_eq!(second.workflow_id(), first_id, "the second call coalesced");
    let held = row(&reader, &first_id).await;
    assert_eq!(held.queue_name.as_deref(), Some(INTERNAL_QUEUE));
    assert_eq!(held.deduplication_id.as_deref(), Some("echo-key"));
    assert!(held.is_debounced);
    assert_eq!(result(first).await, 1, "the run takes the last input");
    assert_eq!(result(second).await, 1);
    assert_eq!(rows_named(&reader, "echo").await.len(), 1, "one run");

    // Released, so the key is free: a fresh workflow.
    let third = debouncer.debounce("key", PERIOD, 2).await.unwrap();
    let fourth = debouncer.debounce("key", PERIOD, 3).await.unwrap();
    assert_ne!(third.workflow_id(), first_id);
    assert_eq!(third.workflow_id(), fourth.workflow_id());
    assert_eq!(result(third).await, 3);
    assert_eq!(result(fourth).await, 3);

    // A pinned id names the workflow a debounce enqueues.
    let handle = debouncer
        .debounce_with("key", PERIOD, 4, pinned("pinned-debounce"))
        .await
        .unwrap();
    assert_eq!(handle.workflow_id(), "pinned-debounce");
    assert_eq!(result(handle).await, 4);

    // Different keys are different debounces.
    let a = debouncer.debounce("a", PERIOD, 5).await.unwrap();
    let b = debouncer.debounce("b", PERIOD, 6).await.unwrap();
    assert_ne!(a.workflow_id(), b.workflow_id());
    assert_eq!(result(a).await, 5);
    assert_eq!(result(b).await, 6);

    dbos.shutdown().await;
}

/// A pinned id on a debounce that coalesces is not used, and pinning the same id across calls of
/// one key still coalesces onto the workflow that holds it.
#[tokio::test]
async fn a_pinned_id_applies_only_to_a_fresh_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-pinned-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let debouncer = Debouncer::new(&echo);

    // Unpinned first, so the holder has a generated id; the pinned bounce leaves it alone.
    let holder = debouncer.debounce("key", FOREVER, 1).await.unwrap();
    let bounced = debouncer
        .debounce_with("key", FOREVER, 2, pinned("never-used"))
        .await
        .unwrap();
    assert_eq!(bounced.workflow_id(), holder.workflow_id());
    assert!(
        reader.get_workflow("never-used").await.unwrap().is_none(),
        "a coalesced debounce creates nothing under its pinned id"
    );

    // The same pinned id on both calls: the second bounces the first, and a short period
    // shortens the huge one so the workflow runs.
    let first = debouncer
        .debounce_with("other", FOREVER, 3, pinned("pinned-twice"))
        .await
        .unwrap();
    let second = debouncer
        .debounce_with(
            "other",
            Duration::from_millis(500),
            4,
            pinned("pinned-twice"),
        )
        .await
        .unwrap();
    assert_eq!(first.workflow_id(), "pinned-twice");
    assert_eq!(second.workflow_id(), "pinned-twice");
    assert_eq!(result(second).await, 4);

    dbos.shutdown().await;
}

/// The workflow waits for the period after the **last** call, not the first.
///
/// The first call holds the workflow back for days, so however slowly it runs it cannot let the
/// workflow go before the second call arrives; the second call's period then shortens the delay.
///
/// Timing-sensitive: asserts a lower bound only — the run starts no earlier than the last call
/// plus the period, less a small allowance for clock granularity — so a slow machine cannot fail
/// it.
#[tokio::test]
async fn the_run_waits_for_the_period_after_the_last_call() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-wait-app", &db));
    let ran_at: Arc<Mutex<Option<Instant>>> = Arc::default();
    let echo = {
        let ran_at = Arc::clone(&ran_at);
        dbos.register_workflow("echo", move |x: u32| {
            let ran_at = Arc::clone(&ran_at);
            async move {
                ran_at.lock().unwrap().get_or_insert_with(Instant::now);
                Ok::<u32, Error>(x)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");
    let debouncer = Debouncer::new(&echo);

    let first = debouncer.debounce("key", FOREVER, 1).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let last_call = Instant::now();
    let second = debouncer.debounce("key", PERIOD, 2).await.unwrap();
    assert_eq!(first.workflow_id(), second.workflow_id());
    assert_eq!(result(second).await, 2);

    let ran_at = ran_at.lock().unwrap().expect("the workflow never ran");
    assert!(
        ran_at + Duration::from_millis(100) >= last_call + PERIOD,
        "ran {:?} after the last call, before its period of {PERIOD:?} was up",
        ran_at.duration_since(last_call)
    );

    dbos.shutdown().await;
}

/// The debounce timeout caps the total delay: a workflow given a period of days still runs once
/// the timeout is up, and the fresh row's release is capped at its deadline from the insert. A
/// later bounce with a short period shortens a long one.
///
/// Timing-sensitive only in waiting for the two-second timeout to pass, with a generous bound.
#[tokio::test]
async fn the_debounce_timeout_caps_the_total_delay() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-timeout-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let capped = Debouncer {
        debounce_timeout: Some(Duration::from_secs(2)),
        ..Debouncer::new(&echo)
    };
    let first = capped.debounce("key", FOREVER, 1).await.unwrap();
    let held = row(&reader, first.workflow_id()).await;
    assert_eq!(held.status, WorkflowStatus::Delayed);
    let deadline = held.debounce_deadline.expect("the deadline is recorded");
    let delay_until = held.delay_until.expect("the row is delayed");
    assert!(
        delay_until <= deadline,
        "the first delay is capped at the deadline"
    );
    let second = capped.debounce("key", FOREVER, 2).await.unwrap();
    assert_eq!(first.workflow_id(), second.workflow_id());
    let bounced = row(&reader, first.workflow_id()).await;
    assert!(
        bounced.delay_until.expect("still delayed") <= deadline,
        "a bounce never moves the release past the deadline"
    );
    assert_eq!(
        result(first).await,
        2,
        "ran at the timeout, with the last input"
    );

    // A long period, then a short one: the short one wins.
    let plain = Debouncer::new(&echo);
    let long = plain.debounce("key", FOREVER, 3).await.unwrap();
    let short = plain
        .debounce("key", Duration::from_secs(1), 4)
        .await
        .unwrap();
    assert_eq!(long.workflow_id(), short.workflow_id());
    assert_ne!(long.workflow_id(), second.workflow_id());
    assert_eq!(result(long).await, 4);

    dbos.shutdown().await;
}

/// The release from `DELAYED` frees the key at once: a debounce of the key while the released
/// workflow is still running starts a fresh one.
#[tokio::test]
async fn a_key_reused_after_release_starts_a_fresh_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-release-app", &db));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let blocking = {
        let (started, release) = (Arc::clone(&started), Arc::clone(&release));
        dbos.register_workflow("blocking", move |x: u32| {
            let (started, release) = (Arc::clone(&started), Arc::clone(&release));
            async move {
                started.notify_one();
                release.acquire().await.unwrap().forget();
                Ok::<u32, Error>(x)
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let debouncer = Debouncer::new(&blocking);

    let handle = debouncer.debounce("key", FOREVER, 1).await.unwrap();
    let id = handle.workflow_id().to_owned();
    let held = row(&reader, &id).await;
    assert_eq!(held.status, WorkflowStatus::Delayed);
    assert!(held.is_debounced);
    assert_eq!(held.deduplication_id.as_deref(), Some("blocking-key"));

    // A short period shortens the delay and replaces the input; the same workflow runs.
    let bounced = debouncer
        .debounce("key", Duration::from_millis(500), 2)
        .await
        .unwrap();
    assert_eq!(bounced.workflow_id(), id);
    tokio::time::timeout(WAIT, started.notified())
        .await
        .expect("the workflow never started");

    // Still running, and the key already cleared by the release.
    let running = row(&reader, &id).await;
    assert_eq!(running.status, WorkflowStatus::Pending);
    assert_eq!(running.deduplication_id, None);

    let fresh = debouncer
        .debounce("key", Duration::from_millis(500), 3)
        .await
        .unwrap();
    assert_ne!(
        fresh.workflow_id(),
        id,
        "a fresh workflow, not the running one"
    );

    release.add_permits(2);
    assert_eq!(result(bounced).await, 2);
    assert_eq!(result(fresh).await, 3);

    dbos.shutdown().await;
}

/// A cancelled debounced workflow no longer holds its key.
#[tokio::test]
async fn a_cancelled_debounced_workflow_frees_its_key() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-cancel-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let debouncer = Debouncer::new(&echo);

    let handle = debouncer.debounce("key", FOREVER, 1).await.unwrap();
    dbos.cancel(handle.workflow_id()).await.unwrap();
    let fresh = debouncer
        .debounce("key", Duration::from_millis(500), 2)
        .await
        .unwrap();
    assert_ne!(fresh.workflow_id(), handle.workflow_id());
    assert_eq!(result(fresh).await, 2);

    dbos.shutdown().await;
}

/// A debounce inside a workflow is checkpointed: the fresh enqueue is recorded as a child start,
/// and a replay hands back the same workflow without enqueueing another. The debounced workflow
/// does not inherit the parent's deadline.
#[tokio::test]
async fn a_fresh_debounce_inside_a_workflow_replays_without_enqueueing_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-replay-fresh-app", &db));
    let child = dbos
        .register_workflow("child", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    let debouncer: Debouncer<'static, u32, u32> = Debouncer::new(&child);
    let parent = dbos
        .register_workflow("parent", move |()| {
            let debouncer = debouncer.clone();
            async move {
                let handle = debouncer.debounce("key", FOREVER, 7).await?;
                Ok::<_, Error>(handle.workflow_id().to_owned())
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let parent_id = "debouncing-parent";
    let child_id = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(parent_id),
                timeout: Timeout::Explicit(Duration::from_secs(60)),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(
        child_id,
        format!("{parent_id}-1"),
        "the child id is derived"
    );

    let steps = reader
        .list_workflow_steps(parent_id, true, None, None, None)
        .await
        .expect("read failed");
    let names: Vec<_> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        names,
        [(0, "DBOS.debounce_delayed_workflow"), (1, "child")],
        "the bounce and the start are both recorded"
    );
    assert_eq!(
        steps[1].child_workflow_id.as_deref(),
        Some(child_id.as_str())
    );

    let held = row(&reader, &child_id).await;
    assert_eq!(held.status, WorkflowStatus::Delayed);
    assert_eq!(held.parent_workflow_id.as_deref(), Some(parent_id));
    assert_eq!(
        held.deadline, None,
        "the parent's deadline is not inherited"
    );
    assert_eq!(held.timeout, None);

    // A fork past both steps replays them: the same child, and no second one.
    let replayed = dbos
        .fork::<String, Error>(parent_id, 2)
        .await
        .expect("fork failed");
    assert_eq!(
        tokio::time::timeout(WAIT, replayed.result())
            .await
            .expect("the replay never finished")
            .expect("the replay failed"),
        child_id
    );
    assert_eq!(
        rows_named(&reader, "child").await.len(),
        1,
        "no second child"
    );

    dbos.shutdown().await;
}

/// A bounce inside a workflow commits with its checkpoint, and a replay reads it back rather than
/// bouncing again: the holder's release does not move a second time.
#[tokio::test]
async fn a_bounce_inside_a_workflow_replays_without_bouncing_again() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-replay-bounce-app", &db));
    let child = dbos
        .register_workflow("child", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    let debouncer: Debouncer<'static, u32, u32> = Debouncer::new(&child);
    let parent = {
        let debouncer = debouncer.clone();
        dbos.register_workflow("parent", move |period_secs: u64| {
            let debouncer = debouncer.clone();
            async move {
                let handle = debouncer
                    .debounce("key", Duration::from_secs(period_secs), 2)
                    .await?;
                Ok::<_, Error>(handle.workflow_id().to_owned())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let holder = debouncer.debounce("key", FOREVER, 1).await.unwrap();
    let before = row(&reader, holder.workflow_id())
        .await
        .delay_until
        .unwrap();

    let parent_id = "bouncing-parent";
    let bounced_id = parent
        .run_with(
            FOREVER.as_secs() * 2,
            RunOptions {
                workflow_id: Some(parent_id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert_eq!(bounced_id, holder.workflow_id(), "the parent coalesced");
    let after = row(&reader, holder.workflow_id())
        .await
        .delay_until
        .unwrap();
    assert!(after > before, "the bounce moved the release out");
    let steps = reader
        .list_workflow_steps(parent_id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "only the bounce is recorded: {steps:?}");
    assert_eq!(steps[0].step_name, "DBOS.debounce_delayed_workflow");

    // The replay returns the same holder, and the release stays where the first run put it.
    let replayed = dbos
        .fork::<String, Error>(parent_id, 2)
        .await
        .expect("fork failed");
    assert_eq!(
        tokio::time::timeout(WAIT, replayed.result())
            .await
            .expect("the replay never finished")
            .expect("the replay failed"),
        bounced_id
    );
    assert_eq!(
        row(&reader, holder.workflow_id()).await.delay_until,
        Some(after),
        "the replay did not bounce again"
    );
    assert_eq!(rows_named(&reader, "child").await.len(), 1);

    dbos.shutdown().await;
}

/// Concurrent debounces of one key race to enqueue; every loser bounces the winner, so they all
/// end up with one workflow.
///
/// The race is not forced — whether any insert loses depends on scheduling — so this exercises
/// the retry rather than proving it is taken. Both the application and a client contend. The
/// period is long enough that every call lands before the release, however slowly they run.
#[tokio::test]
async fn concurrent_debounces_of_one_key_coalesce_onto_one_workflow() {
    const RACE_PERIOD: Duration = Duration::from_secs(30);
    let db = test_database().await;
    let app = "debounce-race-app";
    let dbos = DBOS::new(config(app, &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let client = client(Some(app), &db).await;
    let debouncer: Debouncer<'static, u32, u32> = Debouncer::new(&echo);

    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..8u32 {
        let debouncer = debouncer.clone();
        tasks.spawn(async move {
            let handle = debouncer.debounce("race", RACE_PERIOD, n).await.unwrap();
            handle.workflow_id().to_owned()
        });
        let client = client.clone();
        tasks.spawn(async move {
            let debouncer =
                DebouncerClient::new(&client, "echo", EnqueueOptions::new(INTERNAL_QUEUE));
            let handle: WorkflowHandle<u32> = debouncer
                .debounce("race", RACE_PERIOD, 100 + n)
                .await
                .unwrap();
            handle.workflow_id().to_owned()
        });
    }
    let mut ids = Vec::new();
    while let Some(id) = tasks.join_next().await {
        ids.push(id.expect("a debounce panicked"));
    }
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        1,
        "every debounce got the same workflow: {ids:?}"
    );
    assert_eq!(rows_named(&reader, "echo").await.len(), 1);

    let handle: WorkflowHandle<u32> = dbos.retrieve_workflow(&ids[0]).unwrap();
    let ran = result(handle).await;
    assert!(ran < 8 || (100..108).contains(&ran), "ran with {ran}");

    client.close().await;
    dbos.shutdown().await;
}

/// Each option a debounce owns is refused, from an app and from a client, and nothing is written.
/// So are a zero period, a `workflow_id` stored on the debouncer, and an empty `app_name`.
#[tokio::test]
async fn each_option_a_debounce_owns_is_refused() {
    let db = test_database().await;
    let app = "debounce-refuse-app";
    let dbos = DBOS::new(config(app, &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let client = client(Some(app), &db).await;

    let owned: [(&str, Enqueue<'static>); 5] = [
        (
            "deduplication_id",
            Enqueue {
                deduplication_id: Some("mine"),
                ..Enqueue::new("q")
            },
        ),
        (
            "delay",
            Enqueue {
                delay: Some(Duration::from_secs(100)),
                ..Enqueue::new("q")
            },
        ),
        (
            "ReturnExisting",
            Enqueue {
                deduplication_id: None,
                duplication_policy: DuplicationPolicy::ReturnExisting,
                ..Enqueue::new("q")
            },
        ),
        (
            "priority",
            Enqueue {
                priority: Some(1),
                ..Enqueue::new("q")
            },
        ),
        (
            "partition_key",
            Enqueue {
                partition_key: Some("p"),
                ..Enqueue::new("q")
            },
        ),
    ];
    let refused = |error: Error, field: &str| {
        assert!(
            matches!(&error, Error::InvalidArgument { detail, .. } if detail.contains(field)),
            "{field}: {error}"
        );
    };
    for (field, queue) in owned {
        let debouncer = Debouncer {
            options: StartOptions {
                queue: Some(queue.clone()),
                ..StartOptions::default()
            },
            ..Debouncer::new(&echo)
        };
        refused(debouncer.debounce("k", PERIOD, 1).await.unwrap_err(), field);
        let debouncer = DebouncerClient::new(&client, "echo", EnqueueOptions::on(queue));
        refused(
            debouncer
                .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 1)
                .await
                .unwrap_err(),
            field,
        );
    }

    refused(
        Debouncer::new(&echo)
            .debounce("k", Duration::ZERO, 1)
            .await
            .unwrap_err(),
        "period",
    );
    refused(
        DebouncerClient::new(&client, "echo", EnqueueOptions::new(INTERNAL_QUEUE))
            .debounce::<_, u32, dbos::EngineOnly>("k", Duration::ZERO, 1)
            .await
            .unwrap_err(),
        "period",
    );

    // A pinned id stored on a debouncer would name every workflow it enqueues: refused by its
    // public name, pointing at the per-call form.
    let stored_id = Debouncer {
        options: StartOptions {
            workflow_id: Some("stored"),
            ..StartOptions::default()
        },
        ..Debouncer::new(&echo)
    };
    refused(
        stored_id.debounce("k", PERIOD, 1).await.unwrap_err(),
        "options.workflow_id",
    );
    let stored_id = DebouncerClient::new(
        &client,
        "echo",
        EnqueueOptions {
            workflow_id: Some("stored"),
            ..EnqueueOptions::new(INTERNAL_QUEUE)
        },
    );
    let error = stored_id
        .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 1)
        .await
        .unwrap_err();
    refused(error, "debounce_with");

    // An empty app name is refused as the API spells it, not as the column it would fill.
    let error = Debouncer {
        app_name: Some(""),
        ..Debouncer::new(&echo)
    }
    .debounce("k", PERIOD, 1)
    .await
    .unwrap_err();
    assert!(!error.to_string().contains("application_name"), "{error}");
    refused(error, "`app_name`");
    let error = DebouncerClient::new(
        &client,
        "echo",
        EnqueueOptions {
            app_name: Some(""),
            ..EnqueueOptions::new(INTERNAL_QUEUE)
        },
    )
    .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 1)
    .await
    .unwrap_err();
    assert!(!error.to_string().contains("application_name"), "{error}");
    refused(error, "`app_name`");

    assert!(
        rows_named(&reader, "echo").await.is_empty(),
        "nothing was written"
    );

    client.close().await;
    dbos.shutdown().await;
}

/// A key held by an ordinary deduplicated enqueue is not this debounce's: the debounce fails with
/// the deduplication error, naming the holder, and leaves it alone.
#[tokio::test]
async fn a_key_held_by_a_workflow_that_is_not_debounced_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-held-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    // Never registered, so nothing dequeues the holder and the key stays held.
    let queue = "held-queue";
    let holder = echo
        .start_with(
            1,
            StartOptions {
                queue: Some(Enqueue {
                    deduplication_id: Some("echo-key"),
                    ..Enqueue::new(queue)
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the holder was not enqueued");

    let debouncer = Debouncer {
        options: StartOptions {
            queue: Some(Enqueue::new(queue)),
            ..StartOptions::default()
        },
        ..Debouncer::new(&echo)
    };
    let error = debouncer.debounce("key", PERIOD, 2).await.unwrap_err();
    assert!(
        matches!(&error, Error::SystemDatabase(SysdbError::QueueDeduplicated { workflow_id, .. })
            if workflow_id == holder.workflow_id()),
        "{error}"
    );
    let untouched = row(&reader, holder.workflow_id()).await;
    assert_eq!(untouched.status, WorkflowStatus::Enqueued);
    assert!(!untouched.is_debounced);

    dbos.shutdown().await;
}

/// `"colw"` with key `"b-k"` and `"colw-b"` with key `"k"` spell the same deduplication id. The
/// second debounce fails rather than overwrite the first's workflow, which a same-workflow bounce
/// still coalesces onto.
#[tokio::test]
async fn a_key_collision_between_two_workflows_is_refused() {
    let db = test_database().await;
    let client = client(None, &db).await;
    let reader = reader(&db).await;
    let queue = "collision-queue";
    let a = DebouncerClient::new(&client, "colw", EnqueueOptions::new(queue));
    let b = DebouncerClient::new(&client, "colw-b", EnqueueOptions::new(queue));

    let held: WorkflowHandle<u32> = a.debounce("b-k", FOREVER, 1).await.unwrap();
    let error = b
        .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 2)
        .await
        .unwrap_err();
    assert!(is_deduplicated(&error), "{error}");
    let victim = row(&reader, held.workflow_id()).await;
    assert_eq!(victim.status, WorkflowStatus::Delayed);
    assert_eq!(victim.name.as_deref(), Some("colw"));
    assert_eq!(victim.deduplication_id.as_deref(), Some("colw-b-k"));

    let again: WorkflowHandle<u32> = a.debounce("b-k", FOREVER, 3).await.unwrap();
    assert_eq!(again.workflow_id(), held.workflow_id());

    client.close().await;
}

/// The client's debouncer coalesces as the application's does, runs the workflow in the
/// application, and honours a pinned id, a queue and a timeout from its options.
#[tokio::test]
async fn the_client_debouncer_coalesces_and_honours_its_options() {
    let db = test_database().await;
    let app = "debounce-client-app";
    let dbos = DBOS::new(config(app, &db));
    let runs = Arc::new(AtomicU32::new(0));
    {
        let runs = Arc::clone(&runs);
        dbos.register_workflow("echo", move |x: u32| {
            let runs = Arc::clone(&runs);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok::<u32, Error>(x)
            }
        })
        .unwrap();
    }
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "client-queue",
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");
    let reader = reader(&db).await;
    let client = client(Some(app), &db).await;

    let debouncer = DebouncerClient::new(&client, "echo", EnqueueOptions::new("client-queue"));
    let first: WorkflowHandle<u32> = debouncer.debounce("key", PERIOD, 0).await.unwrap();
    let second: WorkflowHandle<u32> = debouncer.debounce("key", PERIOD, 1).await.unwrap();
    let first_id = first.workflow_id().to_owned();
    assert_eq!(second.workflow_id(), first_id);
    assert_eq!(result(first).await, 1);
    assert_eq!(result(second).await, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "one run");
    assert_eq!(
        row(&reader, &first_id).await.queue_name.as_deref(),
        Some("client-queue")
    );

    let third: WorkflowHandle<u32> = debouncer.debounce("key", PERIOD, 2).await.unwrap();
    let fourth: WorkflowHandle<u32> = debouncer.debounce("key", PERIOD, 3).await.unwrap();
    assert_ne!(third.workflow_id(), first_id);
    assert_eq!(third.workflow_id(), fourth.workflow_id());
    assert_eq!(result(fourth).await, 3);

    let timed = DebouncerClient {
        options: EnqueueOptions {
            timeout: Some(Duration::from_secs(5)),
            ..EnqueueOptions::new("client-queue")
        },
        ..debouncer.clone()
    };
    let handle: WorkflowHandle<u32> = timed
        .debounce_with("key", PERIOD, 4, pinned("client-pinned"))
        .await
        .unwrap();
    assert_eq!(handle.workflow_id(), "client-pinned");
    assert_eq!(result(handle).await, 4);
    let ran = row(&reader, "client-pinned").await;
    assert_eq!(ran.timeout, Some(Duration::from_secs(5)));
    assert!(
        ran.deadline.is_some(),
        "the budget became a deadline on dequeue"
    );

    client.close().await;
    dbos.shutdown().await;
}

/// Both debouncers can act for a named application: the fresh enqueue is written for it, later
/// debounces naming the same target coalesce onto it, and a caller acting for another
/// application collides.
#[tokio::test]
async fn a_debounce_can_target_another_application() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-target-app", &db));
    let wf = dbos
        .register_workflow("wf", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let client = client(Some("app-a"), &db).await;

    let debouncer = Debouncer {
        app_name: Some("other-app"),
        ..Debouncer::new(&wf)
    };
    let first = debouncer.debounce("k", FOREVER, 1).await.unwrap();
    let held = row(&reader, first.workflow_id()).await;
    assert_eq!(held.status, WorkflowStatus::Delayed);
    assert_eq!(held.app_name.as_deref(), Some("other-app"));
    assert_eq!(
        held.app_version, None,
        "the target's latest version runs it"
    );
    let again = debouncer.debounce("k", FOREVER, 2).await.unwrap();
    assert_eq!(again.workflow_id(), first.workflow_id());

    // A client acting for itself coalesces once it names the target.
    let targeted = DebouncerClient::new(
        &client,
        "wf",
        EnqueueOptions {
            app_name: Some("other-app"),
            ..EnqueueOptions::new(INTERNAL_QUEUE)
        },
    );
    let coalesced: WorkflowHandle<u32> = targeted.debounce("k", FOREVER, 3).await.unwrap();
    assert_eq!(coalesced.workflow_id(), first.workflow_id());

    // Without naming it, the client acts as `app-a` and collides.
    let error = DebouncerClient::new(&client, "wf", EnqueueOptions::new(INTERNAL_QUEUE))
        .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 4)
        .await
        .unwrap_err();
    assert!(is_deduplicated(&error), "{error}");
    assert_eq!(
        row(&reader, first.workflow_id()).await.app_name.as_deref(),
        Some("other-app")
    );

    client.close().await;
    dbos.shutdown().await;
}

/// A deduplication key is a global address: a bounce claims an unclaimed holder for its
/// application, another application's debounce collides rather than overwriting the owner's
/// input, and a nameless client leaves the owner alone.
#[tokio::test]
async fn a_bounce_claims_an_unclaimed_holder_and_another_application_collides() {
    let db = test_database().await;
    let reader = reader(&db).await;
    let nameless = client(None, &db).await;
    let app_a = client(Some("app-a"), &db).await;
    let app_b = client(Some("app-b"), &db).await;
    let options = || EnqueueOptions::new("cross-app-queue");
    let owner = |record: WorkflowRecord| {
        assert_eq!(record.status, WorkflowStatus::Delayed);
        record.app_name
    };

    let handle: WorkflowHandle<u32> = DebouncerClient::new(&nameless, "shared", options())
        .debounce("k", FOREVER, 1)
        .await
        .unwrap();
    let id = handle.workflow_id().to_owned();
    assert_eq!(owner(row(&reader, &id).await), None);

    let claimed: WorkflowHandle<u32> = DebouncerClient::new(&app_a, "shared", options())
        .debounce("k", FOREVER, 2)
        .await
        .unwrap();
    assert_eq!(claimed.workflow_id(), id);
    assert_eq!(owner(row(&reader, &id).await).as_deref(), Some("app-a"));

    let error = DebouncerClient::new(&app_b, "shared", options())
        .debounce::<_, u32, dbos::EngineOnly>("k", PERIOD, 3)
        .await
        .unwrap_err();
    assert!(is_deduplicated(&error), "{error}");
    assert_eq!(owner(row(&reader, &id).await).as_deref(), Some("app-a"));

    let _: WorkflowHandle<u32> = DebouncerClient::new(&nameless, "shared", options())
        .debounce("k", FOREVER, 4)
        .await
        .unwrap();
    assert_eq!(owner(row(&reader, &id).await).as_deref(), Some("app-a"));

    for client in [nameless, app_a, app_b] {
        client.close().await;
    }
}

/// A pinned id names one workflow: once the first cycle's workflow has run, a second fresh
/// debounce under the same id fails with a conflict rather than joining the finished workflow and
/// dropping its input. The same debounce without the pin starts a fresh workflow, from an app and
/// from a client.
#[tokio::test]
async fn a_pinned_id_reused_after_its_workflow_ran_is_a_conflict_not_a_join() {
    let db = test_database().await;
    let app = "debounce-pinned-cycles-app";
    let dbos = DBOS::new(config(app, &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;
    let client = client(Some(app), &db).await;
    let short = Duration::from_millis(500);

    let debouncer = Debouncer::new(&echo);
    let first = debouncer
        .debounce_with("key", short, 1, pinned("pinned-cycle"))
        .await
        .unwrap();
    assert_eq!(first.workflow_id(), "pinned-cycle");
    assert_eq!(result(first).await, 1);

    // The key is free again, so this enqueues fresh, under an id the finished workflow holds.
    let error = debouncer
        .debounce_with("key", short, 2, pinned("pinned-cycle"))
        .await
        .unwrap_err();
    assert!(is_conflicting(&error), "{error}");
    let finished = row(&reader, "pinned-cycle").await;
    assert_eq!(finished.status, WorkflowStatus::Success);
    assert_eq!(finished.deduplication_id, None, "the release freed the key");
    // So is another key's fresh enqueue under the same id.
    let error = debouncer
        .debounce_with("another-key", short, 3, pinned("pinned-cycle"))
        .await
        .unwrap_err();
    assert!(is_conflicting(&error), "{error}");
    assert_eq!(
        rows_named(&reader, "echo").await.len(),
        1,
        "nothing new was written"
    );

    // Without the pin, the next cycle is a fresh workflow.
    let fresh = debouncer.debounce("key", short, 4).await.unwrap();
    assert_ne!(fresh.workflow_id(), "pinned-cycle");
    assert_eq!(result(fresh).await, 4);

    // The client refuses the same way.
    let through_client = DebouncerClient::new(&client, "echo", EnqueueOptions::new(INTERNAL_QUEUE));
    let error = through_client
        .debounce_with::<_, u32, dbos::EngineOnly>("key", short, 5, pinned("pinned-cycle"))
        .await
        .unwrap_err();
    assert!(is_conflicting(&error), "{error}");
    let fresh: WorkflowHandle<u32> = through_client
        .debounce_with("key", short, 6, pinned("client-pinned-cycle"))
        .await
        .unwrap();
    assert_eq!(fresh.workflow_id(), "client-pinned-cycle");
    assert_eq!(result(fresh).await, 6);

    client.close().await;
    dbos.shutdown().await;
}

/// A debounce cannot be made from inside a step, as a start cannot.
#[tokio::test]
async fn a_debounce_inside_a_step_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-in-step-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    let debouncer: Debouncer<'static, u32, u32> = Debouncer::new(&echo);
    let parent = dbos
        .register_workflow("parent", move |()| {
            let debouncer = debouncer.clone();
            async move {
                dbos::step::<String, dbos::EngineOnly, _, _>("tries_to_debounce", || {
                    let debouncer = debouncer.clone();
                    async move {
                        let handle = debouncer.debounce("key", FOREVER, 1).await?;
                        Ok(handle.workflow_id().to_owned())
                    }
                })
                .await
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let error = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some("debounces-in-a-step"),
                ..RunOptions::default()
            },
        )
        .await
        .expect_err("the parent succeeded");
    assert!(
        matches!(&error, Error::InsideStep { operation } if operation.contains("debounc")),
        "expected an inside-a-step refusal, got {error:?}"
    );
    assert!(
        rows_named(&reader, "echo").await.is_empty(),
        "nothing was debounced"
    );

    dbos.shutdown().await;
}

/// A key held by an ordinary enqueue, met from inside a workflow, is the deduplication error; a
/// replay reads the bounce's checkpoint back and raises it again, even once the key is free,
/// rather than bouncing again and enqueueing a workflow the first run never did.
#[tokio::test]
async fn a_deduplicated_debounce_inside_a_workflow_replays_its_error() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-replay-dedup-app", &db));
    let queue = "replay-dedup-queue";
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    let debouncer: Debouncer<'static, u32, u32> = Debouncer {
        options: StartOptions {
            queue: Some(Enqueue::new(queue)),
            ..StartOptions::default()
        },
        ..Debouncer::new(&echo)
    };
    let parent = dbos
        .register_workflow("parent", move |()| {
            let debouncer = debouncer.clone();
            async move {
                match debouncer.debounce("key", PERIOD, 2).await {
                    Ok(handle) => Ok::<_, Error>(format!("debounced {}", handle.workflow_id())),
                    Err(e @ Error::SystemDatabase(SysdbError::QueueDeduplicated { .. })) => {
                        Ok(format!("deduplicated: {e}"))
                    }
                    Err(e) => Err(e),
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    // The queue is not registered yet, so nothing dequeues the holder and the key stays held.
    let holder = echo
        .start_with(
            1,
            StartOptions {
                queue: Some(Enqueue {
                    deduplication_id: Some("echo-key"),
                    ..Enqueue::new(queue)
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("the holder was not enqueued");

    let parent_id = "deduplicated-debouncing-parent";
    let first = parent
        .run_with(
            (),
            RunOptions {
                workflow_id: Some(parent_id),
                ..RunOptions::default()
            },
        )
        .await
        .expect("the parent failed");
    assert!(first.starts_with("deduplicated: "), "{first}");
    assert!(first.contains(holder.workflow_id()), "{first}");
    let steps = reader
        .list_workflow_steps(parent_id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "only the bounce is recorded: {steps:?}");
    assert_eq!(steps[0].step_name, "DBOS.debounce_delayed_workflow");

    // Let the holder run, which frees the key.
    dbos.register_queue(
        queue,
        QueueOptions::default(),
        QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");
    assert_eq!(result(holder).await, 1);

    // A fork past the bounce copies its checkpoint: the same error, and no debounced workflow.
    let replayed = dbos
        .fork::<String, Error>(parent_id, 1)
        .await
        .expect("fork failed");
    assert_eq!(
        tokio::time::timeout(WAIT, replayed.result())
            .await
            .expect("the replay never finished")
            .expect("the replay failed"),
        first,
        "the replay took a different branch from the run it replayed"
    );
    assert_eq!(
        rows_named(&reader, "echo").await.len(),
        1,
        "the replay did not bounce again and enqueue"
    );

    dbos.shutdown().await;
}

/// A bounce in another encoding than the waiting row's replaces the input and its serialization
/// tag together, so the run decodes the last input in the encoding it was written in.
#[tokio::test]
async fn a_portable_bounce_over_a_native_row_replaces_input_and_tag_together() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-portable-app", &db));
    let echo = dbos
        .register_workflow("echo", |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let native = Debouncer::new(&echo);
    let portable = Debouncer {
        options: StartOptions {
            queue: Some(Enqueue {
                serialization: SerializationFormat::Portable,
                ..Enqueue::new(INTERNAL_QUEUE)
            }),
            ..StartOptions::default()
        },
        ..Debouncer::new(&echo)
    };

    let first = native.debounce("key", FOREVER, 1).await.unwrap();
    let before = row(&reader, first.workflow_id()).await;
    assert_ne!(before.serialization.as_deref(), Some(PORTABLE_JSON));

    // FOREVER again, so the row stays `DELAYED` while it is read.
    let second = portable.debounce("key", FOREVER, 2).await.unwrap();
    assert_eq!(second.workflow_id(), first.workflow_id(), "coalesced");
    let after = row(&reader, first.workflow_id()).await;
    assert_eq!(after.status, WorkflowStatus::Delayed);
    assert_eq!(after.serialization.as_deref(), Some(PORTABLE_JSON));
    assert_ne!(after.input, before.input, "the input was replaced");

    // A short period releases it; the run decodes the portable input.
    let third = portable
        .debounce("key", Duration::from_millis(500), 3)
        .await
        .unwrap();
    assert_eq!(third.workflow_id(), first.workflow_id());
    assert_eq!(result(third).await, 3);

    dbos.shutdown().await;
}

/// A workflow registered under a class and a configured instance debounces under its full
/// identity: calls for the same instance coalesce and its row carries both, while another
/// instance whose key spells the same deduplication id is refused rather than extending it.
#[tokio::test]
async fn a_configured_instance_workflow_debounces_under_its_full_identity() {
    let db = test_database().await;
    let dbos = DBOS::new(config("debounce-instance-app", &db));
    let key = |config: &str| WorkflowKey::instance("echo", "Billing", config);
    let eu = dbos
        .register_workflow_as(key("eu"), |x: u32| async move { Ok::<u32, Error>(x) })
        .unwrap();
    let us = dbos
        .register_workflow_as(
            key("us"),
            |x: u32| async move { Ok::<u32, Error>(x + 1000) },
        )
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let reader = reader(&db).await;

    let eu_debouncer = Debouncer::new(&eu);
    let first = eu_debouncer.debounce("key", FOREVER, 1).await.unwrap();
    let held = row(&reader, first.workflow_id()).await;
    assert_eq!(held.class_name.as_deref(), Some("Billing"));
    assert_eq!(held.config_name.as_deref(), Some("eu"));
    assert!(held.is_debounced);

    let error = Debouncer::new(&us)
        .debounce("key", PERIOD, 2)
        .await
        .unwrap_err();
    assert!(is_deduplicated(&error), "{error}");

    let second = eu_debouncer
        .debounce("key", Duration::from_millis(500), 3)
        .await
        .unwrap();
    assert_eq!(second.workflow_id(), first.workflow_id());
    assert_eq!(
        result(second).await,
        3,
        "the eu instance ran, with the last input"
    );

    dbos.shutdown().await;
}
