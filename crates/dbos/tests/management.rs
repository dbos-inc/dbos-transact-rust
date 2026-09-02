//! The management surface, against real databases.
//!
//! `resume` and `fork` put a workflow on a queue and leave it there, so their tests end by waiting
//! on a polling handle — which is the dequeue loop doing the actual work. `cancel`, `delete` and
//! the two listings are reads and writes against the row itself, and are checked against the
//! system database directly.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::WorkflowStatus;
use dbos::{
    Children, Config, DBOS, EngineOnly, Enqueue, Error, ForkFrom, ForkOptions, ResumeOptions,
    StartOptions,
};

use dbos_test_support::{TestDatabase, test_database};

/// The version an instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
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

/// **A resumed workflow is dequeued and run, which is what makes `resume` more than a status
/// change.**
///
/// The workflow starts on a queue nothing polls, so it sits `ENQUEUED` and untouched. Resuming
/// moves it to the internal queue, which this executor does poll — and it runs.
#[tokio::test]
async fn resuming_puts_a_workflow_on_a_queue_that_runs_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("resume-app", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("resumable", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(3)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "stalled";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("a-queue-with-no-row")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a queue with no row was polled"
    );

    let handle = dbos
        .resume::<u32, EngineOnly>(id)
        .await
        .expect("resume failed");
    assert_eq!(handle.workflow_id(), id, "resuming changed the id");
    assert_eq!(handle.result().await.expect("the workflow failed"), 3);
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    dbos.shutdown().await;
}

/// Resuming an id with no row says so, rather than silently doing nothing.
#[tokio::test]
async fn resuming_a_workflow_that_does_not_exist_is_an_error() {
    let db = test_database().await;
    let dbos = DBOS::new(config("resume-missing-app", &db));
    dbos.launch().await.expect("launch failed");

    let error = dbos
        .resume::<u32, EngineOnly>("never-existed")
        .await
        .expect_err("a missing workflow was resumed");
    assert!(
        matches!(&error, Error::SystemDatabase(inner) if format!("{inner}").contains("never-existed")),
        "expected a non-existent-workflow refusal, got {error:?}"
    );

    dbos.shutdown().await;
}

/// **A fork is a new workflow that replays what came before the fork point.**
///
/// The source fails on its first attempt and records no steps at all, so there is nothing below
/// the fork point to replay and the body runs from the top. The fork gets its own id, and the
/// source keeps the outcome it had. Where the fork point falls when there *are* steps is
/// [`forking_from_a_chosen_step_replays_the_steps_below_it`] and the two tests after it.
#[tokio::test]
async fn forking_from_the_beginning_runs_the_workflow_again_under_a_new_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-app", &db));
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("the first attempt fails")]
    struct FirstAttempt;

    let attempts = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("forkable", {
            let attempts = Arc::clone(&attempts);
            move |()| {
                let attempts = Arc::clone(&attempts);
                async move {
                    // Fails the first time, succeeds for the fork.
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(FirstAttempt)?;
                    }
                    Ok::<u32, dbos::Error<FirstAttempt>>(8)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "failed-once";
    let first = workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await;
    assert!(first.is_err(), "the first attempt was supposed to fail");

    let forked = dbos
        .fork::<u32, FirstAttempt>(id, ForkFrom::Beginning)
        .await
        .expect("fork failed");
    assert_ne!(forked.workflow_id(), id, "the fork reused the source's id");
    assert_eq!(forked.result().await.expect("the fork failed"), 8);

    // The source keeps its own outcome.
    assert_eq!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Error,
        "forking changed the source's outcome"
    );

    dbos.shutdown().await;
}

/// A fork can be given its own id, and put on a queue of the caller's choosing.
#[tokio::test]
async fn a_fork_takes_the_id_and_queue_it_is_given() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-options-app", &db));
    let workflow = dbos
        .register_workflow("placed", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "forks",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    let id = "the-source";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the source failed");

    let forked = dbos
        .fork_with::<u32, EngineOnly>(
            id,
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("the-fork"),
                queue: Some("forks"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(forked.workflow_id(), "the-fork");
    assert_eq!(forked.result().await.expect("the fork failed"), 1);

    assert_eq!(
        reader(&db)
            .await
            .get_workflow("the-fork")
            .await
            .expect("read failed")
            .expect("the row is missing")
            .queue_name
            .as_deref(),
        Some("forks"),
        "the fork was not enqueued where it was asked to be"
    );

    dbos.shutdown().await;
}

/// **A fork onto a partitioned queue needs its own partition key, and is not given one.**
///
/// A partitioned queue is swept one partition at a time and every read that finds those
/// partitions is keyed, so an unkeyed row is in none of them and no sweep can see it. The fork's
/// key is written from [`ForkOptions::queue_partition_key`] rather than inherited from the
/// source, which is what makes leaving it out a workflow that never runs rather than one that
/// lands where its source did.
///
/// Both halves are asserted from the same sweeps: the keyed fork runs to completion, and the
/// unkeyed one — enqueued first, onto the same queue — is still `ENQUEUED` afterwards. No sleep
/// decides that. A row the partitioned dequeue could see would have been claimed by one of the
/// polls that ran the keyed fork.
#[tokio::test]
async fn a_fork_onto_a_partitioned_queue_carries_the_key_it_is_given() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-partition-app", &db));
    let workflow = dbos
        .register_workflow("sharded", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    let queue = dbos
        .register_queue(
            "partitioned-forks",
            dbos::QueueOptions {
                partition_concurrency: Some(1),
                ..dbos::QueueOptions::default()
            },
            dbos::QueueConflict::UpdateIfLatestVersion,
        )
        .await
        .expect("registration failed");
    assert!(queue.is_partitioned());

    let id = "the-source";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the source failed");

    // Enqueued first, so it has had every sweep the keyed fork had.
    dbos.fork_with::<u32, EngineOnly>(
        id,
        ForkFrom::Beginning,
        ForkOptions {
            forked_id: Some("unkeyed-fork"),
            queue: Some("partitioned-forks"),
            ..ForkOptions::default()
        },
    )
    .await
    .expect("fork failed");

    let keyed = dbos
        .fork_with::<u32, EngineOnly>(
            id,
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("keyed-fork"),
                queue: Some("partitioned-forks"),
                queue_partition_key: Some("tenant-a"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), keyed.result())
            .await
            .expect("the keyed fork never ran")
            .expect("the fork failed"),
        7
    );

    let reader = reader(&db).await;
    let keyed_row = reader
        .get_workflow("keyed-fork")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        keyed_row.queue_partition_key.as_deref(),
        Some("tenant-a"),
        "the fork did not carry the partition key it was given"
    );

    let unkeyed_row = reader
        .get_workflow("unkeyed-fork")
        .await
        .expect("read failed")
        .expect("the row is missing");
    assert_eq!(
        unkeyed_row.queue_partition_key, None,
        "an unkeyed fork inherited a key from somewhere"
    );
    assert_eq!(
        unkeyed_row.status,
        WorkflowStatus::Enqueued,
        "a partitioned sweep claimed a row belonging to no partition"
    );

    dbos.shutdown().await;
}

/// **A fork from a chosen step replays everything below it and re-runs the rest.**
///
/// The steps a fork copies are those with `function_id < start_step`, so forking from step 1
/// carries step 0's recorded result across and leaves 1 and 2 to run again. This is the half of
/// [`ForkFrom`] that needs no lookup — the caller supplied the number, and `fork_batch` goes
/// straight to `fork_workflows` with it.
#[tokio::test]
async fn forking_from_a_chosen_step_replays_the_steps_below_it() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-step-app", &db));
    let ran: Arc<Mutex<Vec<String>>> = Arc::default();
    let workflow = dbos
        .register_workflow("staged", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    for name in ["one", "two", "three"] {
                        dbos::step(name, || async {
                            ran.lock().unwrap().push(name.to_owned());
                            Ok::<u32, Error>(0)
                        })
                        .await?;
                    }
                    Ok::<u32, Error>(0)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "the-source";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the source failed");
    assert_eq!(*ran.lock().unwrap(), ["one", "two", "three"]);
    ran.lock().unwrap().clear();

    let forked = dbos
        .fork::<u32, EngineOnly>(id, ForkFrom::Step(1))
        .await
        .expect("fork failed");
    let forked_id = forked.workflow_id().to_owned();
    forked.result().await.expect("the fork failed");
    assert_eq!(
        *ran.lock().unwrap(),
        ["two", "three"],
        "step 0 was supposed to replay from the checkpoint copied to the fork"
    );

    // The copy is the fork's own history, not a view of the source's: it lists three steps,
    // one of which it never ran.
    let steps = dbos
        .list_workflow_steps(&forked_id)
        .await
        .expect("listing failed");
    assert_eq!(
        steps
            .iter()
            .map(|s| s.step_name.as_str())
            .collect::<Vec<_>>(),
        ["one", "two", "three"],
    );

    dbos.shutdown().await;
}

/// **`LastFailure` resolves against each source's own history, not a step worked out once.**
///
/// Two sources of the same workflow, failing at different steps: one at its first, one at its
/// last. A single [`fork_all`](dbos::DBOS::fork_all) with [`ForkFrom::LastFailure`] forks each
/// from wherever *it* failed, so one fork re-runs everything and the other re-runs one step.
/// That is the claim the batch makes, and it cannot be checked by forking a single workflow.
#[tokio::test]
async fn forking_from_the_last_failure_uses_each_sources_own_history() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-failure-app", &db));
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("the first attempt fails")]
    struct FirstAttempt;

    let ran: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen: Arc<Mutex<HashSet<String>>> = Arc::default();
    let workflow = dbos
        .register_workflow("staged", {
            let ran = Arc::clone(&ran);
            let seen = Arc::clone(&seen);
            move |source: String| {
                let ran = Arc::clone(&ran);
                let seen = Arc::clone(&seen);
                async move {
                    // `a` fails at its first step and `b` at its last, and only on the run that
                    // first sees the id — so the forks get through.
                    let fails_at = if source == "a" { 0 } else { 2 };
                    let first_run = seen.lock().unwrap().insert(source.clone());
                    for (index, name) in ["one", "two", "three"].into_iter().enumerate() {
                        let source = &source;
                        let ran = &ran;
                        dbos::step(name, || async {
                            ran.lock().unwrap().push(format!("{source}:{name}"));
                            if first_run && index == fails_at {
                                Err(FirstAttempt)?;
                            }
                            Ok::<u32, dbos::Error<FirstAttempt>>(0)
                        })
                        .await?;
                    }
                    Ok::<u32, dbos::Error<FirstAttempt>>(0)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    for id in ["a", "b"] {
        let outcome = workflow
            .run_with(
                id.to_owned(),
                dbos::RunOptions {
                    workflow_id: Some(id),
                    ..dbos::RunOptions::default()
                },
            )
            .await;
        assert!(outcome.is_err(), "the source was supposed to fail");
    }
    ran.lock().unwrap().clear();

    let forks = dbos
        .fork_all::<u32, FirstAttempt>(&["a", "b"], ForkFrom::LastFailure, ForkOptions::default())
        .await
        .expect("bulk fork failed");
    assert_eq!(forks.len(), 2);
    for fork in forks {
        fork.result().await.expect("the fork failed");
    }

    // Cloned out rather than held: the two forks ran concurrently, so this is a snapshot to
    // split by source, and nothing below needs the lock.
    let ran = ran.lock().unwrap().clone();
    let for_source = |source: &str| -> Vec<String> {
        ran.iter()
            .filter(|entry| entry.starts_with(&format!("{source}:")))
            .cloned()
            .collect()
    };
    assert_eq!(
        for_source("a"),
        ["a:one", "a:two", "a:three"],
        "`a` failed at step 0, so its fork had nothing to replay"
    );
    assert_eq!(
        for_source("b"),
        ["b:three"],
        "`b` failed at step 2, so its fork should have replayed the two below it"
    );

    dbos.shutdown().await;
}

/// **`LastStep`, `StepNamed`, and the fallback that makes `LastFailure` useful.**
///
/// One source, which succeeded — so nothing recorded an error, and
/// [`ForkFrom::LastFailure`] has nothing to filter on. Its `COALESCE` falls back to the last
/// recorded step, which is what makes it work on a workflow killed mid-step: such a workflow
/// records no error at all, and reporting "no failure to fork from" would be useless.
///
/// So `LastStep` and `LastFailure` land on the same step here, and `StepNamed` finds `two`
/// wherever it happens to fall. The forks run one at a time because they share an id and an
/// input, and only the order they run in tells them apart.
#[tokio::test]
async fn forking_from_the_last_step_and_from_a_named_one() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-named-app", &db));
    let ran: Arc<Mutex<Vec<String>>> = Arc::default();
    let workflow = dbos
        .register_workflow("staged", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    for name in ["one", "two", "three"] {
                        dbos::step(name, || async {
                            ran.lock().unwrap().push(name.to_owned());
                            Ok::<u32, Error>(0)
                        })
                        .await?;
                    }
                    Ok::<u32, Error>(0)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "the-source";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the source failed");

    for (from, expected) in [
        (ForkFrom::LastStep, &["three"][..]),
        (ForkFrom::LastFailure, &["three"][..]),
        (ForkFrom::StepNamed("two"), &["two", "three"][..]),
    ] {
        ran.lock().unwrap().clear();
        let forked = dbos
            .fork::<u32, EngineOnly>(id, from)
            .await
            .expect("fork failed");
        forked.result().await.expect("the fork failed");
        assert_eq!(
            *ran.lock().unwrap(),
            expected,
            "{from:?} forked from the wrong step"
        );
    }

    // A name the source never recorded resolves to no step at all. That is its own refusal,
    // naming the step as well as the workflow — not the beginning, and not the missing-workflow
    // error an unresolvable id gets.
    let error = dbos
        .fork::<u32, EngineOnly>(id, ForkFrom::StepNamed("never-ran"))
        .await
        .expect_err("a fork from a step that does not exist was allowed");
    assert!(
        matches!(
            &error,
            Error::SystemDatabase(dbos::sysdb::Error::NoForkPoint { step_name, .. })
                if step_name.as_deref() == Some("never-ran")
        ),
        "expected a no-fork-point refusal naming the step, got {error:?}"
    );

    dbos.shutdown().await;
}

/// Nothing on this surface is available before launch — the reads no more than the writes, since
/// every one of them needs the system database the launch opens.
#[tokio::test]
async fn the_management_surface_needs_a_launched_instance() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-unlaunched-app", &db));

    macro_rules! refused {
        ($what:expr, $call:expr) => {
            assert!(
                matches!($call, Err(Error::NotLaunched { .. })),
                concat!("an unlaunched instance ", $what)
            );
        };
    }

    refused!(
        "resumed a workflow",
        dbos.resume::<u32, EngineOnly>("x").await
    );
    refused!(
        "resumed workflows",
        dbos.resume_all::<u32, EngineOnly>(&["x"], ResumeOptions::default())
            .await
    );
    refused!(
        "forked a workflow",
        dbos.fork::<u32, EngineOnly>("x", ForkFrom::Beginning).await
    );
    refused!(
        "forked workflows",
        dbos.fork_all::<u32, EngineOnly>(&["x"], ForkFrom::Beginning, ForkOptions::default())
            .await
    );
    // The launch check outranks the option check, so a call that is wrong in both ways still
    // reports the launch.
    refused!(
        "forked workflows with an id it should have refused",
        dbos.fork_all::<u32, EngineOnly>(
            &["x"],
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("one-id-for-many"),
                ..ForkOptions::default()
            },
        )
        .await
    );
    refused!("cancelled a workflow", dbos.cancel("x").await);
    refused!(
        "cancelled workflows",
        dbos.cancel_all(&["x"], Children::Skip).await
    );
    refused!("deleted a workflow", dbos.delete("x").await);
    refused!(
        "deleted workflows",
        dbos.delete_all(&["x"], Children::Include).await
    );
    refused!(
        "listed workflows",
        dbos.list_workflows(&dbos::sysdb::types::WorkflowFilter::default())
            .await
    );
    refused!(
        "listed a workflow's steps",
        dbos.list_workflow_steps("x").await
    );
}

/// **Cancelling is a pause an operator can undo.**
///
/// The row goes terminal, and awaiting it raises rather than returning a value — but the recorded
/// steps stay, so resuming picks the workflow up rather than starting it over. That is the whole
/// difference from [`DBOS::delete`].
#[tokio::test]
async fn cancelling_makes_a_workflow_terminal_and_leaves_it_resumable() {
    let db = test_database().await;
    let dbos = DBOS::new(config("cancel-app", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("cancellable", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(5)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // A queue nothing polls, so the row sits still and the test is not racing the runner.
    let id = "to-be-cancelled";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("no-runner-here")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    dbos.cancel(id).await.expect("cancel failed");
    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Cancelled,
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0, "a cancelled workflow ran");

    // And back again: resume moves it onto a queue this executor does poll.
    let handle = dbos
        .resume::<u32, EngineOnly>(id)
        .await
        .expect("resume failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 5);
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    dbos.shutdown().await;
}

/// Cancelling an id with no row is not an error: the end state is what was asked for.
///
/// The opposite of `resume`, which refuses one — a resume that finds nothing has done nothing,
/// while a cancel that finds nothing has the outcome it wanted.
#[tokio::test]
async fn cancelling_a_workflow_that_does_not_exist_is_not_an_error() {
    let db = test_database().await;
    let dbos = DBOS::new(config("cancel-missing-app", &db));
    dbos.launch().await.expect("launch failed");

    let cancelled = dbos
        .cancel_all(&["never-existed"], Children::Skip)
        .await
        .expect("cancelling a missing workflow failed");
    assert!(cancelled.is_empty(), "a workflow with no row was cancelled");

    dbos.shutdown().await;
}

/// `Children::Include` reaches workflows the caller never named.
///
/// The parent starts a child and both then wait, so both are `PENDING` when the cancel arrives.
/// The returned list is the proof: it carries the child's id, which the caller did not give.
#[tokio::test]
async fn cancelling_a_tree_reaches_the_children() {
    let db = test_database().await;
    let dbos = DBOS::new(config("cancel-tree-app", &db));
    let child_started = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Notify::new());

    let child = dbos
        .register_workflow("waiting-child", {
            let started = Arc::clone(&child_started);
            let gate = Arc::clone(&gate);
            move |()| {
                let started = Arc::clone(&started);
                let gate = Arc::clone(&gate);
                async move {
                    started.notify_one();
                    gate.notified().await;
                    Ok::<u32, Error>(1)
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("waiting-parent", {
            let gate = Arc::clone(&gate);
            move |()| {
                let child = child.clone();
                let gate = Arc::clone(&gate);
                async move {
                    child.start(()).await?;
                    gate.notified().await;
                    Ok::<u32, Error>(0)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "tree-root";
    let _running = parent
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    child_started.notified().await;

    let cancelled = dbos
        .cancel_all(&[id], Children::Include)
        .await
        .expect("cancel failed");
    // `{parent}-{step_id}`, and the launch is the parent's step zero.
    let child_id = format!("{id}-0");
    assert!(
        cancelled.iter().any(|c| c == id),
        "the parent was not cancelled: {cancelled:?}"
    );
    assert!(
        cancelled.contains(&child_id),
        "the child was not cancelled: {cancelled:?}"
    );

    let reader = reader(&db).await;
    for workflow_id in [id, child_id.as_str()] {
        assert_eq!(
            reader
                .get_workflow(workflow_id)
                .await
                .expect("read failed")
                .expect("the row is missing")
                .status,
            WorkflowStatus::Cancelled,
            "{workflow_id} was not cancelled",
        );
    }

    dbos.shutdown().await;
}

/// **Deleting takes the steps with the row**, which is what makes it not a status change.
#[tokio::test]
async fn deleting_a_workflow_removes_its_row_and_its_steps() {
    let db = test_database().await;
    let dbos = DBOS::new(config("delete-app", &db));
    let workflow = dbos
        .register_workflow("with-steps", |()| async move {
            let mut total = 0;
            for name in ["first", "second"] {
                total += dbos::step(name, || async { Ok::<u32, Error>(1) }).await?;
            }
            Ok::<u32, Error>(total)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "doomed";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");
    assert_eq!(
        dbos.list_workflow_steps(id)
            .await
            .expect("listing failed")
            .len(),
        2,
    );

    let deleted = dbos
        .delete_all(&[id], Children::Skip)
        .await
        .expect("delete failed");
    assert_eq!(deleted, 1, "one row was named and one row should have gone");
    assert!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .is_none(),
        "the row survived its deletion"
    );
    assert!(
        dbos.list_workflow_steps(id)
            .await
            .expect("listing failed")
            .is_empty(),
        "the steps outlived the workflow they belong to"
    );

    dbos.shutdown().await;
}

/// A step listing is in execution order, and names each step.
#[tokio::test]
async fn listing_a_workflows_steps_reports_them_in_execution_order() {
    let db = test_database().await;
    let dbos = DBOS::new(config("steps-listing-app", &db));
    let workflow = dbos
        .register_workflow("three-steps", |()| async move {
            for name in ["one", "two", "three"] {
                dbos::step(name, || async { Ok::<u32, Error>(0) }).await?;
            }
            Ok::<u32, Error>(0)
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "stepped";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");

    let steps = dbos.list_workflow_steps(id).await.expect("listing failed");
    assert_eq!(
        steps
            .iter()
            .map(|s| s.step_name.as_str())
            .collect::<Vec<_>>(),
        ["one", "two", "three"],
    );
    assert_eq!(
        steps.iter().map(|s| s.step_id).collect::<Vec<_>>(),
        [0, 1, 2],
        "steps are numbered from zero, in order",
    );

    // An id with no workflow behind it has no steps, rather than failing.
    assert!(
        dbos.list_workflow_steps("never-existed")
            .await
            .expect("listing failed")
            .is_empty()
    );

    dbos.shutdown().await;
}

/// The filter is a `WHERE` clause, and every field narrows.
#[tokio::test]
async fn listing_workflows_narrows_by_the_filter() {
    use dbos::sysdb::types::WorkflowFilter;

    let db = test_database().await;
    let dbos = DBOS::new(config("listing-app", &db));
    let ok = dbos
        .register_workflow("succeeds", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    for id in ["listed-one", "listed-two"] {
        ok.run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");
    }

    let all = dbos
        .list_workflows(&WorkflowFilter::default())
        .await
        .expect("listing failed");
    assert_eq!(all.len(), 2, "the unfiltered listing missed a workflow");

    let by_name = dbos
        .list_workflows(&WorkflowFilter {
            names: vec!["succeeds"],
            status: vec![WorkflowStatus::Success],
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert_eq!(by_name.len(), 2);

    let by_id = dbos
        .list_workflows(&WorkflowFilter {
            workflow_ids: vec!["listed-two"],
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert_eq!(
        by_id
            .iter()
            .map(|w| w.workflow_id.as_str())
            .collect::<Vec<_>>(),
        ["listed-two"],
    );

    let none = dbos
        .list_workflows(&WorkflowFilter {
            names: vec!["never-registered"],
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert!(
        none.is_empty(),
        "a filter that matches nothing returned something"
    );

    dbos.shutdown().await;
}

/// The bulk forms take many ids and hand back one handle each, in the order given.
#[tokio::test]
async fn resuming_and_forking_in_bulk_hand_back_a_handle_each() {
    let db = test_database().await;
    let dbos = DBOS::new(config("bulk-app", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("bulk", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(9)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let ids = ["bulk-one", "bulk-two"];
    for id in ids {
        workflow
            .start_with(
                (),
                StartOptions {
                    workflow_id: Some(id),
                    queue: Some(Enqueue::new("still-nothing-polling")),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("enqueue failed");
    }

    let handles = dbos
        .resume_all::<u32, EngineOnly>(&ids, ResumeOptions::default())
        .await
        .expect("bulk resume failed");
    assert_eq!(
        handles.iter().map(|h| h.workflow_id()).collect::<Vec<_>>(),
        ids,
        "the handles came back in another order",
    );
    for handle in handles {
        assert_eq!(handle.result().await.expect("the workflow failed"), 9);
    }
    assert_eq!(ran.load(Ordering::SeqCst), 2);

    // From the top rather than from either source's history: neither recorded a step, and the
    // count below is what says the bodies ran again.
    let forks = dbos
        .fork_all::<u32, EngineOnly>(&ids, ForkFrom::Beginning, ForkOptions::default())
        .await
        .expect("bulk fork failed");
    assert_eq!(forks.len(), 2);
    for fork in forks {
        assert!(
            !ids.contains(&fork.workflow_id()),
            "a fork reused its source's id"
        );
        assert_eq!(fork.result().await.expect("the fork failed"), 9);
    }
    assert_eq!(ran.load(Ordering::SeqCst), 4, "both forks ran");

    dbos.shutdown().await;
}

/// One id cannot name many forks, so asking for one in the bulk form is refused rather than
/// quietly ignored.
#[tokio::test]
async fn forking_in_bulk_refuses_a_chosen_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("bulk-fork-id-app", &db));
    dbos.launch().await.expect("launch failed");

    let error = dbos
        .fork_all::<u32, EngineOnly>(
            &["a", "b"],
            ForkFrom::Beginning,
            ForkOptions {
                forked_id: Some("the-only-one"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect_err("a chosen id was accepted for a batch");
    assert!(
        matches!(&error, Error::Config(message) if message.contains("forked_id")),
        "expected a configuration refusal, got {error:?}"
    );

    dbos.shutdown().await;
}

/// **A resume can name the queue it goes back on**, which is how a backlog is resumed without
/// flooding the fleet — the internal queue takes no limits.
///
/// All four references offer this and Rust was the only one that did not. The proof is the row's
/// own `queue_name` after the resume, not just that the workflow ran.
#[tokio::test]
async fn resuming_onto_a_named_queue_puts_the_workflow_there() {
    let db = test_database().await;
    let dbos = DBOS::new(config("resume-queue-app", &db));
    let workflow = dbos
        .register_workflow("re-queued", |()| async move { Ok::<u32, Error>(4) })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "recovery",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    let id = "parked";
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue::new("a-queue-with-no-runner")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    dbos.cancel(id).await.expect("cancel failed");

    let handle = dbos
        .resume_with::<u32, EngineOnly>(
            id,
            ResumeOptions {
                queue: Some("recovery"),
            },
        )
        .await
        .expect("resume failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 4);

    assert_eq!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .queue_name
            .as_deref(),
        Some("recovery"),
        "the resume ignored the queue it was given",
    );

    dbos.shutdown().await;
}

/// **A handle for a workflow this process did not start**, which is what makes a listing useful.
#[tokio::test]
async fn a_workflow_can_be_retrieved_by_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("retrieve-app", &db));
    let workflow = dbos
        .register_workflow("retrievable", |()| async move { Ok::<u32, Error>(6) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "already-run";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");

    // Nothing is read until the handle is used, so this is the id and nothing else.
    let handle = dbos
        .retrieve_workflow::<u32, EngineOnly>(id)
        .expect("retrieve failed");
    assert_eq!(handle.workflow_id(), id);
    assert_eq!(
        handle.status().await.expect("status failed"),
        WorkflowStatus::Success
    );
    assert_eq!(handle.result().await.expect("the workflow failed"), 6);

    // An id with no row is handed back too, and the two halves of that handle answer differently:
    // a status read reports the absence, because a single read has nothing to wait for.
    let missing = dbos
        .retrieve_workflow::<u32, EngineOnly>("never-existed")
        .expect("retrieve failed");
    assert!(
        matches!(
            missing.status().await,
            Err(Error::WorkflowNotFound { workflow_id }) if workflow_id == "never-existed"
        ),
        "reading the status of a workflow with no row should report it"
    );

    // Awaiting waits instead: nothing here has seen this row, so an absence is "not enqueued yet".
    // Nothing in this test ever creates it, so the wait is still running when the timeout takes it.
    let missing = dbos
        .retrieve_workflow::<u32, EngineOnly>("never-existed")
        .expect("retrieve failed");
    assert!(
        tokio::time::timeout(Duration::from_millis(750), missing.result())
            .await
            .is_err(),
        "awaiting an id with no row should wait for it to appear, not report it"
    );

    dbos.shutdown().await;
}

/// A delay can be moved while the workflow is still `DELAYED`, and the move is what decides when
/// the supervisor releases it.
#[tokio::test]
async fn a_delayed_workflow_can_be_released_sooner() {
    let db = test_database().await;
    let dbos = DBOS::new(config("delay-app", &db));
    let ran = Arc::new(AtomicU32::new(0));
    let workflow = dbos
        .register_workflow("delayable", {
            let ran = Arc::clone(&ran);
            move |()| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, Error>(2)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "delayed-work",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    // Far enough out that the test is not racing the supervisor.
    let id = "held-back";
    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                queue: Some(Enqueue {
                    delay: Some(std::time::Duration::from_secs(3600)),
                    ..Enqueue::new("delayed-work")
                }),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Delayed,
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0);

    // Bring it forward to now, and the supervisor releases it on its next pass.
    dbos.set_workflow_delay(id, dbos::WorkflowDelay::For(std::time::Duration::ZERO))
        .await
        .expect("delay failed");
    assert_eq!(handle.result().await.expect("the workflow failed"), 2);
    assert_eq!(ran.load(Ordering::SeqCst), 1);

    dbos.shutdown().await;
}

/// Attributes are replaced rather than merged, cleared by `None`, and searchable by containment.
#[tokio::test]
async fn attributes_are_replaced_and_can_be_searched() {
    use dbos::sysdb::types::WorkflowFilter;

    let db = test_database().await;
    let dbos = DBOS::new(config("attributes-app", &db));
    let workflow = dbos
        .register_workflow("tagged", |()| async move { Ok::<u32, Error>(0) })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "carries-tags";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect("the workflow failed");

    let tags = serde_json::json!({ "tenant": "acme", "tier": "gold" });
    dbos.update_workflow_attributes(id, tags.as_object())
        .await
        .expect("update failed");

    // Containment, not equality: one key out of two matches.
    let found = dbos
        .list_workflows(&WorkflowFilter {
            attributes: Some(r#"{"tenant":"acme"}"#),
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert_eq!(
        found
            .iter()
            .map(|w| w.workflow_id.as_str())
            .collect::<Vec<_>>(),
        [id],
    );

    // A replacement, not a merge: the key that is not sent again is gone.
    let fewer = serde_json::json!({ "tenant": "acme" });
    dbos.update_workflow_attributes(id, fewer.as_object())
        .await
        .expect("update failed");
    let after_replacement = dbos
        .list_workflows(&WorkflowFilter {
            attributes: Some(r#"{"tier":"gold"}"#),
            ..WorkflowFilter::default()
        })
        .await
        .expect("listing failed");
    assert!(
        after_replacement.is_empty(),
        "the replaced attributes were merged instead"
    );

    // And `None` clears them.
    dbos.update_workflow_attributes(id, None)
        .await
        .expect("clear failed");
    assert!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .attributes
            .is_none(),
        "clearing left the attributes behind"
    );

    dbos.shutdown().await;
}
