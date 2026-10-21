//! The management surface, against real databases.
//!
//! `resume` and `fork` put a workflow on a queue and leave it there, so their tests end by waiting
//! on a polling handle — which is the dequeue loop doing the actual work. `cancel`, `delete` and
//! the two listings are reads and writes against the row itself, and are checked against the
//! system database directly.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowFilter, WorkflowStatus};
use dbos::{
    Children, Client, ClientConfig, Config, DBOS, EngineOnly, Enqueue, Error, ForkOptions,
    ListWorkflowStepsOptions, ResumeOptions, StartOptions,
};

use dbos_test_support::{TestDatabase, test_database};

/// The version an instance in this file launches with: DBOS computes none, so a launch without
/// one fails.
///
/// Derived from the application name, as `events.rs` and `children.rs` do, because a version row
/// is claimed by the first application to register it — so two instances sharing a database and a
/// version, which is what the two-instance tests below set up, would have the second refused with
/// `RegisteredByAnother` before it could get to what it is testing.
fn app_version(app_name: &str) -> String {
    format!("{app_name}-1.0.0")
}

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(app_version(app_name)),
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
/// [`forking_from_a_chosen_step_replays_the_steps_below_it`].
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
        .fork::<u32, FirstAttempt>(id, 0)
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
            0,
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
        0,
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
            0,
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
/// carries step 0's recorded result across and leaves 1 and 2 to run again.
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
        .fork::<u32, EngineOnly>(id, 1)
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

/// **A fork can adopt a replacement for a child that failed.**
///
/// The parent runs a child, which fails, failing the parent. The child is re-run as a fork of
/// itself, which succeeds. The parent is then forked from the step that awaits the child, mapping
/// the failed child to its replacement: the copied start step names the replacement, so the fork
/// waits on it and finishes with its result. Neither the parent's earlier step nor either child
/// runs again.
///
/// Without the map, or with a map naming a child the parent never started, the same fork waits
/// on the original child and fails with it. The client's fork takes the map too, and `fork_all`
/// applies it to every fork in the batch. A map naming a child twice, or holding an empty id, is
/// refused.
#[tokio::test]
async fn a_fork_adopts_the_replacement_for_a_failed_child() {
    let db = test_database().await;
    let dbos = DBOS::new(config("fork-replace-child-app", &db));
    let broken = Arc::new(AtomicBool::new(true));
    let (child_runs, before_runs) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let child = dbos
        .register_workflow("child", {
            let (broken, child_runs) = (Arc::clone(&broken), Arc::clone(&child_runs));
            move |()| {
                let (broken, child_runs) = (Arc::clone(&broken), Arc::clone(&child_runs));
                async move {
                    child_runs.fetch_add(1, Ordering::SeqCst);
                    if broken.load(Ordering::SeqCst) {
                        return Err(Error::Portable {
                            name: "Broken".to_owned(),
                            message: "the child is broken".to_owned(),
                            code: None,
                            data: None,
                        });
                    }
                    Ok::<u32, Error>(42)
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let before_runs = Arc::clone(&before_runs);
            move |()| {
                let (child, before_runs) = (child.clone(), Arc::clone(&before_runs));
                async move {
                    dbos::step("before", || async {
                        before_runs.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), Error>(())
                    })
                    .await?;
                    // Step 1 starts the child, step 2 awaits it.
                    let value = child.run(()).await?;
                    Ok::<u32, Error>(value + 1)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "replace-parent";
    let original_child = "replace-parent-1";
    parent
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect_err("the child fails the parent");
    assert_eq!(child_runs.load(Ordering::SeqCst), 1);
    // A second parent, whose child fails the same way, for `fork_all` below.
    let other_id = "replace-other-parent";
    let other_child = "replace-other-parent-1";
    parent
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(other_id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect_err("the child fails the parent");
    assert_eq!(child_runs.load(Ordering::SeqCst), 2);

    broken.store(false, Ordering::SeqCst);
    let replacement = dbos
        .fork_with::<u32, EngineOnly>(
            original_child,
            0,
            ForkOptions {
                forked_id: Some("replacement-child"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(
        replacement.result().await.expect("the replacement failed"),
        42
    );
    assert_eq!(child_runs.load(Ordering::SeqCst), 3);

    let map = [(original_child, "replacement-child")];
    let adopted = dbos
        .fork_with::<u32, EngineOnly>(
            id,
            2,
            ForkOptions {
                replacement_children: &map,
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(adopted.result().await.expect("the fork failed"), 43);

    let client = Client::connect(ClientConfig {
        app_name: Some("fork-replace-child-app".to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");
    let by_client = client
        .fork_with::<u32, EngineOnly>(
            id,
            2,
            ForkOptions {
                replacement_children: &map,
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork failed");
    assert_eq!(by_client.result().await.expect("the fork failed"), 43);

    // One map for the whole batch: each parent adopts the replacement for its own child.
    let both = [
        (original_child, "replacement-child"),
        (other_child, "replacement-child"),
    ];
    let batch = dbos
        .fork_all::<u32, EngineOnly>(
            &[id, other_id],
            2,
            ForkOptions {
                replacement_children: &both,
                ..ForkOptions::default()
            },
        )
        .await
        .expect("fork_all failed");
    assert_eq!(batch.len(), 2);
    for fork in batch {
        assert_eq!(fork.result().await.expect("a fork in the batch failed"), 43);
    }

    assert_eq!(before_runs.load(Ordering::SeqCst), 2, "step 0 ran again");
    assert_eq!(child_runs.load(Ordering::SeqCst), 3, "a child ran again");

    // A child named twice, or an empty id on either side, is refused before anything is forked.
    let twice = [
        (original_child, "replacement-child"),
        (original_child, "replace-other-parent-1"),
    ];
    let no_original = [("", "replacement-child")];
    let no_replacement = [(original_child, "")];
    for replacement_children in [&twice[..], &no_original[..], &no_replacement[..]] {
        let options = || ForkOptions {
            forked_id: Some("refused-fork"),
            replacement_children,
            ..ForkOptions::default()
        };
        let refused = |result: dbos::Result<_>| {
            matches!(
                result,
                Err(Error::SystemDatabase(dbos::sysdb::Error::InvalidInput { ref field, .. }))
                    if field == "replacement_children"
            )
        };
        assert!(
            refused(
                dbos.fork_with::<u32, EngineOnly>(id, 2, options())
                    .await
                    .map(drop)
            ),
            "{replacement_children:?}"
        );
        assert!(
            refused(
                client
                    .fork_with::<u32, EngineOnly>(id, 2, options())
                    .await
                    .map(drop)
            ),
            "{replacement_children:?}"
        );
    }
    assert!(
        client
            .workflow_status("refused-fork")
            .await
            .expect("status read failed")
            .is_none(),
        "a refused fork was written"
    );

    // Without the map, and with a map naming a child that was never started, the fork waits on
    // the original child and fails with it.
    let unrelated = [("never-started", "replacement-child")];
    for replacement_children in [&[][..], &unrelated[..]] {
        let plain = dbos
            .fork_with::<u32, EngineOnly>(
                id,
                2,
                ForkOptions {
                    replacement_children,
                    ..ForkOptions::default()
                },
            )
            .await
            .expect("fork failed");
        let error = plain
            .result()
            .await
            .expect_err("the original child's failure");
        assert!(error.to_string().contains("broken"), "{error}");
    }
    assert_eq!(child_runs.load(Ordering::SeqCst), 3, "a child ran again");

    client.close().await;
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
        dbos.fork::<u32, EngineOnly>("x", 0).await
    );
    refused!(
        "forked workflows",
        dbos.fork_all::<u32, EngineOnly>(&["x"], 0, ForkOptions::default())
            .await
    );
    // The launch check outranks the option check, so a call that is wrong in both ways still
    // reports the launch.
    refused!(
        "forked workflows with an id it should have refused",
        dbos.fork_all::<u32, EngineOnly>(
            &["x"],
            0,
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

/// A step listing with `load_output` off reads each step's metadata and leaves its output and
/// error out, on both the instance and a client. Left on, the default, both come back.
#[tokio::test]
async fn a_step_listing_can_leave_the_outputs_out() {
    const BIG: usize = 1 << 20;

    let db = test_database().await;
    let dbos = DBOS::new(config("steps-no-output-app", &db));
    let workflow = dbos
        .register_workflow("big-then-fails", |()| async move {
            dbos::step("big", || async { Ok::<String, Error>("x".repeat(BIG)) }).await?;
            dbos::step("fails", || async {
                Err::<(), Error>(Error::Portable {
                    name: "Declined".to_owned(),
                    message: "declined".to_owned(),
                    code: None,
                    data: None,
                })
            })
            .await?;
            Ok::<(), Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "big-then-fails";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..dbos::RunOptions::default()
            },
        )
        .await
        .expect_err("the second step fails the workflow");

    let client = Client::connect(ClientConfig {
        app_name: Some("steps-no-output-app".to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");
    let without = ListWorkflowStepsOptions {
        load_output: false,
        ..ListWorkflowStepsOptions::default()
    };

    for steps in [
        dbos.list_workflow_steps_with(id, without)
            .await
            .expect("listing failed"),
        client
            .list_workflow_steps_with(id, without)
            .await
            .expect("listing failed"),
    ] {
        assert_eq!(
            steps
                .iter()
                .map(|s| (s.step_id, s.step_name.as_str()))
                .collect::<Vec<_>>(),
            [(0, "big"), (1, "fails")],
        );
        for step in &steps {
            assert_eq!(step.output, None, "{} was read", step.step_name);
            assert_eq!(step.error, None, "{} was read", step.step_name);
            assert!(step.started_at.is_some() && step.completed_at.is_some());
            assert!(step.serialization.is_some());
        }
    }

    for steps in [
        dbos.list_workflow_steps(id).await.expect("listing failed"),
        client
            .list_workflow_steps(id)
            .await
            .expect("listing failed"),
    ] {
        assert!(
            steps[0].output.as_ref().is_some_and(|o| o.len() > BIG),
            "the big step's output is read by default"
        );
        assert!(
            steps[1].error.is_some(),
            "the failed step's error is read by default"
        );
    }

    client.close().await;
    dbos.shutdown().await;
}

/// A step listing pages with `limit` and `offset`, in step order, on both the instance and a
/// client.
#[tokio::test]
async fn a_step_listing_reads_one_page_at_a_time() {
    let db = test_database().await;
    let dbos = DBOS::new(config("steps-paging-app", &db));
    let workflow = dbos
        .register_workflow("five-steps", |()| async move {
            for name in ["a", "b", "c", "d", "e"] {
                dbos::step(name, || async { Ok::<u32, Error>(0) }).await?;
            }
            Ok::<(), Error>(())
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "paged";
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
    let client = Client::connect(ClientConfig {
        app_name: Some("steps-paging-app".to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");

    let page = |limit, offset| ListWorkflowStepsOptions {
        limit,
        offset,
        ..ListWorkflowStepsOptions::default()
    };
    let names = |steps: Vec<dbos::sysdb::types::StepRecord>| {
        steps.into_iter().map(|s| s.step_name).collect::<Vec<_>>()
    };
    for (limit, offset, expected) in [
        (Some(2), None, vec!["a", "b"]),
        (Some(2), Some(2), vec!["c", "d"]),
        (None, Some(3), vec!["d", "e"]),
        (Some(10), Some(4), vec!["e"]),
        (None, Some(5), vec![]),
    ] {
        let by_instance = dbos
            .list_workflow_steps_with(id, page(limit, offset))
            .await
            .expect("listing failed");
        let by_client = client
            .list_workflow_steps_with(id, page(limit, offset))
            .await
            .expect("listing failed");
        assert_eq!(
            names(by_instance),
            expected,
            "limit {limit:?}, offset {offset:?}"
        );
        assert_eq!(
            names(by_client),
            expected,
            "limit {limit:?}, offset {offset:?}"
        );
    }

    client.close().await;
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
        .fork_all::<u32, EngineOnly>(&ids, 0, ForkOptions::default())
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
/// quietly ignored — whatever the step, since the objection is the batch.
#[tokio::test]
async fn forking_in_bulk_refuses_a_chosen_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("bulk-fork-id-app", &db));
    dbos.launch().await.expect("launch failed");

    let error = dbos
        .fork_all::<u32, EngineOnly>(
            &["a", "b"],
            0,
            ForkOptions {
                forked_id: Some("the-only-one"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect_err("a chosen id was accepted for a batch");
    assert!(
        matches!(&error, Error::InvalidArgument { detail, .. } if detail.contains("forked_id")),
        "expected a configuration refusal, got {error:?}"
    );

    // Refused before the ids are looked at: neither `a` nor `b` has a row.
    assert!(
        reader(&db)
            .await
            .get_workflow("the-only-one")
            .await
            .expect("read failed")
            .is_none(),
        "a fork was written under the refused id",
    );

    dbos.shutdown().await;
}

/// A fork from a negative step is refused before anything is written, on the single and the bulk
/// form alike.
#[tokio::test]
async fn forking_from_a_negative_step_is_refused() {
    let db = test_database().await;
    let dbos = DBOS::new(config("negative-fork-step-app", &db));
    let workflow = dbos
        .register_workflow("forkable", |()| async move {
            dbos::step("only", || async { Ok::<u32, Error>(1) }).await
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "has-a-step";
    workflow
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect("the workflow failed");

    let error = dbos
        .fork_with::<u32, EngineOnly>(
            id,
            -1,
            ForkOptions {
                forked_id: Some("chosen"),
                ..ForkOptions::default()
            },
        )
        .await
        .expect_err("a negative step was accepted");
    assert!(
        matches!(&error, Error::InvalidArgument { detail, .. } if detail.contains("start_step")),
        "{error:?}"
    );
    let error = dbos
        .fork_all::<u32, EngineOnly>(&[id], -1, ForkOptions::default())
        .await
        .expect_err("a negative step was accepted in bulk");
    assert!(
        matches!(&error, Error::InvalidArgument { detail, .. } if detail.contains("start_step")),
        "{error:?}"
    );

    let reader = reader(&db).await;
    assert!(
        reader
            .get_workflow("chosen")
            .await
            .expect("read failed")
            .is_none(),
        "a fork was written under the refused call's id",
    );
    // The bulk call generates its ids, so the source is what shows whether anything was forked.
    assert!(
        !reader
            .get_workflow(id)
            .await
            .expect("read failed")
            .expect("the source is gone")
            .was_forked_from,
        "a refused call forked the source",
    );

    dbos.shutdown().await;
}

/// **A resume can name the queue it goes back on**, which is how a backlog is resumed without
/// flooding the fleet — the internal queue takes no limits.
///
/// All four references offer it too. The proof is the row's own `queue_name` after the resume,
/// not just that the workflow ran.
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

/// **A management call made from inside a workflow is a step of that workflow.**
///
/// Each call is checkpointed under its `DBOS.*` name, so a replay reads back what the first run
/// did instead of acting again.
#[tokio::test]
async fn management_calls_from_inside_a_workflow_are_recorded_as_steps() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-step-app", &db));
    let target = dbos
        .register_workflow("target", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let operator = dbos
        .register_workflow("operator", {
            let dbos = dbos.clone();
            move |id: String| {
                let dbos = dbos.clone();
                async move {
                    dbos.cancel(&id).await?;
                    let rows = dbos
                        .list_workflows(&WorkflowFilter {
                            workflow_ids: vec![&id],
                            ..WorkflowFilter::default()
                        })
                        .await?;
                    Ok::<usize, Error>(rows.len())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // The target sits on a queue nothing polls, so it is there to be cancelled.
    let target_id = "cancelled-from-inside";
    target
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(target_id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let operator_id = "the-operator";
    let seen = operator
        .run_with(
            target_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(operator_id),
                ..Default::default()
            },
        )
        .await
        .expect("the operator workflow failed");
    assert_eq!(
        seen, 1,
        "the listing did not see the workflow it filtered on"
    );

    let reader = reader(&db).await;
    assert_eq!(
        reader
            .get_workflow(target_id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Cancelled,
    );

    let steps = reader
        .list_workflow_steps(operator_id, true, None, None, None)
        .await
        .expect("read failed");
    let recorded: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        recorded,
        [(0, "DBOS.cancel_workflow"), (1, "DBOS.list_workflows")],
        "the management calls were not checkpointed under the library's names",
    );

    dbos.shutdown().await;
}

/// **A management call's checkpoint is in the configured serializer, and replays by its tag.**
///
/// A listing is the system database's own value, but only this SDK replays it, so it is written
/// the way a step's output is: `rust_serde` by default, under that tag. A checkpoint tagged
/// `portable_json`, as listings were before, still replays, because the built-in encodings are
/// always readable.
///
/// The fork is what replays it, and the status in the result is what tells a replay from a re-run:
/// the first run listed itself while `PENDING`, and a re-run would find it `SUCCESS`.
#[tokio::test]
async fn a_management_checkpoint_is_in_the_configured_serializer_and_replays_by_its_tag() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-codec-app", &db));
    let lister = dbos
        .register_workflow("lister", {
            let dbos = dbos.clone();
            move |id: String| {
                let dbos = dbos.clone();
                async move {
                    let rows = dbos
                        .list_workflows(&WorkflowFilter {
                            workflow_ids: vec![&id],
                            ..WorkflowFilter::default()
                        })
                        .await?;
                    Ok::<Vec<WorkflowStatus>, Error>(rows.into_iter().map(|r| r.status).collect())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "the-lister";
    let seen = lister
        .run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect("the lister failed");
    assert_eq!(seen, [WorkflowStatus::Pending]);

    let steps = reader(&db)
        .await
        .list_workflow_steps(id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(steps.len(), 1, "{steps:?}");
    assert_eq!(steps[0].step_name, "DBOS.list_workflows");
    assert_eq!(
        steps[0].serialization.as_deref(),
        Some("rust_serde"),
        "the listing was not written in the configured serializer"
    );

    // As a listing was tagged before it went through the configured serializer.
    sqlx::query(
        r#"UPDATE "dbos"."operation_outputs" SET serialization = 'portable_json'
           WHERE workflow_uuid = $1 AND function_id = 0"#,
    )
    .bind(id)
    .execute(&db.pool().await)
    .await
    .expect("retag failed");

    let forked = dbos
        .fork::<Vec<WorkflowStatus>, EngineOnly>(id, 1)
        .await
        .expect("fork failed");
    assert_eq!(
        forked.result().await.expect("the fork failed"),
        [WorkflowStatus::Pending],
        "the fork re-ran the listing instead of replaying the portable_json checkpoint"
    );

    // A tag this process cannot read fails the replay as it would on any step's checkpoint.
    sqlx::query(
        r#"UPDATE "dbos"."operation_outputs" SET serialization = 'pickle'
           WHERE workflow_uuid = $1 AND function_id = 0"#,
    )
    .bind(id)
    .execute(&db.pool().await)
    .await
    .expect("retag failed");
    let unreadable = dbos
        .fork::<Vec<WorkflowStatus>, EngineOnly>(id, 1)
        .await
        .expect("fork failed");
    match tokio::time::timeout(Duration::from_secs(60), unreadable.result()).await {
        Ok(Err(Error::UnknownSerialization { name })) => assert_eq!(name, "pickle"),
        other => panic!("expected UnknownSerialization, got {other:?}"),
    }

    dbos.shutdown().await;
}

/// **A replayed fork hands back the id it recorded rather than forking again.**
///
/// The case that makes this more than bookkeeping: a fork generates a new workflow id, so a
/// replay without a checkpoint would write a second fork under a second id — work run twice, and
/// an id the first execution never saw.
///
/// Replay has to come from recovery. Running the operator a second time under its own id is not
/// one: the row is `SUCCESS` by then, the submission does not claim it, and the second call joins
/// the finished row and hands back its output without ever entering the function — which passes
/// whether or not the fork is checkpointed. So the operator is killed mid-flight instead, after
/// the fork is recorded, and the next launch is what replays it.
#[tokio::test]
async fn a_replayed_fork_returns_the_id_it_recorded_and_does_not_fork_again() {
    let db = test_database().await;
    let reader = reader(&db).await;
    let source_id = "the-source";
    let operator_id = "the-forker";

    let forks_of_source = || async {
        let mut ids: Vec<String> = reader
            .list_workflows(&WorkflowFilter::default(), None)
            .await
            .expect("read failed")
            .into_iter()
            .filter(|row| row.forked_from.as_deref() == Some(source_id))
            .map(|row| row.workflow_id)
            .collect();
        ids.sort();
        ids
    };

    // First process: the operator forks, its fork is checkpointed, and it is killed before it can
    // finish — so the row is left mid-flight for recovery to pick up.
    {
        let dbos = DBOS::new(config("management-replay-app", &db));
        let target = dbos
            .register_workflow("forked_target", |()| async move { Ok::<u32, Error>(7) })
            .unwrap();
        let operator = dbos
            .register_workflow("forking_operator", {
                let dbos = dbos.clone();
                move |source: String| {
                    let dbos = dbos.clone();
                    async move {
                        let fork = dbos.fork::<u32, EngineOnly>(&source, 0).await?;
                        let forked_id = fork.workflow_id().to_owned();
                        // Long enough that shutdown lands after the fork is recorded.
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Ok::<String, Error>(forked_id)
                    }
                }
            })
            .unwrap();
        dbos.launch().await.expect("launch failed");

        target
            .run_with(
                (),
                dbos::RunOptions {
                    workflow_id: Some(source_id),
                    ..Default::default()
                },
            )
            .await
            .expect("the source failed");
        operator
            .start_with(
                source_id.to_owned(),
                StartOptions {
                    workflow_id: Some(operator_id),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("start failed");

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let steps = reader
                .list_workflow_steps(operator_id, false, None, None, None)
                .await
                .expect("read failed");
            if !steps.is_empty() {
                assert_eq!(steps[0].step_name, "DBOS.fork_workflow");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fork was never recorded"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        dbos.shutdown().await;
    }

    let first = forks_of_source().await;
    assert_eq!(first.len(), 1, "the first execution forked once: {first:?}");

    // Second process: recovery replays the operator, which must take the id off its checkpoint
    // rather than forking the source a second time.
    let dbos = DBOS::new(config("management-replay-app", &db));
    let target = dbos
        .register_workflow("forked_target", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    let operator = dbos
        .register_workflow("forking_operator", {
            let dbos = dbos.clone();
            move |source: String| {
                let dbos = dbos.clone();
                async move {
                    let fork = dbos.fork::<u32, EngineOnly>(&source, 0).await?;
                    Ok::<String, Error>(fork.workflow_id().to_owned())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    drop((target, operator));

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let output = loop {
        let row = reader
            .get_workflow(operator_id)
            .await
            .expect("read failed")
            .expect("the row is missing");
        if row.status == WorkflowStatus::Success {
            break row.output.expect("a successful workflow has an output");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replayed operator did not finish; status {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        output.contains(&first[0]),
        "the replay returned {output} rather than the id it recorded, {}",
        first[0],
    );
    assert_eq!(
        forks_of_source().await,
        first,
        "the replay forked the source a second time",
    );

    dbos.shutdown().await;
}

/// **A resume refused because the workflow does not exist replays as the same refusal once it
/// does.**
///
/// The workflow branches on the refusal. Recorded, the replay takes the same branch; unrecorded,
/// it would resume the workflow that has since appeared.
#[tokio::test]
async fn a_refused_resume_replays_its_refusal_after_the_workflow_appears() {
    let db = test_database().await;
    let dbos = DBOS::new(config("refused-resume-app", &db));
    let target = dbos
        .register_workflow("late-target", |()| async move { Ok::<u32, Error>(7) })
        .unwrap();
    let operator = dbos
        .register_workflow("resuming-operator", {
            let dbos = dbos.clone();
            move |missing: String| {
                let dbos = dbos.clone();
                async move {
                    match dbos.resume::<u32, EngineOnly>(&missing).await {
                        Ok(_) => Ok::<_, Error>("resumed".to_owned()),
                        Err(Error::SystemDatabase(dbos::sysdb::Error::NonExistentWorkflow {
                            ..
                        })) => Ok("missing".to_owned()),
                        Err(e) => Err(e),
                    }
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let first = operator
        .run_with(
            "late-arrival".to_owned(),
            dbos::RunOptions {
                workflow_id: Some("resuming-operator-run"),
                ..Default::default()
            },
        )
        .await
        .expect("the first run failed");
    assert_eq!(first, "missing");

    let steps = reader(&db)
        .await
        .list_workflow_steps("resuming-operator-run", true, None, None, None)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1, "{steps:?}");
    assert_eq!(steps[0].step_name, "DBOS.resume_workflow");
    assert!(
        steps[0].error.is_some() && steps[0].output.is_none(),
        "the refusal is recorded as the step's error: {:?}",
        steps[0]
    );

    // The workflow now exists. A fork from the second step copies the resume's checkpoint, so
    // the resume replays, and must give back its refusal rather than resume it.
    target
        .run_with(
            (),
            dbos::RunOptions {
                workflow_id: Some("late-arrival"),
                ..Default::default()
            },
        )
        .await
        .expect("the late arrival failed");
    let forked = dbos
        .fork::<String, EngineOnly>("resuming-operator-run", 1)
        .await
        .expect("fork failed");
    assert_eq!(
        forked.result().await.expect("the replay failed"),
        "missing",
        "the replay took a different branch from the run it replayed"
    );

    dbos.shutdown().await;
}

/// **A workflow cannot delete itself, and is told so.**
///
/// From inside a workflow the delete is a step, and the step's checkpoint lands in
/// `operation_outputs` in the same transaction — for the `workflow_status` row the delete has just
/// removed. Allowed, it would leave an orphan checkpoint and a workflow running with no row to
/// finish on.
#[tokio::test]
async fn a_workflow_cannot_delete_itself() {
    let db = test_database().await;
    let dbos = DBOS::new(config("self-delete-app", &db));
    let deleter = dbos
        .register_workflow("self-deleter", {
            let dbos = dbos.clone();
            move |own_id: String| {
                let dbos = dbos.clone();
                async move {
                    dbos.delete(&own_id).await?;
                    Ok::<(), Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "deletes-itself";
    let error = deleter
        .run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect_err("the workflow deleted itself");
    let message = error.to_string();
    assert!(
        message.contains("cannot delete itself"),
        "the refusal did not reach the caller: {message}",
    );

    assert!(
        reader(&db)
            .await
            .get_workflow(id)
            .await
            .expect("read failed")
            .is_some(),
        "the row went even though the delete was refused",
    );

    dbos.shutdown().await;
}

/// **Nor an ancestor, when the tree it names is the one it is standing in.**
///
/// The caller is not named in the call at all — it arrives in the target set from the descendant
/// walk — so the check has to be on the expanded set rather than on the ids the caller passed.
#[tokio::test]
async fn a_workflow_cannot_delete_an_ancestors_tree() {
    let db = test_database().await;
    let dbos = DBOS::new(config("ancestor-delete-app", &db));
    let child = dbos
        .register_workflow("deleting-child", {
            let dbos = dbos.clone();
            move |root: String| {
                let dbos = dbos.clone();
                async move { dbos.delete_all(&[&root], Children::Include).await }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("deleted-parent", {
            move |root: String| {
                let child = child.clone();
                async move { child.run(root).await }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let root = "the-ancestor";
    let error = parent
        .run_with(
            root.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(root),
                ..Default::default()
            },
        )
        .await
        .expect_err("the child deleted the tree it was in");
    let message = error.to_string();
    assert!(
        message.contains("cannot delete itself"),
        "the refusal did not reach the caller: {message}",
    );

    assert!(
        reader(&db)
            .await
            .get_workflow(root)
            .await
            .expect("read failed")
            .is_some(),
        "the ancestor went even though the delete was refused",
    );

    dbos.shutdown().await;
}

/// **A workflow can't cancel or resume itself, and is told so before anything runs.** Both
/// would clear its own owner token, so its next write would be refused and it would wait for an
/// outcome nobody computes. Refused, the workflow can catch the error and go on, and finishes.
#[tokio::test]
async fn a_workflow_cannot_cancel_or_resume_itself() {
    let db = test_database().await;
    let dbos = DBOS::new(config("self-cancel-app", &db));
    let workflow = dbos
        .register_workflow("self-targeting", {
            let dbos = dbos.clone();
            move |own_id: String| {
                let dbos = dbos.clone();
                async move {
                    let cancelled = dbos
                        .cancel(&own_id)
                        .await
                        .expect_err("the workflow cancelled itself")
                        .to_string();
                    let resumed = dbos
                        .resume::<(), Error>(&own_id)
                        .await
                        .expect_err("the workflow resumed itself")
                        .to_string();
                    Ok::<_, Error>((cancelled, resumed))
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "targets-itself";
    let (cancelled, resumed) = tokio::time::timeout(
        Duration::from_secs(10),
        workflow.run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the workflow never finished")
    .expect("the workflow failed");
    assert!(cancelled.contains("cannot cancel itself"), "{cancelled}");
    assert!(resumed.contains("cannot resume itself"), "{resumed}");
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Success);

    dbos.shutdown().await;
}

/// **Nor from inside one of its steps**, where the call records nothing of its own: the step's
/// record would be refused, and a resume would have every new execution run the step again and
/// resume itself again, forever.
#[tokio::test]
async fn a_step_cannot_cancel_or_resume_its_own_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("step-self-cancel-app", &db));
    let workflow = dbos
        .register_workflow("step-self-targeting", {
            let dbos = dbos.clone();
            move |own_id: String| {
                let dbos = dbos.clone();
                async move {
                    dbos::step("kick", || {
                        let (dbos, own_id) = (dbos.clone(), own_id.clone());
                        async move {
                            let cancelled = dbos
                                .cancel(&own_id)
                                .await
                                .expect_err("the step cancelled its workflow")
                                .to_string();
                            let resumed = dbos
                                .resume::<(), Error>(&own_id)
                                .await
                                .expect_err("the step resumed its workflow")
                                .to_string();
                            Ok::<_, Error>((cancelled, resumed))
                        }
                    })
                    .await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "step-targets-its-workflow";
    let (cancelled, resumed) = tokio::time::timeout(
        Duration::from_secs(10),
        workflow.run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the workflow never finished")
    .expect("the workflow failed");
    assert!(cancelled.contains("cannot cancel itself"), "{cancelled}");
    assert!(resumed.contains("cannot resume itself"), "{resumed}");
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Success);

    dbos.shutdown().await;
}

/// **Nor cancel an ancestor's tree it is inside**, which reaches it through the cascade rather
/// than by name.
#[tokio::test]
async fn a_workflow_cannot_cancel_an_ancestors_tree() {
    let db = test_database().await;
    let dbos = DBOS::new(config("ancestor-cancel-app", &db));
    let child = dbos
        .register_workflow("cancelling-child", {
            let dbos = dbos.clone();
            move |root: String| {
                let dbos = dbos.clone();
                async move {
                    Ok::<_, Error>(
                        dbos.cancel_all(&[&root], Children::Include)
                            .await
                            .expect_err("the child cancelled the tree it was in")
                            .to_string(),
                    )
                }
            }
        })
        .unwrap();
    let parent = dbos
        .register_workflow("cancelled-parent", {
            move |root: String| {
                let child = child.clone();
                async move { child.run(root).await }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let root = "the-cancelled-ancestor";
    let message = tokio::time::timeout(
        Duration::from_secs(10),
        parent.run_with(
            root.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(root),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the workflow never finished")
    .expect("the workflow failed");
    assert!(
        message.contains("cannot cancel the tree of the-cancelled-ancestor"),
        "{message}"
    );
    let row = reader(&db).await.get_workflow(root).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Success);

    dbos.shutdown().await;
}

/// **A refusal is the call's recorded answer**, so a replay raises it again from the checkpoint
/// rather than deciding it afresh. A fork from the step after the refused cancel replays it: the
/// fork is a different workflow, so deciding afresh would let it cancel the original.
#[tokio::test]
async fn a_refused_self_cancel_is_recorded_and_replayed() {
    let db = test_database().await;
    let dbos = DBOS::new(config("refusal-replay-app", &db));
    let workflow = dbos
        .register_workflow("refused-then-on", {
            let dbos = dbos.clone();
            move |own_id: String| {
                let dbos = dbos.clone();
                async move {
                    let refused = dbos
                        .cancel(&own_id)
                        .await
                        .expect_err("the workflow cancelled itself")
                        .to_string();
                    let after = dbos::step("after", || async { Ok::<_, Error>(7u32) }).await?;
                    Ok::<_, Error>((refused, after))
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "refused-and-recorded";
    let original = workflow
        .run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect("the workflow failed");
    let steps = dbos.list_workflow_steps(id).await.unwrap();
    assert_eq!(steps.len(), 2, "{steps:?}");
    assert!(
        steps[0].error.is_some(),
        "the refusal is recorded: {steps:?}"
    );
    assert_eq!(
        (steps[1].step_id, steps[1].step_name.as_str()),
        (1, "after")
    );

    let forked = dbos
        .fork::<(String, u32), Error>(id, 1)
        .await
        .expect("fork failed");
    let replayed = tokio::time::timeout(Duration::from_secs(10), forked.result())
        .await
        .expect("the fork never finished")
        .expect("the fork failed");
    assert_eq!(replayed, original, "the fork replays the recorded refusal");
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Success, "nothing cancelled it");

    dbos.shutdown().await;
}

/// **The cascade is refused from further down too**, through a grandparent, from inside a step,
/// and when the caller is one of several ids named. Each refusal comes before anything moves, so
/// the other ids named alongside are left as they were: one held `DELAYED`, which a cancel would
/// move, and one already `CANCELLED`, which a resume would.
#[tokio::test]
async fn self_targeting_is_refused_through_any_ancestor_and_among_other_ids() {
    let db = test_database().await;
    let dbos = DBOS::new(config("self-target-cases-app", &db));
    let bystander = dbos
        .register_workflow("bystander", |(): ()| async { Ok::<_, Error>(()) })
        .unwrap();
    let grandchild = dbos
        .register_workflow("targeting-grandchild", {
            let dbos = dbos.clone();
            move |(own_id, root, held, stopped): (String, String, String, String)| {
                let dbos = dbos.clone();
                async move {
                    let tree = dbos
                        .cancel_all(&[&root], Children::Include)
                        .await
                        .expect_err("the grandchild cancelled its grandparent's tree")
                        .to_string();
                    let in_step = dbos::step("kick", || {
                        let (dbos, root) = (dbos.clone(), root.clone());
                        async move {
                            Ok::<_, Error>(
                                dbos.cancel_all(&[&root], Children::Include)
                                    .await
                                    .expect_err("a step cancelled its grandparent's tree")
                                    .to_string(),
                            )
                        }
                    })
                    .await?;
                    let among = dbos
                        .cancel_all(&[&held, &own_id], Children::Skip)
                        .await
                        .expect_err("the grandchild cancelled itself among others")
                        .to_string();
                    let resumed = dbos
                        .resume_all::<(), Error>(&[&stopped, &own_id], ResumeOptions::default())
                        .await
                        .expect_err("the grandchild resumed itself among others")
                        .to_string();
                    Ok::<_, Error>(vec![tree, in_step, among, resumed])
                }
            }
        })
        .unwrap();
    let child = dbos
        .register_workflow("middle-child", {
            move |(root, held, stopped): (String, String, String)| {
                let grandchild = grandchild.clone();
                async move {
                    let own = format!("{root}-grandchild");
                    grandchild
                        .run_with(
                            (own.clone(), root, held, stopped),
                            dbos::RunOptions {
                                workflow_id: Some(&own),
                                ..Default::default()
                            },
                        )
                        .await
                }
            }
        })
        .unwrap();
    let root_workflow = dbos
        .register_workflow("the-grandparent", {
            move |(root, held, stopped): (String, String, String)| {
                let child = child.clone();
                async move { child.run((root, held, stopped)).await }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "held-bystanders",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    // Far enough out that neither is released while the test runs.
    let (held, stopped) = ("a-held-bystander", "a-stopped-bystander");
    for id in [held, stopped] {
        bystander
            .start_with(
                (),
                StartOptions {
                    workflow_id: Some(id),
                    queue: Some(Enqueue {
                        delay: Some(Duration::from_secs(3600)),
                        ..Enqueue::new("held-bystanders")
                    }),
                    ..StartOptions::default()
                },
            )
            .await
            .expect("enqueue failed");
    }
    dbos.cancel(stopped).await.expect("cancel failed");
    let root = "the-grandparent-root";
    let messages = tokio::time::timeout(
        Duration::from_secs(10),
        root_workflow.run_with(
            (root.to_owned(), held.to_owned(), stopped.to_owned()),
            dbos::RunOptions {
                workflow_id: Some(root),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the workflow never finished")
    .expect("the workflow failed");
    let tree = format!("cannot cancel the tree of {root}");
    assert!(messages[0].contains(&tree), "{messages:?}");
    assert!(messages[1].contains(&tree), "{messages:?}");
    assert!(messages[2].contains("cannot cancel itself"), "{messages:?}");
    assert!(messages[3].contains("cannot resume itself"), "{messages:?}");
    for (id, status) in [
        (root, WorkflowStatus::Success),
        (held, WorkflowStatus::Delayed),
        (stopped, WorkflowStatus::Cancelled),
    ] {
        let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
        assert_eq!(row.status, status, "{id} was moved");
    }

    dbos.shutdown().await;
}

/// **A client is outside every workflow, even one driven from inside a workflow's code**, so its
/// cancel of the very workflow running it is not refused: the cancel lands, and the workflow stops
/// at its next step.
#[tokio::test]
async fn a_client_inside_a_workflow_may_cancel_that_workflow() {
    let db = test_database().await;
    let dbos = DBOS::new(config("client-self-cancel-app", &db));
    let client = Client::connect(ClientConfig {
        app_name: Some("client-self-cancel-app".to_owned()),
        ..ClientConfig::new(db.url())
    })
    .await
    .expect("connect failed");
    let answer = Arc::new(Mutex::new(None));
    let workflow = dbos
        .register_workflow("cancelled-by-its-client", {
            let answer = Arc::clone(&answer);
            move |own_id: String| {
                let (client, answer) = (client.clone(), Arc::clone(&answer));
                async move {
                    let cancelled = client.cancel(&own_id).await.map_err(|e| e.to_string());
                    *answer.lock().unwrap() = Some(cancelled);
                    dbos::step("after", || async { Ok::<_, Error>(()) }).await
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let id = "cancelled-by-its-own-client";
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        workflow.run_with(
            id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("the workflow never finished");
    assert_eq!(
        *answer.lock().unwrap(),
        Some(Ok(())),
        "the client's cancel was refused"
    );
    assert!(
        outcome.is_err(),
        "the cancelled workflow finished: {outcome:?}"
    );
    let row = reader(&db).await.get_workflow(id).await.unwrap().unwrap();
    assert_eq!(row.status, WorkflowStatus::Cancelled);

    dbos.shutdown().await;
}

/// A management call through another instance, from inside a workflow, is refused.
///
/// The third place [`Error::WrongInstance`] is reachable from, and it is here for the reason the
/// other two are: the step id comes from *this* workflow's counter while the checkpoint is written
/// through the other instance's system database, landing where the workflow that allocated it
/// cannot see it — usually as an orphan, since the other instance's database has no such
/// workflow. `DBOS::get_event` refuses the same combination, and
/// `WorkflowRef::parent` refuses it for starting a child.
///
/// The second half of the test is the leaf rule: from inside a *step* nothing is checkpointed, so
/// the two halves are never combined and there is nothing to refuse.
#[tokio::test]
async fn management_through_another_instance_from_inside_a_workflow_is_refused() {
    let db = test_database().await;
    let other = DBOS::new(config("other-management-app", &db));
    let owner = DBOS::new(config("owner-management-app", &db));

    let target = owner
        .register_workflow("target", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();

    // A write and a fork, so both kinds of call meet the guard in `StepPlacement::of`.
    let cancels_through_other = {
        let other = other.clone();
        owner
            .register_workflow("cancels_through_other", move |id: String| {
                let other = other.clone();
                async move {
                    other.cancel(&id).await?;
                    Ok::<u32, Error>(1)
                }
            })
            .unwrap()
    };
    let forks_through_other = {
        let other = other.clone();
        owner
            .register_workflow("forks_through_other", move |id: String| {
                let other = other.clone();
                async move {
                    let handle = other
                        .fork_with::<u32, EngineOnly>(
                            &id,
                            0,
                            ForkOptions {
                                forked_id: Some("refused-fork"),
                                ..ForkOptions::default()
                            },
                        )
                        .await?;
                    Ok::<String, Error>(handle.workflow_id().to_owned())
                }
            })
            .unwrap()
    };
    // From inside a *step* the call is plain — nothing is checkpointed, so the two halves are
    // never combined and there is nothing to refuse.
    let lists_in_step = {
        let other = other.clone();
        owner
            .register_workflow("lists_in_step", move |id: String| {
                let other = other.clone();
                async move {
                    let seen = dbos::step("list", || {
                        let other = other.clone();
                        let id = id.clone();
                        async move {
                            let rows = other
                                .list_workflows(&WorkflowFilter {
                                    workflow_ids: vec![&id],
                                    ..WorkflowFilter::default()
                                })
                                .await?;
                            Ok(rows.len())
                        }
                    })
                    .await?;
                    Ok::<usize, Error>(seen)
                }
            })
            .unwrap()
    };
    other.launch().await.expect("launch failed");
    owner.launch().await.expect("launch failed");

    // The target sits on a queue nothing polls, so it is there to be cancelled or forked.
    let target_id = "target-of-another-instance";
    target
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(target_id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    for (workflow_id, error) in [
        (
            "refused-cancel",
            cancels_through_other
                .run_with(
                    target_id.to_owned(),
                    dbos::RunOptions {
                        workflow_id: Some("refused-cancel"),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
                .expect_err("the cancel through another instance was accepted"),
        ),
        (
            "refused-fork",
            forks_through_other
                .run_with(
                    target_id.to_owned(),
                    dbos::RunOptions {
                        workflow_id: Some("refused-fork-caller"),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
                .expect_err("the fork through another instance was accepted"),
        ),
    ] {
        assert!(
            matches!(&error, Error::WrongInstance { operation } if operation.contains("workflow")),
            "expected a wrong-instance refusal for {workflow_id}, got {error:?}"
        );
    }

    let reader = reader(&db).await;

    // Refused before anything was written: the target is untouched and no fork exists.
    assert_eq!(
        reader
            .get_workflow(target_id)
            .await
            .expect("read failed")
            .expect("the row is missing")
            .status,
        WorkflowStatus::Enqueued,
        "the target moved even though the cancel was refused",
    );
    assert!(
        reader
            .get_workflow("refused-fork")
            .await
            .expect("read failed")
            .is_none(),
        "a fork was written even though the call was refused",
    );

    // And no step id was spent: `StepPlacement::of` runs the guard before it allocates one.
    for caller in ["refused-cancel", "refused-fork-caller"] {
        assert!(
            reader
                .list_workflow_steps(caller, true, None, None, None)
                .await
                .expect("read failed")
                .is_empty(),
            "{caller} checkpointed a step for a call that was refused",
        );
    }

    // The same call from inside a step is plain, and reaches the other instance's database.
    let seen = lists_in_step
        .run_with(
            target_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some("lists-in-step"),
                ..Default::default()
            },
        )
        .await
        .expect("a listing from inside a step was refused");
    assert_eq!(seen, 1, "the plain listing did not see the target");

    // One step, the caller's own — the listing under it recorded nothing of its own.
    let steps = reader
        .list_workflow_steps("lists-in-step", true, None, None, None)
        .await
        .expect("read failed");
    let recorded: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        recorded,
        [(0, "list")],
        "a listing from inside a step allocated a step id of its own",
    );

    owner.shutdown().await;
    other.shutdown().await;
}

/// An empty management batch inside a workflow records its step like any other.
///
/// The batch takes its id where it was written, so it owes that slot an outcome whatever it was
/// passed — the rule the empty waits follow. A list derived from state can be empty on one
/// execution and not on the next, and a batch that spent an id without recording anything would
/// let the replay *perform* the operation against the fuller list rather than replay the first
/// run's answer. Recorded here, so the replay reads the row back instead.
#[tokio::test]
async fn an_empty_management_batch_inside_a_workflow_records_its_step() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-empty-batch-app", &db));
    let operator = dbos
        .register_workflow("operator", {
            let dbos = dbos.clone();
            move |()| {
                let dbos = dbos.clone();
                async move {
                    let none: [&str; 0] = [];
                    assert!(
                        dbos.cancel_all(&none, Children::Skip).await?.is_empty(),
                        "an empty cancel cancelled something"
                    );
                    assert_eq!(
                        dbos.delete_all(&none, Children::Skip).await?,
                        0,
                        "an empty delete removed something"
                    );
                    dbos::step("after", || async { Ok::<u32, Error>(1) }).await?;
                    Ok::<u32, Error>(1)
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let handle = operator.start(()).await.expect("start failed");
    let id = handle.workflow_id().to_owned();
    assert_eq!(handle.result().await.expect("the workflow failed"), 1);

    let steps = dbos
        .list_workflow_steps(&id)
        .await
        .expect("could not read the steps");
    let seen: Vec<(i32, &str)> = steps
        .iter()
        .map(|step| (step.step_id, step.step_name.as_str()))
        .collect();
    assert_eq!(
        seen,
        [
            (0, "DBOS.cancel_workflow"),
            (1, "DBOS.delete_workflow"),
            (2, "after")
        ],
        "each empty batch recorded its own row and left `after` where it would have been anyway"
    );

    dbos.shutdown().await;
}

/// **Management calls take their step ids where they are built, not where they are first polled.**
///
/// The counterpart in `events.rs` makes the argument for the shape: `join!` builds every branch
/// before polling any and then first-polls them in source order, so a test that builds and drives
/// in the same order passes against poll-time ids too. These three are built `a, b, c` and handed
/// to `join!` as `c, b, a`.
#[tokio::test]
async fn management_calls_driven_out_of_build_order_keep_the_ids_they_were_built_with() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-join-app", &db));
    let target = dbos
        .register_workflow("target", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let operator = dbos
        .register_workflow("operator", {
            let dbos = dbos.clone();
            move |id: String| {
                let dbos = dbos.clone();
                async move {
                    let filter = WorkflowFilter {
                        workflow_ids: vec![&id],
                        ..WorkflowFilter::default()
                    };
                    // Built a, b, c...
                    let a = dbos.cancel(&id);
                    let b = dbos.list_workflow_steps(&id);
                    let c = dbos.list_workflows(&filter);
                    // ...whose ids are already decided, before anything has been polled. The
                    // rows below say the same thing from the database's side; this says it of
                    // the values themselves, which is the only thing that speaks for the branch
                    // built last and polled first.
                    assert_eq!(
                        (a.step_id(), b.step_id(), c.step_id()),
                        (Some(0), Some(1), Some(2)),
                        "the ids were taken at the call, in source order"
                    );
                    // ...and driven c, b, a.
                    let (c, b, a) = tokio::join!(c, b, a);
                    a?;
                    b?;
                    Ok::<usize, Error>(c?.len())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    // The target sits on a queue nothing polls, so it is there to be cancelled.
    let target_id = "cancelled-out-of-order";
    target
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(target_id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let operator_id = "the-joining-operator";
    let seen = operator
        .run_with(
            target_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(operator_id),
                ..Default::default()
            },
        )
        .await
        .expect("the operator workflow failed");
    assert_eq!(
        seen, 1,
        "the listing did not see the workflow it filtered on"
    );

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(operator_id, true, None, None, None)
        .await
        .expect("read failed");
    let recorded: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        recorded,
        [
            (0, "DBOS.cancel_workflow"),
            (1, "DBOS.list_workflow_steps"),
            (2, "DBOS.list_workflows"),
        ],
        "the ids follow the order the calls were built in, not the order they were polled in",
    );

    dbos.shutdown().await;
}

/// **A management call refused by its own arguments moves no step counter.**
///
/// The launch check comes first, then the call's own argument check, and only then the step id —
/// so `fork_all` refusing a chosen id, and `fork` refusing a negative step, leave the step after
/// them on the slot it would have had. The instance-level refusals are
/// covered by `management_through_another_instance_from_inside_a_workflow_is_refused`; these are
/// the ones the method itself raises.
#[tokio::test]
async fn a_management_call_refused_by_its_arguments_spends_no_step_id() {
    let db = test_database().await;
    let dbos = DBOS::new(config("management-refusal-app", &db));
    let target = dbos
        .register_workflow("target", |()| async move { Ok::<u32, Error>(1) })
        .unwrap();
    let operator = dbos
        .register_workflow("operator", {
            let dbos = dbos.clone();
            move |id: String| {
                let dbos = dbos.clone();
                async move {
                    // One id cannot name many forks.
                    let refused = dbos
                        .fork_all::<u32, EngineOnly>(
                            &[&id],
                            0,
                            ForkOptions {
                                forked_id: Some("chosen"),
                                ..ForkOptions::default()
                            },
                        )
                        .await;
                    assert!(
                        matches!(refused.map(|_| ()), Err(Error::InvalidArgument { .. })),
                        "a chosen id should be refused in bulk"
                    );

                    // And there is no step below zero to fork from.
                    let refused = dbos.fork::<u32, EngineOnly>(&id, -1).await;
                    assert!(
                        matches!(refused.map(|_| ()), Err(Error::InvalidArgument { .. })),
                        "a negative step should be refused"
                    );

                    dbos::step("after", || async { Ok(()) }).await?;
                    Ok::<(), Error>(())
                }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");

    let target_id = "never-forked";
    target
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(target_id),
                queue: Some(Enqueue::new("nothing-polls-this")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");

    let operator_id = "the-refused-operator";
    operator
        .run_with(
            target_id.to_owned(),
            dbos::RunOptions {
                workflow_id: Some(operator_id),
                ..Default::default()
            },
        )
        .await
        .expect("the operator workflow failed");

    let reader = reader(&db).await;
    let steps = reader
        .list_workflow_steps(operator_id, true, None, None, None)
        .await
        .expect("read failed");
    let recorded: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(
        recorded,
        [(0, "after")],
        "a refused fork took the step id the step after it should have had",
    );
    assert!(
        reader
            .get_workflow("chosen")
            .await
            .expect("read failed")
            .is_none(),
        "a fork was written even though the call was refused",
    );

    dbos.shutdown().await;
}

/// **No workflow write reaches the legacy payload columns.**
///
/// Every kind of workflow this crate writes — run directly, enqueued and dequeued, started as a
/// child, failed, and forked — and then a sweep of the whole database: no `workflow_status` row
/// carries `inputs`, `output` or `error`, and every workflow has its input in `workflow_input`.
/// A sweep rather than a check per call, so a write path added later that forgets the payload
/// tables fails here without anyone having to remember to test it.
#[tokio::test]
async fn no_workflow_write_reaches_the_legacy_payload_columns() {
    let db = test_database().await;
    let dbos = DBOS::new(config("legacy-sweep-app", &db));
    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
    #[error("this workflow always fails")]
    struct Boom;

    let doubles = dbos
        .register_workflow("doubles", |n: u32| async move { Ok::<u32, Error>(n * 2) })
        .unwrap();
    let fails = dbos
        .register_workflow("fails", |()| async move {
            Err(Boom)?;
            Ok::<(), dbos::Error<Boom>>(())
        })
        .unwrap();
    let parent = dbos
        .register_workflow("parent", {
            let doubles = doubles.clone();
            move |()| {
                let doubles = doubles.clone();
                async move { doubles.start(5).await?.result().await }
            }
        })
        .unwrap();
    dbos.launch().await.expect("launch failed");
    dbos.register_queue(
        "sweep-queue",
        dbos::QueueOptions::default(),
        dbos::QueueConflict::UpdateIfLatestVersion,
    )
    .await
    .expect("registration failed");

    let direct = doubles
        .run_with(
            1,
            dbos::RunOptions {
                workflow_id: Some("sweep-direct"),
                ..Default::default()
            },
        )
        .await
        .expect("the direct run failed");
    assert_eq!(direct, 2);

    let queued = doubles
        .start_with(
            2,
            StartOptions {
                workflow_id: Some("sweep-queued"),
                queue: Some(Enqueue::new("sweep-queue")),
                ..StartOptions::default()
            },
        )
        .await
        .expect("enqueue failed");
    assert_eq!(queued.result().await.expect("the queued run failed"), 4);

    assert_eq!(parent.run(()).await.expect("the parent failed"), 10);
    assert!(
        fails.run(()).await.is_err(),
        "the failing workflow succeeded"
    );

    let forked = dbos
        .fork::<u32, EngineOnly>("sweep-direct", 0)
        .await
        .expect("fork failed");
    assert_eq!(forked.result().await.expect("the fork failed"), 2);

    let pool = db.pool().await;
    let workflows: i64 = sqlx::query_scalar(r#"SELECT count(*) FROM "dbos"."workflow_status""#)
        .fetch_one(&pool)
        .await
        .unwrap();
    // Direct, queued, parent, its child, the failure and the fork.
    assert_eq!(workflows, 6, "the sweep should cover every kind of write");

    let legacy: Vec<String> = sqlx::query_scalar(
        r#"SELECT workflow_uuid FROM "dbos"."workflow_status"
           WHERE inputs IS NOT NULL OR output IS NOT NULL OR error IS NOT NULL"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        legacy.is_empty(),
        "payloads written to workflow_status: {legacy:?}"
    );

    let without_input: Vec<String> = sqlx::query_scalar(
        r#"SELECT s.workflow_uuid FROM "dbos"."workflow_status" s
           LEFT JOIN "dbos"."workflow_input" i ON i.workflow_uuid = s.workflow_uuid
           WHERE i.workflow_uuid IS NULL"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        without_input.is_empty(),
        "workflows with no workflow_input row: {without_input:?}"
    );

    let finished_without_output: Vec<String> = sqlx::query_scalar(
        r#"SELECT s.workflow_uuid FROM "dbos"."workflow_status" s
           LEFT JOIN "dbos"."workflow_output" o ON o.workflow_uuid = s.workflow_uuid
           WHERE s.status IN ('SUCCESS', 'ERROR') AND o.workflow_uuid IS NULL"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        finished_without_output.is_empty(),
        "finished workflows with no workflow_output row: {finished_without_output:?}"
    );

    dbos.shutdown().await;
}
