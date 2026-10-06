//! Recovery on launch, against real databases.
//!
//! Every abandonment here is a real one: a workflow is started, cut down mid-flight by
//! `shutdown`, and left `PENDING` — then a relaunch recovers it. That is the crash-and-resume
//! demonstration recovery exists for, minus only the process boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowRecord, WorkflowStatus};
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
use dbos::{Config, DBOS, Enqueue, Error, QueueConflict, QueueOptions, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

/// Everything here waits on real databases and background tasks; nothing legitimate takes this
/// long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

/// The version every instance in this file launches with: DBOS computes none, so a launch without
/// one fails — and sharing it is what lets a relaunch recover what the previous launch left.
const APP_VERSION: &str = "1.0.0";

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

/// A handle for reading rows behind the instance's back.
async fn reader(db: &TestDatabase) -> PostgresSystemDatabase {
    PostgresSystemDatabase::from_pool(db.pool().await, &Settings::default())
}

/// Polls until the workflow reaches `status`, returning its row.
async fn await_status(
    reader: &PostgresSystemDatabase,
    workflow_id: &str,
    status: WorkflowStatus,
) -> WorkflowRecord {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = reader
                .get_workflow(workflow_id)
                .await
                .expect("read failed")
                .expect("the row exists");
            if row.status == status {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{workflow_id} never reached {status:?}"))
}

/// The whole feature in one test: steps that finished before the abandonment do not run again.
///
/// The workflow runs two steps, signals, and blocks; `shutdown` abandons it exactly there. The
/// relaunch must recover it, replay both steps from their checkpoints without entering either
/// body, run the remainder, and record the outcome.
#[tokio::test]
async fn a_relaunch_resumes_at_the_step_after_the_last_one_recorded() {
    let one = Arc::new(AtomicU32::new(0));
    let two = Arc::new(AtomicU32::new(0));
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let dbos = DBOS::new(config("recovery-app", &db));
    let resumes = {
        let (one, two) = (Arc::clone(&one), Arc::clone(&two));
        let (reached, release) = (Arc::clone(&reached_gate), Arc::clone(&release_gate));
        dbos.register_workflow("resumes", move |()| {
            let (one, two) = (Arc::clone(&one), Arc::clone(&two));
            let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
            async move {
                dbos::step("one", || {
                    let one = Arc::clone(&one);
                    async move {
                        one.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await?;
                dbos::step("two", || {
                    let two = Arc::clone(&two);
                    async move {
                        two.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await?;
                reached.notify_one();
                release.notified().await;
                Ok::<_, dbos::Error>("resumed to completion".to_owned())
            }
        })
        .unwrap()
    };
    dbos.launch().await.expect("launch failed");

    // Run to the gate, then abandon: shutdown aborts the task and the row stays PENDING.
    let running = tokio::spawn(async move { resumes.run(()).await });
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never reached its gate");
    dbos.shutdown().await;
    let err = running.await.expect("the task panicked").unwrap_err();
    assert!(matches!(err, Error::Interrupted { .. }), "{err}");

    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed");
    let [row] = &rows[..] else {
        panic!("expected exactly one workflow, got {}", rows.len())
    };
    assert_eq!(row.status, WorkflowStatus::Pending, "abandoned, not failed");
    let workflow_id = row.workflow_id.clone();
    let steps = reader
        .list_workflow_steps(&workflow_id, true, None, None, None)
        .await
        .expect("read failed");
    assert_eq!(
        steps.iter().map(|s| s.step_id).collect::<Vec<_>>(),
        [0, 1],
        "both steps checkpointed before the abandonment"
    );

    // The relaunch is the crash-and-resume demo: recovery lists the workflow before launch
    // returns and re-executes it in the background.
    dbos.launch().await.expect("relaunch failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered workflow never reached its gate again");
    release_gate.notify_one();

    let row = await_status(&reader, &workflow_id, WorkflowStatus::Success).await;
    assert_eq!(row.output.as_deref(), Some("\"resumed to completion\""));
    assert!(
        row.recovery_attempts >= 1,
        "the recovery submission counts: {}",
        row.recovery_attempts
    );

    // The assertion recovery exists for.
    assert_eq!(one.load(Ordering::SeqCst), 1, "step one ran exactly once");
    assert_eq!(two.load(Ordering::SeqCst), 1, "step two ran exactly once");

    dbos.shutdown().await;
}

/// A row whose registration is gone is skipped, and does not strand the workflows behind it.
///
/// The first instance registers two workflows and abandons both. The second registers only one —
/// the other's code was "removed" — and its launch must recover the one it knows and leave the
/// other PENDING, rather than failing the sweep.
#[tokio::test]
async fn an_unregistered_workflow_is_skipped_and_the_rest_recover() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let first = DBOS::new(config("skip-app", &db));
    let blocked = |entered: &Arc<tokio::sync::Notify>, gate: &Arc<tokio::sync::Notify>| {
        let (entered, gate) = (Arc::clone(entered), Arc::clone(gate));
        move |()| {
            let (entered, gate) = (Arc::clone(&entered), Arc::clone(&gate));
            async move {
                entered.notify_one();
                gate.notified().await;
                Ok::<_, dbos::Error>(())
            }
        }
    };
    // `ghost` is started first, so a sweep in creation order meets the skip before the recovery.
    let ghost = first
        .register_workflow("ghost", blocked(&entered, &gate))
        .unwrap();
    let keeper = first
        .register_workflow("keeper", blocked(&entered, &gate))
        .unwrap();
    first.launch().await.expect("launch failed");

    for workflow in [
        tokio::spawn(async move { ghost.run(()).await }),
        tokio::spawn(async move { keeper.run(()).await }),
    ] {
        tokio::time::timeout(DEADLINE, entered.notified())
            .await
            .expect("a workflow never started");
        // Not awaited further: both block at the gate until shutdown interrupts them.
        drop(workflow);
    }
    first.shutdown().await;

    // A new instance on the same database, with `ghost`'s code "removed".
    let second = DBOS::new(config("skip-app", &db));
    second
        .register_workflow("keeper", |()| async { Ok::<_, dbos::Error>(()) })
        .unwrap();
    second.launch().await.expect("relaunch failed");

    let reader = reader(&db).await;
    let rows = reader
        .list_workflows(&Default::default(), None)
        .await
        .expect("read failed");
    let id_of = |name: &str| {
        rows.iter()
            .find(|r| r.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("no row for {name}"))
            .workflow_id
            .clone()
    };

    await_status(&reader, &id_of("keeper"), WorkflowStatus::Success).await;
    let ghost_row = reader
        .get_workflow(&id_of("ghost"))
        .await
        .expect("read failed")
        .expect("the ghost row exists");
    assert_eq!(
        ghost_row.status,
        WorkflowStatus::Pending,
        "skipped, not failed: it waits for a launch that knows its code"
    );

    second.shutdown().await;
}

/// **Recovery goes through the queue, and the row proves it.**
///
/// A workflow cut down mid-flight comes back on [`INTERNAL_QUEUE`] rather than being executed by
/// the process that found it. That is what makes a repeat sweep harmless, lets any executor on
/// the version pick the work up, and turns "how many at once" into the queue's question.
#[tokio::test]
async fn a_recovered_workflow_comes_back_through_the_internal_queue() {
    let db = test_database().await;
    let reader = reader(&db).await;

    let entered = Arc::new(AtomicU32::new(0));
    // The body signals its own entry: the row turns PENDING when the workflow is started, well
    // before the task that runs it is scheduled, so the row is no evidence that the body ran.
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let build = |db: &TestDatabase| {
        let dbos = DBOS::new(config("recovery-reenqueue-app", db));
        let entered = Arc::clone(&entered);
        let reached = Arc::clone(&reached_gate);
        let workflow = dbos
            .register_workflow("held", move |()| {
                let entered = Arc::clone(&entered);
                let reached = Arc::clone(&reached);
                async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    reached.notify_one();
                    // Long enough that shutdown catches it mid-flight.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok::<u32, Error>(1)
                }
            })
            .unwrap();
        (dbos, workflow)
    };

    let id = "cut-down-mid-flight";
    let (first, workflow) = build(&db);
    first.launch().await.expect("launch failed");
    workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never entered its body");
    await_status(&reader, id, WorkflowStatus::Pending).await;
    // Aborts the task without writing anything durable, so the row stays PENDING.
    first.shutdown().await;
    assert_eq!(entered.load(Ordering::SeqCst), 1);

    // The relaunch re-enqueues rather than running it here; the dequeue loop then picks it up.
    let (second, _workflow) = build(&db);
    second.launch().await.expect("relaunch failed");

    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow did not run again after being recovered");
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    let row = await_status(&reader, id, WorkflowStatus::Pending).await;
    assert_eq!(
        row.queue_name.as_deref(),
        Some(INTERNAL_QUEUE),
        "recovery ran the workflow in place instead of returning it to a queue"
    );

    second.shutdown().await;
}

/// An execution that loses a step-checkpoint race returns the winner's outcome, not an error.
///
/// Recovery hands a workflow to a second execution while the first is still running — the first
/// was presumed dead, and was not. Both run the same step; the second records it and finishes the
/// workflow; the first then finishes its step and finds the position already taken. Its caller
/// must get the workflow's recorded outcome, which is the second execution's, rather than a
/// system-database error for a workflow that succeeded.
#[tokio::test]
async fn the_loser_of_a_step_checkpoint_race_returns_the_winners_outcome() {
    let entered = Arc::new(AtomicU32::new(0));
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let release_gate = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let build = |db: &TestDatabase| {
        let dbos = DBOS::new(config("step-race-app", db));
        let (entered, reached, release) = (
            Arc::clone(&entered),
            Arc::clone(&reached_gate),
            Arc::clone(&release_gate),
        );
        let workflow = dbos
            .register_workflow("racy", move |()| {
                let (entered, reached, release) = (
                    Arc::clone(&entered),
                    Arc::clone(&reached),
                    Arc::clone(&release),
                );
                async move {
                    let output = dbos::step("work", || {
                        let (entered, reached, release) = (
                            Arc::clone(&entered),
                            Arc::clone(&reached),
                            Arc::clone(&release),
                        );
                        async move {
                            // The first execution holds its step open until the second has
                            // recorded the same one; every later execution completes at once.
                            if entered.fetch_add(1, Ordering::SeqCst) == 0 {
                                reached.notify_one();
                                release.notified().await;
                                Ok("first".to_owned())
                            } else {
                                Ok("second".to_owned())
                            }
                        }
                    })
                    .await?;
                    Ok::<_, dbos::Error>(output)
                }
            })
            .unwrap();
        (dbos, workflow)
    };
    let reader = reader(&db).await;
    let id = "step-race";

    let (first, workflow) = build(&db);
    first.launch().await.expect("launch failed");
    let handle = workflow
        .start_with(
            (),
            StartOptions {
                workflow_id: Some(id),
                ..StartOptions::default()
            },
        )
        .await
        .expect("start failed");
    tokio::time::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the first execution never entered its step");

    // Same executor id, so this launch recovers the workflow the first instance is still running.
    let (second, _workflow) = build(&db);
    second.launch().await.expect("second launch failed");
    let row = await_status(&reader, id, WorkflowStatus::Success).await;
    assert_eq!(row.output.as_deref(), Some("\"second\""));

    release_gate.notify_one();
    let result = tokio::time::timeout(DEADLINE, handle.result())
        .await
        .expect("the losing execution never returned");
    assert_eq!(
        result.expect("the losing execution's caller got an error"),
        "second",
        "the caller adopts the recorded outcome, not what its own execution computed"
    );
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "both executions ran the step"
    );

    let row = reader
        .get_workflow(id)
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(row.status, WorkflowStatus::Success);
    assert_eq!(
        row.output.as_deref(),
        Some("\"second\""),
        "the loser wrote nothing"
    );

    second.shutdown().await;
    first.shutdown().await;
}

/// A dequeued execution that loses a step race gives up its queue slot before it waits.
///
/// The queue allows one workflow per process. The first instance dequeues the racing workflow and
/// holds its step open; a second instance with the same executor id recovers it, dequeues it in
/// turn, records the step, and holds the workflow open afterwards. When the first instance's step
/// loses, that execution waits for the second's outcome — and the next workflow on the queue must
/// still run on the first instance meanwhile, which it can only do if the waiting execution has
/// let its slot go.
#[tokio::test]
async fn a_superseded_execution_releases_its_queue_slot_while_it_waits() {
    const QUEUE: &str = "race-queue";

    let entered = Arc::new(AtomicU32::new(0));
    let reached_step = Arc::new(tokio::sync::Notify::new());
    let release_step = Arc::new(tokio::sync::Notify::new());
    let winner_holding = Arc::new(tokio::sync::Notify::new());
    let release_winner = Arc::new(tokio::sync::Notify::new());

    let db = test_database().await;
    let build = async |db: &TestDatabase| {
        let dbos = DBOS::new(config("superseded-slot-app", db));
        let gates = (
            Arc::clone(&entered),
            Arc::clone(&reached_step),
            Arc::clone(&release_step),
            Arc::clone(&winner_holding),
            Arc::clone(&release_winner),
        );
        let racy = dbos
            .register_workflow("racy", move |()| {
                let (entered, reached, release, holding, hold) = (
                    Arc::clone(&gates.0),
                    Arc::clone(&gates.1),
                    Arc::clone(&gates.2),
                    Arc::clone(&gates.3),
                    Arc::clone(&gates.4),
                );
                async move {
                    let output = dbos::step("work", || {
                        let (entered, reached, release) = (
                            Arc::clone(&entered),
                            Arc::clone(&reached),
                            Arc::clone(&release),
                        );
                        async move {
                            if entered.fetch_add(1, Ordering::SeqCst) == 0 {
                                reached.notify_one();
                                release.notified().await;
                                Ok("first".to_owned())
                            } else {
                                Ok("second".to_owned())
                            }
                        }
                    })
                    .await?;
                    // Only the execution that recorded the step gets here.
                    holding.notify_one();
                    hold.notified().await;
                    Ok::<_, dbos::Error>(output)
                }
            })
            .unwrap();
        let next = dbos
            .register_workflow("next", async |()| Ok::<_, dbos::Error>(()))
            .unwrap();
        dbos.launch().await.expect("launch failed");
        dbos.register_queue(
            QUEUE,
            QueueOptions {
                worker_concurrency: Some(1),
                polling_interval: Duration::from_millis(100),
                ..QueueOptions::default()
            },
            QueueConflict::UpdateIfLatestVersion,
        )
        .await
        .expect("queue registration failed");
        (dbos, racy, next)
    };
    let enqueue = |id| StartOptions {
        workflow_id: Some(id),
        queue: Some(Enqueue::new(QUEUE)),
        ..StartOptions::default()
    };
    let reader = reader(&db).await;

    let (first, racy, next) = build(&db).await;
    racy.start_with((), enqueue("racy-1"))
        .await
        .expect("enqueue failed");
    tokio::time::timeout(DEADLINE, reached_step.notified())
        .await
        .expect("the first instance never ran the step");

    // Same executor id, so this launch returns the running workflow to its queue, and the second
    // instance — the one with a free slot — dequeues it.
    let (second, _racy, _next) = build(&db).await;
    tokio::time::timeout(DEADLINE, winner_holding.notified())
        .await
        .expect("the second instance never recorded the step");

    release_step.notify_one();
    next.start_with((), enqueue("next-1"))
        .await
        .expect("enqueue failed");
    // The second instance's one slot is held by the winner, so only the first can run this.
    tokio::time::timeout(
        Duration::from_secs(20),
        await_status(&reader, "next-1", WorkflowStatus::Success),
    )
    .await
    .expect("the next workflow never ran: the superseded execution kept its slot");
    let racy_row = reader
        .get_workflow("racy-1")
        .await
        .expect("read failed")
        .expect("the row exists");
    assert_eq!(
        racy_row.status,
        WorkflowStatus::Pending,
        "the winner is still holding the racing workflow open"
    );

    release_winner.notify_one();
    let row = await_status(&reader, "racy-1", WorkflowStatus::Success).await;
    assert_eq!(row.output.as_deref(), Some("\"second\""));
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "both executions ran the step"
    );

    second.shutdown().await;
    first.shutdown().await;
}
