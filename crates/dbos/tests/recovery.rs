//! Recovery on launch, against real databases.
//!
//! Every abandonment here is a real one: a workflow is started, cut down mid-flight by
//! `shutdown`, and left `PENDING` — then a relaunch recovers it. That is the crash-and-resume
//! demonstration the workstream exists for, minus only the process boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowRecord, WorkflowStatus};
use dbos::sysdb::{INTERNAL_QUEUE, SystemDatabase};
use dbos::{Config, DBOS, Error};

use dbos_test_support::{TestDatabase, test_database};

/// Everything here waits on real databases and background tasks; nothing legitimate takes this
/// long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
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
        .list_workflows(&Default::default())
        .await
        .expect("read failed");
    let [row] = &rows[..] else {
        panic!("expected exactly one workflow, got {}", rows.len())
    };
    assert_eq!(row.status, WorkflowStatus::Pending, "abandoned, not failed");
    let workflow_id = row.workflow_id.clone();
    let steps = reader
        .list_workflow_steps(&workflow_id, true, None, None)
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

    // The assertion the whole workstream exists for.
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
        .list_workflows(&Default::default())
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
            dbos::StartOptions {
                workflow_id: Some(id),
                ..dbos::StartOptions::default()
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
