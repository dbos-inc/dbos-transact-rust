//! Recovery on launch, against real databases.
//!
//! Every abandonment here is a real one: a workflow is started, cut down mid-flight by
//! `shutdown`, and left `PENDING` — then a relaunch recovers it. That is the crash-and-resume
//! demonstration the workstream exists for, minus only the process boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use dbos::sysdb::SystemDatabase;
use dbos::sysdb::postgres::{PostgresSystemDatabase, Settings};
use dbos::sysdb::types::{WorkflowRecord, WorkflowStatus};
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
                dbos::step("one", move || async move {
                    one.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await?;
                dbos::step("two", move || async move {
                    two.fetch_add(1, Ordering::SeqCst);
                    Ok(())
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

/// Recovery runs no more workflows at once than it is allowed to.
///
/// Five workflows are abandoned at a gate, then a second instance recovers them with the cap set
/// to two. Without a bound the sweep claims and spawns all five at once, every one of them
/// contending for a pool of ten — the case a process that died holding a large backlog turns into.
#[tokio::test]
async fn recovery_runs_no_more_workflows_at_once_than_its_cap() {
    const ABANDONED: usize = 5;
    const CAP: usize = 2;

    /// Counts a workflow for as long as its body exists.
    ///
    /// A guard rather than a pair of writes, because a cancelled workflow's body never reaches the
    /// line after its await — the future is simply dropped. That is also what makes `running`
    /// reaching zero a real statement about shutdown: it goes to zero only once every body has
    /// actually been dropped, which is what `abort_all` waits for.
    struct Live(Arc<AtomicUsize>);

    impl Live {
        fn enter(running: &Arc<AtomicUsize>, peak: &AtomicUsize) -> Self {
            let live = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(live, Ordering::SeqCst);
            Self(Arc::clone(running))
        }
    }

    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    // Starts empty, so every workflow blocks in its body until the test hands out permits.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let db = test_database().await;
    let blocks = |dbos: &DBOS| {
        let (running, peak, gate) = (Arc::clone(&running), Arc::clone(&peak), Arc::clone(&gate));
        dbos.register_workflow("blocks", move |()| {
            let (running, peak, gate) =
                (Arc::clone(&running), Arc::clone(&peak), Arc::clone(&gate));
            async move {
                let _live = Live::enter(&running, &peak);
                gate.acquire()
                    .await
                    .expect("the gate is never closed")
                    .forget();
                Ok::<_, dbos::Error>(())
            }
        })
        .unwrap()
    };

    // Abandon five at the gate.
    let first = DBOS::new(config("cap-app", &db));
    let workflow = blocks(&first);
    first.launch().await.expect("launch failed");
    for _ in 0..ABANDONED {
        drop(workflow.start(()).await.expect("start failed"));
    }
    await_at_least(&running, ABANDONED).await;
    first.shutdown().await;
    assert_eq!(
        running.load(Ordering::SeqCst),
        0,
        "shutdown waits for the bodies it cancelled"
    );
    peak.store(0, Ordering::SeqCst);

    // Recover them with the cap on.
    let second = DBOS::new(Config {
        recovery_concurrency: Some(CAP),
        ..config("cap-app", &db)
    });
    let workflow = blocks(&second);
    second.launch().await.expect("relaunch failed");

    await_at_least(&running, CAP).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        peak.load(Ordering::SeqCst),
        CAP,
        "the sweep ran more than its cap allows"
    );

    // Let them drain: as each finishes it frees a slot for the next, and the cap still holds.
    gate.add_permits(ABANDONED);
    let reader = reader(&db).await;
    for row in reader
        .list_workflows(&Default::default())
        .await
        .expect("read failed")
    {
        await_status(&reader, &row.workflow_id, WorkflowStatus::Success).await;
    }
    assert_eq!(peak.load(Ordering::SeqCst), CAP, "the cap held throughout");

    second.shutdown().await;
    drop(workflow);
}

/// Polls until `counter` reaches at least `want`, which is how a test waits on work it cannot
/// join.
///
/// At least, not exactly: an unbounded sweep blows straight past the cap, and this should hand
/// back so the assertion on the peak can say so, rather than waiting out the deadline for a count
/// that will never be hit on the nose.
async fn await_at_least(counter: &AtomicUsize, want: usize) {
    tokio::time::timeout(DEADLINE, async {
        while counter.load(Ordering::SeqCst) < want {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "wanted {want} running, saw {}",
            counter.load(Ordering::SeqCst)
        )
    });
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
