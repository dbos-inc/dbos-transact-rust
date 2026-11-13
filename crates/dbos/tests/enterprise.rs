//! The enterprise seam, from the enterprise crate's side: a fake registered on the configuration,
//! seeing exactly what `dbos::__enterprise` shows the real one.

// Runs off tokio too, through `#[dbos_test_support::test]`, so spawning and timers go through
// `dbos::__rt`: a direct tokio call would panic there. `clippy.toml` lists what that rules out.
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbos::__enterprise::{
    ConductorConnection, ConductorEngine, ConductorOptions, EnterpriseProvider, async_trait,
};
use dbos::{Config, DBOS, Enterprise, Error, StartOptions};

use dbos_test_support::{TestDatabase, test_database};

/// Nothing legitimate here takes this long, so a hang fails fast instead of holding CI.
const DEADLINE: Duration = Duration::from_secs(60);

const APP_VERSION: &str = "1.0.0";

/// What the fake saw, in order, and the engine it was handed.
#[derive(Default)]
struct Seen {
    events: Mutex<Vec<String>>,
    options: Mutex<Option<ConductorOptions>>,
    engine: Mutex<Option<Arc<ConductorEngine>>>,
}

impl Seen {
    fn record(&self, event: impl Into<String>) {
        self.events.lock().unwrap().push(event.into());
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn engine(&self) -> Arc<ConductorEngine> {
        Arc::clone(self.engine.lock().unwrap().as_ref().expect("never built"))
    }
}

struct FakeEnterprise {
    seen: Arc<Seen>,
    /// Never finishes stopping, like a client stuck on an unreachable Conductor.
    hangs_on_stop: bool,
}

impl EnterpriseProvider for FakeEnterprise {
    fn conductor(
        &self,
        engine: ConductorEngine,
        options: ConductorOptions,
    ) -> dbos::Result<Box<dyn ConductorConnection>> {
        self.seen.record("build");
        *self.seen.options.lock().unwrap() = Some(options);
        let engine = Arc::new(engine);
        *self.seen.engine.lock().unwrap() = Some(Arc::clone(&engine));
        Ok(Box::new(FakeConductor {
            seen: Arc::clone(&self.seen),
            engine,
            hangs_on_stop: self.hangs_on_stop,
        }))
    }
}

struct FakeConductor {
    seen: Arc<Seen>,
    engine: Arc<ConductorEngine>,
    hangs_on_stop: bool,
}

#[async_trait]
impl ConductorConnection for FakeConductor {
    fn start(&mut self) {
        self.seen.record("start");
    }

    async fn stop(&mut self) {
        if self.hangs_on_stop {
            std::future::pending::<()>().await;
        }
        // A read through the engine is what a real client does until it is stopped, so the system
        // database has to still be open here.
        let open = self.engine.sysdb().get_workflow("anything").await.is_ok();
        self.seen.record(format!("stop (database open: {open})"));
    }
}

fn config(app_name: &str, db: &TestDatabase) -> Config {
    Config {
        migrate: false,
        app_version: Some(APP_VERSION.to_owned()),
        ..Config::new(app_name, db.url())
    }
}

fn with_conductor(config: Config, seen: &Arc<Seen>) -> Config {
    with_fake(config, seen, false)
}

fn with_fake(config: Config, seen: &Arc<Seen>, hangs_on_stop: bool) -> Config {
    Config {
        conductor_key: Some("conductor-key".to_owned()),
        conductor_url: Some("wss://conductor.invalid/conductor/v1alpha1".to_owned()),
        enterprise: Some(Enterprise::new(FakeEnterprise {
            seen: Arc::clone(seen),
            hangs_on_stop,
        })),
        ..config
    }
}

/// Launch builds the client and starts it; shutdown stops it while the database is still open.
#[dbos_test_support::test]
async fn the_conductor_client_lives_inside_the_executor_s_lifetime() {
    let db = test_database().await;
    let seen = Arc::new(Seen::default());
    let mut metadata = serde_json::Map::new();
    metadata.insert("region".to_owned(), "eu".into());
    let dbos = DBOS::new(with_conductor(
        Config {
            executor_id: Some("configured".to_owned()),
            conductor_executor_metadata: Some(metadata.clone()),
            ..config("enterprise-lifecycle-app", &db)
        },
        &seen,
    ));

    dbos.launch().await.expect("launch failed");
    assert_eq!(seen.events(), ["build", "start"]);

    let options = seen.options.lock().unwrap().clone().expect("built");
    assert_eq!(options.url, "wss://conductor.invalid/conductor/v1alpha1");
    assert_eq!(options.key, "conductor-key");
    assert_eq!(options.app_name, "enterprise-lifecycle-app");
    assert_eq!(options.executor_metadata, Some(metadata));
    assert!(!options.metadata_only_mode);

    let engine = seen.engine();
    let executor_id = dbos.executor_id().expect("launched");
    assert_eq!(engine.executor_id(), executor_id);
    assert_ne!(executor_id, "configured", "a Conductor key replaces the id");
    assert_eq!(engine.app_name(), "enterprise-lifecycle-app");
    assert_eq!(engine.app_version(), APP_VERSION);
    assert_eq!(engine.dbos_version(), env!("CARGO_PKG_VERSION"));

    dbos.shutdown().await;
    assert_eq!(
        seen.events(),
        ["build", "start", "stop (database open: true)"]
    );
}

/// Conductor's recovery request: another executor's abandoned workflow goes back to a queue, and
/// this executor runs it.
#[dbos_test_support::test]
async fn the_engine_recovers_the_executors_conductor_names() {
    let db = test_database().await;
    let entered = Arc::new(AtomicU32::new(0));
    let reached_gate = Arc::new(tokio::sync::Notify::new());
    let build = |config: Config| {
        let dbos = DBOS::new(config);
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
                    dbos::__rt::sleep(Duration::from_secs(30)).await;
                    Ok::<u32, Error>(1)
                }
            })
            .unwrap();
        (dbos, workflow)
    };

    // Abandoned by an executor Conductor will later report gone.
    let id = "abandoned-by-gone";
    let (gone, workflow) = build(Config {
        executor_id: Some("gone".to_owned()),
        ..config("enterprise-recovery-app", &db)
    });
    gone.launch().await.expect("launch failed");
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
    dbos::__rt::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the workflow never entered its body");
    gone.shutdown().await;

    let seen = Arc::new(Seen::default());
    let (survivor, _workflow) = build(with_conductor(
        config("enterprise-recovery-app", &db),
        &seen,
    ));
    survivor.launch().await.expect("launch failed");
    assert_eq!(entered.load(Ordering::SeqCst), 1);

    let recovered = seen
        .engine()
        .recover(&["gone"])
        .await
        .expect("recovery failed");
    assert_eq!(recovered, [id]);
    dbos::__rt::timeout(DEADLINE, reached_gate.notified())
        .await
        .expect("the recovered workflow never ran");
    assert_eq!(entered.load(Ordering::SeqCst), 2);

    survivor.shutdown().await;
}

/// A client that never finishes stopping is abandoned, and shutdown still closes the database.
#[dbos_test_support::test]
async fn a_conductor_client_stuck_stopping_does_not_hold_shutdown() {
    let db = test_database().await;
    let seen = Arc::new(Seen::default());
    let dbos = DBOS::new(with_fake(config("enterprise-stuck-app", &db), &seen, true));
    dbos.launch().await.expect("launch failed");

    dbos::__rt::timeout(DEADLINE, dbos.shutdown())
        .await
        .expect("shutdown waited on the Conductor client");
    assert!(!dbos.is_launched());
    // A relaunch takes the lifecycle lock, which a stuck shutdown would still hold.
    dbos::__rt::timeout(DEADLINE, dbos.launch())
        .await
        .expect("relaunch waited on the previous shutdown")
        .expect("relaunch failed");
    dbos.shutdown().await;
}
