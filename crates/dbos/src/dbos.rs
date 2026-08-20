//! The instance and the executor: the two halves of the public lifecycle.

use std::sync::{Arc, RwLock};

use crate::config::{APP_VERSION_ENV, Config, Serializer};
use crate::registry::{Registry, Snapshot};
use crate::sysdb::{SystemDatabase, postgres};
use crate::workflow::Tasks;
use crate::{Error, Result};

/// The running half of DBOS, created by [`DBOS::launch`] and dropped by [`DBOS::shutdown`].
///
/// Everything that needs a database or a runtime lives here rather than on the instance, so that
/// "not launched" is one absent value rather than a scatter of `Option`s.
pub struct Executor {
    sysdb: Box<dyn SystemDatabase>,
    executor_id: String,
    application_version: String,
    serializer: Serializer,
    runtime: tokio::runtime::Handle,
    workflows: Snapshot,
    app_name: String,
    tasks: Tasks,
}

/// The executor is assembled whole here rather than grown a field at a time, so several of these
/// are read only by the commits that follow — registration, workflows, steps. Resolving them at
/// launch is the point: an executor that exists is one whose identity is already settled.
#[allow(
    dead_code,
    reason = "consumed by the workflow commits; resolved at launch regardless"
)]
impl Executor {
    /// Resolves this executor's identity, connects, and registers the running version.
    ///
    /// The mirror of [`shutdown`](Self::shutdown), and here for the same reason: these are the
    /// executor's own fields, so the executor is what sets them up. [`DBOS::launch`] is left with
    /// what is actually its business — deciding whether to build one at all, and installing it.
    async fn start(config: &Config, workflows: Snapshot) -> Result<Self> {
        config.validate()?;

        if workflows.is_empty() {
            tracing::warn!(
                "no workflows are registered: this executor will recover nothing and dequeue \
                 nothing. Register before calling `launch`."
            );
        }

        let executor_id = config
            .executor_id
            .clone()
            .unwrap_or_else(|| "local".to_owned());
        let application_version = match &config.application_version {
            Some(version) => version.clone(),
            None => compute_application_version(&config.app_name).await?,
        };

        let sysdb = postgres::PostgresSystemDatabase::connect(&postgres::Config {
            url: &config.database_url,
            max_connections: config.max_connections,
            use_listen_notify: config.use_listen_notify,
            migrate: config.migrate,
            settings: postgres::Settings {
                schema: &config.schema,
                executor_id: Some(&executor_id),
                application_name: Some(&config.app_name),
                polling_concurrency: config.polling_concurrency,
                notification_coalesce: config.notification_coalesce,
                ..postgres::Settings::default()
            },
        })
        .await?;

        register_version(&sysdb, &application_version).await?;

        tracing::info!(
            app_name = config.app_name,
            executor_id,
            application_version,
            "DBOS launched"
        );

        Ok(Self {
            sysdb: Box::new(sysdb),
            executor_id,
            application_version,
            serializer: config.serializer.clone(),
            // Decision 20: the runtime is the executor's, taken here. `start` is `async`, so a
            // runtime is necessarily current.
            runtime: tokio::runtime::Handle::current(),
            workflows,
            app_name: config.app_name.clone(),
            tasks: Tasks::default(),
        })
    }

    /// The system database this executor is running against.
    pub(crate) fn sysdb(&self) -> &dyn SystemDatabase {
        &*self.sysdb
    }

    /// Identifies this process among the executors sharing the database.
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// Names the application whose rows this executor owns.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// The workflows this executor started and has not seen finish.
    pub(crate) fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// The version of the application's code, as workflow rows record it.
    pub fn application_version(&self) -> &str {
        &self.application_version
    }

    /// How payloads are encoded.
    pub(crate) fn serializer(&self) -> &Serializer {
        &self.serializer
    }

    /// The workflows this executor was launched with.
    ///
    /// A snapshot, frozen at launch. Recovery reads it while the application may still be calling
    /// into the instance, and a map that could grow underneath a recovery pass is a recovery pass
    /// that may or may not find a workflow depending on timing.
    pub(crate) fn workflows(&self) -> &Snapshot {
        &self.workflows
    }

    /// The runtime workflows are spawned onto.
    ///
    /// Captured at launch rather than taken from whoever is calling, for two reasons that both
    /// come down to ownership. Recovery spawns from a background task rather than from user code,
    /// so "the caller's runtime" is not one answer. And shutdown has to abort exactly the tasks
    /// this executor started, which needs a handle it chose rather than one it inherited.
    pub(crate) fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    /// Stops everything this executor started and closes the system database.
    ///
    /// Here rather than in [`DBOS::shutdown`] because this is where the things being stopped live,
    /// and there will be more of them: the task set holding running workflows, and the recovery
    /// task. Teardown that grows a step at a time wants one place to grow, which is also how Java
    /// draws it — `DBOS.shutdown()` calls `DBOSExecutor.close()` and knows nothing about what that
    /// entails.
    ///
    /// Takes `&self`, not `self`: the instance drops its handle here, but a workflow still running
    /// may hold another, and shutting down is precisely the moment when that is true.
    pub(crate) async fn shutdown(&self) {
        // Cancel first, then close: a workflow still running would otherwise fail its next query
        // against a closed pool and record an *error* outcome, turning an interrupted workflow
        // into a permanently failed one. Cancelling leaves every row `PENDING`, which is what a
        // later executor recovers.
        let cancelled = self.tasks.abort_all();
        if cancelled > 0 {
            tracing::info!(
                cancelled,
                "cancelled workflows still running; they stay PENDING"
            );
        }
        self.sysdb.close().await;
    }
}

impl std::fmt::Debug for Executor {
    /// Hand-written because a `dyn SystemDatabase` is not `Debug` and should not become so: the
    /// trait exists to keep a driver type out of the engine, and a `Debug` bound would put one
    /// back in through the derive.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executor")
            .field("executor_id", &self.executor_id)
            .field("application_version", &self.application_version)
            .finish_non_exhaustive()
    }
}

/// A DBOS instance: the whole public surface of the library.
///
/// Cheap to clone — it is an `Arc` internally, so a clone is another handle to the same instance
/// rather than another instance. Clone it into whatever holds application state.
///
/// The instance owns configuration and, from the next commit, the workflow registry; it holds them
/// for the life of the application. [`launch`](Self::launch) constructs an [`Executor`] and
/// [`shutdown`](Self::shutdown) drops it, so relaunching in one process is supported — which is why
/// this is an instance whose state changes rather than a builder that becomes a handle. A type-state
/// would make "not launched" a compile error and would also make relaunching inexpressible.
#[derive(Clone)]
pub struct DBOS(Arc<Inner>);

struct Inner {
    config: Config,
    /// Every workflow registered so far. Lives on the instance rather than the executor, because
    /// registration happens before there is an executor.
    registry: Registry,
    /// `None` until launched. Read on every operation, written twice in a process's life, so a
    /// reader-biased lock rather than the async one below.
    executor: RwLock<Option<Arc<Executor>>>,
    /// Serializes launch against shutdown, and either against itself.
    ///
    /// **This guard is deliberately held across await points** — `Executor::start` connects and
    /// migrates — which is exactly why it is a `tokio` mutex and the slot above is a `std` one.
    /// The two are not interchangeable. A `std` guard is `!Send` and blocks the *thread*, so
    /// holding one across an await stops the future being `Send` and risks a worker thread
    /// blocking on a lock whose holder needs that same thread to make progress. A `tokio` guard
    /// parks the *task*, and is meant to be held.
    ///
    /// The rule this leaves is narrow and worth stating in those terms: no guard from the `std`
    /// `RwLock` above ever crosses an await, which `the_lifecycle_futures_are_send` checks by
    /// construction rather than by inspection.
    lifecycle: tokio::sync::Mutex<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // `shutdown` is async and `Drop` is not, so this can only report. Closing here would mean
        // blocking on a runtime from inside a drop, which deadlocks when the drop is itself on
        // that runtime.
        if self.executor.get_mut().is_ok_and(|slot| slot.is_some()) {
            tracing::warn!(
                app_name = self.config.app_name,
                "DBOS dropped while still launched: call `shutdown().await` to close the system \
                 database cleanly. Its connections are closed by the pool's own drop, but any \
                 workflow still running is left where it stood."
            );
        }
    }
}

impl DBOS {
    /// A new instance. Nothing is connected until [`launch`](Self::launch).
    pub fn new(config: Config) -> Self {
        Self(Arc::new(Inner {
            config,
            registry: Registry::default(),
            executor: RwLock::new(None),
            lifecycle: tokio::sync::Mutex::new(()),
        }))
    }

    /// The configuration this instance was built with.
    pub fn config(&self) -> &Config {
        &self.0.config
    }

    /// Everything registered so far.
    pub(crate) fn registry(&self) -> &Registry {
        &self.0.registry
    }

    /// Whether an executor is running.
    pub fn is_launched(&self) -> bool {
        self.read_executor().is_some()
    }

    /// Connects to the system database, migrates it, registers this application version, and starts
    /// the executor.
    ///
    /// **Idempotent**: launching an already-launched instance warns and returns, which is what
    /// Python does (`_launch`) and what Java's compare-and-set amounts to. It is not an error
    /// because the common cause is two entry points both being defensive, and failing there would
    /// punish the caller who did nothing wrong.
    pub async fn launch(&self) -> Result<()> {
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.read_executor().is_some() {
            tracing::warn!("DBOS was already launched");
            return Ok(());
        }

        let executor = Executor::start(&self.0.config, self.0.registry.snapshot()).await?;
        *self.write_executor() = Some(Arc::new(executor));
        Ok(())
    }

    /// Stops the executor and closes the system database.
    ///
    /// **Idempotent**: shutting down an instance that is not launched does nothing, as in Java.
    /// Relaunching afterwards is supported and is what test suites do.
    pub async fn shutdown(&self) {
        let _lifecycle = self.0.lifecycle.lock().await;
        let Some(executor) = self.write_executor().take() else {
            return;
        };
        executor.shutdown().await;
        tracing::info!(app_name = self.0.config.app_name, "DBOS shut down");
    }

    /// Identifies this process among the executors sharing the database.
    pub fn executor_id(&self) -> Result<String> {
        Ok(self.executor("executor_id")?.executor_id().to_owned())
    }

    /// The version of the application's code, as workflow rows record it.
    pub fn application_version(&self) -> Result<String> {
        Ok(self
            .executor("application_version")?
            .application_version()
            .to_owned())
    }

    /// The running executor, or [`Error::NotLaunched`] naming the operation that wanted it.
    ///
    /// Java's `ensureLaunched(caller)`, and for its reason: the useful half of the error is which
    /// call was too early.
    pub(crate) fn executor(&self, operation: &'static str) -> Result<Arc<Executor>> {
        self.read_executor().ok_or(Error::NotLaunched { operation })
    }

    fn read_executor(&self) -> Option<Arc<Executor>> {
        // A poisoned lock here cannot mean torn state: the guarded value is one `Option<Arc<_>>`
        // and every section under the lock is infallible, so a panic elsewhere leaves it whole.
        self.0
            .executor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn write_executor(&self) -> std::sync::RwLockWriteGuard<'_, Option<Arc<Executor>>> {
        self.0
            .executor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for DBOS {
    /// Hand-written because the derive would print the whole configuration, including the database
    /// URL and whatever credentials it carries.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBOS")
            .field("app_name", &self.0.config.app_name)
            .field("launched", &self.is_launched())
            .finish_non_exhaustive()
    }
}

/// Registers this version and warns if it is not the one a rolling deploy would prefer.
///
/// A warning rather than a refusal: running an older version alongside a newer one is exactly what
/// a rollback looks like, and it is the deployment's call. What it must not be is silent, because
/// the symptom — an executor that starts cleanly and is handed no unversioned work — reads as a
/// broken queue rather than as a deliberate policy.
async fn register_version(sysdb: &impl SystemDatabase, version: &str) -> Result<()> {
    sysdb.create_application_version(version, None).await?;
    match sysdb.get_latest_application_version(None).await? {
        Some(latest) if latest.version_name != version => {
            tracing::warn!(
                application_version = version,
                latest_version = latest.version_name,
                "this executor is not running the latest registered application version: it will \
                 recover and dequeue only work stamped with its own version"
            );
        }
        _ => {}
    }
    Ok(())
}

/// The SHA-256 of the running executable, with the application name mixed in.
///
/// Go's rule, plus §4.14's application name — without which two applications shipping one binary
/// would share a version, and each would treat the other's workflows as its own to recover.
///
/// Hashing the *binary* is the strictest reading of "the code that started this workflow", and it
/// is what makes recovery safe across a deploy. It is also why development wants
/// `DBOS__APPVERSION`: a rebuild changes the hash, so a crash and a restart around one leave the
/// earlier run's `PENDING` rows to an executor that no longer exists.
async fn compute_application_version(app_name: &str) -> Result<String> {
    let app_name = app_name.to_owned();
    // Reading a binary is blocking and it can be tens of megabytes; launching is exactly the moment
    // where a stalled runtime is least visible.
    tokio::task::spawn_blocking(move || {
        use sha2::Digest as _;

        use std::io::Read as _;

        let exe = std::env::current_exe().map_err(|e| version_error("find", &e))?;
        let mut file = std::fs::File::open(&exe).map_err(|e| version_error("read", &e))?;
        let mut hasher = sha2::Sha256::new();
        // The name and the bytes are separated by a byte that cannot appear in a name, so that no
        // two (name, binary) pairs can produce the same input.
        hasher.update(app_name.as_bytes());
        hasher.update([0u8]);

        // In chunks rather than into memory: an executable is tens of megabytes, and there is
        // nothing to be gained by holding all of it at once.
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut chunk)
                .map_err(|e| version_error("read", &e))?;
            if read == 0 {
                break;
            }
            hasher.update(&chunk[..read]);
        }

        let mut hex = String::with_capacity(64);
        for byte in hasher.finalize() {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
        }
        Ok(hex)
    })
    .await
    .map_err(|e| Error::Config(format!("could not compute the application version: {e}")))?
}

fn version_error(verb: &str, cause: &std::io::Error) -> Error {
    Error::Config(format!(
        "could not {verb} the running executable to compute the application version ({cause}); \
         set `application_version`, or the {APP_VERSION_ENV} environment variable"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config::new("test-app", "postgres://localhost/nothing")
    }

    #[tokio::test]
    async fn nothing_is_launched_until_launch() {
        let dbos = DBOS::new(config());
        assert!(!dbos.is_launched());
        let err = dbos.application_version().unwrap_err();
        assert!(
            matches!(
                err,
                Error::NotLaunched {
                    operation: "application_version"
                }
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn not_launched_names_the_operation_that_was_too_early() {
        let dbos = DBOS::new(config());
        let err = dbos.executor("do_something").unwrap_err();
        assert_eq!(
            err.to_string(),
            "cannot do_something before DBOS is launched"
        );
    }

    #[test]
    fn the_lifecycle_futures_are_send() {
        // A `std::sync::RwLockWriteGuard` is `!Send`, so a future holding one across an await is
        // `!Send` and this stops compiling. That makes the "no `std` guard crosses an await" rule
        // a compile-time check rather than a comment — and `shutdown` is where it is easy to get
        // wrong, since it takes the write guard and then awaits the executor's teardown.
        fn assert_send<F: Send>(_: F) {}
        let dbos = DBOS::new(config());
        assert_send(dbos.launch());
        assert_send(dbos.shutdown());
    }

    #[tokio::test]
    async fn shutting_down_an_unlaunched_instance_does_nothing() {
        let dbos = DBOS::new(config());
        dbos.shutdown().await;
        assert!(!dbos.is_launched());
    }

    #[tokio::test]
    async fn an_invalid_config_is_refused_before_anything_is_connected() {
        let dbos = DBOS::new(Config::new("No", "postgres://localhost/nothing"));
        let err = dbos.launch().await.unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(!dbos.is_launched());
    }

    #[tokio::test]
    async fn the_version_is_the_executable_hash_and_it_is_stable_across_calls() {
        let once = compute_application_version("test-app").await.unwrap();
        let twice = compute_application_version("test-app").await.unwrap();
        assert_eq!(once, twice);
        assert_eq!(once.len(), 64, "a SHA-256 in hex");
        assert!(once.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn the_application_name_mixes_into_the_version() {
        // Two applications shipping one binary must not share a version, or each would treat the
        // other's abandoned workflows as its own to recover.
        let one = compute_application_version("app-one").await.unwrap();
        let other = compute_application_version("app-two").await.unwrap();
        assert_ne!(one, other);
    }

    #[test]
    fn debug_does_not_print_the_database_url() {
        let dbos = DBOS::new(Config::new("test-app", "postgres://user:hunter2@host/db"));
        let shown = format!("{dbos:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("test-app"), "{shown}");
    }
}
