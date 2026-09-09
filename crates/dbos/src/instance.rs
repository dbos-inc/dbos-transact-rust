//! The instance and the executor: the two halves of the public lifecycle.

use std::sync::{Arc, RwLock};

use crate::config::{Config, Serializer};
use crate::connection::Connection;
use crate::identity::{Environment, Identity};
use crate::registry::{Registry, Snapshot};
use crate::sysdb::SystemDatabase;
use crate::workflow::Tasks;
use crate::{Error, Result};

/// The running half of DBOS, created by [`DBOS::launch`] and dropped by [`DBOS::shutdown`].
///
/// Everything that needs a database or a runtime lives here rather than on the instance, so that
/// "not launched" is one absent value rather than a scatter of `Option`s.
///
/// **The connection is held rather than embedded**, which is the split a [`Client`](crate::Client)
/// rests on. Every other field here exists because this process *runs* workflows: the id, version
/// and application name a claim stamps, the runtime they are spawned onto, the registry snapshot
/// they are resolved through, the task set shutdown reaches. A client runs none, so it holds the
/// connection alone, and everything both surfaces do is a method on that rather than on this
/// type.
pub struct Executor {
    conn: Arc<Connection>,
    executor_id: String,
    app_version: String,
    app_name: String,
    app_id: String,
    runtime: tokio::runtime::Handle,
    workflows: Snapshot,
    listen_queues: Option<Vec<String>>,
    tasks: Tasks,
}

impl Executor {
    /// Resolves this executor's identity, connects, and registers the running version.
    ///
    /// The mirror of [`shutdown`](Self::shutdown), and here for the same reason: these are the
    /// executor's own fields, so the executor is what sets them up. [`DBOS::launch`] is left with
    /// what is actually its business — deciding whether to build one at all, and installing it.
    ///
    /// Also **recovers**, returning the ids it re-enqueued. Done *here*, before launch returns,
    /// because what counts as abandoned must be decided before the application can start anything:
    /// a workflow started the instant launch returns is `PENDING` under this executor's id too,
    /// and a sweep running any later would tear it off its runner and offer it to the fleet. It is
    /// one write rather than a list of workflows to run, so nothing about it is backgrounded.
    async fn start(config: &Config, workflows: Snapshot) -> Result<(Self, Vec<String>)> {
        config.validate()?;

        if workflows.is_empty() {
            tracing::warn!(
                "no workflows are registered: this executor will recover nothing and dequeue \
                 nothing. Register before calling `launch`."
            );
        }

        // The name, the version and the executor id are the deployment's business as much as the
        // configuration's, and one place reconciles them.
        let Identity {
            app_name,
            app_version,
            executor_id,
            app_id,
        } = crate::identity::resolve(config, &Environment::read())?;

        let conn = Connection::for_application(config, &executor_id, &app_name).await?;

        // Everything that can fail after the connect goes through one call, so there is one error
        // path and it closes the handle. `connect` has already spawned the listener and the
        // notifier, and the listener's only way out of its loop is the pool closing — so a handle
        // dropped without `close` leaves both tasks and every connection alive for the life of the
        // process, with each retried `launch` adding another set.
        let recovered = match prepare(conn.sysdb(), &executor_id, &app_version).await {
            Ok(recovered) => recovered,
            Err(error) => {
                conn.close().await;
                return Err(error);
            }
        };

        tracing::info!(app_name, executor_id, app_version, "DBOS launched");

        let executor = Self {
            conn: Arc::new(conn),
            executor_id,
            app_version,
            app_id,
            // The runtime is the executor's, taken here. `start` is `async`, so a
            // runtime is necessarily current.
            runtime: tokio::runtime::Handle::current(),
            workflows,
            app_name,
            listen_queues: config.listen_queues.clone(),
            tasks: Tasks::default(),
        };
        Ok((executor, recovered))
    }

    /// The connection this executor talks through, which is all a caller needs to read or write a
    /// row.
    ///
    /// Handed out rather than proxied so that everything both surfaces do can be a method on the
    /// connection and be called by a [`Client`](crate::Client) too — see that module.
    pub(crate) fn connection(&self) -> &Arc<Connection> {
        &self.conn
    }

    /// The system database this executor is running against.
    pub(crate) fn sysdb(&self) -> &dyn SystemDatabase {
        self.conn.sysdb()
    }

    /// Identifies this process among the executors sharing the database.
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// Names the application whose rows this executor owns.
    ///
    /// **Its own, and not an [`Option`].** The connection reports a name that may be absent,
    /// because the one a [`Client`](crate::Client) opens may speak for every application at once.
    /// An executor may not: the identity is resolved and validated at launch, before anything
    /// connects, and the name that wins — on DBOS Cloud the deployment's `DBOS_APP_NAME` rather
    /// than [`Config::app_name`](crate::Config::app_name) — is the ownership key on every row this
    /// process writes. Holding it here rather than unwrapping the database's is what keeps that a
    /// fact of the type instead of an invariant each caller has to remember.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// The workflows this executor started and has not seen finish.
    pub(crate) fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// Which queues this executor dequeues from, or `None` for all of them.
    ///
    /// Captured at launch: it is configuration, not a knob. The *set* it narrows is still rebuilt
    /// from the table on every sweep, which is where the dynamic behaviour lives.
    pub(crate) fn listen_queues(&self) -> Option<&[String]> {
        self.listen_queues.as_deref()
    }

    /// How often an adopting caller asks whether the run that won has finished.
    pub(crate) fn outcome_poll_interval(&self) -> std::time::Duration {
        self.conn.outcome_poll_interval()
    }

    /// The version of the application's code, as workflow rows record it.
    ///
    /// **`app_` here, `application_` in `sysdb`**, and the two meet at exactly this call: the
    /// engine names the pair [`app_name`](Self::app_name) and `app_version`, matching
    /// [`APP_VERSION_ENV`](crate::APP_VERSION_ENV) and the `DBOS__APPVERSION` every
    /// implementation reads, while `sysdb` spells them `application_name` and
    /// `application_version` because those are the columns' own names. The conversion happens
    /// where a value crosses into a `sysdb` type — `NewWorkflow { application_version:
    /// Some(executor.app_version()), .. }` — which is the same boundary every other schema word
    /// stops at.
    pub fn app_version(&self) -> &str {
        &self.app_version
    }

    /// This application's DBOS Cloud id, empty when the process is not running on DBOS Cloud.
    ///
    /// Read from [`APP_ID_ENV`](crate::APP_ID_ENV) and from nowhere else: an id is issued to a
    /// deployment rather than chosen by an application, so there is no configuration field for it.
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// How payloads are encoded.
    pub(crate) fn serializer(&self) -> &Serializer {
        self.conn.serializer()
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
    /// Here rather than in [`DBOS::shutdown`] because this is where the things being stopped live:
    /// the task set holding running workflows and the dequeue loop, and the connection. Teardown
    /// that grows a step at a time wants one place to grow, which is also how Java draws it —
    /// `DBOS.shutdown()` calls `DBOSExecutor.close()` and knows nothing about what that entails.
    ///
    /// Takes `&self`, not `self`: the instance drops its handle here, but a workflow still running
    /// may hold another, and shutting down is precisely the moment when that is true.
    pub(crate) async fn shutdown(&self) {
        // Cancel, wait, then close — and the wait is what the ordering rests on. `abort` only
        // schedules cancellation at a task's next yield point, so without waiting this would close
        // the pool while the bodies it cancelled were still running, and returning would mean
        // "told to stop" rather than "stopped". Waiting is also what lets a relaunch in this
        // process start against a genuinely quiet executor.
        //
        // What keeps an interrupted workflow from being *recorded* as failed is not this ordering
        // but `Error::control`, which treats every system-database failure as a signal rather than
        // an outcome. Cancelling leaves every row `PENDING`, which is what a later executor
        // recovers.
        let cancelled = self.tasks.abort_all().await;
        if cancelled > 0 {
            tracing::info!(
                cancelled,
                "cancelled workflows still running; they stay PENDING"
            );
        }
        self.conn.close().await;
    }
}

impl std::fmt::Debug for Executor {
    /// Hand-written because a `dyn SystemDatabase` is not `Debug` and should not become so: the
    /// trait exists to keep a driver type out of the engine, and a `Debug` bound would put one
    /// back in through the derive.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executor")
            .field("executor_id", &self.executor_id)
            .field("app_version", &self.app_version)
            .finish_non_exhaustive()
    }
}

/// A DBOS instance: the whole public surface of the library.
///
/// Cheap to clone — it is an `Arc` internally, so a clone is another handle to the same instance
/// rather than another instance. Clone it into whatever holds application state.
///
/// The instance owns the configuration and the workflow registry, holding them for the life of the
/// application. [`launch`](Self::launch) constructs an [`Executor`] and
/// [`shutdown`](Self::shutdown) drops it, so relaunching in one process is supported — which is
/// why this is an instance whose state changes rather than a builder that becomes a handle. A
/// type-state would make "not launched" a compile error and would also make relaunching
/// inexpressible.
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

        // Freezing the registry and reading it are one act, so a registration cannot land between
        // the two and be handed back a `WorkflowRef` for a workflow this executor will not have.
        let workflows = self.0.registry.snapshot();
        let (executor, recovered) = match Executor::start(&self.0.config, workflows).await {
            Ok(started) => started,
            Err(error) => {
                // Nothing was installed, so nothing holds the snapshot and registration is open
                // again. Leaving it frozen would make one failed launch permanent.
                self.0.registry.thaw();
                return Err(error);
            }
        };
        let executor = Arc::new(executor);
        *self.write_executor() = Some(Arc::clone(&executor));
        // Recovery already happened, inside `Executor::start`: it is one write, and doing it before
        // launch returns is what keeps it clear of workflows the application starts next. What is
        // left is to start polling, which is also what will run the re-enqueued work.
        if !recovered.is_empty() {
            tracing::debug!(
                workflows = recovered.len(),
                "recovered workflows are on their queues"
            );
        }
        // The dequeue loop starts unconditionally, because a queue this process never registered
        // is still one it should dequeue from: the worker set is rebuilt from the `queues` table
        // on every supervisor sweep, not from this instance's `register_queue` calls. A queue
        // another *process of this application* registered is therefore picked up, as is one
        // registered against this instance after launch, without a restart.
        //
        // Another *application's* queue is not, and that is the boundary: the search scopes to
        // this application's rows plus the unclaimed ones, so a peer application's backlog is
        // never taken. Sharing a backlog is what a fleet of one application does; reaching into
        // another's would be the same redirection that makes registering over its queue name an
        // error.
        crate::dequeue::spawn(executor);
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
        // The instance outlives the executor, so registration opens again: relaunching takes a
        // fresh snapshot, and what that executor holds is its own.
        self.0.registry.thaw();
        tracing::info!(app_name = self.0.config.app_name, "DBOS shut down");
    }

    /// Identifies this process among the executors sharing the database.
    pub fn executor_id(&self) -> Result<String> {
        Ok(self.executor("executor_id")?.executor_id().to_owned())
    }

    /// The version of the application's code, as workflow rows record it.
    pub fn app_version(&self) -> Result<String> {
        Ok(self.executor("app_version")?.app_version().to_owned())
    }

    /// This application's DBOS Cloud id, empty off DBOS Cloud.
    pub fn app_id(&self) -> Result<String> {
        Ok(self.executor("app_id")?.app_id().to_owned())
    }

    /// The running executor, or [`Error::NotLaunched`] naming the operation that wanted it.
    ///
    /// Java's `ensureLaunched(caller)`, and for its reason: the useful half of the error is which
    /// call was too early.
    pub(crate) fn executor(&self, operation: &'static str) -> Result<Arc<Executor>> {
        self.read_executor().ok_or(Error::NotLaunched {
            operation: operation.into(),
        })
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

/// Registers this version and returns what a previous run of this executor abandoned to its queue.
///
/// One function rather than two calls inline so that [`Executor::start`] has a single error path
/// to close the system database on; see the comment at its call site. Everything between the
/// connect and the executor being built belongs here for that reason, and anything added later
/// belongs here too.
async fn prepare(
    sysdb: &dyn SystemDatabase,
    executor_id: &str,
    app_version: &str,
) -> Result<Vec<String>> {
    register_version(sysdb, app_version).await?;
    crate::recovery::reenqueue(sysdb, executor_id, app_version).await
}

/// Registers this version and warns if it is not the one a rolling deploy would prefer.
///
/// A warning rather than a refusal: running an older version alongside a newer one is exactly what
/// a rollback looks like, and it is the deployment's call. What it must not be is silent, because
/// the symptom — an executor that starts cleanly and is handed no unversioned work — reads as a
/// broken queue rather than as a deliberate policy.
async fn register_version(sysdb: &dyn SystemDatabase, version: &str) -> Result<()> {
    sysdb
        .create_application_version(version, None)
        .await
        .map_err(Error::SystemDatabase)?;
    match sysdb
        .get_latest_application_version(None)
        .await
        .map_err(Error::SystemDatabase)?
    {
        Some(latest) if latest.version_name != version => {
            tracing::warn!(
                app_version = version,
                latest_version = latest.version_name,
                "this executor is not running the latest registered application version: it will \
                 recover and dequeue only work stamped with its own version"
            );
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            app_version: Some("1.0.0".to_owned()),
            ..Config::new("test-app", "postgres://localhost/nothing")
        }
    }

    #[tokio::test]
    async fn nothing_is_launched_until_launch() {
        let dbos = DBOS::new(config());
        assert!(!dbos.is_launched());
        let err = dbos.app_version().unwrap_err();
        assert!(
            matches!(
                err,
                Error::NotLaunched {
                    operation: std::borrow::Cow::Borrowed("app_version")
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
        let dbos = DBOS::new(Config {
            app_version: Some("1.0.0".to_owned()),
            ..Config::new("No", "postgres://localhost/nothing")
        });
        let err = dbos.launch().await.unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(!dbos.is_launched());
    }

    /// The registry lives on the instance, so anything a registered closure captures is held by
    /// the instance holding it. A closure capturing a `DBOS` is therefore a cycle and the
    /// instance — with its executor and its connection pool — is never freed, which is why
    /// nothing the engine puts in there may capture one. This pins that: `register_workflow` must
    /// not quietly store a handle to `self` alongside the caller's closure, and the workflow-facing
    /// calls must stay reachable without one.
    #[test]
    fn registration_does_not_make_the_instance_hold_itself() {
        let dbos = DBOS::new(config());
        let alive = Arc::downgrade(&dbos.0);
        dbos.register_workflow("noop", |()| async { Ok::<_, Error>(()) })
            .unwrap();
        drop(dbos);
        assert_eq!(
            alive.strong_count(),
            0,
            "the instance outlived every handle to it: something in the registry holds it"
        );
    }

    #[test]
    fn debug_does_not_print_the_database_url() {
        let dbos = DBOS::new(Config::new("test-app", "postgres://user:hunter2@host/db"));
        let shown = format!("{dbos:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("test-app"), "{shown}");
    }
}
