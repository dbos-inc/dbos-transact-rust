//! The client: reaching a DBOS application from outside it.
//!
//! A [`Client`] talks only to the system database. It enqueues workflows, sends them messages,
//! reads their events, waits on their results, and manages the queues they run on — without
//! registering a workflow, running one, or holding any of the application's code. The natural
//! callers are the processes an application is surrounded by rather than the application itself:
//! an HTTP front end that hands work to a worker fleet, a cron host, an operator's tool, a test
//! that drives an application it did not start.
//!
//! A [`Client`] is a separate type over the system database alone, not a mode of the runtime. It
//! holds a connection — the system-database handle, the serializer, the application name, the poll
//! interval — and no [`Executor`](crate::Executor), because an executor is the machinery for
//! running workflows and a client runs none. What both surfaces need is a method on that
//! connection, so a client calls it with no executor in sight.
//!
//! The type system then makes the boundary concrete twice over. Nothing on this type can fail with
//! [`Error::NotLaunched`], because **a client is always connected** — [`connect`](Client::connect)
//! hands back a usable client or an error, with no second state to check afterwards. And nothing on
//! it can run a workflow, because it holds nothing that could.
//!
//! # What a client is not
//!
//! **It never runs a workflow.** There is no registry behind it, so a workflow it enqueues is one
//! *some other process* is expected to have registered — it names the workflow by its
//! [`QualifiedWorkflowName`], and the row waits on its queue until an executor that registered that
//! name dequeues it. A name nothing registers is not an error here and cannot be: the client has no
//! way to know, and the whole point is that the code lives elsewhere.
//!
//! **It never migrates**, and refuses a database no application has created. Connecting with
//! migrations off verifies the schema rather than ignoring it, so a missing or outdated one is
//! reported at [`connect`](Client::connect) naming the version it found, rather than as whatever
//! SQL error the first real call happens to raise.
//!
//! **It has no application version of its own**, so it stamps none unless an enqueue names one.
//! A client's binary is not the application's.
//!
//! What this module holds is the part that is a client's alone: connecting without an executor.
//! The surfaces a client shares with an application live with their types — enqueueing a workflow
//! by name in [`enqueue`](crate::enqueue), the queue operations in [`queue`](crate::queue),
//! messages in [`message`](crate::message), events in [`event`](crate::event), application
//! versions in [`version`](crate::version), and workflow management in
//! [`management`](crate::management).

use std::sync::Arc;
use std::time::Duration;

use crate::config::SYSTEM_DATABASE_URL_ENV;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::identity::validate_app_name;
use crate::serialization::Serializer;
use crate::sysdb::DEFAULT_SCHEMA;
use crate::sysdb::types::WorkflowStatus;
use crate::{Queue, QueueChange, QueueConflict, QueueOptions};

/// Everything a [`Client`] needs.
///
/// Constructed with [`ClientConfig::new`] or [`ClientConfig::from_env`] and adjusted by functional
/// update, exactly as [`Config`](crate::Config) is:
///
/// ```no_run
/// # use dbos::ClientConfig;
/// let config = ClientConfig {
///     app_name: Some("my-app".to_owned()),
///     ..ClientConfig::from_env()
/// };
/// ```
///
/// **A separate type from [`Config`](crate::Config) rather than a mode of it**, because the two
/// differ in what they are *allowed to say*, not only in what they happen to set. A client has no
/// executor id (it claims nothing), no application version (it runs nothing), no listen set (it
/// dequeues nothing) and no `migrate` (it is a guest in the database). Four fields that must not be
/// set is four fields that should not exist, and an application name that may be absent is a fifth
/// difference in the opposite direction.
#[derive(Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// The application this client acts for, or `None` to act for every application.
    ///
    /// **The one field that may be absent, and the choice it makes is real.** Named, this client
    /// is that application: rows it writes are stamped with the name and belong to it, and a search
    /// it makes is scoped to it. Nameless, it writes *unclaimed* rows — which every application
    /// matches, and any may run — and reads across all of them.
    ///
    /// **Serving all applications includes editing their rows.** A registration a named
    /// application already owns keeps its owner, but a nameless client still writes through it: it
    /// replaces a queue's limits, a schedule's definition, or the timestamp deciding which version
    /// is latest, where a client naming a *different* application would be refused. Whether a
    /// nameless writer should get through is an open question shared with the other SDKs on the
    /// same database; until it is settled, name this client if it is meant to touch only its own.
    ///
    /// Set it whenever several applications share a system database. Leave it unset for a tool
    /// whose job is to look at all of them.
    ///
    /// Held to the same rule a launched application's name is: three to 256 characters of
    /// lowercase letters, digits, dashes and underscores.
    pub app_name: Option<String>,

    /// Connection URL for the system database.
    pub database_url: String,

    /// Maximum pooled connections.
    ///
    /// Smaller than an application's default: a client runs no workflows, so its pool serves the
    /// calls its caller makes rather than a fleet of executing bodies.
    pub max_connections: u32,

    /// Schema holding the DBOS tables.
    ///
    /// Must match the application's — a client that looks in the wrong schema finds an empty
    /// database rather than an error.
    pub schema: String,

    /// How payloads are encoded, by default.
    ///
    /// **Must match the application's** where it is [custom](Serializer::Custom): a workflow this
    /// client enqueues in a serializer its executor is not configured with cannot be read, and
    /// neither can that executor's results here. The built-in encodings are readable by both
    /// sides whatever each is configured with.
    pub serializer: Serializer,

    /// Whether to listen for notifications rather than poll.
    ///
    /// **Off by default.** With it off, [`get_event`](crate::Client::get_event) looks again every
    /// second, and [`read_stream`](crate::Client::read_stream) every
    /// [`polling_interval`](crate::ReadStreamOptions::polling_interval). Both find a value
    /// promptly whatever the application is configured with.
    ///
    /// On, a listener wakes those waits as soon as the value is written, provided the
    /// application's executors push notifications: they run on PostgreSQL with their own
    /// `use_listen_notify` on. Turn it on only when they do. Against executors that don't push, an
    /// opted-in client hears nothing, and each event wait falls back to re-reading once a minute.
    /// The listener's startup check can't catch this: it proves this client can receive a
    /// notification, not that the application sends any. A stream read is safe either way: it
    /// keeps re-reading on its own interval, because it also watches for the producer to finish,
    /// which nothing announces.
    ///
    /// On also starts the notifier that pushes this process's own writes to other processes. A
    /// client writes no events or stream values, so it has nothing to send.
    ///
    /// It makes no difference to a wait on a workflow's outcome. An outcome is announced on no
    /// channel, so a result wait looks again every
    /// [`outcome_poll_interval`](field@Self::outcome_poll_interval) whatever this is set to.
    /// CockroachDB has no LISTEN/NOTIFY, so there the client polls whatever this says.
    pub use_listen_notify: bool,

    /// How many polling reads may run at once: half the pool by default; see
    /// [`PollingConcurrency`](crate::PollingConcurrency).
    pub polling_concurrency: crate::PollingConcurrency,

    /// How often a caller waiting on a workflow asks whether it has finished.
    ///
    /// `None` is one second. Every wait a client makes on an outcome is this kind — it owns none of
    /// the workflows it watches — so this is the interval that decides how promptly it learns of
    /// one, and, an outcome having no channel to be announced on,
    /// [`use_listen_notify`](Self::use_listen_notify) does not shorten it.
    pub outcome_poll_interval: Option<Duration>,

    /// How long a written key waits for company before a wakeup is pushed for it.
    ///
    /// `None` is ten milliseconds; `Some(Duration::ZERO)` turns coalescing off.
    pub notification_coalesce: Option<Duration>,

    /// How long a system-database connection may sit idle inside an open transaction before the
    /// server ends its session.
    ///
    /// Sixty seconds by default; see [`IdleTransactionTimeout`](crate::IdleTransactionTimeout).
    pub idle_transaction_timeout: crate::IdleTransactionTimeout,
}

/// Hand-written rather than derived, so that the database URL's passwords are never printed.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            app_name,
            database_url,
            max_connections,
            schema,
            serializer,
            use_listen_notify,
            polling_concurrency,
            outcome_poll_interval,
            notification_coalesce,
            idle_transaction_timeout,
        } = self;
        f.debug_struct("ClientConfig")
            .field("app_name", app_name)
            .field("database_url", &crate::sysdb::masked_url(database_url))
            .field("max_connections", max_connections)
            .field("schema", schema)
            .field("serializer", serializer)
            .field("use_listen_notify", use_listen_notify)
            .field("polling_concurrency", polling_concurrency)
            .field("outcome_poll_interval", outcome_poll_interval)
            .field("notification_coalesce", notification_coalesce)
            .field("idle_transaction_timeout", idle_transaction_timeout)
            .finish()
    }
}

impl ClientConfig {
    /// A nameless client reaching the database at `database_url`.
    ///
    /// Nameless because a name is a claim: a client that names an application writes rows owned by
    /// it, and taking that from a bare constructor's argument would make the safe-looking call the
    /// one with consequences. Set [`app_name`](Self::app_name) to make the claim deliberately.
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            app_name: None,
            database_url: database_url.into(),
            max_connections: 5,
            schema: DEFAULT_SCHEMA.to_owned(),
            serializer: Serializer::default(),
            use_listen_notify: false,
            polling_concurrency: crate::PollingConcurrency::Default,
            outcome_poll_interval: None,
            notification_coalesce: None,
            idle_transaction_timeout: crate::IdleTransactionTimeout::Default,
        }
    }

    /// [`ClientConfig::new`], taking the database URL from [`SYSTEM_DATABASE_URL_ENV`].
    ///
    /// A missing or empty variable leaves [`database_url`](Self::database_url) empty rather than
    /// failing here, so a caller who sets it afterwards is not forced through an error path;
    /// [`Client::connect`] is where an empty URL is reported.
    ///
    /// **The URL is the only thing it reads, and [`app_name`](Self::app_name) stays `None`.** That
    /// is the deliberate half. An application resolves its whole identity against the environment
    /// at launch — on DBOS Cloud the deployment's `DBOS_APP_NAME` outranks the configuration
    /// entirely — and a client does none of that, because naming an application is a *claim*
    /// rather than an observation: it decides which rows this client writes as its own and which
    /// it can see. Picking a name up from an ambient variable would silently narrow an operator's
    /// tool to whichever application happened to be deployed around it.
    ///
    /// So a client deployed alongside an application must be told its name, here or on the field:
    ///
    /// ```no_run
    /// # use dbos::ClientConfig;
    /// let config = ClientConfig {
    ///     app_name: Some("my-app".to_owned()),
    ///     ..ClientConfig::from_env()
    /// };
    /// ```
    pub fn from_env() -> Self {
        Self::new(std::env::var(SYSTEM_DATABASE_URL_ENV).unwrap_or_default())
    }

    /// Checks what can be checked before anything is connected.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.database_url.is_empty() {
            return Err(Error::Config(format!(
                "no database URL: set `database_url`, or the {SYSTEM_DATABASE_URL_ENV} environment \
                 variable if the configuration came from `ClientConfig::from_env`"
            )));
        }
        if let Some(app_name) = &self.app_name {
            validate_app_name(app_name)?;
        }
        if self.schema.is_empty() {
            return Err(Error::Config("`schema` cannot be empty".to_owned()));
        }
        if self.max_connections == 0 {
            return Err(Error::Config("`max_connections` cannot be zero".to_owned()));
        }
        self.serializer.validate()?;
        if self.outcome_poll_interval == Some(Duration::ZERO) {
            return Err(Error::Config(
                "`outcome_poll_interval` cannot be zero".to_owned(),
            ));
        }
        self.idle_transaction_timeout
            .validate()
            .map_err(|error| crate::Error::Config(error.to_string()))?;
        Ok(())
    }

    /// How often a waiting caller looks, resolved.
    pub(crate) fn outcome_poll_interval(&self) -> Duration {
        self.outcome_poll_interval
            .unwrap_or(crate::config::DEFAULT_OUTCOME_POLL_INTERVAL)
    }
}

/// A connection to a DBOS application's system database, from outside the application.
///
/// **It holds a connection and nothing else.** Not an executor: an executor is the machinery for
/// *running* workflows — an id, a version and an application name to stamp claims with, a runtime
/// to spawn onto, a registry to resolve names through, task sets to shut down — and a client does
/// none of that. The operations both surfaces share are methods on the connection, so calling one
/// here costs no executor and implies none.
///
/// Cheap to clone — it is an `Arc` internally — so a clone is another handle to the same
/// connection rather than a second one. Clone it into whatever holds application state.
///
/// Dropping the last clone stops the listener and the notifier and leaves the pool to its own
/// teardown, which closes the connections but says nothing about when. That is a safety net —
/// without it the listener would hold a `LISTEN` connection for the life of the process — and not a
/// shutdown: nothing waits for either task, and whatever the notifier had inside its coalescing
/// window may go out after the drop returns or not at all. [`close`](Self::close) is how a client
/// is put away deliberately, and is what a process that connects repeatedly wants.
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// use dbos::{Client, ClientConfig, WorkflowHandle};
///
/// let client = Client::connect(ClientConfig::from_env()).await?;
///
/// // The type parameter says what the workflow returns; nothing here can check it.
/// let handle: WorkflowHandle<String> = client.enqueue("process", "tasks", 42).await?;
/// println!("{}", handle.result().await?);
///
/// client.close().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client(Arc<Connection>);

impl Client {
    /// Connects to the system database.
    ///
    /// Eager: an unreachable database is reported here rather than on the first call that needed
    /// it, because a client is usually built at start-up where a failure is cheap and diagnosable.
    /// The price is one round trip at construction.
    ///
    /// **Migrates nothing, and creates nothing.** It verifies instead: a schema that is missing or
    /// behind what this build's queries are written against fails here, naming the version it
    /// found. A client is a guest in the application's database, and an operator's tool that can
    /// rewrite the schema it is inspecting is a tool that can break the application it was pointed
    /// at.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        Ok(Self(Arc::new(Connection::for_client(&config).await?)))
    }

    /// Closes the connection.
    ///
    /// Idempotent, and safe to call while clones are still alive: what it closes is the pool they
    /// share, so a later call through one of them fails as an unreachable database rather than
    /// doing something surprising. `Drop` cannot do this — closing is asynchronous — which is the
    /// same reason [`DBOS::shutdown`](crate::DBOS::shutdown) is a method rather than a destructor;
    /// dropping the last clone stops the tasks but waits for nothing.
    ///
    /// What this adds over the drop is the waiting: the notifier's queued wakeups go out, the
    /// pool's connections are closed, and the listener has *ended* rather than merely been told to,
    /// so afterwards nothing of this client is still running.
    pub async fn close(&self) {
        self.0.close().await;
        tracing::info!(
            app_name = self.0.app_name().unwrap_or("<none>"),
            "DBOS client closed"
        );
    }

    /// The application this client acts for, or `None` if it is nameless.
    pub fn app_name(&self) -> Option<&str> {
        self.0.app_name()
    }

    /// A handle to a workflow that already exists, by id.
    ///
    /// Always a polling handle — nothing runs here — and it is **not** checked: the id is not read
    /// until the handle is used, so this cannot fail and does not go to the database. The two
    /// halves of the handle then answer an unknown id differently, deliberately:
    /// [`status`](crate::WorkflowHandle::status) reports [`Error::WorkflowNotFound`], while
    /// [`result`](crate::WorkflowHandle::result) **waits** — a client is the caller most likely to
    /// hold an id before whoever owns it has committed the enqueue, and this is the one handle in
    /// the crate that waits for a row to appear rather than reporting its absence.
    ///
    /// A caller who wants an id that names nothing refused calls
    /// [`workflow_status`](Self::workflow_status) before taking the handle, at the cost of a round
    /// trip — or bounds the wait instead, since dropping the future ends it and
    /// `tokio::time::timeout` is all a bounded wait needs.
    pub fn retrieve_workflow<R, E>(&self, workflow_id: &str) -> WorkflowHandle<R, E> {
        WorkflowHandle::polling(Arc::clone(&self.0), workflow_id.to_owned(), false)
    }

    /// A workflow's status, or `None` if there is no such workflow.
    ///
    /// The one-shot read behind [`WorkflowHandle::status`](crate::WorkflowHandle::status), for a
    /// caller who has an id and does not want a handle. Absence is a value here rather than
    /// [`Error::WorkflowNotFound`], because a client asking after an id it was given has every
    /// reason to be told plainly that it does not exist.
    pub async fn workflow_status(&self, workflow_id: &str) -> Result<Option<WorkflowStatus>> {
        Ok(self
            .0
            .sysdb()
            .get_workflow(workflow_id)
            .await
            .map_err(Error::SystemDatabase)?
            .map(|row| row.status))
    }

    /// Registers a queue, or reports the one already registered under this name.
    ///
    /// The same operation [`DBOS::register_queue`](crate::DBOS::register_queue) performs, and the
    /// reason a client has it is that a queue is a row: a fleet may be configured by the tool that
    /// deploys it rather than by the code that drains it.
    ///
    /// The limits and the policy are the same [`QueueOptions`] and [`QueueConflict`] an application
    /// states — but [`UpdateIfLatestVersion`](crate::QueueConflict::UpdateIfLatestVersion) is
    /// [`Error::Config`] here rather than a registration: a client runs none of the application's
    /// code, so there is no version of it to be the latest of. Ask for
    /// [`AlwaysUpdate`](crate::QueueConflict::AlwaysUpdate) or
    /// [`NeverUpdate`](crate::QueueConflict::NeverUpdate).
    ///
    /// **A client with no application name of its own registers over a peer's queue rather than
    /// being refused**, replacing its stored limits: the ownership check lets a nameless writer
    /// through. Whether it should is an open question shared with the other SDKs on the same
    /// database. Give the client an [`app_name`](ClientConfig::app_name) to get the refusal
    /// [`QueueConflict`] describes.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let queue = client.register_queue("fleet", dbos::QueueOptions {
    ///     worker_concurrency: Some(3),
    ///     ..Default::default()
    /// }, dbos::QueueConflict::AlwaysUpdate).await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_queue(
        &self,
        name: &str,
        options: QueueOptions,
        on_conflict: QueueConflict,
    ) -> Result<Queue> {
        // `None`: a client runs none of the application's code, so it has no version to be the
        // latest of. That is also what refuses `UpdateIfLatestVersion` — the registration reports
        // the missing version rather than guessing at an answer.
        self.0
            .register_queue(name, options, on_conflict, None)
            .await
    }

    /// The queue registered under this name, or `None` if there is none.
    pub async fn queue(&self, name: &str) -> Result<Option<Queue>> {
        self.0.queue(name).await
    }

    /// Every queue this client's application can see: its own, plus the unclaimed ones.
    ///
    /// A nameless client sees every queue, which is the read half of what namelessness means.
    pub async fn list_queues(&self) -> Result<Vec<Queue>> {
        self.0.list_queues().await
    }

    /// Changes a registered queue's limits, leaving what the change does not name.
    ///
    /// The whole fleet picks the change up within a poll, without a restart — which is what makes
    /// this worth having on a client rather than only in the application: the process that decides
    /// a queue should slow down is rarely one of the processes draining it.
    pub async fn update_queue(&self, name: &str, change: QueueChange) -> Result<Queue> {
        self.0.update_queue(name, change).await
    }

    /// Removes a queue's registration, leaving whatever was enqueued on it where it stands.
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.0.delete_queue(name).await
    }

    /// The connection this client talks through, for surfaces implemented against one.
    pub(crate) fn connection(&self) -> &Arc<Connection> {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_the_database_password() {
        let printed = format!(
            "{:?}",
            ClientConfig::new("postgres://dbos:s3cret@db.example/app")
        );
        assert!(!printed.contains("s3cret"), "{printed}");
        assert!(
            printed.contains("postgres://dbos:***@db.example/app"),
            "{printed}"
        );
    }

    #[test]
    fn a_bare_configuration_speaks_for_no_application() {
        let config = ClientConfig::new("postgres://localhost/app");
        assert_eq!(
            config.app_name, None,
            "a name is a claim, and a bare constructor should not make one"
        );
        assert_eq!(config.schema, DEFAULT_SCHEMA);
        assert!(
            !config.use_listen_notify,
            "a client polls unless told the application pushes notifications"
        );
        assert_eq!(config.outcome_poll_interval(), Duration::from_secs(1));
    }

    /// `use_listen_notify` decides whether a client starts its notification tasks. Off by
    /// default, so a client polls unless told the application pushes. CockroachDB declines them
    /// whatever the setting.
    #[tokio::test]
    async fn a_client_starts_notifications_only_when_asked() {
        let db = dbos_test_support::test_database().await;
        let default = Client::connect(ClientConfig::new(db.url()))
            .await
            .expect("connect failed");
        let opted_in = Client::connect(ClientConfig {
            use_listen_notify: true,
            ..ClientConfig::new(db.url())
        })
        .await
        .expect("connect failed");

        let started = (
            default.connection().sysdb().is_pushing(),
            opted_in.connection().sysdb().is_pushing(),
        );
        default.close().await;
        opted_in.close().await;
        assert_eq!(
            started,
            (false, db.backend() == dbos_test_support::Backend::Postgres),
            "(default, opted in)"
        );
    }

    /// An opted-in client's `get_event` is woken by the application's notification.
    ///
    /// Once the client's listener delivers, its event waits look again only once a minute, so a
    /// wait that sees the event within the bound below was woken by a notification. The wait
    /// starts before the event is written; one that started after would find it on its first
    /// look and pass without proving anything, which the head start makes unlikely.
    #[tokio::test]
    async fn an_opted_in_client_is_woken_by_the_application() {
        let db = dbos_test_support::test_database().await;
        if db.backend() == dbos_test_support::Backend::Cockroach {
            return; // No LISTEN/NOTIFY, so nothing to be woken by.
        }
        let dbos = crate::DBOS::new(crate::Config {
            migrate: false,
            app_version: Some("1.0.0".to_owned()),
            ..crate::Config::new("client-wake-app", db.url())
        });
        let release = Arc::new(tokio::sync::Notify::new());
        let workflow = dbos
            .register_workflow("publish-later", {
                let release = Arc::clone(&release);
                move |()| {
                    let release = Arc::clone(&release);
                    async move {
                        release.notified().await;
                        crate::set_event("ready", &1u32).await?;
                        Ok::<(), Error>(())
                    }
                }
            })
            .unwrap();
        dbos.launch().await.expect("launch failed");

        let client = Client::connect(ClientConfig {
            use_listen_notify: true,
            ..ClientConfig::new(db.url())
        })
        .await
        .expect("connect failed");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !client.connection().sysdb().is_delivering() && std::time::Instant::now() < deadline {
            crate::rt::sleep(Duration::from_millis(50)).await;
        }
        let delivering = client.connection().sysdb().is_delivering();

        let handle = workflow.start(()).await.expect("start failed");
        let id = handle.workflow_id().to_owned();
        let waiting = crate::rt::spawn({
            let client = client.clone();
            async move {
                let started = std::time::Instant::now();
                let value = client
                    .get_event::<u32>(&id, "ready", Duration::from_secs(50))
                    .await;
                (value, started.elapsed())
            }
        });
        crate::rt::sleep(Duration::from_millis(200)).await;
        release.notify_one();
        let (value, elapsed) = waiting.await.expect("the wait panicked");

        client.close().await;
        dbos.shutdown().await;
        assert!(delivering, "the client's listener never proved it delivers");
        assert_eq!(value.expect("read failed"), Some(1));
        assert!(
            elapsed < Duration::from_secs(20),
            "took {elapsed:?}: the wait fell back to its re-read rather than being woken"
        );
    }

    #[test]
    fn a_client_s_application_name_is_held_to_the_shared_rule() {
        let named = |app_name: &str| ClientConfig {
            app_name: Some(app_name.to_owned()),
            ..ClientConfig::new("postgres://localhost/app")
        };
        assert!(named("my-app").validate().is_ok());
        for bad in ["ab", "My-App", "my app"] {
            let err = named(bad)
                .validate()
                .expect_err("the name should have been refused");
            assert!(err.to_string().contains("app_name"), "{err}");
        }
        assert!(
            ClientConfig::new("postgres://localhost/app")
                .validate()
                .is_ok(),
            "namelessness is a setting, not a missing name"
        );
    }

    #[test]
    fn validation_reports_a_missing_url_by_the_name_of_the_variable_that_sets_it() {
        let err = ClientConfig::new("").validate().unwrap_err();
        assert!(err.to_string().contains(SYSTEM_DATABASE_URL_ENV), "{err}");
    }

    /// A client is meant to be cloned into whatever holds application state, and used from
    /// several tasks at once. That is a property of the type rather than of any method, so it is
    /// asserted by construction here rather than discovered at a call site.
    #[test]
    fn a_client_is_shareable() {
        fn assert_shareable<T: Send + Sync + Clone + std::fmt::Debug>() {}
        assert_shareable::<Client>();
    }
}
