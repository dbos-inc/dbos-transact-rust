//! The client: reaching a DBOS application from outside it.
//!
//! A [`Client`] talks only to the system database. It enqueues workflows, sends them messages,
//! reads their events, waits on their results, and manages the queues they run on — without
//! registering a workflow, running one, or holding any of the application's code. The natural
//! callers are the processes an application is surrounded by rather than the application itself:
//! an HTTP front end that hands work to a worker fleet, a cron host, an operator's tool, a test
//! that drives an application it did not start.
//!
//! Every implementation ships one — `DBOSClient` in Python, TypeScript and Java, `dbos.Client` in
//! Go — and they agree on what it is for and disagree on what it *is*. Go makes a client a
//! `Context` with a flag on it, so every runtime method is on the client and every client method is
//! on the runtime. The other three make it a separate class over the system database alone. Rust
//! follows the three, and follows **Java** in particular for the internals: a [`Client`] holds a
//! connection — the system-database handle, the serializer, the application name, the poll
//! interval — and no [`Executor`](crate::Executor), because an executor is the machinery for
//! running workflows and a client runs none. What both surfaces need is a method on that
//! connection, which is what Java's `static DBOSExecutor.enqueueWorkflow(...)` is for: its client
//! calls it with no executor in sight.
//!
//! The type system then makes the boundary concrete twice over. Nothing on this type can fail with
//! [`Error::NotLaunched`](crate::Error::NotLaunched), because **a client is always connected** —
//! [`connect`](Client::connect) hands back a usable client or an error, with no second state to
//! check afterwards. And nothing on it can run a workflow, because it holds nothing that could.
//!
//! # What a client is not
//!
//! **It never runs a workflow.** There is no registry behind it, so a workflow it enqueues is one
//! *some other process* is expected to have registered — it names the workflow by string, and the
//! row waits on its queue until an executor that knows that name dequeues it. A name nothing
//! registers is not an error here and cannot be: the client has no way to know, and the whole point
//! is that the code lives elsewhere.
//!
//! **It never migrates**, and refuses a database no application has created. Connecting with
//! migrations off verifies the schema rather than ignoring it, so a missing or outdated one is
//! reported at [`connect`](Client::connect) naming the version it found, rather than as whatever
//! SQL error the first real call happens to raise.
//!
//! **None of the four implementations migrates from a client**, and only Go checks. Go's
//! `NewClient` sets `SkipMigrations: true` — *"Clients never own the schema"* — which routes it
//! through the same `VerifyMigrations` this does, and fails construction on a database behind the
//! build. Python's client says it does not migrate (*"We only create database connections but do
//! not run migrations"*) and does not verify either: its `verify_migrations` exists but is reached
//! only from `DBOS.launch`. TypeScript's is the same shape — `SystemDatabase.init`, which picks
//! between migrating and verifying, is called by the executor and never by `DBOSClient`. Java's
//! client constructs a `SystemDatabase` directly and `MigrationManager` is reached only from
//! `DBOS.launch`, so its `getCurrentSysDbVersion` is never consulted from a client at all. In
//! those three a mismatched schema surfaces as whatever SQL error the first real call raises.
//!
//! **It has no application version of its own**, so it stamps none unless an enqueue names one.
//! A client's binary is not the application's.
//!
//! # What is not here yet
//!
//! The workflow-management surface — cancelling, resuming, forking, deleting, listing — is
//! arriving separately and lands on both surfaces at once. Schedules, durable streams and the
//! debouncer are each their own feature and reach the client when they reach the crate. What this
//! module has is the part that is a client's alone: connecting without an executor, and enqueueing
//! a workflow by name.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::config::{DATABASE_URL_ENV, Serializer};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::identity::validate_app_name;
use crate::serialization::encode;
use crate::sysdb::types::{
    Message as EncodedMessage, NewWorkflow, Submission, Timestamp, VersionInfo, WorkflowStatus,
};
use crate::sysdb::{DEFAULT_SCHEMA, Error as SysdbError};
use crate::workflow::{Enqueue, MAX_RECOVERY_ATTEMPTS};
use crate::{Queue, QueueChange, QueueOptions};

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
/// difference in the opposite direction. Go, whose client is its runtime, carries a `ClientConfig`
/// separate from its `Config` for the same reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// The application this client acts for, or `None` to act for every application.
    ///
    /// **The one field that may be absent, and the choice it makes is real.** Named, this client
    /// is that application: rows it writes are stamped with the name and belong to it, and a search
    /// it makes is scoped to it. Nameless, it writes *unclaimed* rows — which every application
    /// matches, and any may run — and reads across all of them. Go's client says the same in its
    /// own words: *"Leave empty to list all workflows, but beware that writing will serve all
    /// applications."*
    ///
    /// **Serving all applications includes editing their rows.** A registration a named
    /// application already owns keeps its owner, but a nameless client still writes through it: it
    /// replaces a queue's limits, a schedule's definition, or the timestamp deciding which version
    /// is latest, where a client naming a *different* application would be refused. Every
    /// implementation behaves this way and UPSTREAM item 26 asks whether it should, so name this
    /// client if it is meant to touch only its own.
    ///
    /// Set it whenever several applications share a system database. Leave it unset for a tool
    /// whose job is to look at all of them.
    ///
    /// Held to the same rule a launched application's name is: three to thirty characters of
    /// lowercase letters, digits, dashes and underscores.
    pub app_name: Option<String>,

    /// Connection URL for the system database.
    pub database_url: String,

    /// Maximum pooled connections.
    ///
    /// Smaller than an application's default, as in Python (`DEFAULT_CLIENT_POOL_SIZE`): a client
    /// runs no workflows, so its pool serves the calls its caller makes rather than a fleet of
    /// executing bodies.
    pub max_connections: u32,

    /// Schema holding the DBOS tables.
    ///
    /// Must match the application's — a client that looks in the wrong schema finds an empty
    /// database rather than an error.
    pub schema: String,

    /// How payloads are encoded.
    ///
    /// **Must match the application's**, or a workflow this client enqueues is handed arguments its
    /// executor cannot read. There is one encoding today, so nothing can currently disagree; the
    /// field is here because the day there are two, this is the knob that has to say which.
    pub serializer: Serializer,

    /// Whether to use LISTEN/NOTIFY rather than polling.
    ///
    /// On, so [`get_event`](crate::Client::get_event) is woken rather than polling. That is the one
    /// wait it serves here: a workflow's *outcome* is announced on no channel, in this
    /// implementation or any other — Python's `await_workflow_result` polls too — so a result wait
    /// looks again every [`outcome_poll_interval`](field@Self::outcome_poll_interval) whatever this is
    /// set to.
    ///
    /// Go's client starts its listener for the same reason. Python defaults its client's off — a
    /// client there is often a short-lived script, and the listener is a thread and a held
    /// connection — and this differs from Python deliberately: an event wait is the operation most
    /// worth making cheap, and a client that never waits on one pays only the connection.
    pub use_listen_notify: bool,

    /// How many polling reads may run at once.
    ///
    /// `None` is half the pool and at least one; `Some(0)` switches the cap off.
    pub polling_concurrency: Option<u32>,

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
            use_listen_notify: true,
            polling_concurrency: None,
            outcome_poll_interval: None,
            notification_coalesce: None,
        }
    }

    /// [`ClientConfig::new`], taking the database URL from `DBOS_DATABASE_URL`.
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
    /// The other implementations draw the line in the same place. Python's `DBOSClient` takes
    /// `application_name` as a constructor argument and reads no environment variable anywhere in
    /// its client module; Go's `ClientConfig.AppName` is likewise configuration only — the
    /// `DBOS__APPVERSION`, `DBOS__VMID`, `DBOS__APPID` and `DBOS__CLOUD` reads all sit on its
    /// executor's path — and its doc says what a nameless one costs: *"Leave empty to list all
    /// workflows, but beware that writing will serve all applications."*
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
        Self::new(std::env::var(DATABASE_URL_ENV).unwrap_or_default())
    }

    /// Checks what can be checked before anything is connected.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.database_url.is_empty() {
            return Err(Error::Config(format!(
                "no database URL: set `database_url`, or the {DATABASE_URL_ENV} environment \
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
        if self.outcome_poll_interval == Some(Duration::ZERO) {
            return Err(Error::Config(
                "`outcome_poll_interval` cannot be zero".to_owned(),
            ));
        }
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
/// to spawn onto, a registry to resolve names through, a task set to shut down — and a client does
/// none of that. Java draws the same line, its `DBOSClient` holding a `SystemDatabase` and a
/// serializer; the operations both surfaces share are methods on the connection, so calling one
/// here costs no executor and implies none.
///
/// Cheap to clone — it is an `Arc` internally — so a clone is another handle to the same
/// connection rather than a second one. Clone it into whatever holds application state.
///
/// Dropping the last clone stops the listener and the notifier and leaves the pool to its own
/// teardown, which closes the connections but says nothing about when. That is a safety net —
/// without it the listener would hold a `LISTEN` connection for the life of the process — and not a
/// shutdown: nothing waits for either task, and whatever the notifier had inside its coalescing
/// window may go out after the drop returns or not at all. [`close`](Self::close) is how a client is
/// put away deliberately, and is what a process that connects repeatedly wants.
#[derive(Clone, Debug)]
pub struct Client(Arc<Connection>);

impl Client {
    /// Connects to the system database.
    ///
    /// Eager, like Go's, TypeScript's and Java's: an unreachable database is reported here rather
    /// than on the first call that needed it, because a client is usually built at start-up where
    /// a failure is cheap and diagnosable. Python's `lazy=True` defers it; nothing here does, and
    /// the price of that choice is one round trip at construction.
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

    /// Enqueues a workflow by name, onto a queue, and hands back a handle to it.
    ///
    /// **The client's signature operation.** The workflow is recorded `ENQUEUED` and nothing runs
    /// it here; whichever executor next polls that queue claims it, and only that executor needs to
    /// have the code. `workflow` is the name it was registered under — the same string
    /// [`WorkflowKey::name`](crate::WorkflowKey::name) holds — and the handle is a polling one,
    /// because the process that asks is by definition not the one that runs it.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let handle: dbos::WorkflowHandle<String> =
    ///     client.enqueue("send_email", "email-queue", "hello").await?;
    /// let sent = handle.result().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The type parameters are the caller's assertion, not a checked fact: there is no registry
    /// here to look the name up in, so `R` is what the caller knows the workflow returns and a
    /// mismatch surfaces when the result is decoded. That is the trade every implementation's
    /// client makes — Java calls its return `WorkflowHandle<T, E>` on the same terms — and it is
    /// the price of naming code this process does not have.
    ///
    /// **Called from inside a workflow body this is not a checkpoint**, unlike
    /// [`WorkflowRef::start_with`](crate::WorkflowRef::start_with), which records the launch
    /// against the parent and re-derives the same child on a replay. A client knows nothing of the
    /// ambient workflow — it is a connection, not an execution — so a replayed body enqueues
    /// again. Reach for the runtime's own start when the caller is a workflow, and for this when
    /// it is not.
    pub async fn enqueue<P, R, E>(
        &self,
        workflow: &str,
        queue: &str,
        input: P,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        self.enqueue_with(workflow, input, EnqueueOptions::new(queue))
            .await
    }

    /// [`enqueue`](Self::enqueue), with something to say about how.
    ///
    /// See [`EnqueueOptions`] for what may be said, and [`Enqueue`] for the queue-shaped half of
    /// it — a deduplication id, a priority, a partition key, a delay.
    pub async fn enqueue_with<P, R, E>(
        &self,
        workflow: &str,
        input: P,
        options: EnqueueOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        // Refused before anything is written, as `start_with` refuses it: an enqueue no queue could
        // honour should cost a round trip, not a row.
        options.queue.validate()?;
        if options.duplication == Duplication::ReturnExisting
            && options.queue.deduplication_id.is_none()
        {
            return Err(Error::Config(
                "`Duplication::ReturnExisting` needs a `deduplication_id` on the enqueue: with no \
                 key there is no collision to resolve"
                    .to_owned(),
            ));
        }

        let input = encode(&input, "argument")?;
        let attributes = options
            .attributes
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| Error::Serialization {
                what: "attributes".into(),
                message: error.to_string(),
                source: Some(error),
            })?;
        // Generated once, outside the retry below: a second attempt under a fresh id would enqueue
        // a *second* workflow if the first insert had in fact landed.
        let generated;
        let workflow_id = match options.workflow_id {
            Some(id) => id,
            None => {
                generated = uuid::Uuid::new_v4().to_string();
                &generated
            }
        };

        let new = NewWorkflow {
            name: Some(workflow),
            class_name: options.class_name,
            config_name: options.config_name,
            input: Some(&input),
            serialization: Some(self.0.serializer().name()),
            // **Nobody claims it.** A client runs nothing, and a queued row's executor is stamped
            // by whichever one dequeues it — writing a claimant here would name a process that is
            // never going to run it.
            executor_id: None,
            // The enqueue's own, then this client's. Naming another application deliberately is
            // what lets a tool hand work to an application it is not; naming none at all writes an
            // unclaimed row, which any application may run.
            application_name: options.app_name.or_else(|| self.0.app_name()),
            // **Only if the caller said so.** A version pins the row to executors running exactly
            // that code, and a client has no version of its own to offer — see
            // [`EnqueueOptions::app_version`].
            application_version: options.app_version,
            // A budget, never a deadline: a queued workflow's deadline is computed on dequeue, so
            // the wait in the queue is not part of the budget. `start_with` writes the pair the
            // same way for the same reason.
            timeout: options.timeout,
            deadline: None,
            queue_name: Some(options.queue.name),
            deduplication_id: options.queue.deduplication_id,
            priority: options.queue.stored_priority(),
            queue_partition_key: options.queue.partition_key,
            delay: options.queue.delay,
            attributes: attributes.as_deref(),
            ..NewWorkflow::new(workflow_id)
        };

        loop {
            match self
                .0
                .sysdb()
                .init_workflow(&new, Some(MAX_RECOVERY_ATTEMPTS), Submission::Fresh)
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        workflow_id,
                        workflow,
                        queue = options.queue.name,
                        "the workflow is enqueued"
                    );
                    return Ok(WorkflowHandle::polling(
                        Arc::clone(&self.0),
                        workflow_id.to_owned(),
                    ));
                }
                // **A key another workflow holds, and a caller who asked to join it.** The insert
                // lost on the partial unique index over `(queue_name, deduplication_id)`; the
                // holder's id is the answer, and a handle to it is what
                // [`Duplication::ReturnExisting`] promises.
                Err(SysdbError::QueueDeduplicated {
                    queue_name,
                    deduplication_id,
                    ..
                }) if options.duplication == Duplication::ReturnExisting => {
                    match self
                        .0
                        .sysdb()
                        .get_deduplication_key_holder(&queue_name, &deduplication_id)
                        .await
                        .map_err(Error::SystemDatabase)?
                    {
                        Some(holder) => {
                            tracing::debug!(
                                workflow_id = holder,
                                deduplication_id,
                                "the deduplication key is held; the handle joins its holder"
                            );
                            return Ok(WorkflowHandle::polling(Arc::clone(&self.0), holder));
                        }
                        // The holder finished between the conflict and this read, so the key is
                        // free again: retry the insert rather than report a collision with a
                        // workflow that is over. Python and TypeScript both loop here.
                        None => continue,
                    }
                }
                Err(error) => return Err(Error::SystemDatabase(error)),
            }
        }
    }

    /// A handle to a workflow that already exists, by id.
    ///
    /// Always a polling handle — nothing runs here — and it is **not** checked: the id is not read
    /// until the handle is used, so this cannot fail and does not go to the database. Asking for
    /// the status of a workflow that does not exist is [`Error::WorkflowNotFound`] at that point.
    /// Python's client returns an unchecked handle in the same way; Java's `retrieveWorkflow` and
    /// Go's take a round trip to verify the row first.
    pub fn retrieve_workflow<R, E>(&self, workflow_id: &str) -> WorkflowHandle<R, E> {
        WorkflowHandle::polling(Arc::clone(&self.0), workflow_id.to_owned())
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

    /// Sends a message to a workflow, for it to [`recv`] when it is ready.
    ///
    /// [`recv`]: crate::sysdb::SystemDatabase::recv
    ///
    /// The message waits in the database until the destination reads it, so sending to a workflow
    /// that has not reached its receive — or is not running at all — is normal rather than an
    /// error. Sending to a workflow that *does not exist* is
    /// [`Error::SystemDatabase`](crate::Error::SystemDatabase) carrying the system database's
    /// non-existent-workflow error: the foreign key catches it, so a message is never left
    /// addressed to nothing.
    ///
    /// **A client's send is not a step**, and that is the difference from the send a workflow body
    /// will make. A workflow's send is checkpointed, so a replay does not send twice; a client has
    /// no replay and no step sequence, and [`Message::idempotency_key`] is the mechanism it has
    /// instead — the key becomes the row's identity, so a retried request delivers one message.
    pub async fn send<T: Serialize>(&self, message: Message<'_, T>) -> Result<()> {
        self.send_with(message, Forks::Skip).await
    }

    /// [`send`](Self::send), saying whether the message also reaches the destination's forks.
    pub async fn send_with<T: Serialize>(
        &self,
        message: Message<'_, T>,
        forks: Forks,
    ) -> Result<()> {
        self.send_all(std::slice::from_ref(&message), forks).await
    }

    /// Sends many messages in one transaction.
    ///
    /// **All or none**, which is the reason to prefer this over a loop of [`send`](Self::send):
    /// the batch is one insert, so a failure halfway through delivers nothing rather than a prefix.
    /// Python and Java expose the same as `send_bulk`; TypeScript and Go have no equivalent, and a
    /// caller there writes the loop and lives with the prefix.
    ///
    /// One payload type for the whole batch, which is what typing it costs. A batch of genuinely
    /// different shapes is a batch of `serde_json::Value`, or two calls.
    pub async fn send_all<T: Serialize>(
        &self,
        messages: &[Message<'_, T>],
        forks: Forks,
    ) -> Result<()> {
        // Encoded up front so that nothing is sent when one payload cannot be: the whole batch is
        // one transaction, and failing halfway through the encoding would be the prefix this
        // method exists to avoid.
        let encoded = messages
            .iter()
            .map(|message| encode(message.message, "message"))
            .collect::<Result<Vec<_>>>()?;
        let messages: Vec<EncodedMessage<'_>> = messages
            .iter()
            .zip(&encoded)
            .map(|(message, encoded)| EncodedMessage {
                destination_id: message.destination_id,
                topic: message.topic,
                message: encoded,
                idempotency_key: message.idempotency_key,
            })
            .collect();

        self.0
            .sysdb()
            .send_messages(
                &messages,
                Some(self.0.serializer().name()),
                // No caller: a client is never inside a workflow, so there is no step to record
                // the batch against and nothing to replay it for.
                None,
                forks == Forks::Include,
            )
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Registers a queue, or reports the one already registered under this name.
    ///
    /// The same operation [`DBOS::register_queue`](crate::DBOS::register_queue) performs, and the
    /// reason a client has it is that a queue is a row: a fleet may be configured by the tool that
    /// deploys it rather than by the code that drains it.
    ///
    /// An unstated [`on_conflict`](crate::QueueOptions::on_conflict) is
    /// [`AlwaysUpdate`](crate::QueueConflict::AlwaysUpdate) here — the operator's intent, and
    /// Python's client default — where an application would get the latest-version check.
    /// Asking for [`UpdateIfLatestVersion`](crate::QueueConflict::UpdateIfLatestVersion)
    /// *explicitly* is refused, because a client has no application version to be the latest of;
    /// Python refuses the same combination for the same reason.
    ///
    /// **A client with no application name of its own registers over a peer's queue rather than
    /// being refused**, replacing its stored limits — the ownership check every implementation
    /// shares lets a nameless writer through. Give the client an
    /// [`app_name`](ClientConfig::app_name) to get the refusal
    /// [`QueueConflict`](crate::QueueConflict) describes. UPSTREAM item 26.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let queue = client.register_queue("fleet", dbos::QueueOptions {
    ///     worker_concurrency: Some(3),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_queue(&self, name: &str, options: QueueOptions) -> Result<Queue> {
        // `None`: a client runs none of the application's code, so it has no version to be the
        // latest of. That is also what settles an unstated policy to `AlwaysUpdate`, and what
        // refuses an explicit `UpdateIfLatestVersion` — both inside `register_queue`, not here.
        self.0.register_queue(name, options, None).await
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

    /// Every application version this client can see, newest first: the ones its own application
    /// registered, plus the unclaimed. A nameless client sees every one, which is the read half of
    /// what namelessness means — the same scoping [`list_queues`](Self::list_queues) has.
    ///
    /// What an operator reads to find out which versions of the code have ever announced
    /// themselves, and which one a workflow row's `application_version` refers to. A *named*
    /// client answers that question for its own application only — a peer's versions are not in
    /// the listing, and nothing here reads them, which is why a tool that has to see every
    /// application leaves [`app_name`](ClientConfig::app_name) unset. Python and TypeScript scope
    /// their listing the same way, and take no target either.
    pub async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.0
            .sysdb()
            .list_application_versions()
            .await
            .map_err(Error::SystemDatabase)
    }

    /// The version a rolling deploy currently prefers, or `None` if none is registered.
    ///
    /// Scoped to this client's application, or across all of them when it is nameless. Latest by
    /// its registered timestamp rather than by creation, which is how a rollback is expressed.
    pub async fn latest_application_version(&self) -> Result<Option<VersionInfo>> {
        self.0
            .sysdb()
            .get_latest_application_version(self.0.app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes an already-registered version to be the latest, by moving its timestamp to now.
    ///
    /// The write half of [`latest_application_version`](Self::latest_application_version), and how
    /// a rolling deploy is steered from outside the fleet: the latest version is chosen by
    /// timestamp rather than by creation order, so promoting an older one is a **rollback** — the
    /// executors running it start dequeuing again, and an enqueue that names no version goes to it.
    ///
    /// The version must already exist; this does not register one. Registration is an executor's
    /// business, because a version is a claim about code that is running somewhere. **A name that
    /// matches nothing is not an error**, though: the write moves no row and reports success, so a
    /// misspelled rollback reads as a rollback. Every implementation discards the row count here;
    /// UPSTREAM item 2.
    ///
    /// Scoped to this client's application, as every write here is. Promoting a version another
    /// application registered fails rather than moving it — a timestamp is what a peer's fleet is
    /// rolling on, so it is not this client's to move without saying so. Saying so is
    /// [`set_latest_application_version_for`](Self::set_latest_application_version_for).
    ///
    /// **That scoping needs the client to have a name.** A client configured without an
    /// [`app_name`](ClientConfig::app_name) has nothing to be refused under, so it
    /// promotes whatever version it names, a peer's included — which is either what namelessness
    /// is for or a shared gap, and UPSTREAM item 26 asks the team which.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// // Roll the fleet back to the version before the bad deploy.
    /// client.set_latest_application_version("1.4.2").await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_latest_application_version(&self, version_name: &str) -> Result<()> {
        self.0
            .sysdb()
            .update_application_version_timestamp(version_name, Timestamp::now(), self.0.app_name())
            .await
            .map_err(Error::SystemDatabase)
    }

    /// Promotes a version on behalf of the application that registered it.
    ///
    /// [`set_latest_application_version`](Self::set_latest_application_version) promotes within
    /// this client's own application; this one names the application to act as, which is what lets
    /// a single operator tool roll several applications on a shared system database without
    /// connecting a client per application.
    ///
    /// **The setter takes a name where the reader does not, and that asymmetry is the references'
    /// too** — Python's `set_latest_application_version` has an `application_name` keyword and
    /// TypeScript's an `applicationName` option, while neither `get_latest_application_version`
    /// takes anything. Promotion is the operation that *claims*: the write also adopts a version
    /// left unclaimed, which would otherwise read as every peer's latest. Reading claims nothing,
    /// so it has nothing to say a name about. Go is the one implementation whose client promotes
    /// without an override at all.
    ///
    /// A version some *third* application registered is still refused — naming an application is
    /// how a caller says which fleet it means, not a way around ownership.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// // One operator tool, rolling a peer application back.
    /// client.set_latest_application_version_for("1.4.2", "billing").await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_latest_application_version_for(
        &self,
        version_name: &str,
        application_name: &str,
    ) -> Result<()> {
        self.0
            .sysdb()
            .update_application_version_timestamp(
                version_name,
                Timestamp::now(),
                Some(application_name),
            )
            .await
            .map_err(Error::SystemDatabase)
    }

    /// The connection this client talks through, for surfaces implemented against one.
    pub(crate) fn connection(&self) -> &Arc<Connection> {
        &self.0
    }
}

/// What a client may say about an enqueue, beyond the input.
///
/// [`queue`](Self::queue) is not optional, which is the difference from
/// [`StartOptions`](crate::StartOptions): a client cannot run a workflow, so every enqueue it makes
/// names a queue. That is also why there is no [`Default`] — a default would have to invent a queue
/// name — and why the shape is [`EnqueueOptions::new`] plus functional update:
///
/// ```no_run
/// # use dbos::{Enqueue, EnqueueOptions};
/// let options = EnqueueOptions {
///     workflow_id: Some("order-42"),
///     queue: Enqueue {
///         priority: Some(1),
///         ..Enqueue::new("orders")
///     },
///     ..EnqueueOptions::new("orders")
/// };
/// ```
///
/// The queue-shaped options — deduplication, priority, partition, delay — live on [`Enqueue`],
/// where the runtime's enqueue keeps them too, so the two surfaces do not spell the same four
/// things differently.
#[derive(Debug, Clone)]
pub struct EnqueueOptions<'a> {
    /// The queue to leave the workflow on, and what to ask of it.
    pub queue: Enqueue<'a>,

    /// The workflow's id, in place of a generated one.
    ///
    /// An idempotency key, as everywhere else: enqueueing the same id twice records one workflow,
    /// and the second call hands back a handle to it rather than failing. The natural use from a
    /// client is a request id, so a retried HTTP request enqueues one workflow.
    pub workflow_id: Option<&'a str>,

    /// The class the workflow's function belongs to, for class-bound workflows.
    ///
    /// This and [`config_name`](Self::config_name) are the other two thirds of a
    /// [`WorkflowKey`](crate::WorkflowKey): a registration is identified by the triple, so a
    /// workflow registered under a class and an instance is only reachable by naming both. Java's
    /// client spells them `withClassName` and `withInstanceName`.
    pub class_name: Option<&'a str>,

    /// The configured instance name, for instance-bound workflows.
    pub config_name: Option<&'a str>,

    /// The application this workflow belongs to, in place of this client's own.
    ///
    /// For a tool that hands work to an application it is not — the name decides which
    /// application's executors may dequeue it. Leaving both this and
    /// [`ClientConfig::app_name`] unset writes an unclaimed workflow, which any application may
    /// run.
    pub app_name: Option<&'a str>,

    /// The version of the application's code that must run this workflow.
    ///
    /// **`None` records no version, which is the right default for a client**, and is not quite the
    /// same as *any* version: an unversioned row is dequeued by an executor running the **latest
    /// registered** version, and by no other. That is what keeps a rolling deploy from handing new
    /// work to the code being replaced, and it is the same rule an application's own unversioned
    /// rows follow.
    ///
    /// The case worth knowing is a rollback. A version's timestamp is what makes it the latest, and
    /// re-registering one that already exists does not move it — so redeploying an earlier version
    /// leaves the version that was rolled back from still holding the title, and unversioned work
    /// waits for executors that are gone. Promoting the version being rolled back to, which is what
    /// an operator's rollback does, is what releases it.
    ///
    /// Naming a version instead pins the row: only an executor running exactly that code will
    /// dequeue it, so a stale value here is a workflow that waits forever. Name one when a
    /// deployment is being pinned deliberately — draining work to a version that is still up during
    /// a rollback, say — and otherwise let the fleet take it.
    pub app_version: Option<&'a str>,

    /// How long the whole workflow may take, once it starts.
    ///
    /// A budget rather than a deadline, and the clock starts on **dequeue**: a workflow given five
    /// minutes that waits an hour in the queue still gets its five minutes.
    ///
    /// A plain [`Option`] rather than [`Timeout`](crate::Timeout), whose three states answer a
    /// question that only exists inside a workflow — whether to inherit a parent's deadline. A
    /// client has no parent, so `Inherit` and `None` would mean the same thing and the enum would
    /// spell one state twice.
    pub timeout: Option<Duration>,

    /// Caller-supplied attributes, stored as JSON on the workflow's row.
    ///
    /// Searchable metadata: a tenant, a request id, a trace context. Plain JSON, never the
    /// configured [`Serializer`] — the column is read by containment and by every other
    /// implementation, so a workflow's own payload encoding has no say in it.
    pub attributes: Option<&'a serde_json::Map<String, serde_json::Value>>,

    /// What to do when the deduplication key is already held.
    pub duplication: Duplication,
}

impl<'a> EnqueueOptions<'a> {
    /// A plain enqueue onto `queue`, asking for nothing else.
    pub fn new(queue: &'a str) -> Self {
        Self {
            queue: Enqueue::new(queue),
            workflow_id: None,
            class_name: None,
            config_name: None,
            app_name: None,
            app_version: None,
            timeout: None,
            attributes: None,
            duplication: Duplication::default(),
        }
    }
}

/// What an enqueue does when its [`deduplication_id`](Enqueue::deduplication_id) is already held.
///
/// Only meaningful with a key: an enqueue with no deduplication id has nothing to collide with, and
/// asking for [`ReturnExisting`](Self::ReturnExisting) without one is refused rather than ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Duplication {
    /// Refuse the enqueue, reporting the collision.
    ///
    /// The default, and what the runtime's enqueue does with no way to ask for anything else. A
    /// caller who did not think about deduplication is a caller who should hear that the key was
    /// taken.
    #[default]
    Reject,
    /// Hand back a handle to the workflow already holding the key.
    ///
    /// **Idempotent enqueue**: the first caller's workflow is the one that runs, and every later
    /// caller waits on it instead of being told no. TypeScript introduced it and Python ported it;
    /// Java is the one implementation that always rejects.
    ///
    /// The key is held only while the holder is *waiting*, so this joins a backlog, not a history:
    /// once the holder has finished, the same key enqueues a new workflow.
    ReturnExisting,
}

/// A message for a workflow, and how to address it.
///
/// [`Message::new`] plus functional update, like everything else that takes options here:
///
/// ```no_run
/// # use dbos::Message;
/// let message = Message {
///     topic: Some("approvals"),
///     ..Message::new("order-42", &"approved")
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a, T> {
    /// The workflow it is for.
    pub destination_id: &'a str,
    /// The payload, encoded by this client's serializer on the way in.
    pub message: &'a T,
    /// The topic it is filed under, or `None` for the default one.
    ///
    /// A receiver selects on the topic, so a message sent under one nobody is receiving on waits
    /// forever rather than being delivered to a different receive.
    pub topic: Option<&'a str>,
    /// A key that makes re-sending this message a no-op.
    ///
    /// It becomes the row's identity, so a second send under the same key is discarded by the
    /// database rather than delivered twice. **This is a client's only idempotency**: a workflow's
    /// send is a checkpointed step, and a client has no step to be checkpointed.
    pub idempotency_key: Option<&'a str>,
}

impl<'a, T> Message<'a, T> {
    /// A message for `destination_id`, on the default topic.
    pub fn new(destination_id: &'a str, message: &'a T) -> Self {
        Self {
            destination_id,
            message,
            topic: None,
            idempotency_key: None,
        }
    }
}

/// Whether a message also reaches the workflows forked from its destination.
///
/// A fork is a new workflow replaying an old one's steps, so a message the original was waiting for
/// is one the fork will wait for too — and it was consumed by the original. Including the forks is
/// how a send reaches both.
///
/// The fork set is resolved inside the sending transaction, so a fork created while the send is in
/// flight cannot make it stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Forks {
    /// The destination named, and nothing else.
    #[default]
    Skip,
    /// The destination, and everything recursively forked from it.
    Include,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_configuration_speaks_for_no_application() {
        let config = ClientConfig::new("postgres://localhost/app");
        assert_eq!(
            config.app_name, None,
            "a name is a claim, and a bare constructor should not make one"
        );
        assert_eq!(config.schema, DEFAULT_SCHEMA);
        assert!(
            config.use_listen_notify,
            "a client's waits are woken rather than polled, as Go's are"
        );
        assert_eq!(config.outcome_poll_interval(), Duration::from_secs(1));
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
        assert!(err.to_string().contains(DATABASE_URL_ENV), "{err}");
    }

    #[test]
    fn an_enqueue_asks_for_a_queue_and_nothing_else() {
        let options = EnqueueOptions::new("work");
        assert_eq!(options.queue, Enqueue::new("work"));
        assert_eq!(options.workflow_id, None);
        assert_eq!(options.app_version, None);
        assert_eq!(options.timeout, None);
        assert_eq!(
            options.duplication,
            Duplication::Reject,
            "a caller who did not think about deduplication should hear that a key was taken"
        );
    }

    #[test]
    fn a_message_is_untopicked_and_sent_once_per_call() {
        let message = Message::new("workflow-1", &"payload");
        assert_eq!(message.destination_id, "workflow-1");
        assert_eq!(message.topic, None);
        assert_eq!(
            message.idempotency_key, None,
            "each send is a distinct message unless a key says otherwise"
        );
        assert_eq!(Forks::default(), Forks::Skip);
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
