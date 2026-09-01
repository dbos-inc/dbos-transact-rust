//! The connection to the system database, and the settings every call through it carries.
//!
//! **What a [`Client`](crate::Client) is, and what an [`Executor`](crate::Executor) has.** Every
//! DBOS operation that is not *running a workflow* is a read or a write against the system
//! database plus three things that decide how it is spelled: which serializer encodes the
//! payloads, which application owns the rows, and how often a wait re-asks. That bundle is this
//! type. An executor is this **plus** everything execution needs — an executor id and an
//! application version to stamp claims with, a runtime to spawn onto, a registry snapshot, a task
//! set to shut down.
//!
//! Splitting it this way is what lets a client hold no executor. Java is the model: its
//! `DBOSClient` holds a `SystemDatabase` and nothing else, its workflow handle is a small class
//! over that same database, and the operations both surfaces need take what they use rather than a
//! whole executor — `DBOSExecutor.enqueueWorkflow(...)` is called by the client with no executor in
//! sight.
//!
//! # `Connection`, not `Database`
//!
//! [`SystemDatabase`] is already a type here — the trait a backend implements, which exists so a
//! second one can be dropped in without its callers changing. A second name a word away from it
//! would make every mention of "the database" a question about which layer is meant. What this
//! type adds to that trait object is not another database; it is *how this process talks to the one
//! it has*, which is what a connection is.
//!
//! # Where its methods live
//!
//! An operation both surfaces need is an inherent method here, **written in the module that owns
//! its types** rather than in this one: the five queue operations are in [`queue`](crate::queue),
//! reading an event is in [`event`](crate::event), and awaiting an outcome another execution owns
//! is in [`workflow`](crate::workflow). That is already how this crate spreads `impl DBOS`, and it
//! keeps this module to what a connection *is* while each operation stays next to the types it
//! speaks in. Java's statics are the same idea reached differently: a language that cannot add
//! methods to a class from another file has to put the shared code somewhere, and it chose the
//! executor's class.
//!
//! It is also what keeps [`WorkflowHandle`](crate::WorkflowHandle) one type. A handle needs the
//! connection, the serializer and the poll interval, and none of the executing half — so it holds
//! an `Arc<Connection>`, and a handle from a client and a handle from a running application are the
//! same thing rather than two types a caller has to tell apart.

use std::time::Duration;

use crate::config::{Config, Serializer};
use crate::sysdb::{SystemDatabase, postgres};
use crate::{ClientConfig, Error, Result};

/// A connection to the system database, and the settings every call through it carries.
pub(crate) struct Connection {
    sysdb: Box<dyn SystemDatabase>,
    serializer: Serializer,
    app_name: Option<String>,
    outcome_poll_interval: Duration,
}

impl Connection {
    /// Connects for an application, migrating unless it was told not to.
    ///
    /// The executor's half of the connect: it takes an id, because an application's every
    /// statement is made by an identified process, and it may create and migrate the database
    /// because the application owns it.
    ///
    /// `app_name` is the **resolved** name — [`identity::resolve`](crate::identity::resolve)'s,
    /// not [`Config::app_name`](crate::Config::app_name)'s. On DBOS Cloud the deployment's
    /// `DBOS_APP_NAME` outranks whatever the application was built believing, and the name that
    /// stamps rows has to be the one that won.
    pub(crate) async fn for_application(
        config: &Config,
        executor_id: &str,
        app_name: &str,
    ) -> Result<Self> {
        let sysdb = postgres::PostgresSystemDatabase::connect(&postgres::Config {
            url: &config.database_url,
            max_connections: config.max_connections,
            use_listen_notify: config.use_listen_notify,
            migrate: config.migrate,
            settings: postgres::Settings {
                schema: &config.schema,
                executor_id: Some(executor_id),
                application_name: Some(app_name),
                polling_concurrency: config.polling_concurrency,
                notification_coalesce: config.notification_coalesce,
                ..postgres::Settings::default()
            },
        })
        .await
        .map_err(Error::SystemDatabase)?;

        Ok(Self {
            sysdb: Box::new(sysdb),
            serializer: config.serializer.clone(),
            app_name: Some(app_name.to_owned()),
            outcome_poll_interval: config.outcome_poll_interval(),
        })
    }

    /// Connects for a [`Client`](crate::Client), which owns nothing here.
    ///
    /// The differences from [`for_application`](Self::for_application) are the whole of what a
    /// client is:
    ///
    /// - **No executor id.** A client claims no workflow, so there is no process for the column to
    ///   name and no statement of its is made on one's behalf.
    /// - **It does not migrate**, and does not create the database either. A client is a guest —
    ///   Python says so in as many words (*"Unlike DBOS itself, the client never runs schema
    ///   migrations: the system database must already have been created by a DBOS application"*),
    ///   and an operator's tool that can rewrite the schema it is inspecting is a tool that can
    ///   break the application it was pointed at. There is no knob:
    ///   [`ClientConfig`](crate::ClientConfig) has no `migrate` field to set.
    ///   It does **verify**, because that is what this crate's `migrate: false` has always meant:
    ///   a schema that is missing or behind what this build's queries are written against fails
    ///   here, naming the version it found, instead of surfacing as a confusing SQL error on the
    ///   first real call. **Go's client does the same** (`SkipMigrations: true`, *"Clients never
    ///   own the schema"*); Python's, TypeScript's and Java's neither migrate nor check.
    /// - **The application name may be absent**, which no application's may be. A nameless client
    ///   writes unclaimed rows and reads across every application.
    ///
    /// What it does *not* differ in is the connection itself: the listener and notifier start the
    /// same way, so a client's blocking reads are woken rather than polled. Go's `NewClient` calls
    /// the same `Launch` on its system database for the same reason.
    pub(crate) async fn for_client(config: &ClientConfig) -> Result<Self> {
        config.validate()?;

        let sysdb = postgres::PostgresSystemDatabase::connect(&postgres::Config {
            url: &config.database_url,
            max_connections: config.max_connections,
            use_listen_notify: config.use_listen_notify,
            migrate: false,
            settings: postgres::Settings {
                schema: &config.schema,
                executor_id: None,
                application_name: config.app_name.as_deref(),
                polling_concurrency: config.polling_concurrency,
                notification_coalesce: config.notification_coalesce,
                ..postgres::Settings::default()
            },
        })
        .await
        .map_err(Error::SystemDatabase)?;

        tracing::info!(
            app_name = config.app_name.as_deref().unwrap_or("<none>"),
            "DBOS client connected"
        );

        Ok(Self {
            sysdb: Box::new(sysdb),
            serializer: config.serializer.clone(),
            app_name: config.app_name.clone(),
            outcome_poll_interval: config.outcome_poll_interval(),
        })
    }

    /// The system database itself.
    pub(crate) fn sysdb(&self) -> &dyn SystemDatabase {
        &*self.sysdb
    }

    /// How payloads are encoded on their way in and out.
    pub(crate) fn serializer(&self) -> &Serializer {
        &self.serializer
    }

    /// The application whose rows this handle owns, or `None` for a nameless one.
    ///
    /// A nameless handle writes *unclaimed* rows, which belong to every application, and reads
    /// across all of them. Only a [`Client`](crate::Client) can be one:
    /// [`Config::app_name`](crate::Config::app_name) is required and validated, so an application
    /// always has a name.
    pub(crate) fn app_name(&self) -> Option<&str> {
        self.app_name.as_deref()
    }

    /// How often a caller waiting on a workflow it does not own asks whether it has finished.
    pub(crate) fn outcome_poll_interval(&self) -> Duration {
        self.outcome_poll_interval
    }

    /// Releases the connections, the listener and the notifier.
    ///
    /// Idempotent, and on this type rather than on its holders because the connection is what is
    /// being closed: [`DBOS::shutdown`](crate::DBOS::shutdown) reaches it after stopping the
    /// workflows an executor was running, and [`Client::close`](crate::Client::close) reaches it
    /// with nothing to stop first.
    pub(crate) async fn close(&self) {
        self.sysdb.close().await;
    }
}

impl std::fmt::Debug for Connection {
    /// Hand-written because a `dyn SystemDatabase` is not `Debug` and should not become so: the
    /// trait exists to keep a driver type out of the engine, and a `Debug` bound would put one back
    /// in through the derive. The connection URL is deliberately not among the fields — see
    /// [`DBOS`](crate::DBOS)'s own `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("app_name", &self.app_name)
            .field("serializer", &self.serializer)
            .finish_non_exhaustive()
    }
}
