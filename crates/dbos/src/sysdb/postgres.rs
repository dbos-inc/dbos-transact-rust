//! The PostgreSQL implementation of [`SystemDatabase`], which also serves CockroachDB.
//!
//! The pool lives here and is never exposed: the whole point of the trait is that callers
//! cannot depend on which driver is underneath.
//!
//! # Logging
//!
//! Events go through `tracing` with structured fields rather than interpolated messages, and the
//! crate installs no subscriber — a host chooses what to do with them.
//!
//! What gets logged is taken from the four references, which agree on the shape:
//!
//! - **`debug!` on a replay-or-run decision**, for any operation gated by a recorded step. Java
//!   logs `"Replaying setEvent, workflow: {}, step: {}, key: {}"` against `"Running setEvent…"`,
//!   Python the same pair, on `set_event`, `get_event`, `recv`, `sleep`, `send_bulk`, and
//!   `write_stream`. It is the single most useful line when a workflow behaves oddly on recovery,
//!   because it says which side of the replay the caller is on.
//! - **`debug!` with a count** on anything that sweeps or acts in bulk — Go, Python and
//!   TypeScript all log dequeue counts this way. A caller that asked for five and moved three
//!   wants to know without a second query.
//! - **`debug!` on a non-error outcome that changes what the caller should do**: a workflow
//!   recorded but not claimed, an outcome another run already wrote.
//! - **`warn!` before returning a conflict.** Java and TypeScript both warn that a step "was
//!   already recorded" before throwing, because the error alone does not say which execution
//!   lost.
//!
//! Two things deliberately absent. **No `error!`** — errors are returned, and Go and TypeScript
//! logging them before returning double-reports every failure the caller then handles. And **no
//! per-method entry logging**, which Java has (`debug("initWorkflowStatus workflowId {}")`) and
//! nothing else does; a span belongs to the caller, not to every statement.
//!
//! # Application scoping
//!
//! Statements that search or sweep are scoped to the rows this handle may act on, written the
//! same way each time so the shape is recognisable:
//!
//! ```sql
//! ($3::text IS NULL OR application_name = $3 OR application_name IS NULL)
//! ```
//!
//! bound to `self.application_name`. Three parts, each load-bearing:
//!
//! - **`$n IS NULL`** — a handle with no application of its own is not scoped to anything, so it
//!   matches every row. The cast is what lets the driver infer the parameter's type when the
//!   value is `None`.
//! - **`application_name = $n`** — this application's own rows.
//! - **`application_name IS NULL`** — unclaimed rows, which belong to every application. They come
//!   from writers that had no name: an older SDK, or a client acting for nobody in particular.
//!
//! Written as one clause with the name bound rather than as conditional SQL text, so a statement
//! is the same string whether or not the handle has an application — the parameter numbering
//! cannot drift out of step with the binds, and the backend sees one prepared statement instead
//! of two. It is spelled out at each site rather than built by a helper, so the numbering is
//! visible beside the binds it has to match.
//!
//! **Only searches and sweeps carry it.** Anything addressed by workflow id — cancel, resume,
//! delete, fork, the messaging verbs — does not: an id is a global address, so asking for one by
//! id is an identity read, and answering "no such workflow" for one that plainly exists would be
//! a lie. See [`Applications`] for how a caller overrides the default.

// The two halves of the wakeup path, both of which are this backend's rather than the trait's:
// `LISTEN`/`NOTIFY` is PostgreSQL's, and CockroachDB has neither it nor `pg_notify`. The registry
// they feed is in `sysdb::notify`, where it is backend-neutral.
mod listener;
mod notifier;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool, Row};

use self::listener::Listener;
use self::notifier::Notifier;
use super::PARTITIONED_DEQUEUE_SWEEP_CAP;
use super::migrations::{self, quote_identifier};
use super::notify::{EVENTS_CHANNEL, Registry, STREAMS_CHANNEL, event_key, message_key};
use super::retry::{RetryPolicy, with_retry};
use std::sync::Arc;
use std::time::Duration;

use super::types::step_names;
use super::types::{
    ApplicationRowCounts, Applications, AwaitedOutcome, Change, Debounce, DebounceHolder,
    DebounceRequest, EncodedValue, EventRecord, Fork, ForkOptions, ForkPoint, GetEventCaller,
    Message, NewQueue, NewSchedule, NewWorkflow, NotificationRecord, OnExistingQueue, Outcome,
    QueueRecord, QueueUpdate, RateLimit, RenameBatching, RenameFrom, ScheduleFilter,
    ScheduleRecord, ScheduleStatus, ScheduleUpdate, StepRecord, StepTiming, StreamRead,
    StreamRecord, Submission, Timestamp, VersionInfo, WorkflowDelay, WorkflowFilter,
    WorkflowRecord, WorkflowStatus, WrittenBy, duration_from_ms, duration_from_secs,
    is_valid_application_name, validate_attributes,
};
use super::{
    BackendError, BackendErrorKind, DEFAULT_SCHEMA, Error, INTERNAL_QUEUE, NULL_TOPIC,
    OutcomeWrite, STREAM_CLOSED, SystemDatabase, WorkflowInitResult,
};

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        let sqlstate = e
            .as_database_error()
            .and_then(|d| d.code())
            .map(|c| c.into_owned());
        Error::Backend(BackendError {
            kind: classify(&e, sqlstate.as_deref()),
            message: e.to_string(),
            sqlstate,
        })
    }
}

/// Whether a failure is a primary-key or unique-index collision.
///
/// `23505 unique_violation`. For streams this means another writer claimed the offset this one
/// computed, which is contention rather than an error.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| code == "23505")
}

/// Whether a failure is the destination foreign key rejecting an address that does not exist.
///
/// `23503 foreign_key_violation`. Checked by code rather than message text, which varies by
/// server version and locale.
fn is_foreign_key_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| code == "23503")
}

/// Treats an empty string as an absent value.
///
/// Only for the fields where another SDK legitimately writes `""`; everywhere else an empty
/// string is a caller error and [`NewWorkflow::validate`] rejects it.
fn empty_to_none(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.is_empty())
}

/// Whether a driver failure is worth asking again about.
///
/// The classes are the union of the other implementations', which agree on the SQLSTATE prefixes
/// and differ only in how they group them:
///
/// | Prefix | Meaning | Java | Python | Go |
/// |---|---|---|---|---|
/// | `08` | connection exception | connection | connection | retryable |
/// | `57` | operator intervention — shutdown, cannot connect now | connection | connection | retryable |
/// | `53` | insufficient resources — out of memory, too many connections | transient | connection | — |
/// | `40` | serialization failure, deadlock detected | transient | — | transaction-retryable |
///
/// `53` is grouped with connections here, following Python: `53300 too_many_connections` is a
/// failure to obtain a connection, and the difference only decides whether
/// [`RetryPolicy::retry_connection_errors`] can opt out of it.
///
/// Prefixes rather than exact codes, deliberately. The first version of the migration runner's
/// classifier listed codes and was wrong twice; matching the class the standard defines is what
/// stopped that.
///
/// TODO(dbos-team): UPSTREAM item 8. Classifying by class prefix means a *protocol* violation
/// lands in whichever class it happens to carry rather than being called permanent, so some are
/// retried. Worth checking whether the other implementations' classifiers have the same property —
/// they classify by code lists, which have the opposite failure mode.
fn classify(error: &sqlx::Error, sqlstate: Option<&str>) -> BackendErrorKind {
    if let Some(code) = sqlstate {
        return match &code[..2.min(code.len())] {
            "40" => BackendErrorKind::Transient,
            "08" | "53" | "57" => BackendErrorKind::Connection,
            _ => BackendErrorKind::Permanent,
        };
    }
    // No SQLSTATE means the database never answered, so the request never reached it. Java
    // matches driver message text for this case; sqlx gives the variants directly.
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::WorkerCrashed => BackendErrorKind::Connection,
        // Terminal, despite looking like every other connection failure. A closed pool is closed
        // for good — sqlx has no way to reopen one — so waiting for it to come back never ends.
        // Calling it a connection error makes any operation issued after shutdown hang instead of
        // returning, which is the one outcome the retry layer must never produce.
        sqlx::Error::PoolClosed => BackendErrorKind::Permanent,
        _ => BackendErrorKind::Permanent,
    }
}

/// How a handle behaves against a database it is already connected to.
///
/// Split from [`Config`] because the two constructors need different halves of it.
/// [`PostgresSystemDatabase::connect`] has to be told how to *reach* the database;
/// [`from_pool`](PostgresSystemDatabase::from_pool) is handed a live pool and only needs this.
/// Sharing one type would leave `from_pool` ignoring a URL, and giving it setters instead would
/// mean two ways to say the same thing that drift apart as fields are added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings<'a> {
    /// Schema holding the DBOS tables. Defaults to [`DEFAULT_SCHEMA`].
    pub schema: &'a str,
    /// How failures that may pass are waited out.
    pub retry: RetryPolicy,
    /// Identifies this process among the executors sharing the database.
    ///
    /// `None` leaves the executor column alone entirely, which is Python's behaviour when it has
    /// no id to stamp.
    pub executor_id: Option<&'a str>,
    /// Names the application this handle acts for, among the applications sharing the database.
    ///
    /// Rows this handle writes are stamped with it, which is what makes them *owned*. `None` is
    /// the pre-existing world: an anonymous handle writes unclaimed rows, and unclaimed rows
    /// belong to every application. That is why the column is nullable and why nothing requires
    /// a name — a database written by an older SDK contains nothing but unclaimed rows.
    ///
    /// Not an identity of the process, which is [`executor_id`](Self::executor_id): several
    /// executors of one application share a name, and that is the point of having one.
    pub application_name: Option<&'a str>,
    /// How many polling reads may run at once against this handle's pool.
    ///
    /// Every wait here is a loop that re-queries the database, and each pass takes a connection.
    /// Uncapped, enough waiters empty the pool and starve the control plane — enqueue, dequeue,
    /// status writes, recovery, cancellation — leaving the waiters blocked on writes that can no
    /// longer happen.
    ///
    /// `None` is half the pool and at least one, which is Python's default
    /// (`sys_db_polling_concurrency`) and TypeScript's. `Some(0)` switches the cap off, which the
    /// references spell as any non-positive number.
    ///
    /// Named without a `sys_db_` prefix, unlike Python: that prefix distinguishes the system
    /// database's pool from its others, and this field already sits on a `sysdb` type.
    pub polling_concurrency: Option<u32>,
    /// How long a written key waits for company before this handle pushes a wakeup for it.
    ///
    /// `None` is ten milliseconds, which is what Python, TypeScript and Go all default the same
    /// setting to. Longer trades wakeup latency for fewer notifying transactions;
    /// `Some(Duration::ZERO)` turns coalescing off, which is a push per write — the behaviour the
    /// database triggers had, and what migrations 43 and 44 dropped them to get away from.
    ///
    /// Nothing here is load-bearing whatever it is set to: a wakeup only ever shortens a wait that
    /// re-queries on its own interval regardless.
    pub notification_coalesce: Option<Duration>,
}

impl Default for Settings<'_> {
    /// The defaults every implementation shares.
    ///
    /// Hand-written rather than derived because `<&str as Default>` is the empty string, and a
    /// handle addressing schema `""` would fail on its first query rather than at construction.
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA,
            retry: RetryPolicy::default(),
            executor_id: None,
            application_name: None,
            polling_concurrency: None,
            notification_coalesce: None,
        }
    }
}

/// How to reach and set up the system database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config<'a> {
    /// Connection URL for the system database.
    pub url: &'a str,
    /// Maximum pooled connections.
    pub max_connections: u32,
    /// Whether to use LISTEN/NOTIFY rather than polling.
    ///
    /// Here rather than on [`Settings`] because it is a *migration* input as well as a runtime
    /// one: it decides which variant of the schema is applied, and `from_pool` never migrates.
    /// CockroachDB has no LISTEN/NOTIFY, so the dialect forces polling there whatever this says.
    ///
    /// TODO(dbos-team): UPSTREAM item 15. Deciding the schema from a per-process setting is
    /// permanent and affects every application sharing the database — see the note on
    /// [`build_migrations`](crate::sysdb::migrations::build_migrations).
    pub use_listen_notify: bool,
    /// Whether [`PostgresSystemDatabase::connect`] brings the schema up to date.
    ///
    /// On by default. Turning it off suits an application whose database is migrated by something
    /// else — a deployment step, or another executor that got there first — and it then connects
    /// to whatever is already there. Java gates the same step on `DBOSConfig.migrate`.
    pub migrate: bool,
    /// How the resulting handle behaves.
    pub settings: Settings<'a>,
}

impl<'a> Config<'a> {
    /// A configuration with the defaults every implementation shares.
    pub fn new(url: &'a str) -> Self {
        Self {
            url,
            max_connections: 10,
            use_listen_notify: true,
            migrate: true,
            settings: Settings::default(),
        }
    }
}

/// A system database backed by PostgreSQL or CockroachDB.
/// The polling cap for a pool of `pool_size` connections.
///
/// `configured` is the caller's choice: `None` takes the default, and `Some(0)` switches the cap
/// off. Python and TypeScript spell the second as any non-positive number, which they need because
/// their integers are signed and their configuration untyped; here the only unsigned value that can
/// mean "off" is zero.
///
/// **The default is half the pool, and at least one.** Both references use exactly this, and the
/// half is what leaves the other half for the control plane. The minimum matters at
/// `max_connections = 1`, where half is zero — and a cap of zero would not be a small budget but a
/// permanent block, the opposite of what a caller asking for a small pool wants.
///
/// Off is a permit count nothing will exhaust rather than an absent semaphore, so the handle holds
/// one unconditional [`Semaphore`](tokio::sync::Semaphore) with no branch on the acquire path.
fn polling_limit(configured: Option<u32>, pool_size: u32) -> usize {
    match configured {
        Some(0) => tokio::sync::Semaphore::MAX_PERMITS,
        Some(n) => n as usize,
        None => usize::max((pool_size / 2) as usize, 1),
    }
}

pub struct PostgresSystemDatabase {
    pool: PgPool,
    /// Schema-qualified, quoted table names, built once.
    ///
    /// The schema is fixed at construction, so rendering these per query would allocate three
    /// strings on every database operation to produce a value that never changes.
    tables: Tables,
    retry: RetryPolicy,
    executor_id: Option<String>,
    /// The application every row this handle writes is stamped with. See
    /// [`Settings::application_name`].
    application_name: Option<String>,
    /// Bounds the concurrent polling reads this handle's waits make.
    ///
    /// Every wait here is a loop that re-queries the database, and each pass takes a connection.
    /// Uncapped, enough waiters empty the pool and starve the control plane — enqueue, dequeue,
    /// status writes, recovery, cancellation — leaving the waiters blocked on writes that can no
    /// longer happen. Python and TypeScript cap the same thing the same way; see
    /// [`Settings::polling_concurrency`] for the size and [`polling_limit`] for the default.
    ///
    /// **A permit covers one query, never a wait.** A call site acquires, queries, and lets the
    /// permit drop before waiting. Held across the wait it would cap concurrent *waiters* rather
    /// than concurrent queries, and one pool's worth of them would block every later one forever —
    /// a deadlock assembled from operations that are individually fine.
    ///
    /// **It goes inside the retry loop, not around it.** A poll that failed and is backing off is
    /// asleep, not querying, so a permit held through the backoff is held for up to a minute.
    /// Python says the same at each of its call sites: "under the limiter, inside `db_retry` so the
    /// permit frees across backoff."
    ///
    /// Never closed, which is what makes `acquire` infallible at the call sites. Closing the handle
    /// closes the pool, so every in-flight poll fails permanently and releases its permit, and a
    /// waiter parked here then acquires, queries, and gets the same failure one query later.
    polling: tokio::sync::Semaphore,
    /// Who is waiting for what, so that something knowing a row was written can cut a wait short.
    ///
    /// **Nothing wakes it yet**, and every blocking read here is correct anyway: the wait loop is
    /// what delivers, and this only ever shortens an interval. It is subscribed to from the first
    /// caller rather than added alongside the listener because the ordering is the part that cannot
    /// be retrofitted — a caller must be registered *before* it looks, or it misses whatever lands
    /// between the look and the wait.
    notify: Arc<Registry>,
    /// The listener, which is where a wait reads how long it may sleep.
    ///
    /// Held whether or not one is running — an unstarted listener is not delivering, which is the
    /// right answer for CockroachDB and for a handle that never asked for one. Shared with the task
    /// in [`listener_task`](Self::listener_task), which is the only thing that sets the bit.
    listener: Arc<Listener>,
    /// The listener's task, kept so that [`close`](SystemDatabase::close) can wait for it to stop.
    ///
    /// Holding it is what makes closing mean the listener has *ended*, rather than merely that it
    /// has been told to. Without it a listener that failed to recognise the shutdown would go on
    /// reconnecting against a closed pool for the life of the process, and nothing would say so.
    ///
    /// A `std::sync::Mutex` because it is only ever taken and replaced, never held across an await.
    listener_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The other direction: what this handle writes, told to the processes waiting on it.
    ///
    /// Held whether or not anything is being pushed, because the local wake it does is right on
    /// every backend — see [`Notifier::signal`]. Only the outbound half is gated, and by the same
    /// call that starts the listener.
    notifier: Arc<Notifier>,
    /// The notifier's task, kept for the same reason as the listener's — with one difference that
    /// decides the order in [`close`](SystemDatabase::close): its last act is a database write, so
    /// it has to be finished *before* the pool closes rather than by it.
    notifier_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PostgresSystemDatabase {
    /// Connects, creating the database if it does not exist, and migrates it.
    ///
    /// Creating the database is part of connecting rather than part of migrating: you cannot
    /// migrate a database you cannot connect to. Every other DBOS implementation does the same,
    /// so pointing a fresh application at an empty server is expected to work.
    pub async fn connect(config: &Config<'_>) -> Result<Self, Error> {
        // Only when migrating. A caller that has opted out is saying the database is someone
        // else's to set up, and creating one here would hide the fact that it is missing.
        if config.migrate {
            ensure_database_exists(config.url).await?;
        }

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(config.url)
            .await?;

        // Either bring the schema up, or check that whoever was supposed to has. Connecting to a
        // schema this build's queries do not fit is a failure either way; the only question is
        // whether it is reported now or as a missing column on some later statement.
        let prepared = if config.migrate {
            migrations::runner::run(&pool, config.settings.schema, config.use_listen_notify)
                .await
                .map(|_| ())
        } else {
            migrations::runner::verify(&pool, config.settings.schema).await
        };
        // The runner has already retried what it could; whatever reaches here is settled.
        prepared.map_err(|e| {
            Error::Backend(BackendError {
                message: e.to_string(),
                sqlstate: None,
                kind: BackendErrorKind::Permanent,
            })
        })?;

        // Hoisted because the listener needs the same registry the handle's waits subscribe to.
        let notify: Arc<Registry> = Arc::default();
        let handle = Self {
            // The cap's default is read off the pool rather than off `config.max_connections`, so
            // that it is the same expression here and in `from_pool`, which never sees a `Config`.
            polling: tokio::sync::Semaphore::new(polling_limit(
                config.settings.polling_concurrency,
                pool.options().get_max_connections(),
            )),
            listener: Arc::new(Listener::new(pool.clone(), Arc::clone(&notify))),
            notifier: Arc::new(Notifier::new(
                pool.clone(),
                Arc::clone(&notify),
                config.settings.notification_coalesce,
            )),
            pool,
            notify,
            listener_task: std::sync::Mutex::default(),
            notifier_task: std::sync::Mutex::default(),
            tables: Tables::new(config.settings.schema),
            retry: config.settings.retry,
            // Copied out: the handle outlives the borrowed configuration.
            executor_id: config.settings.executor_id.map(str::to_owned),
            application_name: config.settings.application_name.map(str::to_owned),
        };

        // The same flag that decided whether the database got its NOTIFY triggers decides whether
        // this process listens for them, which is UPSTREAM item 15's complaint — but with the
        // triggers absent there would be nothing to hear, so following it here is right whatever
        // the flag's shape should be.
        if config.use_listen_notify {
            handle.start_notifications().await;
        }
        Ok(handle)
    }

    /// Starts this handle's two notification tasks, reporting whether they were started.
    ///
    /// **Both halves of one decision**, which is why there is one call rather than two: a listener,
    /// which turns other processes' writes into wakeups here, and a notifier, which turns this
    /// handle's writes into wakeups there. Neither is much use in a deployment that cannot have the
    /// other, and both are governed by whether the database does `LISTEN`/`NOTIFY` at all.
    ///
    /// **Never load-bearing, and reporting rather than failing for that reason.** Every wait here
    /// re-queries on its own interval and is correct with neither task running; they only shorten
    /// those waits, and let the interval that bounds them lengthen from a second to a minute. So a
    /// deployment that cannot support them is a slower deployment, not a broken one, and nothing
    /// here returns an error a caller would have to decide what to do about.
    ///
    /// Declines on CockroachDB, which has no `LISTEN`/`NOTIFY` — a listener there would be a task
    /// failing and retrying forever, and a push would be a statement the database rejects outright
    /// (`42883`, "unknown function: pg_notify()").
    /// [`connect`](Self::connect) calls this for you when [`Config::use_listen_notify`] is set; a
    /// caller that brought its own pool calls it itself.
    ///
    /// [`close`](SystemDatabase::close) is the shutdown for both.
    pub async fn start_notifications(&self) -> bool {
        match migrations::runner::detect_dialect(&self.pool).await {
            Ok(migrations::Dialect::Cockroach) => {
                tracing::debug!(
                    "not starting a notification listener: CockroachDB has no LISTEN/NOTIFY, so \
                     waits re-query on their own interval, which is what delivers there"
                );
                return false;
            }
            Ok(migrations::Dialect::Postgres) => {}
            Err(e) => {
                tracing::warn!(error = %e, "could not start a notification listener");
                return false;
            }
        }

        // A second call is a caller being careful rather than a mistake, and a second task of
        // either kind would be pure waste: two listeners wake the same waiters twice, and two
        // flush loops drain one queue between them.
        let mut listening = self.listener_task.lock().expect("listener lock");
        if listening.as_ref().is_none_or(|task| task.is_finished()) {
            *listening = Some(tokio::spawn(Arc::clone(&self.listener).run()));
        }
        let mut notifying = self.notifier_task.lock().expect("notifier lock");
        if notifying.as_ref().is_none_or(|task| task.is_finished()) {
            // Enabled first: `signal` queues nothing until it is, and nothing would drain what it
            // queued before the loop exists.
            self.notifier.enable();
            *notifying = Some(tokio::spawn(Arc::clone(&self.notifier).run()));
        }
        true
    }

    /// Whether this handle pushes its own writes to the processes waiting on them.
    ///
    /// The outbound counterpart of [`is_delivering`](Self::is_delivering), and unlike it there is
    /// nothing to prove: a push is a statement that either runs or is logged and dropped, so this
    /// says only that [`start_notifications`](Self::start_notifications) enabled it.
    pub fn is_pushing(&self) -> bool {
        self.notifier.is_pushing()
    }

    /// Whether a notification listener is currently delivering to this handle.
    ///
    /// **Not merely whether one was started.** It is set once a listener has proved a notification
    /// actually arrives, and cleared if it stops — see
    /// [`start_notifications`](Self::start_notifications). False is a working
    /// configuration, not a fault: it means waits re-query every second instead of every minute,
    /// which is what CockroachDB does always and what any deployment does when its subscription
    /// cannot be relied on.
    ///
    /// Java exposes the same as `notificationSource.isRunning()`.
    pub fn is_delivering(&self) -> bool {
        self.listener.is_delivering()
    }

    /// Wraps an existing pool, assuming the schema is already migrated.
    ///
    /// For callers that manage their own connections — and for tests, which migrate through the
    /// harness. Takes the same [`Settings`] as [`connect`](Self::connect), so there is one way to
    /// describe a handle rather than a constructor plus a set of chained overrides.
    pub fn from_pool(pool: PgPool, settings: &Settings<'_>) -> Self {
        // Hoisted because the listener needs the same registry the handle's waits subscribe to.
        let notify: Arc<Registry> = Arc::default();
        Self {
            // A caller's pool knows its own size, so the default needs nothing passed alongside it.
            polling: tokio::sync::Semaphore::new(polling_limit(
                settings.polling_concurrency,
                pool.options().get_max_connections(),
            )),
            listener: Arc::new(Listener::new(pool.clone(), Arc::clone(&notify))),
            notifier: Arc::new(Notifier::new(
                pool.clone(),
                Arc::clone(&notify),
                settings.notification_coalesce,
            )),
            pool,
            notify,
            listener_task: std::sync::Mutex::default(),
            notifier_task: std::sync::Mutex::default(),
            tables: Tables::new(settings.schema),
            retry: settings.retry,
            // Copied out: the handle outlives the borrowed settings.
            executor_id: settings.executor_id.map(str::to_owned),
            application_name: settings.application_name.map(str::to_owned),
        }
    }
}

/// The `WHERE` selecting the rows a rename moves.
///
/// Unclaimed rows are matched only when the source asks for them — the one place in this feature
/// where `IS NULL` does not ride along, because taking a row from every application is a decision.
///
/// Always consumes exactly one parameter, bound to `source.application()`, so a caller's numbering
/// does not depend on which variant it was handed. That is why [`RenameFrom::Unclaimed`] tests the
/// parameter it does not otherwise need: with `NULL` bound, `$n::text IS NULL` is simply true.
fn rename_source_predicate(source: RenameFrom<'_>, param: usize) -> String {
    match source {
        RenameFrom::Application(_) => format!("application_name = ${param}"),
        RenameFrom::ApplicationAndUnclaimed(_) => {
            format!("(application_name = ${param} OR application_name IS NULL)")
        }
        RenameFrom::Unclaimed => format!("(${param}::text IS NULL AND application_name IS NULL)"),
    }
}

impl PostgresSystemDatabase {
    /// Runs `work` as a durable step, on a transaction the step's checkpoint shares.
    ///
    /// The shape every system-database call needs when it must be atomic with the step recording
    /// it: **check, run, record**. Python's `call_txn_as_step` (`_sys_db.py:6407`) and
    /// TypeScript's `runTransactionalStep` (`system_database.ts:1430`) are the same three steps
    /// around a caller's connection; this one owns the transaction instead.
    ///
    /// - **Already recorded** — the stored output is decoded and returned.
    /// - **Succeeds** — the result and the checkpoint commit together, so no crash can leave one
    ///   without the other.
    /// - **Fails** — the transaction rolls back and nothing is recorded, so a replay runs the
    ///   work again. Both references do this, and recording the failure instead would be worse
    ///   than it sounds: a step written from a dropped connection would freeze a transient outage
    ///   into a permanent answer for that workflow.
    ///
    /// TODO(dbos-team): UPSTREAM item 14. That last point is a real asymmetry, not a detail. An *ordinary* step's
    /// failure is recorded and replayed as the same failure in both references; only these
    /// internal ones drop it, so a replay can take a different branch from the run it is
    /// replaying — create fails with "already exists", an operator deletes the schedule, and the
    /// replay succeeds. Worth deciding whether that is intended, and documenting it either way.
    ///
    /// Without a `caller` this is just the work on its own transaction: no step is checked and
    /// none is written.
    ///
    /// `work` takes the transaction by value and hands it back, rather than borrowing it. A
    /// borrowing closure cannot promise its future is `Send`, which every caller needs behind
    /// `#[async_trait]` — the same constraint [`with_retry`] documents.
    async fn run_transactional_step<T, F, Fut>(
        &self,
        caller: Option<(&str, i32)>,
        step_name: &str,
        timing: StepTiming,
        work: F,
    ) -> Result<T, Error>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        F: Fn(sqlx::Transaction<'static, sqlx::Postgres>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(sqlx::Transaction<'static, sqlx::Postgres>, T), Error>> + Send,
    {
        let mut tx = self.pool.begin().await?;

        if let Some((workflow_id, step_id)) = caller
            && let Some(step) = self
                .check_step_on(&mut tx, workflow_id, step_id, step_name)
                .await?
        {
            tx.commit().await?;
            tracing::debug!(workflow_id, step_id, step_name, "replaying a step");
            // Only a success is ever recorded here, so a step carrying an error is a row this
            // path did not write. Python asserts the same thing at its own replay
            // (`_sys_db.py:6420`); reporting beats asserting, but the expectation is identical.
            //
            // A void method reaches here too, and does not trip this: `()` serialises to the
            // four-character string `null`, so the column holds a value rather than SQL NULL.
            // Whether a step ran is answered by the row existing, never by its output being
            // empty — which is also why `Option<T>` returns round-trip correctly, a recorded
            // `None` and an absent step being the same JSON and different answers.
            let recorded = step.output.as_deref().ok_or_else(|| {
                Error::Malformed(format!(
                    "workflow {workflow_id} step {step_id} ({step_name}) has no recorded output"
                ))
            })?;
            return serde_json::from_str(recorded).map_err(|e| {
                Error::Malformed(format!(
                    "workflow {workflow_id} step {step_id} ({step_name}) has an output this \
                     build cannot read: {e}"
                ))
            });
        }

        // A failure rolls the transaction back and records nothing, so the replay runs the work
        // again. Both references do exactly this — Python's `with self.engine.begin()`
        // (`_sys_db.py:6415`) and TypeScript's `catch { ROLLBACK; throw }` (`system_database.ts:1461`)
        // — and the alternative is worse than it sounds: a step recorded from a dropped connection
        // freezes a transient outage into a permanent answer for that workflow.
        let (mut tx, value) = work(tx).await?;

        if let Some((workflow_id, step_id)) = caller {
            // `serde_json` only fails here on a type that cannot be JSON — a non-string map key,
            // a NaN — and every payload this takes is a plain record.
            let recorded = serde_json::to_string(&value)
                .map_err(|e| Error::Malformed(format!("step output is not JSON: {e}")))?;
            self.record_step_on(
                &mut tx,
                workflow_id,
                step_id,
                step_name,
                Outcome::Output(Some(&recorded)),
                Some(PORTABLE_JSON),
                Some(timing),
                None,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(value)
    }

    /// [`upsert_schedule`](SystemDatabase::upsert_schedule) against a caller's transaction.
    ///
    /// Shared with [`apply_schedules`](SystemDatabase::apply_schedules), which differs only in
    /// how many run under one commit.
    ///
    /// Takes a `Transaction` rather than the `&mut PgConnection` its neighbours take, because
    /// this is three statements that have to be one: the resolve, the write, and the read-back
    /// that says whether the write claimed anything. A connection would let them interleave.
    ///
    /// `schedule_id` is a parameter and the application name is not, though both have a field on
    /// [`NewSchedule`] that falls back. The id's fallback generates a UUID, so a caller has to
    /// generate it once outside its retry loop or a second attempt would insert under a fresh id.
    /// The name's fallback is the handle's own, which this can read for itself.
    async fn upsert_schedule_on(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        schedule: &NewSchedule<'_>,
        schedule_id: &str,
    ) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let application_name = schedule
            .application_name
            .or(self.application_name.as_deref());
        // Asked before the write, because a peer's name has to be refused rather than merged into.
        let owner = resolve_owning_application(
            tx,
            schedules_table,
            "schedule_name",
            schedule.schedule_name,
            application_name,
            "Schedule",
        )
        .await?;

        // The conflict clause is the whole design: the definition columns take the new values, and
        // `schedule_id`, `status` and `last_fired_at` are absent, so they keep the stored ones. A
        // redeployment therefore cannot resume a paused schedule or forget where it had got to.
        //
        // Ownership is claimed, never taken: `COALESCE` leaves an owned row alone, so a registration
        // landing between the resolve above and this write keeps the name it took.
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {schedules_table} \
             (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, \
              context, last_fired_at, automatic_backfill, cron_timezone, queue_name, \
              application_name) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (schedule_name) DO UPDATE SET \
               workflow_name = EXCLUDED.workflow_name, \
               workflow_class_name = EXCLUDED.workflow_class_name, \
               schedule = EXCLUDED.schedule, \
               context = EXCLUDED.context, \
               automatic_backfill = EXCLUDED.automatic_backfill, \
               cron_timezone = EXCLUDED.cron_timezone, \
               queue_name = EXCLUDED.queue_name, \
               application_name = COALESCE({schedules_table}.application_name, \
                                           EXCLUDED.application_name)"
        )))
        .bind(schedule_id)
        .bind(schedule.schedule_name)
        .bind(schedule.workflow_name)
        .bind(schedule.workflow_class_name)
        .bind(schedule.schedule)
        .bind(schedule.status.as_str())
        .bind(schedule.context)
        .bind(schedule.last_fired_at.map(Timestamp::to_iso8601))
        .bind(schedule.automatic_backfill)
        .bind(schedule.cron_timezone)
        .bind(schedule.queue_name)
        .bind(owner.as_deref())
        .execute(&mut **tx)
        .await?;

        // Read back, since the `COALESCE` above declines to claim without saying why.
        resolve_owning_application(
            tx,
            schedules_table,
            "schedule_name",
            schedule.schedule_name,
            application_name,
            "Schedule",
        )
        .await?;
        tracing::debug!(
            schedule_name = schedule.schedule_name,
            "registered or updated a schedule"
        );
        Ok(())
    }

    /// Whether `application_version` is the newest this application has registered.
    ///
    /// **True when nothing is registered**, which is what lets a database that has never seen a
    /// deploy dequeue anything at all. Own plus unclaimed: a named peer's registration must not
    /// decide what this application considers current.
    ///
    /// Takes a connection rather than using the pool because a dequeue asks inside its own
    /// transaction — the answer has to be consistent with the rows that transaction goes on to
    /// select.
    async fn is_latest_application_version(
        &self,
        conn: &mut sqlx::PgConnection,
        application_version: &str,
    ) -> Result<bool, Error> {
        let versions_table = self.tables.application_versions.as_str();
        let latest: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT version_name FROM {versions_table} \
             WHERE ($1::text IS NULL OR application_name = $1 OR application_name IS NULL) \
             ORDER BY version_timestamp DESC LIMIT 1"
        )))
        .bind(self.application_name.as_deref())
        .fetch_optional(&mut *conn)
        .await?;
        Ok(latest.is_none_or(|name| name == application_version))
    }
}

/// Which workflows a dequeue on this executor's version may take, with the version at `$param`.
///
/// A workflow that records no version is only eligible for an executor running the latest one, so
/// a rolling deploy stops handing unversioned work to the code being replaced. One recorded on a
/// *different* version is never eligible either way.
fn version_predicate(is_latest: bool, param: usize) -> String {
    if is_latest {
        format!("(application_version = ${param} OR application_version IS NULL)")
    } else {
        format!("application_version = ${param}")
    }
}

/// The tables this backend addresses, quoted and schema-qualified.
///
/// One field per table rather than a map: every lookup is a literal in this file, so a missing
/// table should be a compile error rather than a runtime `None`.
struct Tables {
    workflow_status: String,
    operation_outputs: String,
    application_versions: String,
    notifications: String,
    workflow_events: String,
    workflow_events_history: String,
    streams: String,
    queues: String,
    workflow_schedules: String,
}

impl Tables {
    fn new(schema: &str) -> Self {
        let schema = quote_identifier(schema);
        Self {
            workflow_status: format!("{schema}.{}", quote_identifier("workflow_status")),
            operation_outputs: format!("{schema}.{}", quote_identifier("operation_outputs")),
            application_versions: format!("{schema}.{}", quote_identifier("application_versions")),
            notifications: format!("{schema}.{}", quote_identifier("notifications")),
            workflow_events: format!("{schema}.{}", quote_identifier("workflow_events")),
            workflow_events_history: format!(
                "{schema}.{}",
                quote_identifier("workflow_events_history")
            ),
            streams: format!("{schema}.{}", quote_identifier("streams")),
            queues: format!("{schema}.{}", quote_identifier("queues")),
            workflow_schedules: format!("{schema}.{}", quote_identifier("workflow_schedules")),
        }
    }
}

/// Creates the target database if the server does not already have it.
///
/// Connects to the maintenance database to ask, because you cannot create a database from
/// inside itself. A concurrent creator is not an error — the postcondition is that the database
/// exists, and it does.
async fn ensure_database_exists(url: &str) -> Result<(), Error> {
    let Some((maintenance_url, database)) = split_database(url) else {
        // No database in the URL means the server's default, which necessarily exists.
        return Ok(());
    };

    let admin = match PgPoolOptions::new()
        .max_connections(1)
        .connect(&maintenance_url)
        .await
    {
        Ok(pool) => pool,
        // Reaching the maintenance database is a convenience, not a requirement. If it is not
        // permitted, the target may still exist and be reachable.
        Err(_) => return Ok(()),
    };

    let exists = sqlx::query("SELECT 1 FROM pg_database WHERE datname = $1")
        .bind(&database)
        .fetch_optional(&admin)
        .await?
        .is_some();

    if !exists {
        let quoted = quote_identifier(&database);
        match sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {quoted}")))
            .execute(&admin)
            .await
        {
            Ok(_) => tracing::info!(database = %database, "created the system database"),
            // 42P04 duplicate_database: a peer created it first, which is the outcome wanted.
            Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("42P04") => {}
            Err(e) => return Err(e.into()),
        }
    }
    admin.close().await;
    Ok(())
}

/// Splits a connection URL into a maintenance URL and the database it names.
///
/// Returns `None` when the URL names no database, in which case there is nothing to create.
fn split_database(url: &str) -> Option<(String, String)> {
    let (prefix, tail) = url.split_once("://")?;
    let (authority, rest) = tail.split_once('/')?;
    let (database, query) = match rest.split_once('?') {
        Some((db, q)) => (db, Some(q)),
        None => (rest, None),
    };
    if database.is_empty() {
        return None;
    }
    let mut maintenance = format!("{prefix}://{authority}/postgres");
    if let Some(q) = query {
        maintenance.push('?');
        maintenance.push_str(q);
    }
    Some((maintenance, database.to_owned()))
}

/// Every column `workflow_from_row` reads, in one place so the queries cannot drift from it.
const WORKFLOW_COLUMNS: &str = "workflow_uuid, status, name, class_name, config_name, \
     serialization, executor_id, application_version, recovery_attempts, \
     queue_name, created_at, updated_at, started_at_epoch_ms, completed_at, forked_from, \
     parent_workflow_id, was_forked_from, owner_xid, application_id, authenticated_user, \
     authenticated_roles, assumed_role, request, deduplication_id, priority, \
     queue_partition_key, rate_limited, schedule_name, workflow_timeout_ms, \
     workflow_deadline_epoch_ms, delay_until_epoch_ms, debounce_deadline_epoch_ms, \
     is_debounced, application_name, attributes::text AS attributes";

/// The payload columns, as typed `NULL`s when the caller declines them.
///
/// Held apart from [`WORKFLOW_COLUMNS`] and *appended* rather than substituted into it. Rewriting
/// the list with `str::replace` would match substrings, so a column added later called
/// `last_error` would silently become `last_NULL::text AS error` — valid-looking SQL that
/// selects the wrong thing. The cast is required either way: a bare `NULL` has no type for the
/// driver to decode.
///
/// Order does not matter, because [`workflow_from_row`] reads columns by name.
fn workflow_payloads(load_input: bool, load_output: bool) -> &'static str {
    match (load_input, load_output) {
        (true, true) => "inputs, output, error",
        (true, false) => "inputs, NULL::text AS output, NULL::text AS error",
        (false, true) => "NULL::text AS inputs, output, error",
        (false, false) => "NULL::text AS inputs, NULL::text AS output, NULL::text AS error",
    }
}

/// Encodes the roles list for the column, which is `NULL` when there are none.
///
/// The column is always a JSON array of strings — unlike `input` and the outcome payloads, whose
/// encoding is the caller's. Empty maps to `NULL` so "no roles" has one representation, matching
/// the empty-string normalisation applied to `authenticated_user`.
fn encode_roles(roles: &[&str]) -> Option<String> {
    (!roles.is_empty()).then(|| serde_json::to_string(roles).expect("a list of strings is JSON"))
}

/// Decodes the roles column, treating `NULL` as no roles.
///
/// A value that is not a JSON array of strings is [`Error::Malformed`]: it means another
/// implementation wrote something this one does not understand, which is exactly what that
/// variant is for.
fn decode_roles(stored: Option<String>) -> Result<Vec<String>, Error> {
    let Some(json) = stored else {
        return Ok(Vec::new());
    };
    serde_json::from_str(&json).map_err(|e| {
        Error::Malformed(format!(
            "authenticated_roles is not a JSON array of strings: {e}"
        ))
    })
}

/// Which of the two things a checkpointed sleep is.
///
/// Both write the same row under the same name and both return the same instant. What differs is
/// the `completed_at` the step is stamped with, and so the duration every timeline and step
/// aggregate reports for it.
///
/// **A 2–2 split across the references, but not a coin toss** — it splits by *caller*, and the two
/// implementations that distinguish the callers do so explicitly. Python and TypeScript each carry
/// a flag for exactly this (`project_completion_time`, `recordCompletionAtDeadline`), set for real
/// sleeps and clear for the deadline `recv` and `get_event` register. Java projects both only
/// because `recv` and `getEvent` call the very same `durableSleepEndTime` and it has no flag to do
/// otherwise; Go never projects at all, so its sleeps look instantaneous.
///
/// Private, and the flag those two references expose is not on
/// [`record_sleep`](SystemDatabase::record_sleep): a caller registering a deadline reaches for the
/// blocking read, not for this. Nothing outside this module has both a reason to record a sleep and
/// a reason to choose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SleepKind {
    /// A sleep the caller intends to wait out, stamped complete at the wake time — in the future
    /// when the row is written, so a timeline shows an hour's sleep as an hour.
    Durable,
    /// A deadline the caller registers and usually abandons early, stamped complete now.
    ///
    /// Projecting here would report a `get_event` that answered in 200ms under a 60s timeout as a
    /// minute-long operation, in every step aggregate and on every Conductor timeline.
    Deadline,
}

/// The encoding this layer uses for values it produces itself.
///
/// Distinct from the workflow's own format: a sleep's wake time is a plain number written and
/// read by the system database, so it is stored legibly rather than in whatever the workflow
/// chose. Python does the same for the same value; Java uses the workflow's serializer.
const PORTABLE_JSON: &str = "portable_json";

/// How many times a stream write retries onto a fresh offset before giving up.
///
/// A collision means another writer took the offset this one computed, and the retry recomputes
/// it — so each attempt is lost to a *different* rival, and the loop converges as long as writers
/// are finite. Python loops forever with a 100ms sleep; a bound turns a pathological case into an
/// error a caller can see rather than a call that never returns.
const STREAM_OFFSET_ATTEMPTS: u32 = 16;

/// The database's clock, in epoch milliseconds, as a SQL expression.
///
/// **Used wherever one executor's timestamp is compared against another's.** A rate limit stamps
/// `started_at_epoch_ms` on the row it claims and the next dequeue measures its window back from
/// now; if the two executors read their own clocks, a host running fast writes starts that a peer
/// judges to be outside its window and both admit a full allowance. One clock, so the window
/// means the same thing everywhere. Python spells it `_now_ms_sql` and TypeScript inlines it.
///
/// `now()` is the *transaction's* start time, not the statement's, which is what makes a cutoff
/// and the stamp taken later in the same transaction agree on one instant.
///
/// The `::bigint` is not decoration: `EXTRACT` yields `numeric`, and comparing a `BIGINT` column
/// against a `numeric` casts the column, which costs it its index — and the column this is
/// compared against is the dequeue's own.
const NOW_MS_SQL: &str = "(EXTRACT(epoch FROM now()) * 1000)::bigint";

/// Every column `version_from_row` reads.
/// Every column of `queues` [`queue_from_row`] reads.
const QUEUE_COLUMNS: &str = "name, concurrency, worker_concurrency, rate_limit_max, \
     rate_limit_period_sec, priority_enabled, partition_queue, partition_concurrency, \
     partition_worker_concurrency, partition_rate_limit_max, partition_rate_limit_period_sec, \
     polling_interval_sec, application_name";

/// Every column of a schedule row, in the order [`schedule_from_row`] reads them.
const SCHEDULE_COLUMNS: &str = "schedule_id, schedule_name, workflow_name, workflow_class_name, \
     schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name, \
     application_name";

/// Escapes the wildcards in a `LIKE` prefix so the caller's string matches itself.
///
/// A schedule name containing `%` or `_` is an ordinary name, not a pattern. The backslash is
/// escaped first, or escaping the wildcards would introduce pairs this then re-reads.
///
/// No `ESCAPE` clause accompanies this: backslash is already `LIKE`'s default escape character in
/// both backends, and spelling it out would mean a `'\\'` literal in the statement — which is one
/// character or two depending on `standard_conforming_strings`, and with it off swallows the
/// closing quote and makes the statement a syntax error. The pattern itself is bound, so its
/// backslashes are data and never go through literal parsing at all.
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn schedule_from_row(row: &sqlx::postgres::PgRow) -> Result<ScheduleRecord, Error> {
    let stored: String = row.try_get("status")?;
    let status = ScheduleStatus::parse(&stored).ok_or_else(|| {
        Error::Malformed(format!(
            "schedule status {stored:?} is not one this build knows"
        ))
    })?;
    let stored: Option<String> = row.try_get("last_fired_at")?;
    let last_fired_at = stored
        .map(|stored| {
            Timestamp::parse_iso8601(&stored).ok_or_else(|| {
                Error::Malformed(format!(
                    "last_fired_at {stored:?} is not an ISO-8601 instant"
                ))
            })
        })
        .transpose()?;
    Ok(ScheduleRecord {
        schedule_id: row.try_get("schedule_id")?,
        schedule_name: row.try_get("schedule_name")?,
        workflow_name: row.try_get("workflow_name")?,
        workflow_class_name: row.try_get("workflow_class_name")?,
        schedule: row.try_get("schedule")?,
        status,
        context: row.try_get("context")?,
        last_fired_at,
        automatic_backfill: row.try_get("automatic_backfill")?,
        cron_timezone: row.try_get("cron_timezone")?,
        queue_name: row.try_get("queue_name")?,
        application_name: row.try_get("application_name")?,
    })
}

fn queue_from_row(row: &sqlx::postgres::PgRow) -> Result<QueueRecord, Error> {
    // The periods are `DOUBLE PRECISION` seconds, not the integer milliseconds used elsewhere. A
    // stored value that is negative, infinite or NaN is not a duration any caller can act on, so
    // it is reported rather than clamped.
    let period = |column: &str, value: Option<f64>| match value {
        None => Ok(None),
        Some(secs) => duration_from_secs(secs)
            .map(Some)
            .ok_or_else(|| Error::Malformed(format!("{column} is not a duration: {secs}"))),
    };
    // Both columns or neither: a row carrying one is a state `RateLimit` says cannot exist, and no
    // SDK can write it — all four reject an unpaired limit at their public surface. Read as *no
    // limit* rather than reported, matching TypeScript (`wfqueue.ts:118`), so a hand-edited row
    // does not make a peer and this implementation disagree about what the same queue is.
    //
    // One reader for both pairs, so the queue-wide limit and the per-partition one cannot come to
    // disagree about what half a limit means.
    //
    // TODO(dbos-team): UPSTREAM item 4. Reading it away means a queue whose limit was half
    // written runs unthrottled and says nothing, which is a silent safety failure rather
    // than a cosmetic one; a `CHECK ((rate_limit_max IS NULL) = (rate_limit_period_sec IS
    // NULL))` in a future shared migration would make the question moot for everyone.
    let rate_limit = |max_column: &str, period_column: &str| -> Result<Option<RateLimit>, Error> {
        Ok(
            match (
                row.try_get::<Option<i32>, _>(max_column)?,
                period(period_column, row.try_get(period_column)?)?,
            ) {
                (Some(limit), Some(period)) => Some(RateLimit { limit, period }),
                _ => None,
            },
        )
    };
    Ok(QueueRecord {
        name: row.try_get("name")?,
        concurrency: row.try_get("concurrency")?,
        worker_concurrency: row.try_get("worker_concurrency")?,
        rate_limit: rate_limit("rate_limit_max", "rate_limit_period_sec")?,
        priority_enabled: row.try_get("priority_enabled")?,
        partition_queue: row.try_get("partition_queue")?,
        partition_concurrency: row.try_get("partition_concurrency")?,
        partition_worker_concurrency: row.try_get("partition_worker_concurrency")?,
        partition_rate_limit: rate_limit(
            "partition_rate_limit_max",
            "partition_rate_limit_period_sec",
        )?,
        polling_interval: period("polling_interval_sec", row.try_get("polling_interval_sec")?)?
            .ok_or_else(|| Error::Malformed("polling_interval_sec is null".to_owned()))?,
        application_name: row.try_get("application_name")?,
    })
}

const VERSION_COLUMNS: &str =
    "version_id, version_name, version_timestamp, created_at, application_name";

/// The application a row keyed by name should end up owned by, having read which holds it now.
///
/// Not to be confused with `owner_xid`, the other ownership in this schema: that names an
/// execution attempt, this names an application.
///
/// A nameless writer leaves an existing owner intact; one whose name already matches proceeds;
/// one with a *different* name is refused, because taking the row would redirect the holder's
/// work here. `None` back means the row is unclaimed and this writer has no name to claim it
/// with.
///
/// **Diagnostic, not a guard.** The writes it precedes match only a row that is unclaimed or
/// already this application's, and that is what keeps a peer's row safe. Inside a transaction it
/// still races at READ COMMITTED, where every statement takes a fresh snapshot: a registrar
/// claiming the row in between costs a following write that silently matches nothing.
/// `SELECT … FOR UPDATE` would close that, and neither reference does it — both lock rows only on
/// the dequeue path — so it is a change to raise with them rather than make alone.
///
/// **Exact only while a name is globally unique**, which migrations 9, 13 and 21 guarantee today.
/// When the shared series drops 13's in favour of 106 and 107, a version name may exist once per
/// application and this would return an arbitrary one, so it needs an `application_name` scope at
/// that point — as do Python's and TypeScript's, which also read by name alone. Queue and
/// schedule names have no such replacement.
///
/// TODO(dbos-team): UPSTREAM item 9, per-application queue and schedule names. Their global
/// uniqueness means two applications sharing a system database cannot both register `orders`, so
/// anyone sharing one needs an application prefix by convention because the schema will not
/// disambiguate. Routing is not the obstacle — a dequeue is already scoped by `application_name`,
/// so each would pick up only its own workflows — it is that the registry row *is* the shared
/// configuration: one set of concurrency, rate and polling values per name. A
/// `(application_name, name)` key mirroring 106 and 107 would settle it, but it is a larger change
/// than the version one, because `workflow_status.queue_name` stores a bare string that would then
/// no longer identify a queue on its own.
///
/// TODO(dbos-team): UPSTREAM item 1. This read is exact only while the name is globally unique.
/// Migrations 106 and 107 exist to replace `application_versions`' `UNIQUE (version_name)` with
/// per-application keys, and when the old constraint is finally dropped a name may exist once per
/// application — so this returns an arbitrary matching row and a claimant is non-deterministically
/// refused or allowed. Python's `_resolve_row_owner` and TypeScript's `#resolveRowOwner` read by
/// name alone too, so the drop migration wants an `application_name IS NOT DISTINCT FROM` scope on
/// this read in all three, agreed before anyone writes the drop.
///
/// TODO(dbos-team): UPSTREAM item 2. Callers resolve here and then write, and at READ COMMITTED —
/// the default in all of them — a registrar can claim the row in between. The write is
/// self-guarding, so it matches zero rows rather than landing on the wrong one, but the caller is
/// told `Ok`: an operator can believe a version rollback took effect when it did not. A
/// `SELECT … FOR UPDATE` here would settle it, or reporting rows-affected to the caller.
async fn resolve_owning_application(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    key_column: &str,
    name: &str,
    claimant: Option<&str>,
    kind: &'static str,
) -> Result<Option<String>, Error> {
    let holder: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT application_name FROM {table} WHERE {key_column} = $1"
    )))
    .bind(name)
    .fetch_optional(&mut **tx)
    .await?;

    // No row, or a row nobody owns: this writer's name stands, whatever it is.
    let Some(Some(holder)) = holder else {
        return Ok(claimant.map(str::to_owned));
    };
    match claimant {
        None => Ok(Some(holder)),
        Some(claimant) if claimant == holder => Ok(Some(holder)),
        Some(claimant) => Err(Error::RegisteredByAnother {
            kind: kind.into(),
            name: name.to_owned(),
            holder,
            claimant: Some(claimant.to_owned()),
        }),
    }
}

/// Reads a sleep step's recorded wake time.
///
/// Stored as epoch milliseconds in portable JSON — a bare number, so parsing is the whole of
/// decoding it. A value that is not one means another implementation wrote something this one
/// does not understand, which is what [`Error::Malformed`] is for.
fn decode_wake_time(
    output: Option<&str>,
    workflow_id: &str,
    step_id: i32,
) -> Result<Timestamp, Error> {
    let recorded = output.ok_or_else(|| {
        Error::Malformed(format!(
            "workflow {workflow_id} step {step_id} is a sleep with no recorded wake time"
        ))
    })?;
    recorded
        .trim()
        .parse::<i64>()
        .map(Timestamp::from_epoch_ms)
        .map_err(|e| {
            Error::Malformed(format!(
                "workflow {workflow_id} step {step_id} wake time {recorded:?} is not epoch milliseconds: {e}"
            ))
        })
}

fn version_from_row(row: &sqlx::postgres::PgRow) -> Result<VersionInfo, Error> {
    Ok(VersionInfo {
        version_id: row.try_get("version_id")?,
        version_name: row.try_get("version_name")?,
        version_timestamp: Timestamp::from_epoch_ms(row.try_get("version_timestamp")?),
        created_at: Timestamp::from_epoch_ms(row.try_get("created_at")?),
        application_name: row.try_get("application_name")?,
    })
}

fn workflow_from_row(row: &sqlx::postgres::PgRow) -> Result<WorkflowRecord, Error> {
    let status_text: String = row.try_get("status")?;
    let status = WorkflowStatus::parse(&status_text)
        .ok_or_else(|| Error::Malformed(format!("unknown workflow status {status_text:?}")))?;

    Ok(WorkflowRecord {
        workflow_id: row.try_get("workflow_uuid")?,
        status,
        name: row.try_get("name")?,
        class_name: row.try_get("class_name")?,
        config_name: row.try_get("config_name")?,
        input: row.try_get("inputs")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        serialization: row.try_get("serialization")?,
        executor_id: row.try_get("executor_id")?,
        application_version: row.try_get("application_version")?,
        // `recovery_attempts` is BIGINT; reading it as i64 is exact on both backends.
        recovery_attempts: row
            .try_get::<Option<i64>, _>("recovery_attempts")?
            .unwrap_or(0),
        queue_name: row.try_get("queue_name")?,
        created_at: Timestamp::from_epoch_ms(row.try_get("created_at")?),
        updated_at: Timestamp::from_epoch_ms(row.try_get("updated_at")?),
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at")?
            .map(Timestamp::from_epoch_ms),
        forked_from: row.try_get("forked_from")?,
        parent_workflow_id: row.try_get("parent_workflow_id")?,
        was_forked_from: row
            .try_get::<Option<bool>, _>("was_forked_from")?
            .unwrap_or(false),

        owner_xid: row.try_get("owner_xid")?,
        application_name: row.try_get("application_name")?,
        application_id: row.try_get("application_id")?,
        authenticated_user: row.try_get("authenticated_user")?,
        authenticated_roles: decode_roles(
            row.try_get::<Option<String>, _>("authenticated_roles")?,
        )?,
        assumed_role: row.try_get("assumed_role")?,
        request: row.try_get("request")?,

        deduplication_id: row.try_get("deduplication_id")?,
        priority: row.try_get::<Option<i32>, _>("priority")?.unwrap_or(0),
        queue_partition_key: row.try_get("queue_partition_key")?,
        rate_limited: row
            .try_get::<Option<bool>, _>("rate_limited")?
            .unwrap_or(false),
        schedule_name: row.try_get("schedule_name")?,

        // A duration, where the three below are instants — see `types`.
        timeout: row
            .try_get::<Option<i64>, _>("workflow_timeout_ms")?
            .and_then(duration_from_ms),
        deadline: row
            .try_get::<Option<i64>, _>("workflow_deadline_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        delay_until: row
            .try_get::<Option<i64>, _>("delay_until_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        debounce_deadline: row
            .try_get::<Option<i64>, _>("debounce_deadline_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        is_debounced: row
            .try_get::<Option<bool>, _>("is_debounced")?
            .unwrap_or(false),

        // Cast to text in the query, so no JSON library is needed to move a payload this
        // layer never parses.
        attributes: row.try_get("attributes")?,
    })
}
impl PostgresSystemDatabase {
    /// [`check_step`](SystemDatabase::check_step) against a caller's connection.
    ///
    /// Split out so a transaction can drive it: `set_event` has to check the step and write its
    /// event in one commit, and a step record landing without the write would make a replay skip
    /// the write and lose the event.
    ///
    /// Takes `&mut PgConnection` rather than a generic `PgExecutor` because that trait is
    /// consumed by value — `record_step_on` issues two statements and needs to reborrow. A
    /// transaction derefs to one, and the pool path acquires.
    async fn check_step_on(
        &self,
        conn: &mut sqlx::PgConnection,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let (workflow_table, steps_table) = (workflow_table.as_str(), steps_table.as_str());
        let step_columns = qualified_step_columns("o");
        {
            // One statement, so the status and the step come from a single snapshot. Two queries
            // could straddle a cancellation — read PENDING, get cancelled, then replay a step of
            // a cancelled workflow. Python avoids that by wrapping both in a transaction; the
            // join gets the same consistency, and one round trip instead of two.
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT s.status, {step_columns} \
                 FROM {workflow_table} s \
                 LEFT JOIN {steps_table} o \
                   ON o.workflow_uuid = s.workflow_uuid AND o.function_id = $2 \
                 WHERE s.workflow_uuid = $1"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .fetch_optional(&mut *conn)
            .await?;

            // No row at all means no such workflow — the outer table drives the join.
            let Some(row) = row else {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: vec![workflow_id.to_owned()],
                });
            };
            let status: String = row.try_get("status")?;
            if status == WorkflowStatus::Cancelled.as_str() {
                return Err(Error::WorkflowCancelled {
                    workflow_id: workflow_id.to_owned(),
                });
            }

            // `function_id` is `INT4 NOT NULL` in the table, so a NULL here can only mean the
            // join found nothing: the step has not run.
            if row.try_get::<Option<i32>, _>("function_id")?.is_none() {
                return Ok(None);
            }
            let step = step_from_row(&row, workflow_id)?;

            // The recorded name disagreeing means the workflow's code changed between runs, so
            // this position no longer holds the step that is asking for it.
            if step.step_name != step_name {
                return Err(Error::UnexpectedStep {
                    workflow_id: workflow_id.to_owned(),
                    step_id,
                    expected: step_name.to_owned(),
                    recorded: step.step_name,
                });
            }
            Ok(Some(step))
        }
    }

    /// The one statement behind every recorded step, against a caller's connection.
    ///
    /// Two trait methods reach it and its parameter list is their union, which is why it runs two
    /// over clippy's threshold: [`record_step`](SystemDatabase::record_step) passes no child id,
    /// and [`record_child_result`](SystemDatabase::record_child_result) passes the child whose
    /// outcome the parent is adopting. Both public signatures stay a parameter shorter than this
    /// one, which is the trade — the alternative is a struct that would exist for one call each and
    /// would have to be built by callers chosen so they do not allocate.
    #[allow(clippy::too_many_arguments)]
    async fn record_step_on(
        &self,
        conn: &mut sqlx::PgConnection,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
        // The workflow this step's result was adopted from, for a parent awaiting a child.
        // `None` for a step that did its own work.
        child_workflow_id: Option<&str>,
    ) -> Result<(), Error> {
        if workflow_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "workflow_id".into(),
                detail: "must not be empty".to_owned(),
            });
        }
        if step_id < 0 {
            return Err(Error::InvalidInput {
                field: "step_id".into(),
                detail: "must not be negative".to_owned(),
            });
        }
        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let (workflow_table, steps_table) = (workflow_table.as_str(), steps_table.as_str());
        let executor_id = self.executor_id.as_deref();
        let application_name = self.application_name.as_deref();
        // The sum type collapses to the two nullable columns only here, at the edge.
        let (output, error) = outcome.columns();

        {
            // The step goes in first, and the executor claim follows only if this caller won.
            // Order is what makes two statements safe here: a crash between them leaves a
            // recorded step under a stale executor marker, which the next step corrects. The
            // reverse order — claim, then record — leaves a workflow attributed to an executor
            // with no step to show for it, and nothing later fixes that.
            //
            // And it is why this is not a transaction, which the ordering makes unnecessary. The
            // references split on exactly this line: Python claims before inserting and so wraps
            // both in `engine.begin()`; Java and TypeScript claim after winning and use a plain
            // connection, as here. A transaction would only hold a `workflow_status` row lock
            // across two round trips.
            //
            // The gap it would close is already covered. A *retryable* failure on the second
            // statement re-runs this closure, the insert returns our own completion time, and
            // the claim runs again. A *permanent* one returns an error with the step recorded —
            // and a caller retrying with the same `StepTiming` is recognised as its own write.
            //
            // `DO UPDATE` setting a column to itself rather than `DO NOTHING`: the point is to
            // make `RETURNING` fire on conflict, so the stored completion comes back and can be
            // compared. `DO NOTHING` returns no row, which cannot tell a rival execution from
            // this caller's own retry. Python and TypeScript use the same trick; Java uses
            // `DO NOTHING` and so silently accepts a duplicate.
            //
            // `Option<Option<i64>>`: the outer is "was there a row", the inner is the column,
            // which is nullable because a caller may record a step without timings.
            let stored: Option<Option<i64>> = sqlx::query_scalar(AssertSqlSafe(format!(
                "INSERT INTO {steps_table} (workflow_uuid, function_id, function_name, output, \
                 error, serialization, started_at_epoch_ms, completed_at_epoch_ms, \
                 application_name, child_workflow_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                 ON CONFLICT (workflow_uuid, function_id) DO UPDATE \
                 SET completed_at_epoch_ms = {steps_table}.completed_at_epoch_ms \
                 RETURNING completed_at_epoch_ms"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .bind(step_name)
            .bind(output)
            .bind(error)
            .bind(serialization)
            .bind(timing.map(|t| t.started_at.as_epoch_ms()))
            .bind(timing.map(|t| t.completed_at.as_epoch_ms()))
            // Mirrors the workflow: only the application actually running it records its steps,
            // and the conflict update leaves an existing row's owner alone for the same reason
            // the completion timestamp is left alone.
            .bind(application_name)
            .bind(child_workflow_id)
            .fetch_optional(&mut *conn)
            .await?;

            // The stored completion is ours when it matches, and when neither side has one:
            // without timings we already accept the duplicate as ours, so the claim below has to
            // follow the same reading. A value that differs is another execution's.
            let ours = stored.flatten() == timing.map(|t| t.completed_at.as_epoch_ms());
            if !ours {
                tracing::warn!(
                    workflow_id,
                    step_id,
                    step_name,
                    "step was already recorded by another execution"
                );
                return Err(Error::StepAlreadyRecorded {
                    workflow_id: workflow_id.to_owned(),
                    step_id,
                });
            }

            // Winning the checkpoint is what proves this executor is advancing the workflow, so
            // the claim is conditional on it — an executor that recovered a workflow from a dead
            // peer takes ownership here, and one that lost the race does not. Java guards the
            // same write with `if (won)`; TypeScript's comment reads "Winning the checkpoint
            // proves this executor is advancing the workflow". Python re-stamps unconditionally
            // and beforehand, which claims workflows it has just lost.
            //
            // Skipped when this process has no id, as Python does.
            if let Some(executor_id) = executor_id {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} SET executor_id = $2 \
                     WHERE workflow_uuid = $1 AND executor_id IS DISTINCT FROM $2"
                )))
                .bind(workflow_id)
                .bind(executor_id)
                .execute(&mut *conn)
                .await?;
            }
            Ok(())
        }
    }

    /// Cancels one batch, returning the ids that were still running.
    ///
    /// Not retried in here: that belongs to the trait method that owns the whole operation, since
    /// a retry restarting only this statement would leave the cascade around it half-walked.
    ///
    /// The clock is per level, so a deep tree's `completed_at` values span the walk rather than
    /// sharing one instant. Every other implementation does the same — Python and TypeScript put
    /// `now()` in this statement, Java reads its own inside the equivalent of this method — and
    /// nothing reads the column to decide anything.
    ///
    /// The terminal-status guard is what makes cancellation safe to repeat: a workflow that has
    /// already succeeded keeps its result rather than being overwritten with `CANCELLED`.
    async fn cancel_batch<S>(&self, workflow_ids: &[S]) -> Result<Vec<String>, Error>
    where
        S: AsRef<str> + Sync,
    {
        let workflow_table = &self.tables.workflow_status;
        let ids: Vec<&str> = workflow_ids.iter().map(AsRef::as_ref).collect();
        // TODO(dbos-team): UPSTREAM item 6, clearing `started_at_epoch_ms` here. All four
        // implementations do it identically (`system_database.go:1910`, `system_database.ts:1536`,
        // `_sys_db.py:1062`) and none of them explain it.
        //
        // When this was raised with other members of the DBOS team, the explanation was that
        // clearing the start time was needed for rate limiting.
        //
        // While this explanation is coherent, it does not apply here. The limiter counts starts,
        // not running work: its status filter excludes only `ENQUEUED` and `DELAYED` — the
        // not-yet-started states — so `CANCELLED`, `SUCCESS`, `ERROR` and `PENDING` all count,
        // and a workflow keeps its slot for the rest of the window however it ended. That is
        // deliberate: a workflow that started and then failed still consumed a start, and
        // probably still reached whatever the limiter exists to protect. So a `CANCELLED` row with
        // a recent start really would hold a slot.
        //
        // **But this statement also sets `queue_name = NULL` on the same line, and the count is
        // scoped `WHERE queue_name = $1`.** The row leaves the limiter through the queue name, not
        // through the start time. Clearing the start time buys nothing here.
        //
        // Where the clearing *is* motivated is the paths that put a workflow back on a queue and
        // keep its name — `clear_queue_assignment` and `resume_workflows`. Even there the limiter
        // is not the reason, since those rows land in `ENQUEUED`, which the filter already
        // excludes. The reason is what the column means: "when the current execution started",
        // and a workflow sitting in a queue has not started. Leaving a stale value would also
        // make it ambiguous whether the next dequeue's stamp was the first start.
        //
        // That reasoning looks inherited here, and it runs backwards: a workflow that was running
        // when it was cancelled *did* start, so clearing the column discards true information.
        // The cost is that `started_after`/`started_before` no longer find it, and it ends up
        // with a `completed_at` and no matching start. Kept anyway — diverging from four
        // implementations on a durable column is the worse trade.
        let cancelled: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE {workflow_table} SET status = 'CANCELLED', queue_name = NULL, \
             deduplication_id = NULL, started_at_epoch_ms = NULL, \
             updated_at = {NOW_MS_SQL}, completed_at = {NOW_MS_SQL} \
             WHERE workflow_uuid = ANY($1) \
               AND status NOT IN ('SUCCESS', 'ERROR', 'CANCELLED') \
             RETURNING workflow_uuid"
        )))
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(cancelled)
    }

    /// The workflows whose parent is one of these.
    ///
    /// Generic over the element type so a caller can pass `&[String]` from a frontier or
    /// `&[&str]` from a borrowed seed without cloning either. The only copy is the vector of
    /// pointers the driver needs to encode a `text[]`.
    async fn direct_children<S>(&self, workflow_ids: &[S]) -> Result<Vec<String>, Error>
    where
        S: AsRef<str> + Sync,
    {
        let workflow_table = &self.tables.workflow_status;
        let ids: Vec<&str> = workflow_ids.iter().map(AsRef::as_ref).collect();
        let children: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT workflow_uuid FROM {workflow_table} WHERE parent_workflow_id = ANY($1)"
        )))
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(children)
    }
}

/// Every column of `operation_outputs` [`step_from_row`] reads, except `workflow_uuid`.
///
/// The workflow id is left out because every caller already has it as a parameter, and because
/// `check_step` joins this table against `workflow_status`, where selecting both tables'
/// `workflow_uuid` would be ambiguous.
const STEP_COLUMNS: &str = "function_id, function_name, \
     child_workflow_id, serialization, started_at_epoch_ms, completed_at_epoch_ms";

/// A step's payload columns, on the same terms as [`workflow_payloads`].
fn step_payloads(load_output: bool) -> &'static str {
    if load_output {
        "output, error"
    } else {
        "NULL::text AS output, NULL::text AS error"
    }
}

/// The same columns qualified for a join, derived from [`STEP_COLUMNS`] so the two cannot drift.
///
/// `output`, `error`, and `started_at_epoch_ms` exist on *both* tables, so the qualification is
/// required rather than tidiness.
fn qualified_step_columns(alias: &str) -> String {
    // Payloads included: the replay check always loads them.
    format!("{STEP_COLUMNS}, {}", step_payloads(true))
        .split(',')
        .map(|c| format!("{alias}.{}", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn step_from_row(row: &sqlx::postgres::PgRow, workflow_id: &str) -> Result<StepRecord, Error> {
    Ok(StepRecord {
        workflow_id: workflow_id.to_owned(),
        step_id: row.try_get("function_id")?,
        step_name: row.try_get("function_name")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        child_workflow_id: row.try_get("child_workflow_id")?,
        serialization: row.try_get("serialization")?,
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_epoch_ms")?
            .map(Timestamp::from_epoch_ms),
    })
}

impl PostgresSystemDatabase {
    /// [`record_sleep`](SystemDatabase::record_sleep), for both kinds of caller.
    ///
    /// The trait exposes only the sleep. The deadline a blocking read registers is the same
    /// checkpoint — same step name, same recorded wake time, same replay — differing in the
    /// `completed_at` it is stamped with, and [`SleepKind`] says why that is a distinction worth
    /// making.
    async fn checkpoint_sleep(
        &self,
        workflow_id: &str,
        step_id: i32,
        duration: Duration,
        kind: SleepKind,
    ) -> Result<Timestamp, Error> {
        let pool = &self.pool;
        // Fixed before the retry: the wake time is what a replay must agree on, and re-reading
        // the clock per attempt would push it further out each time.
        let started_at = Timestamp::now();
        let wake_at =
            Timestamp::from_epoch_ms(started_at.as_epoch_ms() + duration.as_millis() as i64);

        with_retry(&self.retry, "checkpoint_sleep", move || async move {
            // No transaction, unlike `set_event`: there is no separate write to orphan here —
            // the step record *is* the write, and the wake time is its output. The check-then-
            // record race is caught by the insert's `ON CONFLICT` below, and the insert and the
            // executor claim inside `record_step_on` are safe by their ordering.
            let mut conn = pool.acquire().await?;

            // A replay wakes at the *original* instant. Starting the clock again would make a
            // workflow that crashed fifty minutes into an hour sleep another full hour.
            if let Some(step) = self
                .check_step_on(&mut conn, workflow_id, step_id, step_names::SLEEP)
                .await?
            {
                tracing::debug!(workflow_id, step_id, "replaying sleep");
                return decode_wake_time(step.output.as_deref(), workflow_id, step_id);
            }
            tracing::debug!(
                workflow_id,
                step_id,
                duration_ms = duration.as_millis() as u64,
                "running sleep"
            );

            let recorded = wake_at.as_epoch_ms().to_string();
            let timing = StepTiming {
                started_at,
                // A sleep is stamped complete at its wake time, which is in the future — so the
                // step's recorded duration is the sleep. A deadline is stamped now, because the
                // caller registering it usually returns long before it. Both are fixed outside the
                // retry, since `completed_at` is also the token that recognises this caller's own
                // write after a lost acknowledgement.
                completed_at: match kind {
                    SleepKind::Durable => wake_at,
                    SleepKind::Deadline => started_at,
                },
            };
            match self
                .record_step_on(
                    &mut conn,
                    workflow_id,
                    step_id,
                    step_names::SLEEP,
                    Outcome::Output(Some(&recorded)),
                    Some(PORTABLE_JSON),
                    Some(timing),
                    None,
                )
                .await
            {
                Ok(()) => Ok(wake_at),
                // A rival recorded the sleep between our check and our write. Its wake time is
                // the one every execution must agree on, so adopt it rather than returning ours.
                // Python swallows this and returns its own, which two runs would disagree about.
                Err(Error::StepAlreadyRecorded { .. }) => {
                    let step = self
                        .check_step_on(&mut conn, workflow_id, step_id, step_names::SLEEP)
                        .await?
                        .ok_or_else(|| {
                            Error::Malformed(
                                "sleep reported as recorded but cannot be read back".to_owned(),
                            )
                        })?;
                    decode_wake_time(step.output.as_deref(), workflow_id, step_id)
                }
                Err(e) => Err(e),
            }
        })
        .await
    }
}

impl PostgresSystemDatabase {
    /// Works out each workflow's start step from its own recorded history.
    ///
    /// One query for the batch, grouped by workflow. Every workflow must contribute a row: a
    /// workflow with no steps produces none, and forking it from "the last step" would silently
    /// mean "from the beginning" — a different request from the one made.
    ///
    /// `aggregate` is the SQL that picks the step, and `named` narrows to one step name. Both
    /// come from the caller's match on [`ForkPoint`], which is where [`ForkPoint::Step`] is
    /// answered — it supplies the step id outright, so there is nothing to look up.
    async fn resolve_fork_points(
        &self,
        workflow_ids: &[&str],
        aggregate: &str,
        named: Option<&str>,
    ) -> Result<Vec<i32>, Error> {
        let steps_table = self.tables.operation_outputs.as_str();
        let pool = &self.pool;

        let filter = if named.is_some() {
            " AND function_name = $2"
        } else {
            ""
        };

        let rows: Vec<(String, i32)> = with_retry(&self.retry, "resolve_fork_points", move || {
            let sql = format!(
                "SELECT workflow_uuid, {aggregate} AS start_step FROM {steps_table} \
                 WHERE workflow_uuid = ANY($1){filter} GROUP BY workflow_uuid"
            );
            async move {
                let mut query = sqlx::query_as(AssertSqlSafe(sql)).bind(workflow_ids);
                if let Some(name) = named {
                    query = query.bind(name);
                }
                Ok(query.fetch_all(pool).await?)
            }
        })
        .await?;

        // Back into the caller's order, which `GROUP BY` does not preserve, reporting anything
        // that produced no row. One pass over the ids, so a workflow is either a start step or a
        // complaint and there is no third case to assert about.
        let resolved: HashMap<&str, i32> =
            rows.iter().map(|(id, step)| (id.as_str(), *step)).collect();
        let mut start_steps = Vec::with_capacity(workflow_ids.len());
        let mut missing = Vec::new();
        for id in workflow_ids {
            match resolved.get(id) {
                Some(&step) => start_steps.push(step),
                None => missing.push((*id).to_owned()),
            }
        }
        if !missing.is_empty() {
            return Err(Error::NoForkPoint {
                workflow_ids: missing,
                step_name: named.map(str::to_owned),
            });
        }

        Ok(start_steps)
    }
}

impl PostgresSystemDatabase {
    /// Every workflow recursively forked from each root, as `root -> descendants`.
    ///
    /// Takes a connection rather than the pool because the answer has to be consistent with the
    /// write that uses it: a fork created between resolving the set and inserting the rows would
    /// otherwise miss the message it should have received.
    ///
    /// Level by level over the whole set of roots at once, as [`Self::direct_children`] does for
    /// the parent/child forest — one query per level of the forest, not per root. The adjacency
    /// is accumulated first and each root's descendants read out of it afterwards, because two
    /// roots can share a subtree and walking per root would visit it twice.
    async fn descendant_forks(
        conn: &mut sqlx::PgConnection,
        table: &str,
        roots: &[&str],
    ) -> Result<HashMap<String, Vec<String>>, Error> {
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut seen: HashSet<String> = roots.iter().map(|r| (*r).to_owned()).collect();
        let mut frontier: Vec<String> = seen.iter().cloned().collect();

        while !frontier.is_empty() {
            let borrowed: Vec<&str> = frontier.iter().map(String::as_str).collect();
            let rows: Vec<(String, String)> = sqlx::query_as(AssertSqlSafe(format!(
                "SELECT workflow_uuid, forked_from FROM {table} \
                 WHERE forked_from = ANY($1) AND forked_from IS NOT NULL"
            )))
            .bind(&borrowed)
            .fetch_all(&mut *conn)
            .await?;

            let mut next = Vec::new();
            for (forked_id, forked_from) in rows {
                children
                    .entry(forked_from)
                    .or_default()
                    .push(forked_id.clone());
                // `seen` only grows, so a cycle — which the data should not contain — stops here
                // rather than looping.
                if seen.insert(forked_id.clone()) {
                    next.push(forked_id);
                }
            }
            frontier = next;
        }

        let mut descendants = HashMap::new();
        for root in roots {
            if descendants.contains_key(*root) {
                continue;
            }
            // A set, so revisiting a node shared by two subtrees costs a hash rather than a
            // scan of everything found so far. `children` stays a `Vec` because its values
            // cannot repeat: an id enters the frontier only once, so each edge is read once.
            let mut found: HashSet<&str> = HashSet::new();
            let mut stack: Vec<&str> = children
                .get(*root)
                .map(|c| c.iter().map(String::as_str).collect())
                .unwrap_or_default();
            while let Some(node) = stack.pop() {
                // A workflow is not its own descendant, stated by comparison rather than by
                // seeding the set with the root.
                if node != *root
                    && found.insert(node)
                    && let Some(grandchildren) = children.get(node)
                {
                    stack.extend(grandchildren.iter().map(String::as_str));
                }
            }
            // Sorted so a caller sees the same order whatever the walk happened to take.
            let mut found: Vec<String> = found.into_iter().map(str::to_owned).collect();
            found.sort();
            descendants.insert((*root).to_owned(), found);
        }
        Ok(descendants)
    }
}

impl PostgresSystemDatabase {
    /// Re-owns a table's rows in half-open key ranges, returning how many moved.
    ///
    /// **Ranges, not `LIMIT`.** A `LIMIT` walks past every row already moved on each pass, turning a
    /// long history into quadratic work, and collecting the keys into an `IN` list plans as a
    /// whole-table hash join. A watermark on the key column reads each row once.
    ///
    /// Two details carry the correctness. The bound is the `batch_size`-th **distinct** key, so a
    /// workflow's steps are never split across two batches. And the final pass — the one that finds
    /// no bound because fewer than a batch remains — **applies no bounds at all**, so rows that
    /// appeared below the watermark while the rename ran still move.
    ///
    /// Every statement is an idempotent re-own, so a run that fails partway can simply be repeated.
    async fn rename_application_in_batches(
        &self,
        table: &str,
        source: RenameFrom<'_>,
        new_name: &str,
        batching: RenameBatching,
    ) -> Result<u64, Error> {
        // Both `workflow_status` and `operation_outputs` tables range over `workflow_uuid`.
        let key_column = "workflow_uuid";
        // `$1` is the new name in the updates, so the source lands on `$2`; the bare `SELECT` below
        // has no new name to bind and starts at `$1`.
        let update_predicate = rename_source_predicate(source, 2);
        let select_predicate = rename_source_predicate(source, 1);
        let update_predicate = update_predicate.as_str();
        let select_predicate = select_predicate.as_str();
        let renamed_from = source.application();
        let pool = &self.pool;

        let RenameBatching::Batched(batch_size) = batching else {
            return with_retry(&self.retry, "rename_rows", move || async move {
                let moved = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET application_name = $1 WHERE {update_predicate}"
                )))
                .bind(new_name)
                .bind(renamed_from)
                .execute(pool)
                .await?
                .rows_affected();
                Ok(moved)
            })
            .await;
        };

        // Python and TypeScript omit each bound until it has a value, so their select comes in two
        // shapes and their update in three, with parameters pushed alongside whichever clauses were
        // appended. `COALESCE($n, '')` collapses the missing-lower-bound case, leaving one select
        // shape and two update shapes here — each with a fixed parameter count, so the bind list
        // cannot drift out of step with the SQL.
        //
        // The `batch_size`-th distinct key bounds each range, inclusively. The offset is one less
        // because `OFFSET` counts from zero, and `DISTINCT` is what keeps one workflow's steps inside a
        // single batch. It is interpolated rather than bound, alone among the values here: a `u32`
        // renders as digits and nothing else, and a literal offset is one the planner can see.
        let select_bound = format!(
            "SELECT DISTINCT {key_column} FROM {table} \
             WHERE {select_predicate} AND {key_column} > COALESCE($2, '') \
             ORDER BY {key_column} LIMIT 1 OFFSET {}",
            batch_size - 1
        );
        // Two update shapes rather than one with nullable bounds, because an upper bound the planner
        // cannot resolve costs as much as a missing lower one: with both bounds bare this is an index
        // range, with either wrapped in a null test it degrades to a scan. Each carries exactly the
        // parameters it names.
        let update_range = format!(
            "UPDATE {table} SET application_name = $1 \
             WHERE {update_predicate} \
               AND {key_column} > COALESCE($3, '') AND {key_column} <= $4"
        );
        // The last pass applies no bounds at all, so rows written below the watermark since the
        // rename began are picked up rather than skipped.
        let update_rest =
            format!("UPDATE {table} SET application_name = $1 WHERE {update_predicate}");
        let select_bound = select_bound.as_str();
        let update_range = update_range.as_str();
        let update_rest = update_rest.as_str();

        let mut total = 0;
        let mut watermark: Option<String> = None;
        loop {
            let above = watermark.clone();
            let (moved, upper) = with_retry(&self.retry, "rename_row_batch", || {
                let above = above.clone();
                async move {
                    let upper: Option<String> = sqlx::query_scalar(AssertSqlSafe(select_bound))
                        .bind(renamed_from)
                        .bind(above.as_deref())
                        .fetch_optional(pool)
                        .await?
                        .flatten();

                    let moved = match &upper {
                        Some(upper) => sqlx::query(AssertSqlSafe(update_range))
                            .bind(new_name)
                            .bind(renamed_from)
                            .bind(above.as_deref())
                            .bind(upper.as_str()),
                        None => sqlx::query(AssertSqlSafe(update_rest))
                            .bind(new_name)
                            .bind(renamed_from),
                    }
                    .execute(pool)
                    .await?
                    .rows_affected();
                    Ok((moved, upper))
                }
            })
            .await?;

            total += moved;
            match upper {
                // Fewer than a full batch remained, so that statement took the rest.
                None => return Ok(total),
                Some(upper) => watermark = Some(upper),
            }
        }
    }
}

#[async_trait]
impl SystemDatabase for PostgresSystemDatabase {
    async fn init_workflow(
        &self,
        workflow: &NewWorkflow,
        max_recovery_attempts: Option<i64>,
        submission: Submission,
    ) -> Result<WorkflowInitResult, Error> {
        workflow.validate()?;
        let workflow_table = &self.tables.workflow_status;
        // Derived, never supplied: a caller cannot enqueue a workflow and label it SUCCESS.
        let initial_status = workflow.initial_status();
        // Queued workflows are not running, so neither the attempt counter nor the executor
        // stamp applies to them — both `CASE` expressions below turn on that distinction.
        let queued = matches!(
            initial_status,
            WorkflowStatus::Enqueued | WorkflowStatus::Delayed
        );
        let initial_attempts = i64::from(!queued);
        // A recovery or a dequeue is being told it owns this workflow; a fresh start is not.
        let claiming = submission.claims_ownership();
        let increment = i64::from(claiming && !queued);
        // Everything below is generated **outside** the retry, and that is the whole reason the
        // retry is a wrapper rather than a loop around the statement.
        //
        // The owner identity is per logical attempt, not per statement. If a commit
        // acknowledgement is lost, the retry must present the same identity or it will fail to
        // recognise its own write and conclude someone else owns the row — which would report
        // `should_execute: false` for a workflow this caller does in fact own. Java's comment
        // says the same: "generated outside of the DB retry loop, in case commit acks get lost".
        let owner_xid = uuid::Uuid::new_v4().to_string();
        // **Every timestamp this row gets is the database's, not this process's**, so none of
        // them carries the enqueuing machine's skew. That matters because the columns are read by
        // *other* processes and compared against instants those processes wrote: `created_at` is
        // the dequeue's FIFO order across the whole fleet, and `delay_until_epoch_ms` is what the
        // supervisor's `transition_delayed_workflows` releases on. A row submitted from a machine
        // running fast would otherwise sort ahead of work that was genuinely enqueued first.
        //
        // [`NOW_MS_SQL`] is `now()`, the *transaction's* start time, so every stamp the statement
        // writes agrees on one instant without a reading being threaded through — which is what a
        // local `Timestamp::now()` was doing here. It also puts the `INSERT` on the same clock as
        // the `ON CONFLICT` arm below, which has always re-stamped `updated_at` from `NOW_MS_SQL`.
        //
        // **The delay crosses the API as a duration for the same reason**, and is added to
        // `NOW_MS_SQL` by the statement rather than resolved here. A retry cannot push it further
        // out even though the addition happens per attempt: the `ON CONFLICT` arm never rewrites
        // `delay_until_epoch_ms`, so an attempt that finds its own earlier write leaves the stamp
        // that landed.
        let delay_ms = workflow.delay.map(|d| d.as_millis() as i64);

        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (workflow_table, pool, owner_xid) =
            (workflow_table.as_str(), &self.pool, owner_xid.as_str());

        with_retry(&self.retry, "init_workflow", move || async move {
            // The column list is Java's INSERT, in its order, plus Python's two debounce
            // columns. Columns absent from it are absent deliberately: `output`, `error`,
            // `started_at`, `completed_at`, `forked_from`, `was_forked_from`, and `rate_limited`
            // are written by execution, forking, and the rate limiter — never at creation.
            //
            // The executor re-stamp is guarded on ownership, which is a deliberate divergence.
            // On conflict the upsert would otherwise hand `executor_id` to whoever submitted
            // last — including a duplicate `Fresh` submission of a workflow another executor is
            // running. That misdirects recovery: `get_pending_workflows` keys on `executor_id`,
            // so the real owner's sweep stops finding the workflow and the submitter's starts,
            // and the second execution is only caught later, at its first step.
            //
            // Java and TypeScript prevent it by letting the write land and rolling the
            // transaction back for a non-owner; Python and Go let it stand. The `CASE` reaches
            // Java's and TypeScript's outcome without their transaction — the row simply keeps
            // its executor.
            //
            // Its middle arm is the lost-acknowledgement case: `owner_xid` is generated once per
            // logical attempt, so it matches an existing row only when this *is* that attempt
            // asking again. Ownership is compared on `owner_xid` and not `executor_id` for the
            // reason migration 7 exists — `executor_id` defaults to `"local"` and collides
            // between processes on one machine.
            //
            // TODO(dbos-team): UPSTREAM item 10, to settle before v1. No implementation does
            // exactly what the `CASE` does, and the 2–2 split is weaker than it looks: Java and
            // TypeScript are the same code (identical `shouldCommit` flag, identical comment),
            // and Go's commit is entangled with its enqueue path, which must commit regardless.
            // Python is the only unambiguous vote for leaving the re-stamp in place, and it may
            // be inheritance rather than intent — the same open question as
            // `started_at_epoch_ms` in `cancel_batch`. Worth confirming, and worth proposing
            // upstream rather than carrying as a Rust-only difference.
            let row = sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {workflow_table} (workflow_uuid, status, inputs, \
                 name, class_name, config_name, \
                 queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms, \
                 authenticated_user, assumed_role, authenticated_roles, \
                 executor_id, application_version, application_id, \
                 created_at, updated_at, recovery_attempts, \
                 workflow_timeout_ms, workflow_deadline_epoch_ms, \
                 parent_workflow_id, owner_xid, serialization, attributes, schedule_name, \
                 debounce_deadline_epoch_ms, is_debounced, application_name) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                 ({NOW_MS_SQL} + $11::bigint), $12, $13, $14, $15, $16, $17, \
                 {NOW_MS_SQL}, {NOW_MS_SQL}, \
                 $18, $19, $20, $21, $22, $23, $24::jsonb, $25, $26, $27, $28) \
                 ON CONFLICT (workflow_uuid) DO UPDATE SET \
                   recovery_attempts = CASE \
                       WHEN {workflow_table}.status != 'ENQUEUED' AND {workflow_table}.status != 'DELAYED' \
                       THEN {workflow_table}.recovery_attempts + $29 \
                       ELSE {workflow_table}.recovery_attempts \
                   END, \
                   updated_at = {NOW_MS_SQL}, \
                   executor_id = CASE \
                       WHEN EXCLUDED.status = 'ENQUEUED' OR EXCLUDED.status = 'DELAYED' \
                       THEN {workflow_table}.executor_id \
                       WHEN {workflow_table}.owner_xid IS NULL \
                         OR {workflow_table}.owner_xid = EXCLUDED.owner_xid \
                         OR $30 \
                       THEN EXCLUDED.executor_id \
                       ELSE {workflow_table}.executor_id \
                   END \
                 RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, \
                 workflow_deadline_epoch_ms, owner_xid, serialization"
            )))
            .bind(workflow.workflow_id)
            .bind(initial_status.as_str())
            .bind(workflow.input)
            .bind(workflow.name)
            .bind(workflow.class_name)
            .bind(workflow.config_name)
            .bind(workflow.queue_name)
            .bind(workflow.deduplication_id)
            .bind(workflow.priority)
            .bind(workflow.queue_partition_key)
            .bind(delay_ms)
            // TypeScript and Go send `""` rather than null when there is no auth context, and
            // Java normalises it on the way in for that reason. Storing both spellings would
            // make an `authenticated_user IS NULL` filter miss rows another SDK wrote.
            .bind(empty_to_none(workflow.authenticated_user))
            .bind(empty_to_none(workflow.assumed_role))
            .bind(encode_roles(&workflow.authenticated_roles))
            .bind(workflow.executor_id)
            .bind(workflow.application_version)
            .bind(workflow.application_id)
            .bind(initial_attempts)
            .bind(workflow.timeout.map(|d| d.as_millis() as i64))
            .bind(workflow.deadline.map(Timestamp::as_epoch_ms))
            .bind(workflow.parent_workflow_id)
            .bind(owner_xid)
            .bind(workflow.serialization)
            .bind(workflow.attributes)
            .bind(workflow.schedule_name)
            .bind(workflow.debounce_deadline.map(Timestamp::as_epoch_ms))
            .bind(workflow.is_debounced)
            // Both Python and TypeScript apply the application name fallback a layer up, in the
            // executor and the client. It is here so that this layer can stand alone.
            //
            // TODO(dbos-team): UPSTREAM item 3, which asks for nothing beyond awareness. Rows come
            // out identical either way, since no upstream path arrives unfilled — but a reviewer
            // comparing implementations should not read the extra fallback as a behavioural
            // difference. It becomes a redundant second line of defence when Phase 2's executor
            // lands, which is a reason to keep it rather than remove it.
            .bind(
                workflow
                    .application_name
                    .or(self.application_name.as_deref()),
            )
            .bind(increment)
            .bind(claiming)
            .fetch_one(pool)
            .await
            // A unique violation here can only be the partial index on
            // `(queue_name, deduplication_id)`: the primary-key conflict is absorbed by
            // `ON CONFLICT (workflow_uuid)` above. Guarded on the caller having supplied a key
            // rather than asserting it, as Python does, so an unexpected violation stays a
            // backend error instead of being mislabelled.
            .map_err(|e| match Error::from(e) {
                Error::Backend(b)
                    if b.sqlstate.as_deref() == Some("23505")
                        && let Some(deduplication_id) = workflow.deduplication_id
                        && let Some(queue_name) = workflow.queue_name =>
                {
                    Error::QueueDeduplicated {
                        workflow_id: workflow.workflow_id.to_owned(),
                        queue_name: queue_name.to_owned(),
                        deduplication_id: deduplication_id.to_owned(),
                    }
                }
                other => other,
            })?;

            let recovery_attempts: i64 = row.try_get("recovery_attempts")?;
            let status_text: String = row.try_get("status")?;
            let status = WorkflowStatus::parse(&status_text).ok_or_else(|| {
                Error::Malformed(format!("unknown workflow status {status_text:?}"))
            })?;
            let stored_owner: Option<String> = row.try_get("owner_xid")?;
            let deadline: Option<i64> = row.try_get("workflow_deadline_epoch_ms")?;
            let serialization: Option<String> = row.try_get("serialization")?;

            // Same id, different function: a programming error rather than a race, because the id
            // is how every implementation decides two attempts are the same workflow.
            for (field, stored, offered) in [
                (
                    "function name",
                    row.try_get::<Option<String>, _>("name")?,
                    &workflow.name,
                ),
                ("class name", row.try_get("class_name")?, &workflow.class_name),
                ("config name", row.try_get("config_name")?, &workflow.config_name),
            ] {
                if stored.as_deref() != offered.as_deref() {
                    return Err(Error::ConflictingWorkflow {
                        workflow_id: workflow.workflow_id.to_owned(),
                        detail: format!(
                            "existing {field} is {stored:?}, but {offered:?} was provided"
                        ),
                    });
                }
            }
            // A differing queue is only a warning: requeueing the same workflow elsewhere is
            // legitimate, and the stored queue wins.
            let stored_queue: Option<String> = row.try_get("queue_name")?;
            if stored_queue.as_deref() != workflow.queue_name {
                tracing::warn!(
                    workflow_id = %workflow.workflow_id,
                    stored = ?stored_queue,
                    provided = ?workflow.queue_name,
                    "workflow already exists on a different queue; the stored queue is kept"
                );
            }

            // Parked once it has been recovered more often than allowed — but only if some *other*
            // attempt is responsible, so a caller retrying its own attempt is not punished for it.
            let owner_differs = stored_owner.as_deref() != Some(owner_xid);
            if let Some(limit) = max_recovery_attempts
                && !status.is_terminal()
                && recovery_attempts > limit + 1
                && owner_differs
            {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} \
                     SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', deduplication_id = NULL, \
                         started_at_epoch_ms = NULL, queue_name = NULL \
                     WHERE workflow_uuid = $1 AND status = 'PENDING'"
                )))
                .bind(workflow.workflow_id)
                .execute(pool)
                .await?;

                return Err(Error::MaxRecoveryAttemptsExceeded {
                    workflow_id: workflow.workflow_id.to_owned(),
                    limit,
                });
            }

            // Another owner holds the row and this is not a recovery, so recording it is right
            // but running it would be a second execution.
            let should_execute = !(owner_differs && !claiming && stored_owner.is_some());
            if !should_execute {
                tracing::debug!(
                    workflow_id = workflow.workflow_id,
                    "another owner holds this workflow; recorded but not claimed"
                );
            }

            Ok(WorkflowInitResult {
                status,
                recovery_attempts,
                deadline: deadline.map(Timestamp::from_epoch_ms),
                serialization,
                should_execute,
            })
        })
        .await
    }

    async fn get_workflow(&self, workflow_id: &str) -> Result<Option<WorkflowRecord>, Error> {
        let workflow_table = &self.tables.workflow_status;
        // Shared references only, so each attempt's future borrows the method rather than the
        // closure. See `with_retry`.
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);

        with_retry(&self.retry, "get_workflow", move || async move {
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT {WORKFLOW_COLUMNS}, {} FROM {workflow_table} WHERE workflow_uuid = $1",
                workflow_payloads(true, true)
            )))
            .bind(workflow_id)
            .fetch_optional(pool)
            .await?;

            row.as_ref().map(workflow_from_row).transpose()
        })
        .await
    }

    async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowRecord>, Error> {
        let workflow_table = &self.tables.workflow_status;
        // `status` is the one filter whose values are not already strings.
        let status: Vec<&str> = filter
            .status
            .iter()
            .copied()
            .map(WorkflowStatus::as_str)
            .collect();
        // `LIKE ANY(...)` needs the wildcard appended to each pattern, and `%` and `_` in a
        // caller's prefix would otherwise be wildcards themselves. Escaped out here because it
        // allocates, and nothing about it depends on the attempt.
        let prefixes: Vec<String> = filter
            .workflow_id_prefixes
            .iter()
            .map(|p| {
                format!(
                    "{}%",
                    p.replace('\\', r"\\")
                        .replace('%', r"\%")
                        .replace('_', r"\_")
                )
            })
            .collect();
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        let (status, prefixes) = (status.as_slice(), prefixes.as_slice());
        let application_name = self.application_name.as_deref();

        with_retry(&self.retry, "list_workflows", move || async move {
            // The builder is rebuilt per attempt, and has to be: `build` borrows it mutably, so
            // a hoisted one would make each attempt's future borrow the closure — which
            // `FnMut() -> Fut` cannot express. It costs nothing on the happy path, where there
            // is one attempt either way.
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            // One row reader serves every query, so a declined column is selected as a typed
            // NULL rather than dropped.
            q.push(WORKFLOW_COLUMNS)
                .push(", ")
                .push(workflow_payloads(filter.load_input, filter.load_output));
            q.push(" FROM ").push(workflow_table);

            // `separated(" AND ")` writes the separator only between clauses, so neither a
            // leading `WHERE` with no filters nor a trailing `AND` is possible by construction.
            let mut first = true;
            let mut clause = |q: &mut sqlx::QueryBuilder<sqlx::Postgres>, sql: &str| {
                q.push(if first { " WHERE " } else { " AND " });
                first = false;
                q.push(sql);
            };

            // `= ANY($n)` rather than `IN ($1, $2, …)`: one placeholder for the whole list,
            // whatever its length, so the SQL text does not vary with the caller's input.
            macro_rules! any_of {
                ($values:expr, $column:literal) => {
                    if !$values.is_empty() {
                        clause(&mut q, concat!($column, " = ANY("));
                        q.push_bind($values).push(")");
                    }
                };
            }
            any_of!(&filter.workflow_ids[..], "workflow_uuid");

            // Whose rows this covers. `Unset` reads differently depending on the rest of the
            // filter: a workflow id is a global address, so asking for one by id is an identity
            // read and answering "no such workflow" for one that plainly exists would be a lie.
            // Any other query is a search, and a search that has not said whose workflows it
            // wants means its own. Prefixes are searches, so they do not count as id-keyed.
            match &filter.applications {
                Applications::Any => {}
                Applications::Named(names) if names.is_empty() => {}
                Applications::Named(names) => {
                    clause(&mut q, "(application_name = ANY(");
                    q.push_bind(&names[..])
                        .push(") OR application_name IS NULL)");
                }
                Applications::Unset if !filter.workflow_ids.is_empty() => {}
                // A handle with no application of its own has nothing to scope to, so it sees
                // every application's rows rather than only the unclaimed ones — which is what
                // `application_name = NULL` would have matched.
                Applications::Unset => {
                    if let Some(name) = application_name {
                        clause(&mut q, "(application_name = ");
                        q.push_bind(name).push(" OR application_name IS NULL)");
                    }
                }
            }

            any_of!(&filter.names[..], "name");
            any_of!(&filter.class_names[..], "class_name");
            any_of!(&filter.config_names[..], "config_name");
            any_of!(status, "status");
            any_of!(&filter.application_versions[..], "application_version");
            any_of!(&filter.executor_ids[..], "executor_id");
            any_of!(&filter.authenticated_users[..], "authenticated_user");
            any_of!(&filter.queue_names[..], "queue_name");
            any_of!(&filter.schedule_names[..], "schedule_name");
            any_of!(&filter.deduplication_ids[..], "deduplication_id");
            any_of!(&filter.parent_workflow_ids[..], "parent_workflow_id");
            any_of!(&filter.forked_from[..], "forked_from");

            if !prefixes.is_empty() {
                clause(&mut q, "workflow_uuid LIKE ANY(");
                q.push_bind(prefixes).push(")");
            }

            macro_rules! compare {
                ($value:expr, $sql:literal) => {
                    if let Some(v) = $value {
                        clause(&mut q, $sql);
                        q.push_bind(v.as_epoch_ms());
                    }
                };
            }
            compare!(filter.created_after, "created_at >= ");
            compare!(filter.created_before, "created_at <= ");
            compare!(filter.completed_after, "completed_at >= ");
            compare!(filter.completed_before, "completed_at <= ");
            compare!(filter.started_after, "started_at_epoch_ms >= ");
            compare!(filter.started_before, "started_at_epoch_ms <= ");

            macro_rules! flag {
                ($value:expr, $sql:literal) => {
                    if let Some(v) = $value {
                        clause(&mut q, $sql);
                        q.push_bind(v);
                    }
                };
            }
            flag!(filter.was_forked_from, "was_forked_from = ");
            flag!(filter.is_debounced, "is_debounced = ");

            if filter.queues_only {
                clause(&mut q, "queue_name IS NOT NULL");
            }
            if let Some(has_parent) = filter.has_parent {
                clause(
                    &mut q,
                    if has_parent {
                        "parent_workflow_id IS NOT NULL"
                    } else {
                        "parent_workflow_id IS NULL"
                    },
                );
            }
            if let Some(attributes) = &filter.attributes {
                // Containment, served by the GIN index. SQLite cannot reproduce `@>` and Go
                // rejects the filter there; both Postgres and CockroachDB support it.
                clause(&mut q, "attributes @> ");
                q.push_bind(attributes).push("::jsonb");
            }

            // **`workflow_uuid` totalizes the order**, which `created_at` alone does not: it is a
            // millisecond stamp, and a fan-out creates far more than one workflow inside a
            // millisecond. Rows sharing one then come back in whatever order the plan produces,
            // and since `limit` and `offset` page through *this* order, a boundary that falls
            // inside a tie can hand the same row to two pages and skip another entirely. The id
            // is what makes a page boundary mean the same thing on the second query as on the
            // first. Migration 46 added the same trailing column to the partition dequeue's index
            // for the same reason, and the dequeue's own `ORDER BY` names it.
            //
            // Python, TypeScript and Go all sort on `created_at` alone and carry the same
            // ambiguity. Sorting more strictly cannot disagree with them: it settles an order
            // they leave unspecified rather than choosing a different one.
            q.push(if filter.sort_desc {
                " ORDER BY created_at DESC, workflow_uuid DESC"
            } else {
                " ORDER BY created_at ASC, workflow_uuid ASC"
            });
            if let Some(limit) = filter.limit {
                q.push(" LIMIT ").push_bind(limit);
            }
            if let Some(offset) = filter.offset {
                q.push(" OFFSET ").push_bind(offset);
            }

            let rows = q.build().fetch_all(pool).await?;
            rows.iter().map(workflow_from_row).collect()
        })
        .await
    }

    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, Error> {
        // The retry wraps the whole walk, and the accumulators sit inside it. Unlike the
        // cascade in `cancel_workflows`, every statement here is a read, so restarting from the
        // root reproduces the same answer — there is nothing a failed attempt did that a later
        // one needs to know about.
        with_retry(&self.retry, "get_workflow_children", move || async move {
            // `descendants` owns every id and the frontier is a *range* into it, because each
            // level is appended in order — so an id is allocated twice, once by the driver and
            // once for the dedup set, rather than three times. The root is excluded by
            // comparison rather than by seeding `seen` with a copy of it, which also states the
            // contract: a workflow is not its own descendant.
            let mut seen = std::collections::HashSet::new();
            let mut descendants: Vec<String> = Vec::new();
            let mut absorb = |into: &mut Vec<String>, children: Vec<String>| {
                for child in children {
                    if child != workflow_id && seen.insert(child.clone()) {
                        into.push(child);
                    }
                }
            };

            absorb(
                &mut descendants,
                self.direct_children(&[workflow_id]).await?,
            );

            // Level by level, as all four do. Terminates because `seen` only grows, so a cycle —
            // which the data should not contain — stops rather than looping.
            let mut start = 0;
            while start < descendants.len() {
                let end = descendants.len();
                let children = self.direct_children(&descendants[start..end]).await?;
                start = end;
                absorb(&mut descendants, children);
            }
            Ok(descendants)
        })
        .await
    }

    async fn record_workflow_outcome(
        &self,
        workflow_id: &str,
        outcome: Outcome<'_>,
    ) -> Result<OutcomeWrite, Error> {
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        let (output, error) = outcome.columns();

        with_retry(&self.retry, "record_workflow_outcome", move || async move {
            // The `status = 'PENDING'` predicate is the whole mechanism: an executor that has
            // been presumed dead and superseded finds zero rows updated, and learns it lost
            // rather than clobbering the winner's result. It also makes this retry-safe — a
            // retry after a lost acknowledgement finds its own write and reports
            // `AlreadyFinished`, which is wrong only in that it is the caller's own outcome.
            // Finishing releases the deduplication key, as it does in all four implementations.
            // The unique index spans every status, so a key left on a finished row would be
            // held forever and nothing could ever be submitted under it again.
            let updated = sqlx::query(AssertSqlSafe(format!(
                "UPDATE {workflow_table} SET status = $2, output = $3, error = $4, \
                 updated_at = {NOW_MS_SQL}, completed_at = {NOW_MS_SQL}, \
                 deduplication_id = NULL \
                 WHERE workflow_uuid = $1 AND status = 'PENDING'"
            )))
            .bind(workflow_id)
            .bind(outcome.status().as_str())
            .bind(output)
            .bind(error)
            .execute(pool)
            .await?
            .rows_affected();

            Ok(if updated > 0 {
                OutcomeWrite::Recorded
            } else {
                tracing::debug!(
                    workflow_id,
                    "outcome not recorded; the workflow is no longer this run's to finish"
                );
                OutcomeWrite::AlreadyFinished
            })
        })
        .await
    }

    async fn await_workflow_result(
        &self,
        workflow_id: &str,
        poll_interval: Duration,
    ) -> Result<AwaitedOutcome, Error> {
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        // Four columns and the attempt count, not the whole row: this runs once per interval for
        // as long as the caller waits, where `get_workflow` reads thirty-odd columns to answer a
        // question about one. Python reads the same narrow set for the same reason.
        let select = format!(
            "SELECT status, output, error, serialization, recovery_attempts \
             FROM {workflow_table} WHERE workflow_uuid = $1"
        );
        let select = &select;
        let polling = &self.polling;

        loop {
            // Inside the loop, so a transient failure is retried by the policy and a lasting one
            // reaches the caller rather than being swallowed by the wait.
            let row = with_retry(&self.retry, "await_workflow_result", move || async move {
                // Inside the retried region, so a poll that is backing off is not holding a permit
                // for the length of its backoff. Released before the sleep below too, so a waiter
                // parked between polls holds nothing: the cap bounds concurrent *queries*, and one
                // that bounded concurrent waiters would deadlock at the first pool's worth of them.
                let _permit = polling
                    .acquire()
                    .await
                    .expect("the polling limiter is never closed");
                Ok(sqlx::query(AssertSqlSafe(select.clone()))
                    .bind(workflow_id)
                    .fetch_optional(pool)
                    .await?)
            })
            .await?;

            match row {
                Some(row) => {
                    let status_text: String = row.try_get("status")?;
                    let status = WorkflowStatus::parse(&status_text).ok_or_else(|| {
                        Error::Malformed(format!("unknown workflow status {status_text:?}"))
                    })?;
                    let settled = match status {
                        WorkflowStatus::Success => Some(AwaitedOutcome::Succeeded {
                            output: row.try_get("output")?,
                            serialization: row.try_get("serialization")?,
                        }),
                        WorkflowStatus::Error => Some(AwaitedOutcome::Failed {
                            // A failed workflow with no error is a row no implementation writes:
                            // the status and the payload are set by one statement. Reported rather
                            // than turned into an empty message, which a caller would try to
                            // deserialize.
                            error: row.try_get::<Option<String>, _>("error")?.ok_or_else(|| {
                                Error::Malformed(format!(
                                    "workflow {workflow_id} failed with no error recorded"
                                ))
                            })?,
                            serialization: row.try_get("serialization")?,
                        }),
                        WorkflowStatus::Cancelled => Some(AwaitedOutcome::Cancelled),
                        WorkflowStatus::MaxRecoveryAttemptsExceeded => {
                            Some(AwaitedOutcome::Parked {
                                recovery_attempts: row
                                    .try_get::<Option<i64>, _>("recovery_attempts")?
                                    .unwrap_or_default(),
                            })
                        }
                        // Still running, queued, or waiting to be released. Nothing to report yet.
                        WorkflowStatus::Pending
                        | WorkflowStatus::Enqueued
                        | WorkflowStatus::Delayed => None,
                    };
                    if let Some(settled) = settled {
                        tracing::debug!(workflow_id, status = status.as_str(), "workflow settled");
                        return Ok(settled);
                    }
                }
                // Waiting for a workflow to finish, not for one to exist. A caller holding an id
                // from outside this process loops over this error.
                None => {
                    return Err(Error::NonExistentWorkflow {
                        workflow_ids: vec![workflow_id.to_owned()],
                    });
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay: WorkflowDelay,
    ) -> Result<(), Error> {
        let workflow_table = &self.tables.workflow_status;
        // **A relative delay is resolved by the database**, for the reason `init_workflow` sends
        // one as a duration: the supervisor that releases the workflow compares this column
        // against its own clock, and a caller's skew must not decide when that happens. An
        // absolute delay is the caller's instant by definition and goes in as it stands.
        //
        // Resolving per attempt cannot make a relative delay creep further out, because the
        // statement is `SET`, not `+=`: a retry overwrites whatever the lost attempt wrote with
        // the same duration measured from the retry — which is the delay the caller asked for,
        // starting from when the request actually landed.
        let (delay_expr, delay_ms) = match delay {
            WorkflowDelay::For(d) => (format!("({NOW_MS_SQL} + $2::bigint)"), d.as_millis() as i64),
            WorkflowDelay::Until(t) => ("$2::bigint".to_owned(), t.as_epoch_ms()),
        };
        let (workflow_table, pool, delay_expr) =
            (workflow_table.as_str(), &self.pool, delay_expr.as_str());

        with_retry(&self.retry, "set_workflow_delay", move || async move {
            // `status = 'DELAYED'` is the guard: a released workflow is running or queued, and
            // pushing its delay out would not recall it.
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {workflow_table} \
                 SET delay_until_epoch_ms = {delay_expr}, updated_at = {NOW_MS_SQL} \
                 WHERE workflow_uuid = $1 AND status = 'DELAYED'"
            )))
            .bind(workflow_id)
            .bind(delay_ms)
            .execute(pool)
            .await?;
            Ok(())
        })
        .await
    }

    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, Error> {
        let workflow_table = &self.tables.workflow_status;
        let now = Timestamp::now().as_epoch_ms();
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);

        with_retry(&self.retry, "clear_queue_assignment", move || async move {
            // `queue_name IS NOT NULL` is what makes this a *return* rather than an enqueue: a
            // workflow that never came from a queue has none to go back to.
            let updated = sqlx::query(AssertSqlSafe(format!(
                "UPDATE {workflow_table} SET started_at_epoch_ms = NULL, status = 'ENQUEUED', \
                 updated_at = $2 \
                 WHERE workflow_uuid = $1 AND queue_name IS NOT NULL AND status = 'PENDING'"
            )))
            .bind(workflow_id)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();
            Ok(updated > 0)
        })
        .await
    }

    async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
    ) -> Result<(), Error> {
        validate_attributes(attributes)?;
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);

        with_retry(
            &self.retry,
            "update_workflow_attributes",
            move || async move {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} \
                     SET attributes = $2::jsonb, updated_at = {NOW_MS_SQL} \
                     WHERE workflow_uuid = $1"
                )))
                .bind(workflow_id)
                .bind(attributes)
                .execute(pool)
                .await?;
                Ok(())
            },
        )
        .await
    }

    async fn get_pending_workflows(
        &self,
        executor_id: &str,
        application_version: &str,
    ) -> Result<Vec<String>, Error> {
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        let application_name = self.application_name.as_deref();
        // Correctness, not tidiness. `executor_id` defaults to `"local"` — Rust follows Go here —
        // so two applications running on one machine present the same executor to this query.
        // Without the scope each would recover the other's workflows: it would find them, decide
        // they are its own to restart, and run functions it has never heard of. Migration 7's
        // `owner_xid` does not help, because a recovery sweep is looking for workflows whose
        // owner is *gone*.

        with_retry(&self.retry, "get_pending_workflows", move || async move {
            let ids: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {workflow_table} \
                 WHERE status = 'PENDING' AND executor_id = $1 AND application_version = $2 \
                   AND ($3::text IS NULL \
                        OR application_name = $3 \
                        OR application_name IS NULL)"
            )))
            .bind(executor_id)
            .bind(application_version)
            .bind(application_name)
            .fetch_all(pool)
            .await?;
            Ok(ids)
        })
        .await
    }

    async fn reenqueue_for_recovery(
        &self,
        executor_ids: &[&str],
        application_version: &str,
        recovery_queue: &str,
    ) -> Result<Vec<String>, Error> {
        if executor_ids.is_empty() {
            return Ok(Vec::new());
        }
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        let application_name = self.application_name.as_deref();

        with_retry(&self.retry, "reenqueue_for_recovery", move || async move {
            // `started_at_epoch_ms` is cleared because the row has not started yet — leaving the
            // dead run's start time would make the queue wait look like execution.
            //
            // `NULLIF` is Go's, and it is for rows this implementation did not write: older
            // versions stored "not queued" as the empty string rather than NULL, and without it
            // such a row would keep `''` as its queue name and never be polled by anything.
            let ids: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "UPDATE {workflow_table} \
                 SET status = 'ENQUEUED', \
                     started_at_epoch_ms = NULL, \
                     updated_at = {NOW_MS_SQL}, \
                     queue_name = COALESCE(NULLIF(queue_name, ''), $1) \
                 WHERE status = 'PENDING' \
                   AND executor_id = ANY($2) \
                   AND application_version = $3 \
                   AND ($4::text IS NULL \
                        OR application_name = $4 \
                        OR application_name IS NULL) \
                 RETURNING workflow_uuid"
            )))
            .bind(recovery_queue)
            .bind(executor_ids)
            .bind(application_version)
            .bind(application_name)
            .fetch_all(pool)
            .await?;
            Ok(ids)
        })
        .await
    }

    async fn transition_delayed_workflows(&self) -> Result<u64, Error> {
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);
        let application_name = self.application_name.as_deref();
        // A sweep, so it is scoped: releasing a peer's delayed workflow would enqueue it against
        // that peer's schedule rather than its own. Unclaimed rows are still released, which is
        // what lets an application take over work an unnamed client enqueued.

        with_retry(
            &self.retry,
            "transition_delayed_workflows",
            move || async move {
                // **The cutoff is the database's clock, not this supervisor's**, and it has to
                // be: `delay_until_epoch_ms` is written by whichever process enqueued the
                // workflow, so comparing it here against a local reading would compare two
                // machines' clocks and release work early or late by their skew. Every writer of
                // the column stamps it from [`NOW_MS_SQL`] too, so both sides read one clock.
                //
                // Read per attempt rather than hoisted, which `now()` gives for free: it is the
                // transaction's start time, so a retry releases whatever has since come due
                // instead of replaying a stale cutoff, and the `updated_at` stamp agrees with the
                // cutoff that selected the row.
                //
                // Clearing the debounce key belongs in this statement, not a second one. The id
                // is held only while the workflow is DELAYED; once released the workflow is
                // committed to running, and a later debounce with the same key must start a
                // fresh workflow rather than bounce this one.
                let moved = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} SET status = 'ENQUEUED', updated_at = {NOW_MS_SQL}, \
                     deduplication_id = CASE WHEN is_debounced THEN NULL \
                                             ELSE deduplication_id END \
                     WHERE status = 'DELAYED' AND delay_until_epoch_ms <= {NOW_MS_SQL} \
                       AND ($1::text IS NULL \
                            OR application_name = $1 \
                            OR application_name IS NULL)"
                )))
                .bind(application_name)
                .execute(pool)
                .await?
                .rows_affected();
                if moved > 0 {
                    tracing::debug!(moved, "released delayed workflows");
                }
                Ok(moved)
            },
        )
        .await
    }

    async fn cancel_workflows(
        &self,
        workflow_ids: &[&str],
        cancel_children: bool,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        // The retry wraps the whole cascade, not each statement inside it. A failure partway
        // through leaves some of the tree cancelled, and restarting from the roots finishes the
        // job — where retrying one statement would return a half-walked tree as a success.
        //
        // `cancelled` survives across attempts, which is only sound because `cancel_batch`
        // excludes rows that are already `CANCELLED`: a workflow can be returned once and never
        // again, so a retry adds what the failed attempt did not reach and cannot double-report
        // what it did. The walk itself restarts from the roots each attempt, since there is no
        // way to know how far the last one got.
        //
        // Not for concurrency — nothing else touches this. It is how an accumulator outlives a
        // retry while the closure stays `FnMut` returning a `Send` future, which is what
        // `with_retry` requires. A plain `&mut Vec` would make the future borrow the closure.
        let cancelled = std::sync::Mutex::new(Vec::new());
        let collected = &cancelled;
        with_retry(&self.retry, "cancel_workflows", move || async move {
            // One statement per *level*, not per workflow: `cancel_batch` takes the whole
            // frontier and matches it with `= ANY($1)`, so the round trips scale with the depth
            // of the tree rather than its size. Cancelling the level before asking for its
            // children is the ordering that stops a parent spawning behind the walk.
            //
            // The roots are used borrowed; only the levels below are owned, because those come
            // back from the database already allocated.
            // Awaited before locking: a guard held across an await would make this future
            // non-`Send`, which `with_retry` requires.
            let roots = self.cancel_batch(workflow_ids).await?;
            collected.lock().expect("cancelled ids").extend(roots);
            if !cancel_children {
                return Ok(());
            }
            // Owned, because the levels below arrive owned from the database and the set has to
            // hold both.
            let mut seen: std::collections::HashSet<String> =
                workflow_ids.iter().map(|id| (*id).to_owned()).collect();
            let mut frontier = self.direct_children(workflow_ids).await?;
            frontier.retain(|c| seen.insert(c.clone()));

            // Terminates because `seen` only grows and a workflow enters a frontier at most once.
            while !frontier.is_empty() {
                let level = self.cancel_batch(&frontier).await?;
                collected.lock().expect("cancelled ids").extend(level);
                let children = self.direct_children(&frontier).await?;
                frontier = children
                    .into_iter()
                    .filter(|c| seen.insert(c.clone()))
                    .collect();
            }
            Ok(())
        })
        .await?;
        let cancelled = cancelled.into_inner().expect("cancelled ids");
        tracing::debug!(
            requested = workflow_ids.len(),
            cancelled = cancelled.len(),
            cancel_children,
            "cancelled workflows"
        );
        Ok(cancelled)
    }

    async fn resume_workflows(
        &self,
        workflow_ids: &[&str],
        queue_name: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }
        let workflow_table = &self.tables.workflow_status;
        let queue = queue_name.unwrap_or(INTERNAL_QUEUE);
        let (workflow_table, pool) = (workflow_table.as_str(), &self.pool);

        with_retry(&self.retry, "resume_workflows", move || async move {
            // Existence is asked separately because a zero-row update conflates "already
            // finished" — which is legal — with "no such workflow", which is not.
            let existing: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {workflow_table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(workflow_ids)
            .fetch_all(pool)
            .await?;
            let missing: Vec<String> = workflow_ids
                .iter()
                .filter(|id| !existing.iter().any(|e| e == *id))
                .map(|id| (*id).to_owned())
                .collect();
            if !missing.is_empty() {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: missing,
                });
            }

            // `completed_at` and `started_at_epoch_ms` are cleared as well as the counters: the
            // workflow is going to run again, and leaving them set would date it to its last
            // attempt.
            let resumed: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "UPDATE {workflow_table} SET status = 'ENQUEUED', queue_name = $2, \
                 recovery_attempts = 0, workflow_deadline_epoch_ms = NULL, \
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL, \
                 updated_at = {NOW_MS_SQL} \
                 WHERE workflow_uuid = ANY($1) AND status NOT IN ('SUCCESS', 'ERROR') \
                 RETURNING workflow_uuid"
            )))
            .bind(workflow_ids)
            .bind(queue)
            .fetch_all(pool)
            .await?;
            tracing::debug!(
                requested = workflow_ids.len(),
                resumed = resumed.len(),
                queue,
                "resumed workflows"
            );
            Ok(resumed)
        })
        .await
    }

    async fn delete_workflows(
        &self,
        workflow_ids: &[&str],
        delete_children: bool,
    ) -> Result<u64, Error> {
        if workflow_ids.is_empty() {
            return Ok(0);
        }
        // Descendants arrive owned from the database and are kept alive here; the roots stay
        // borrowed, so `targets` copies pointers rather than strings.
        let mut children: Vec<String> = Vec::new();
        if delete_children {
            // Collected before the delete rather than interleaved: a deleted parent cannot spawn,
            // so there is no race to close, and one statement is cheaper than one per level.
            for id in workflow_ids {
                children.extend(self.get_workflow_children(id).await?);
            }
        }
        let mut targets: Vec<&str> = workflow_ids.to_vec();
        targets.extend(children.iter().map(String::as_str));
        targets.sort_unstable();
        targets.dedup();
        let workflow_table = &self.tables.workflow_status;
        let (workflow_table, pool, targets) =
            (workflow_table.as_str(), &self.pool, targets.as_slice());

        with_retry(&self.retry, "delete_workflows", move || async move {
            // Steps, notifications, events, and streams go with the row: every child table
            // declares `ON DELETE CASCADE` on this foreign key, from migration 1 onward.
            let deleted = sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {workflow_table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(targets)
            .execute(pool)
            .await?
            .rows_affected();
            tracing::debug!(deleted, targets = targets.len(), "deleted workflows");
            Ok(deleted)
        })
        .await
    }

    async fn fork_workflows(
        &self,
        forks: &[Fork<'_>],
        options: &ForkOptions<'_>,
    ) -> Result<Vec<String>, Error> {
        if forks.is_empty() {
            return Ok(Vec::new());
        }
        options.validate()?;
        for fork in forks {
            fork.validate()?;
        }

        // Generated **outside** the retry, like `init_workflow`'s owner identity and for the same
        // reason: a fresh id on the second attempt would not recognise the first attempt's write,
        // so a lost commit acknowledgement would fork every workflow twice.
        let forked_ids: Vec<String> = forks
            .iter()
            .map(|f| {
                f.forked_id
                    .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned)
            })
            .collect();

        // The batch is passed as three parallel arrays and reassembled server-side by `unnest`,
        // so the SQL text is the same whatever the caller passes: the server can reuse a plan,
        // and a large batch cannot grow a statement without bound.
        //
        // **A deliberate divergence.** None of the references map the batch this way — Python
        // unions a `SELECT literal(...)` per fork, Go does the same with casts, and TypeScript
        // builds a `VALUES` CTE, all three emitting SQL that grows a row per workflow. The
        // construct is not exotic, though: Python and TypeScript both `unnest` a `text[]` to
        // batch `pg_notify`. What is new is unnesting three arrays in parallel, which is why it
        // is checked against CockroachDB as well as PostgreSQL rather than assumed portable.
        //
        // Mismatched lengths would pad with `NULL` rather than fail, so the three are built from
        // one iteration of `forks` and cannot disagree — which is also why [`Fork`] is a struct
        // per workflow and not three slices.
        let source_ids: Vec<&str> = forks.iter().map(|f| f.source_id).collect();
        let start_steps: Vec<i32> = forks.iter().map(|f| f.start_step).collect();
        // Nothing precedes step 0, so a fork from there has nothing to carry. **TypeScript's
        // guard, not Python's**: TypeScript skips `startStep > 0` and Python skips `step > 1`,
        // and with steps_table numbered from zero the latter drops step 0 from every fork that resumes
        // at step 1 — the fork then re-runs a step it was given the result of.
        let copies_anything = start_steps.iter().any(|&step| step > 0);

        let queue_name = options.queue_name.unwrap_or(INTERNAL_QUEUE);
        // Reported rather than saturated, following `Timestamp::checked_add` and the rule stated
        // on `to_system_time`: a value this layer cannot store is the caller's to hear about, not
        // one to quietly replace with a different timeout.
        let timeout_ms = match options.timeout.map(|t| i64::try_from(t.as_millis())) {
            None => None,
            Some(Ok(ms)) => Some(ms),
            Some(Err(_)) => {
                return Err(Error::InvalidInput {
                    field: "timeout".into(),
                    detail: "must fit in milliseconds as a 64-bit integer".to_owned(),
                });
            }
        };
        let (replace_from, replace_to): (Vec<&str>, Vec<&str>) =
            options.replacement_children.iter().copied().unzip();

        let workflow_table = &self.tables.workflow_status;
        let steps_table = &self.tables.operation_outputs;
        let events_table = &self.tables.workflow_events;
        let history_table = &self.tables.workflow_events_history;
        let streams_table = &self.tables.streams;
        let (workflow_table, steps_table, events_table, history_table, streams_table) = (
            workflow_table.as_str(),
            steps_table.as_str(),
            events_table.as_str(),
            history_table.as_str(),
            streams_table.as_str(),
        );
        let pool = &self.pool;
        let (source_ids, forked_ids, start_steps) = (&source_ids, &forked_ids, &start_steps);
        let (replace_from, replace_to) = (&replace_from, &replace_to);
        let application_name = self.application_name.as_deref();

        with_retry(&self.retry, "fork_workflows", move || async move {
            let mut tx = pool.begin().await?;

            // Every source must exist before anything is written. A batch that forked the
            // workflows it could find would leave a caller holding ids for forks that are not
            // there, with nothing to say which.
            let found: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {workflow_table} WHERE workflow_uuid = ANY($1)"
            )))
            .bind(source_ids)
            .fetch_all(&mut *tx)
            .await?;
            let missing: Vec<String> = source_ids
                .iter()
                .filter(|id| !found.iter().any(|f| f == *id))
                .map(|id| (*id).to_owned())
                .collect();
            if !missing.is_empty() {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: missing,
                });
            }

            // The fork inherits its source's identity and starts enqueued. `application_version`
            // falls back to the source's, matching Go and TypeScript: a fork stamped with no
            // version would be invisible to the recovery that scopes by it.
            //
            // `application_name` falls back the other way — the source's owner wins, and this
            // application's name is used only to claim a source nobody owns, which is what a
            // dequeue would do anyway. A fork has to run on the same application as its source:
            // it replays that application's recorded steps.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {workflow_table} (workflow_uuid, status, name, class_name, config_name, \
                    application_version, application_id, authenticated_user, authenticated_roles, \
                    assumed_role, inputs, serialization, request, queue_name, \
                    queue_partition_key, forked_from, attributes, workflow_timeout_ms, \
                    application_name) \
                 SELECT m.fork_id, 'ENQUEUED', w.name, w.class_name, w.config_name, \
                    COALESCE($4, w.application_version), w.application_id, w.authenticated_user, \
                    w.authenticated_roles, w.assumed_role, w.inputs, w.serialization, w.request, \
                    $5, $6, w.workflow_uuid, w.attributes, $7, \
                    COALESCE(w.application_name, $8) \
                 FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                 JOIN {workflow_table} w ON w.workflow_uuid = m.source_id"
            )))
            .bind(source_ids)
            .bind(forked_ids)
            .bind(start_steps)
            .bind(options.application_version)
            .bind(queue_name)
            .bind(options.queue_partition_key)
            .bind(timeout_ms)
            .bind(application_name)
            .execute(&mut *tx)
            .await?;

            // What makes a source discoverable as a fork point afterwards.
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {workflow_table} SET was_forked_from = TRUE WHERE workflow_uuid = ANY($1)"
            )))
            .bind(source_ids)
            .execute(&mut *tx)
            .await?;

            if copies_anything {
                // The recorded steps_table, which are what the fork replays instead of running. The
                // `CASE` rewrites recorded children when the caller is forking a whole tree;
                // with no replacements it collapses to the original column.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {steps_table} (workflow_uuid, function_id, output, error, \
                        serialization, function_name, child_workflow_id, started_at_epoch_ms, \
                        completed_at_epoch_ms, application_name) \
                     SELECT m.fork_id, o.function_id, o.output, o.error, o.serialization, \
                        o.function_name, \
                        COALESCE(r.replacement, o.child_workflow_id), \
                        o.started_at_epoch_ms, o.completed_at_epoch_ms, \
                        COALESCE(w.application_name, $6) \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {steps_table} o \
                       ON o.workflow_uuid = m.source_id AND o.function_id < m.start_step \
                     JOIN {workflow_table} w ON w.workflow_uuid = m.source_id \
                     LEFT JOIN unnest($4::text[], $5::text[]) AS r(original, replacement) \
                       ON r.original = o.child_workflow_id"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .bind(replace_from)
                .bind(replace_to)
                // The same owner the fork's status row just took, computed the same way — one
                // owner per fork, shared by its status row and its copied steps. Read from the
                // source workflow rather than from the copied step, whose own `application_name`
                // records only who ran it and may be a third application entirely.
                .bind(application_name)
                .execute(&mut *tx)
                .await?;

                // The per-step event history_table, bounded the same way.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {history_table} (workflow_uuid, function_id, key, value, serialization) \
                     SELECT m.fork_id, h.function_id, h.key, h.value, h.serialization \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {history_table} h \
                       ON h.workflow_uuid = m.source_id AND h.function_id < m.start_step"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;

                // The current value of each key, rebuilt from the history_table rather than copied
                // from the source's `workflow_events`. The source's current value may have been
                // set *after* the fork point, and a fork must not see the future.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {events_table} (workflow_uuid, key, value, serialization) \
                     SELECT fork_id, key, value, serialization FROM ( \
                       SELECT m.fork_id, h.key, h.value, h.serialization, \
                              row_number() OVER (PARTITION BY m.fork_id, h.key \
                                                 ORDER BY h.function_id DESC) AS rn \
                       FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                       JOIN {history_table} h \
                         ON h.workflow_uuid = m.source_id AND h.function_id < m.start_step \
                     ) latest WHERE rn = 1"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;

                // Stream entries written before the fork point, so a reader replaying the fork
                // sees the same stream the original had produced by then.
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {streams_table} (workflow_uuid, function_id, key, value, \
                        serialization, \"offset\") \
                     SELECT m.fork_id, s.function_id, s.key, s.value, s.serialization, s.\"offset\" \
                     FROM unnest($1::text[], $2::text[], $3::int4[]) AS m(source_id, fork_id, start_step) \
                     JOIN {streams_table} s \
                       ON s.workflow_uuid = m.source_id AND s.function_id < m.start_step"
                )))
                .bind(source_ids)
                .bind(forked_ids)
                .bind(start_steps)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            tracing::info!(
                count = forks.len(),
                queue_name,
                "forked workflows onto the queue"
            );
            Ok(forked_ids.clone())
        })
        .await
    }

    async fn fork_from(
        &self,
        workflow_ids: &[&str],
        point: ForkPoint<'_>,
        options: &ForkOptions<'_>,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        // **No `with_retry` here, and it must not gain one.** Both halves already retry —
        // `resolve_fork_points` wraps its query, `fork_workflows` wraps its transaction — so
        // every statement is covered. Wrapping the pair would re-enter `fork_workflows`, which
        // generates the fork ids *before* its own retry so that a lost commit acknowledgement
        // cannot fork twice. An outer retry would hand it fresh ids and do exactly that.
        //
        // Retrying piecewise is sound because the first half only reads: repeating it is free,
        // and it commits nothing that the second half could duplicate.

        // `MAX(function_id)` is the step itself, not the one after it, so the resolved step
        // *re-runs* — forking from a failure means running the failed step again.
        const LAST_STEP: &str = "MAX(function_id)";
        // A workflow can stop without any step recording an error: a process killed mid-step
        // records nothing. Falling back to the last step resumes it where it stopped, rather
        // than reporting that it has no failure to fork from.
        const LAST_FAILURE: &str =
            "COALESCE(MAX(function_id) FILTER (WHERE error IS NOT NULL), MAX(function_id))";

        let start_steps = match point {
            // Nothing to look up: the caller supplied the step id.
            ForkPoint::Step(step) => vec![step; workflow_ids.len()],
            ForkPoint::LastFailure => {
                self.resolve_fork_points(workflow_ids, LAST_FAILURE, None)
                    .await?
            }
            ForkPoint::LastStep => {
                self.resolve_fork_points(workflow_ids, LAST_STEP, None)
                    .await?
            }
            ForkPoint::StepNamed(name) => {
                self.resolve_fork_points(workflow_ids, LAST_STEP, Some(name))
                    .await?
            }
        };

        // Ids are always generated. A caller who is not choosing the step is not choosing the id.
        let forks: Vec<Fork<'_>> = workflow_ids
            .iter()
            .zip(&start_steps)
            .map(|(source_id, &start_step)| Fork {
                source_id,
                forked_id: None,
                start_step,
            })
            .collect();
        self.fork_workflows(&forks, options).await
    }

    async fn send_messages(
        &self,
        messages: &[Message<'_>],
        serialization: Option<&str>,
        caller: Option<(&str, i32)>,
        send_to_forks: bool,
    ) -> Result<(), Error> {
        // An empty batch still records its step. The workflow spent a step id on this call, and a
        // step id that nothing occupies is a hole a replay has to guess about: the caller would
        // re-run the send, and a message list that came out empty once and non-empty the next
        // time would be delivered for real. Recording it makes the replay a replay.
        //
        // **The two batch implementations disagree.** Python guards only its insert with
        // `if rows` and records the step regardless; Java returns early on an empty list and
        // records nothing. Python's is the safer half of that split, for the reason above, and
        // an empty send is cheap to record.
        //
        // With nothing to send *and* no step to record, there is genuinely nothing to do.
        if messages.is_empty() && caller.is_none() {
            return Ok(());
        }
        // Two messages under one key would give two rows the same primary key, so the second
        // would be silently discarded as a duplicate of the first. A caller who did that does not
        // know they have sent one message, and the database cannot tell them. Both
        // implementations with a batch API reject this — Python raises, Java throws — and
        // neither of the two without one can.
        //
        // The *empty* key is ours to decide, because they disagree: Python treats `""` as falsy
        // and so as absent, giving the message a generated id; Java treats it as a real key, so
        // one message keys on `"::destination"` and two collide. Refusing it picks neither
        // reading of an input that means nothing, and matches how this layer treats every other
        // empty string.
        let mut keys = HashSet::with_capacity(messages.len());
        for message in messages {
            if let Some(key) = message.idempotency_key {
                if key.is_empty() {
                    return Err(Error::InvalidInput {
                        field: "idempotency_key".into(),
                        detail: "must be absent rather than empty".to_owned(),
                    });
                }
                if !keys.insert(key) {
                    return Err(Error::InvalidInput {
                        field: "idempotency_key".into(),
                        detail: format!("{key} is used by more than one message"),
                    });
                }
            }
        }

        // Fixed before the retry, like every other step token: ids generated per attempt would
        // make a retry after a lost commit acknowledgement deliver everything a second time.
        let fallback_ids: Vec<String> = messages
            .iter()
            .map(|_| uuid::Uuid::new_v4().to_string())
            .collect();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        // Which API surface the caller reached for, inferred from the batch size.
        let step_name = if messages.len() == 1 {
            step_names::SEND
        } else {
            step_names::SEND_BULK
        };

        let notifications_table = self.tables.notifications.as_str();
        let workflow_table = self.tables.workflow_status.as_str();
        let pool = &self.pool;
        let fallback_ids = &fallback_ids;

        with_retry(&self.retry, "send_messages", move || async move {
            let mut tx = pool.begin().await?;

            // A replay must not send again — and must not be told it failed either.
            if let Some((workflow_id, step_id)) = caller
                && self
                    .check_step_on(&mut tx, workflow_id, step_id, step_name)
                    .await?
                    .is_some()
            {
                tracing::debug!(
                    workflow_id,
                    step_id,
                    count = messages.len(),
                    "replaying send"
                );
                return Ok(());
            }

            // Inside the transaction, so the recipient set cannot go stale before the insert.
            let forks_of = if send_to_forks {
                let roots: Vec<&str> = messages.iter().map(|m| m.destination_id).collect();
                Self::descendant_forks(&mut tx, workflow_table, &roots).await?
            } else {
                HashMap::new()
            };

            let mut destination_ids = Vec::new();
            let mut topics = Vec::new();
            let mut payloads = Vec::new();
            let mut message_ids = Vec::new();
            for (message, fallback) in messages.iter().zip(fallback_ids) {
                let forks = forks_of
                    .get(message.destination_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for destination in
                    std::iter::once(message.destination_id).chain(forks.iter().map(String::as_str))
                {
                    destination_ids.push(destination);
                    topics.push(message.topic.unwrap_or(NULL_TOPIC));
                    payloads.push(message.message);
                    // The key is scoped per recipient, so one key can fan out to a whole fork
                    // tree and still give each recipient a distinct, repeatable row — and so the
                    // id a destination sees does not depend on whether the send fanned out.
                    message_ids.push(match message.idempotency_key {
                        Some(key) => format!("{key}::{destination}"),
                        None => fallback.clone(),
                    });
                }
            }

            // Skipped when there is nothing to insert — an empty batch, which still has a step
            // to record below.
            let sent = if destination_ids.is_empty() {
                Ok(Default::default())
            } else {
                sqlx::query(AssertSqlSafe(format!(
                    "INSERT INTO {notifications_table} \
                        (destination_uuid, topic, message, message_uuid, serialization) \
                     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[]) \
                        AS m(destination_uuid, topic, message, message_uuid), \
                        (SELECT $5::text) AS s(serialization) \
                     ON CONFLICT (message_uuid) DO NOTHING"
                )))
                .bind(&destination_ids)
                .bind(&topics)
                .bind(&payloads)
                .bind(&message_ids)
                .bind(serialization)
                .execute(&mut *tx)
                .await
            };

            match sent {
                Ok(result) => {
                    tracing::debug!(
                        requested = messages.len(),
                        delivered = result.rows_affected(),
                        send_to_forks,
                        "sent messages"
                    );
                }
                // The foreign key is what catches an address that does not exist, so a message
                // can never be left pointing at nothing.
                Err(e) if is_foreign_key_violation(&e) => {
                    let mut missing: Vec<String> = messages
                        .iter()
                        .map(|m| m.destination_id.to_owned())
                        .collect();
                    missing.sort();
                    missing.dedup();
                    return Err(Error::NonExistentWorkflow {
                        workflow_ids: missing,
                    });
                }
                Err(e) => return Err(e.into()),
            }

            // Recorded last and in the same transaction: a step committed without its messages
            // would make a replay skip a send that never happened.
            if let Some((workflow_id, step_id)) = caller {
                self.record_step_on(
                    &mut tx,
                    workflow_id,
                    step_id,
                    step_name,
                    Outcome::Output(None),
                    None,
                    Some(timing),
                    None,
                )
                .await?;
            }

            tx.commit().await?;
            Ok(())
        })
        .await
    }

    async fn recv(
        &self,
        workflow_id: &str,
        step_id: i32,
        timeout_step_id: i32,
        topic: Option<&str>,
        timeout: Duration,
    ) -> Result<Option<EncodedValue>, Error> {
        let notifications_table = self.tables.notifications.as_str();
        let (pool, polling) = (&self.pool, &self.polling);
        // What `send_messages` stored. The default topic is a sentinel rather than SQL `NULL`,
        // because nothing equals `NULL` and this predicate would never match its own message.
        let stored_topic = topic.unwrap_or(NULL_TOPIC);
        let started_at = Timestamp::now();

        // Two statements, unlike `get_event`'s one, and here the split is forced rather than
        // chosen: a message cannot be read without taking it, and taking it has to be atomic with
        // recording the step. So the poll asks only whether something is waiting, and the taking
        // happens once, below.
        // `LIMIT 1` because this asks a yes/no question: without it the statement returns a row per
        // unconsumed message, once per interval, for as long as the receiver waits — and a producer
        // outrunning its consumer is exactly when that grows. Go asks the same question the same
        // way, as `SELECT EXISTS (SELECT 1 …)`; Python, TypeScript and Java all project `topic` with
        // no bound, and so all three pay for rows they discard. Bounding beats `EXISTS` here only in
        // that nothing has to be decoded — a bare `1` types as `INT4` on PostgreSQL and `INT8` on
        // CockroachDB, and a column never read cannot be read wrongly.
        //
        // TODO(dbos-team): UPSTREAM item 18. The three unbounded ones ship rows they discard, on
        // the statement every waiting `recv` runs once per interval. One word fixes each.
        let probe = format!(
            "SELECT 1 FROM {notifications_table} \
             WHERE destination_uuid = $1 AND topic = $2 AND consumed = FALSE LIMIT 1"
        );
        // The oldest unconsumed message for the topic, marked consumed as it is read.
        //
        // **`AND consumed = FALSE` on the outer statement is not a restatement of the subquery.** At
        // READ COMMITTED two receivers resolve the subquery to the same oldest `message_uuid`; one
        // updates, the other blocks on the row lock and, when it is released, re-evaluates this
        // predicate against the committed version, matching nothing. Without it the loser's qual is
        // still true of the row the winner just took and its `RETURNING` hands the same message
        // back.
        //
        // **It is defence in depth rather than the arbitration**, because the taking and the step
        // record share a transaction: a loser that gets past this predicate still conflicts on the
        // step and rolls its consumption back. Worth keeping anyway — it makes the loser's `UPDATE`
        // a no-op instead of a redundant write, and it means correctness does not rest on the step
        // insert being the thing that fails.
        //
        // The destination and topic are *not* restated out here, though Python, TypeScript and Java
        // all restate them: `message_uuid` is the primary key, so the row the subquery names
        // already carries both and repeating them cannot exclude anything.
        //
        // TODO(dbos-team): UPSTREAM item 16. Go omits this predicate — the only one of the five to
        // do so. Item 16 reads that as a live double-delivery bug; on re-reading Go it is not, for
        // the reason above: `runAsTxn` puts `ConsumeMessage` and `RecordOperationResult` in one
        // transaction under `defer tx.Rollback`, and the second execution's record conflicts on
        // `(workflow_uuid, function_id)`, so its consumption is discarded with it. The divergence
        // stands and the one-line fix is still worth making; the severity does not. Item 16 has
        // been rewritten to say so.
        let consume = format!(
            "UPDATE {notifications_table} SET consumed = TRUE \
             WHERE message_uuid = ( \
                 SELECT message_uuid FROM {notifications_table} \
                 WHERE destination_uuid = $1 AND topic = $2 AND consumed = FALSE \
                 ORDER BY created_at_epoch_ms ASC LIMIT 1 \
             ) AND consumed = FALSE \
             RETURNING message, serialization"
        );
        let (probe, consume) = (&probe, &consume);

        // A replay returns the message the first run took and does not take another. Including the
        // `None` a timeout produced — taking a message on replay would deliver, to one workflow,
        // two messages it only ever recorded one of.
        if let Some(step) = self
            .check_step(workflow_id, step_id, step_names::RECV)
            .await?
        {
            tracing::debug!(
                workflow_id,
                step_id = step_id,
                topic = stored_topic,
                "replaying recv"
            );
            return Ok(step.output.map(|value| EncodedValue {
                value,
                serialization: step.serialization,
            }));
        }

        // Exclusive, and before the first look for both reasons at once: registering after looking
        // would miss a message that landed in between, and a second receiver has to be refused
        // before it starts waiting rather than after it has waited out a timeout for a message it
        // was never going to get.
        let mut subscription = self
            .notify
            .subscribe_exclusive(message_key(workflow_id, topic))
            .ok_or_else(|| Error::ConcurrentRecv {
                workflow_id: workflow_id.to_owned(),
                topic: topic.map(str::to_owned),
            })?;

        // Recorded before the wait and replayed on recovery, so a receive that waited fifty of its
        // sixty seconds and crashed has ten left rather than sixty.
        let deadline = self
            .checkpoint_sleep(workflow_id, timeout_step_id, timeout, SleepKind::Deadline)
            .await?;

        // Look, wait a bounded interval, look again — `get_event`'s loop exactly, and for the same
        // reason: with nothing pushing, this is what delivers.
        loop {
            let has_message = with_retry(&self.retry, "recv", move || async move {
                // Inside the retried region and dropped before the wait, so neither a backoff nor a
                // parked receiver holds a permit. See the `polling` field.
                let _permit = polling
                    .acquire()
                    .await
                    .expect("the polling limiter is never closed");
                Ok(sqlx::query(AssertSqlSafe(probe.clone()))
                    .bind(workflow_id)
                    .bind(stored_topic)
                    .fetch_optional(pool)
                    .await?
                    .is_some())
            })
            .await?;
            if has_message {
                break;
            }

            // Past the deadline and exactly on it are the same answer — no time left — but
            // `duration_since` reports the first as `None` and the second as zero. Folding them
            // keeps that one decision in one place; leaving the zero case to the wait below would
            // spin, since a zero-length timeout returns at once and the next look is another query.
            let remaining = deadline
                .duration_since(Timestamp::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                break;
            }
            let _ = tokio::time::timeout(
                remaining.min(self.listener.poll_interval()),
                subscription.notified(),
            )
            .await;
        }
        // Released here rather than at the end of the call: the taking below does not wait, and
        // holding the topic through it would keep the next `recv` out for no reason.
        drop(subscription);

        // Fixed here rather than inside the retry: `completed_at` is also what distinguishes this
        // caller's own re-attempt from a rival execution, so it must not move between attempts.
        let timing = StepTiming {
            started_at,
            completed_at: Timestamp::now(),
        };

        // **One commit, and it has to be.** A message taken but not recorded is gone: the row is
        // marked consumed, so a replay finds no step, takes nothing, and reports a timeout for a
        // message that was delivered to nobody. That is why this runs once, after the waiting, and
        // never inside the loop.
        //
        // No isolation level is pinned, unlike TypeScript's explicit `READ COMMITTED`. On
        // PostgreSQL that is the default and the predicate above arbitrates; on CockroachDB the
        // default is `SERIALIZABLE`, which aborts the loser instead — a `40` SQLSTATE, which the
        // retry policy classifies as transient, so it comes back round, finds nothing unconsumed,
        // and records the same `None` it would have recorded either way.
        with_retry(&self.retry, "recv", move || async move {
            let mut tx = pool.begin().await?;

            // The check that opened this call cannot stand in for this one: the wait between them
            // is exactly when another execution of this workflow would have recorded its own
            // answer. Same transaction as the write, so there is no window between deciding to
            // take a message and taking it.
            if let Some(step) = self
                .check_step_on(&mut tx, workflow_id, step_id, step_names::RECV)
                .await?
            {
                tx.commit().await?;
                tracing::debug!(
                    workflow_id,
                    step_id = step_id,
                    "adopting a rival execution's recv"
                );
                return Ok(step.output.map(|value| EncodedValue {
                    value,
                    serialization: step.serialization,
                }));
            }

            let row = sqlx::query(AssertSqlSafe(consume.clone()))
                .bind(workflow_id)
                .bind(stored_topic)
                .fetch_optional(&mut *tx)
                .await?;
            let message = match row {
                Some(row) => Some(EncodedValue {
                    value: row.try_get("message")?,
                    serialization: row.try_get("serialization")?,
                }),
                None => None,
            };

            // The sender's own payload under the sender's own format, as `get_event` records the
            // publisher's. A timeout records a NULL output.
            //
            // **A conflict here is reported, and the rollback is what makes that safe.** It means
            // another execution recorded this step between the check above and this write — at READ
            // COMMITTED the check ran on an earlier snapshot than the `UPDATE`, so this attempt may
            // have taken a message. Dropping the transaction un-takes it, so the message stays
            // available and exactly one execution's `recv` is recorded. Adopting the rival's answer
            // instead would be defensible, but nothing in this crate does that: the same conflict
            // from `run_transactional_step` and from `get_event` reaches the caller, and two
            // executors believing they own one workflow is worth reporting rather than smoothing
            // over.
            self.record_step_on(
                &mut tx,
                workflow_id,
                step_id,
                step_names::RECV,
                Outcome::Output(message.as_ref().map(|m| m.value.as_str())),
                message.as_ref().and_then(|m| m.serialization.as_deref()),
                Some(timing),
                None,
            )
            .await?;
            tx.commit().await?;
            tracing::debug!(
                workflow_id,
                step_id = step_id,
                topic = stored_topic,
                received = message.is_some(),
                "recv finished"
            );
            Ok(message)
        })
        .await
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        step_id: i32,
        key: &str,
        value: &str,
        serialization: Option<&str>,
        written_by: WrittenBy,
    ) -> Result<(), Error> {
        // Derived, not passed: a close is recorded as a close whichever entry point reached it.
        let step_name = if value == STREAM_CLOSED {
            step_names::CLOSE_STREAM
        } else {
            step_names::WRITE_STREAM
        };
        // Fixed before the retry, as every step token is.
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        let streams_table = self.tables.streams.as_str();
        let pool = &self.pool;

        // The offset is computed by the insert, so two writers can pick the same one and collide
        // on the primary key. The loser recomputes against the winner's row and appends after it.
        let insert = format!(
            "INSERT INTO {streams_table} \
                (workflow_uuid, function_id, key, value, serialization, \"offset\") \
             SELECT $1, $2, $3, $4, $5, COALESCE( \
                (SELECT MAX(\"offset\") FROM {streams_table} \
                 WHERE workflow_uuid = $1 AND key = $3), -1) + 1"
        );
        let insert = &insert;

        with_retry(&self.retry, "write_stream", move || async move {
            for attempt in 0..STREAM_OFFSET_ATTEMPTS {
                let mut tx = pool.begin().await?;

                // A replay must not append a second entry. Only a workflow-level write records a
                // step to find: one made inside a step is replayed by its step being replayed.
                if written_by == WrittenBy::Workflow
                    && self
                        .check_step_on(&mut tx, workflow_id, step_id, step_name)
                        .await?
                        .is_some()
                {
                    tracing::debug!(workflow_id, step_id, key, "replaying stream write");
                    return Ok(());
                }

                match sqlx::query(AssertSqlSafe(insert.clone()))
                    .bind(workflow_id)
                    .bind(step_id)
                    .bind(key)
                    .bind(value)
                    .bind(serialization)
                    .execute(&mut *tx)
                    .await
                {
                    Ok(_) => {}
                    // Another writer took the offset. Start again, against its row.
                    Err(e) if is_unique_violation(&e) => {
                        tracing::debug!(
                            workflow_id,
                            key,
                            attempt,
                            "stream offset taken; recomputing"
                        );
                        continue;
                    }
                    Err(e) if is_foreign_key_violation(&e) => {
                        return Err(Error::NonExistentWorkflow {
                            workflow_ids: vec![workflow_id.to_owned()],
                        });
                    }
                    Err(e) => return Err(e.into()),
                }

                if written_by == WrittenBy::Workflow {
                    self.record_step_on(
                        &mut tx,
                        workflow_id,
                        step_id,
                        step_name,
                        Outcome::Output(None),
                        None,
                        Some(timing),
                        None,
                    )
                    .await?;
                }

                tx.commit().await?;
                // Committed, so a reader woken by this finds the entry — including the sentinel a
                // close writes, which is how a reader learns the stream has ended. Migration 43
                // dropped the trigger that used to do it. The replay above returns before here on
                // purpose: it wrote nothing, so there is nothing new to look at.
                self.notifier.signal(STREAMS_CHANNEL, workflow_id, key);
                return Ok(());
            }

            Err(Error::Backend(BackendError {
                message: format!(
                    "stream {key} on workflow {workflow_id} lost \
                     {STREAM_OFFSET_ATTEMPTS} offset races"
                ),
                sqlstate: None,
                kind: BackendErrorKind::Transient,
            }))
        })
        .await
    }

    async fn close_stream(&self, workflow_id: &str, step_id: i32, key: &str) -> Result<(), Error> {
        self.write_stream(
            workflow_id,
            step_id,
            key,
            STREAM_CLOSED,
            Some(PORTABLE_JSON),
            WrittenBy::Workflow,
        )
        .await
    }

    async fn close(&self) {
        // **Before the pool closes**, because the notifier's last act is a database write: whatever
        // it was still holding in its coalescing window goes out, so a value written a moment ago
        // wakes readers elsewhere rather than leaving them to wait out an interval. Doing this
        // after `pool.close()` would turn every such flush into a logged failure.
        self.notifier.stop();
        let task = self.notifier_task.lock().expect("notifier lock").take();
        if let Some(task) = task {
            let _ = task.await;
        }

        // Closing the pool is the whole of it for the waits parked on a polling permit: every
        // in-flight poll's query fails permanently and releases its permit, so a parked waiter
        // acquires, queries, and gets the same failure. An earlier version also closed the limiter
        // to end those waits one query sooner; deleting it failed no test, so it is gone.
        self.pool.close().await;

        // The listener is told to stop by the same act — it watches the pool's close event — but
        // being told is not the same as having stopped, and this is where the difference shows.
        // Waiting for it means a caller that closed a handle can rely on nothing of this one's
        // still running, and it is what makes a listener that failed to recognise the shutdown a
        // hang rather than a warning logged every second forever.
        let task = self.listener_task.lock().expect("listener lock").take();
        if let Some(task) = task {
            // The task itself never panics or is aborted, so an error here would be a bug rather
            // than a shutdown to report on; either way the listener is not running.
            let _ = task.await;
        }
    }

    async fn check_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        let pool = &self.pool;
        with_retry(&self.retry, "check_step", move || async move {
            let mut conn = pool.acquire().await?;
            self.check_step_on(&mut conn, workflow_id, step_id, step_name)
                .await
        })
        .await
    }

    async fn record_step(
        &self,
        workflow_id: &str,
        step_id: i32,
        step_name: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error> {
        let pool = &self.pool;
        with_retry(&self.retry, "record_step", move || async move {
            let mut conn = pool.acquire().await?;
            self.record_step_on(
                &mut conn,
                workflow_id,
                step_id,
                step_name,
                outcome,
                serialization,
                timing,
                None,
            )
            .await
        })
        .await
    }

    async fn record_child_result(
        &self,
        parent_workflow_id: &str,
        step_id: i32,
        child_workflow_id: &str,
        outcome: Outcome<'_>,
        serialization: Option<&str>,
        timing: Option<StepTiming>,
    ) -> Result<(), Error> {
        let pool = &self.pool;
        with_retry(&self.retry, "record_child_result", move || async move {
            let mut conn = pool.acquire().await?;
            self.record_step_on(
                &mut conn,
                parent_workflow_id,
                step_id,
                step_names::GET_RESULT,
                outcome,
                serialization,
                timing,
                Some(child_workflow_id),
            )
            .await
        })
        .await
    }

    async fn list_workflow_steps(
        &self,
        workflow_id: &str,
        load_output: bool,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<StepRecord>, Error> {
        let steps_table = &self.tables.operation_outputs;
        let (steps_table, pool) = (steps_table.as_str(), &self.pool);

        with_retry(&self.retry, "list_workflow_steps", move || async move {
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            q.push(STEP_COLUMNS)
                .push(", ")
                .push(step_payloads(load_output))
                .push(" FROM ")
                .push(steps_table)
                // `function_id` is the step order, so ordering by it replays the workflow.
                .push(" WHERE workflow_uuid = ")
                .push_bind(workflow_id)
                .push(" ORDER BY function_id");
            if let Some(limit) = limit {
                q.push(" LIMIT ").push_bind(limit);
            }
            if let Some(offset) = offset {
                q.push(" OFFSET ").push_bind(offset);
            }
            let rows = q.build().fetch_all(pool).await?;
            rows.iter().map(|r| step_from_row(r, workflow_id)).collect()
        })
        .await
    }

    async fn record_sleep(
        &self,
        workflow_id: &str,
        step_id: i32,
        duration: Duration,
    ) -> Result<Timestamp, Error> {
        self.checkpoint_sleep(workflow_id, step_id, duration, SleepKind::Durable)
            .await
    }

    async fn set_event(
        &self,
        workflow_id: &str,
        step_id: i32,
        key: &str,
        value: &str,
        serialization: Option<&str>,
    ) -> Result<(), Error> {
        let events = &self.tables.workflow_events;
        let history = &self.tables.workflow_events_history;
        let (events, history, pool) = (events.as_str(), history.as_str(), &self.pool);
        // Fixed before the retry, like every other step token: a second attempt is recording the
        // same step, and a fresh clock reading would look like a rival execution.
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "set_event", move || async move {
            let mut tx = pool.begin().await?;

            // A replay that already published this key must not publish it again — and must not
            // be told it failed either.
            if self
                .check_step_on(&mut tx, workflow_id, step_id, step_names::SET_EVENT)
                .await?
                .is_some()
            {
                tracing::debug!(workflow_id, step_id, key, "replaying set_event");
                return Ok(());
            }
            tracing::debug!(workflow_id, step_id, key, "running set_event");

            // The current value, which is what a reader sees.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {events} (workflow_uuid, key, value, serialization) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (workflow_uuid, key) DO UPDATE \
                 SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
            )))
            .bind(workflow_id)
            .bind(key)
            .bind(value)
            .bind(serialization)
            .execute(&mut *tx)
            .await?;

            // The per-step history, which is what a fork copies forward.
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {history} (workflow_uuid, function_id, key, value, serialization) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (workflow_uuid, key, function_id) DO UPDATE \
                 SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
            )))
            .bind(workflow_id)
            .bind(step_id)
            .bind(key)
            .bind(value)
            .bind(serialization)
            .execute(&mut *tx)
            .await?;

            self.record_step_on(
                &mut tx,
                workflow_id,
                step_id,
                step_names::SET_EVENT,
                Outcome::Output(None),
                None,
                Some(timing),
                None,
            )
            .await?;

            tx.commit().await?;
            // Committed, so a reader woken by this finds the row. Migration 44 dropped the trigger
            // that used to do it from inside the transaction above.
            self.notifier.signal(EVENTS_CHANNEL, workflow_id, key);
            Ok(())
        })
        .await
    }

    async fn get_event(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
        caller: Option<GetEventCaller<'_>>,
    ) -> Result<Option<EncodedValue>, Error> {
        let events_table = self.tables.workflow_events.as_str();
        let (pool, polling) = (&self.pool, &self.polling);
        // One statement, and the pass that finds a row is the one that answers the call. A miss
        // carries nothing back whatever the select list says, so asking only whether the key exists
        // would buy nothing and would leave the value still to be fetched.
        let select = format!(
            "SELECT value, serialization FROM {events_table} WHERE workflow_uuid = $1 AND key = $2"
        );
        let select = &select;
        // When the step began, for the step it records at the end — so a workflow's timeline shows
        // the wait, not the instant the answer was written down.
        let started_at = Timestamp::now();

        // A replay returns what the first run saw and does not wait again. Including the `None` a
        // timeout produced: that is a result, and re-running the wait would let a value that
        // arrived late change a decision the workflow has already taken.
        if let Some(caller) = caller
            && let Some(step) = self
                .check_step(caller.workflow_id, caller.step_id, step_names::GET_EVENT)
                .await?
        {
            tracing::debug!(
                workflow_id = caller.workflow_id,
                step_id = caller.step_id,
                key,
                "replaying get_event"
            );
            return Ok(step.output.map(|value| EncodedValue {
                value,
                serialization: step.serialization,
            }));
        }

        // **Before the first look, never after.** A caller that looked first and registered second
        // would miss anything written in between and then wait out its whole timeout for a value
        // already in the table. Nothing wakes this yet — the loop below delivers on its own — but
        // the ordering is what makes a wakeup safe to add.
        let mut subscription = self.notify.subscribe(event_key(workflow_id, key));

        // Recorded whether or not the value turns out to be there already, so which steps a run
        // writes does not depend on how a race went. A recovery gets the original instant back, so
        // a read that waited fifty of its sixty seconds and crashed has ten left rather than sixty.
        let deadline = match caller {
            Some(caller) => {
                self.checkpoint_sleep(
                    caller.workflow_id,
                    caller.timeout_step_id,
                    timeout,
                    SleepKind::Deadline,
                )
                .await?
            }
            // A timeout so large it cannot be represented is one that never elapses, which is what
            // the caller asked for.
            None => started_at
                .checked_add(timeout)
                .unwrap_or(Timestamp::from_epoch_ms(i64::MAX)),
        };

        // Look, wait a bounded interval, look again. **This loop is what delivers** — a wakeup only
        // ever shortens the interval, so the wait is correct with nothing pushing at all, which is
        // every SDK's CockroachDB configuration and this crate's CI.
        let found = loop {
            let row = with_retry(&self.retry, "get_event", move || async move {
                // Inside the retried region, so a look that is backing off is not holding a permit
                // through its backoff — and dropped before the wait below, so a caller parked
                // between looks holds nothing. See the `polling` field for why a cap on waiters
                // rather than on queries would deadlock.
                let _permit = polling
                    .acquire()
                    .await
                    .expect("the polling limiter is never closed");
                Ok(sqlx::query(AssertSqlSafe(select.clone()))
                    .bind(workflow_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?)
            })
            .await?;
            if let Some(row) = row {
                break Some(EncodedValue {
                    value: row.try_get("value")?,
                    serialization: row.try_get("serialization")?,
                });
            }

            // Past the deadline and exactly on it are the same answer — no time left — but
            // `duration_since` reports the first as `None` and the second as zero. Folding them
            // keeps that one decision in one place; leaving the zero case to the wait below would
            // spin, since a zero-length timeout returns at once and the next look is another query.
            let remaining = deadline
                .duration_since(Timestamp::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                break None;
            }
            // Wait to be woken, but not past whichever bound comes first. The interval caps it so
            // that a wakeup nothing sends — or one that is dropped — costs one interval rather than
            // the whole timeout; the remaining time caps it so a wait never overruns the deadline.
            // Elapsing is not an error here: both outcomes mean the same thing, which is look
            // again.
            let _ = tokio::time::timeout(
                remaining.min(self.listener.poll_interval()),
                subscription.notified(),
            )
            .await;
        };
        // Nothing below waits, and the registration is only worth its map entry while something
        // does.
        drop(subscription);

        // **Outside a workflow this is the whole call.** There is no step to record, so there is
        // nothing for a transaction to be atomic with — and the last look the loop took is the
        // answer. TypeScript returns its polled row the same way.
        let Some(caller) = caller else {
            tracing::debug!(
                workflow_id,
                key,
                found = found.is_some(),
                "get_event finished"
            );
            return Ok(found);
        };

        // Fixed here rather than inside the retry: `completed_at` is also what distinguishes this
        // caller's own re-attempt from a rival execution, so it must not move between attempts.
        let timing = StepTiming {
            started_at,
            completed_at: Timestamp::now(),
        };
        let found = found.as_ref();

        // Two statements, and they have to be one commit: whether to record and the recording. The
        // check that opened the call cannot stand in for this one, because the wait between them is
        // exactly when another execution of this workflow would have recorded its own answer.
        with_retry(&self.retry, "get_event", move || async move {
            let mut tx = pool.begin().await?;
            if let Some(step) = self
                .check_step_on(
                    &mut tx,
                    caller.workflow_id,
                    caller.step_id,
                    step_names::GET_EVENT,
                )
                .await?
            {
                tx.commit().await?;
                tracing::debug!(
                    workflow_id = caller.workflow_id,
                    step_id = caller.step_id,
                    key,
                    "adopting a rival execution's get_event"
                );
                return Ok(step.output.map(|value| EncodedValue {
                    value,
                    serialization: step.serialization,
                }));
            }

            // The event's own encoded value under the event's own format, not a wrapper around the
            // pair — so the column holds what the publisher wrote and stays legible to whoever
            // reads the row, this crate included. All four references record exactly this. A
            // timeout records a NULL output, which is how they spell "there was nothing" too.
            self.record_step_on(
                &mut tx,
                caller.workflow_id,
                caller.step_id,
                step_names::GET_EVENT,
                Outcome::Output(found.map(|v| v.value.as_str())),
                found.and_then(|v| v.serialization.as_deref()),
                Some(timing),
                None,
            )
            .await?;
            tx.commit().await?;
            tracing::debug!(
                workflow_id = caller.workflow_id,
                step_id = caller.step_id,
                key,
                found = found.is_some(),
                "get_event finished"
            );
            Ok(found.cloned())
        })
        .await
    }

    async fn get_all_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationRecord>, Error> {
        let notifications_table = &self.tables.notifications;
        let (notifications_table, pool) = (notifications_table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_notifications", move || async move {
            // `consumed` rather than a delete on receive, so this reports everything the workflow
            // was sent and not merely what is still waiting.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT message_uuid, topic, message, serialization, created_at_epoch_ms, \
                 consumed \
                 FROM {notifications_table} WHERE destination_uuid = $1 ORDER BY created_at_epoch_ms"
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(NotificationRecord {
                        message_uuid: row.try_get("message_uuid")?,
                        topic: row.try_get("topic")?,
                        message: row.try_get("message")?,
                        serialization: row.try_get("serialization")?,
                        created_at: Timestamp::from_epoch_ms(row.try_get("created_at_epoch_ms")?),
                        consumed: row.try_get("consumed")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn get_all_events(&self, workflow_id: &str) -> Result<Vec<EventRecord>, Error> {
        let events_table = &self.tables.workflow_events;
        let (events_table, pool) = (events_table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_events", move || async move {
            // Ordered by key, which the table does not do for us: its primary key is
            // `(workflow_uuid, key)`, so this is a range scan that happens to be sorted, but
            // saying so keeps the result stable if that ever changes.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT key, value, serialization FROM {events_table} \
                 WHERE workflow_uuid = $1 ORDER BY key"
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(EventRecord {
                        key: row.try_get("key")?,
                        value: row.try_get("value")?,
                        serialization: row.try_get("serialization")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn read_stream_value(
        &self,
        workflow_id: &str,
        key: &str,
        offset: i32,
    ) -> Result<StreamRead, Error> {
        let workflow_table = self.tables.workflow_status.as_str();
        let streams_table = self.tables.streams.as_str();
        let (pool, polling) = (&self.pool, &self.polling);
        // `workflow_status` drives the join, so the statement returns a row whenever the workflow
        // exists — with or without a value at the offset — and no row only when it does not. The
        // join predicate carries the key and offset rather than the `WHERE` clause, which is what
        // makes that distinction possible at all.
        //
        // `"offset"` is quoted throughout because it is a reserved word, and aliased on the way out
        // so it can be read back plainly. Matching it exactly keeps this one lookup on the
        // `(workflow_uuid, key, offset)` primary key.
        let select = format!(
            "SELECT w.status AS status, s.value AS value, s.serialization AS serialization, \
             s.\"offset\" AS stream_offset \
             FROM {workflow_table} w \
             LEFT OUTER JOIN {streams_table} s \
               ON s.workflow_uuid = w.workflow_uuid AND s.key = $2 AND s.\"offset\" = $3 \
             WHERE w.workflow_uuid = $1"
        );
        let select = &select;

        with_retry(&self.retry, "read_stream_value", move || async move {
            // A reader's loop calls this once per offset and then once per interval while it waits,
            // so it is a poll like the other two and is capped like them. Inside the retried region,
            // so a call that is backing off is not holding a permit through its backoff.
            let _permit = polling
                .acquire()
                .await
                .expect("the polling limiter is never closed");
            let row = sqlx::query(AssertSqlSafe(select.clone()))
                .bind(workflow_id)
                .bind(key)
                .bind(offset)
                .fetch_optional(pool)
                .await?;

            // No row at all means no such workflow — the outer table drives the join. Reported
            // rather than returned as an absent status, because a stream nobody can write to is not
            // a stream that is merely empty, and every engine turns the null status into this same
            // error the moment it sees one.
            let Some(row) = row else {
                return Err(Error::NonExistentWorkflow {
                    workflow_ids: vec![workflow_id.to_owned()],
                });
            };

            let status_text: String = row.try_get("status")?;
            let status = WorkflowStatus::parse(&status_text).ok_or_else(|| {
                Error::Malformed(format!("unknown workflow status {status_text:?}"))
            })?;

            // `streams."offset"` is `NOT NULL`, so a NULL here can only mean the join matched
            // nothing: there is no entry at this offset. Python and TypeScript read the same column
            // for the same reason.
            //
            // Probing `value` would answer identically today, since it is `NOT NULL` too — but the
            // question being asked is whether the *join* matched, and keying on a payload column
            // makes the answer hostage to that column staying non-nullable. `serialization`, one
            // column over, already is nullable.
            let value = match row.try_get::<Option<i32>, _>("stream_offset")? {
                Some(_) => Some(EncodedValue {
                    value: row.try_get("value")?,
                    serialization: row.try_get("serialization")?,
                }),
                None => None,
            };

            Ok(StreamRead { status, value })
        })
        .await
    }

    async fn get_all_stream_entries(&self, workflow_id: &str) -> Result<Vec<StreamRecord>, Error> {
        let streams_table = &self.tables.streams;
        let (streams_table, pool) = (streams_table.as_str(), &self.pool);

        with_retry(&self.retry, "get_all_stream_entries", move || async move {
            // `"offset"` is quoted because it is a reserved word, and ordering by it is what
            // makes the result a stream rather than a bag.
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT key, \"offset\", value, serialization, function_id FROM {streams_table} \
                 WHERE workflow_uuid = $1 ORDER BY key, \"offset\""
            )))
            .bind(workflow_id)
            .fetch_all(pool)
            .await?;

            rows.iter()
                .map(|row| {
                    Ok(StreamRecord {
                        key: row.try_get("key")?,
                        offset: row.try_get("offset")?,
                        value: row.try_get("value")?,
                        serialization: row.try_get("serialization")?,
                        step_id: row.try_get("function_id")?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn create_application_version(
        &self,
        version_name: &str,
        application_name: Option<&str>,
    ) -> Result<(), Error> {
        let versions_table = &self.tables.application_versions;
        // Generated outside the retry, like every other identity here. It matters less than
        // `owner_xid` does — a retry finds the row already there and claims nothing — but the
        // rule is worth keeping uniform.
        let version_id = uuid::Uuid::new_v4().to_string();
        let (versions_table, pool, version_id) =
            (versions_table.as_str(), &self.pool, version_id.as_str());
        let application_name = application_name.or(self.application_name.as_deref());

        with_retry(
            &self.retry,
            "create_application_version",
            move || async move {
                let mut tx = pool.begin().await?;

                // Claim a pre-ownership row where it stands, rather than inserting a second one.
                //
                // Today it could not insert one anyway: migration 13's global unique on
                // `version_name` swallows it, and the row would stay unclaimed forever. So this
                // is what adopts a version registered before ownership existed.
                //
                // 106 and 107's per-application keys are already in place beside it, and are
                // strictly weaker — the global unique implies both, so they reject nothing extra
                // today. They exist so that unique can be dropped later without a window where
                // nothing enforces uniqueness; that drop is a future shared migration, blocked
                // until every SDK reaching a database is past 107.
                //
                // Once it lands, a second row does become possible — and it would carry a fresh
                // `version_timestamp`, which is what `ORDER BY version_timestamp DESC LIMIT 1`
                // reads as latest. An operator who had pinned an older version by promoting it
                // would find the pin silently undone, and schedules enqueueing against the
                // version they rolled back from.
                //
                // Guarded on `IS NULL` so it can only ever claim what nobody holds.
                let claimed = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {versions_table} SET application_name = $1 \
                     WHERE version_name = $2 AND application_name IS NULL"
                )))
                .bind(application_name)
                .bind(version_name)
                .execute(&mut *tx)
                .await?
                .rows_affected();

                if claimed == 0 {
                    // Targetless `DO NOTHING`, deliberately: naming `(version_name)` as the
                    // arbiter would stop working the moment migration 13's global uniqueness is
                    // dropped in favour of 106 and 107's per-application keys. Without a target
                    // it absorbs whichever unique index happens to fire, which is what a
                    // concurrent registrar trips.
                    sqlx::query(AssertSqlSafe(format!(
                        "INSERT INTO {versions_table} (version_id, version_name, application_name) \
                         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
                    )))
                    .bind(version_id)
                    .bind(version_name)
                    .bind(application_name)
                    .execute(&mut *tx)
                    .await?;
                }

                // Read back, because both writes above decline silently: the `UPDATE` matches
                // nothing and the `INSERT` does nothing whether the row is this application's
                // own or a peer's, and only one of those is acceptable.
                resolve_owning_application(
                    &mut tx,
                    versions_table,
                    "version_name",
                    version_name,
                    application_name,
                    "Application version",
                )
                .await?;

                tx.commit().await?;
                Ok(())
            },
        )
        .await
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>, Error> {
        let versions_table = &self.tables.application_versions;
        let (versions_table, pool) = (versions_table.as_str(), &self.pool);
        let application_name = self.application_name.as_deref();

        with_retry(
            &self.retry,
            "list_application_versions",
            move || async move {
                let rows = sqlx::query(AssertSqlSafe(format!(
                    "SELECT {VERSION_COLUMNS} FROM {versions_table} \
                     WHERE ($1::text IS NULL \
                            OR application_name = $1 \
                            OR application_name IS NULL) \
                     ORDER BY version_timestamp DESC"
                )))
                .bind(application_name)
                .fetch_all(pool)
                .await?;
                rows.iter().map(version_from_row).collect()
            },
        )
        .await
    }

    async fn get_latest_application_version(
        &self,
        application_name: Option<&str>,
    ) -> Result<Option<VersionInfo>, Error> {
        let versions_table = &self.tables.application_versions;
        let (versions_table, pool) = (versions_table.as_str(), &self.pool);
        let application_name = application_name.or(self.application_name.as_deref());

        with_retry(
            &self.retry,
            "get_latest_application_version",
            move || async move {
                let row = sqlx::query(AssertSqlSafe(format!(
                    "SELECT {VERSION_COLUMNS} FROM {versions_table} \
                     WHERE ($1::text IS NULL \
                            OR application_name = $1 \
                            OR application_name IS NULL) \
                     ORDER BY version_timestamp DESC LIMIT 1"
                )))
                .bind(application_name)
                .fetch_optional(pool)
                .await?;
                row.as_ref().map(version_from_row).transpose()
            },
        )
        .await
    }

    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp: Timestamp,
        application_name: Option<&str>,
    ) -> Result<(), Error> {
        let versions_table = &self.tables.application_versions;
        let (versions_table, pool) = (versions_table.as_str(), &self.pool);
        let application_name = application_name.or(self.application_name.as_deref());

        with_retry(
            &self.retry,
            "update_application_version_timestamp",
            move || async move {
                let mut tx = pool.begin().await?;

                let owner = resolve_owning_application(
                    &mut tx,
                    versions_table,
                    "version_name",
                    version_name,
                    application_name,
                    "Application version",
                )
                .await?;

                // Scoped to the resolved row, not to the name alone: once 106 and 107's
                // per-application keys replace migration 13's, one `version_name` may exist per
                // application and a bare name match would retime every copy. The `SET` also
                // claims an unclaimed row, which would otherwise stay every peer's latest.
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {versions_table} SET version_timestamp = $2, application_name = $3 \
                     WHERE version_name = $1 \
                       AND (application_name IS NULL OR application_name = $3)"
                )))
                .bind(version_name)
                .bind(timestamp.as_epoch_ms())
                .bind(owner.as_deref())
                .execute(&mut *tx)
                .await?;

                tx.commit().await?;
                Ok(())
            },
        )
        .await
    }

    async fn upsert_queue(
        &self,
        queue: &NewQueue<'_>,
        on_existing: OnExistingQueue,
    ) -> Result<bool, Error> {
        let queues_table = self.tables.queues.as_str();
        let pool = &self.pool;
        let application_name = queue.application_name.or(self.application_name.as_deref());
        // Ownership is claimed, never taken: `COALESCE` leaves a row that already has an owner
        // alone, so a registration landing between the resolve below and this write keeps the
        // name it took. The stored limits are a different matter — those the caller asked to
        // replace.
        let on_conflict = match on_existing {
            OnExistingQueue::Update => format!(
                "ON CONFLICT (name) DO UPDATE SET \
                   concurrency = EXCLUDED.concurrency, \
                   worker_concurrency = EXCLUDED.worker_concurrency, \
                   rate_limit_max = EXCLUDED.rate_limit_max, \
                   rate_limit_period_sec = EXCLUDED.rate_limit_period_sec, \
                   priority_enabled = EXCLUDED.priority_enabled, \
                   partition_queue = EXCLUDED.partition_queue, \
                   partition_concurrency = EXCLUDED.partition_concurrency, \
                   partition_worker_concurrency = EXCLUDED.partition_worker_concurrency, \
                   partition_rate_limit_max = EXCLUDED.partition_rate_limit_max, \
                   partition_rate_limit_period_sec = EXCLUDED.partition_rate_limit_period_sec, \
                   polling_interval_sec = EXCLUDED.polling_interval_sec, \
                   updated_at = EXCLUDED.updated_at, \
                   application_name = COALESCE({queues_table}.application_name, EXCLUDED.application_name)"
            ),
            OnExistingQueue::Leave => "ON CONFLICT (name) DO NOTHING".to_owned(),
        };
        let on_conflict = on_conflict.as_str();

        with_retry(&self.retry, "upsert_queue", move || async move {
            let mut tx = pool.begin().await?;

            // Asked before the write, because afterwards there is no way to tell a row this call
            // created from one it found — both leave a row behind.
            let existed: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT name FROM {queues_table} WHERE name = $1"
            )))
            .bind(queue.name)
            .fetch_optional(&mut *tx)
            .await?;

            // A peer holding the name is refused in either mode: the name is the queue's address,
            // so registering over it would point this application at a peer's work.
            let owner = resolve_owning_application(
                &mut tx,
                queues_table,
                "name",
                queue.name,
                application_name,
                "Queue",
            )
            .await?;

            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {queues_table} \
                 (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, \
                  priority_enabled, partition_queue, partition_concurrency, \
                  partition_worker_concurrency, partition_rate_limit_max, \
                  partition_rate_limit_period_sec, polling_interval_sec, updated_at, \
                  application_name) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                 {on_conflict}"
            )))
            .bind(queue.name)
            .bind(queue.concurrency)
            .bind(queue.worker_concurrency)
            .bind(queue.rate_limit.map(|r| r.limit))
            .bind(queue.rate_limit.map(|r| r.period.as_secs_f64()))
            .bind(queue.priority_enabled)
            .bind(queue.partition_queue)
            .bind(queue.partition_concurrency)
            .bind(queue.partition_worker_concurrency)
            .bind(queue.partition_rate_limit.map(|r| r.limit))
            .bind(queue.partition_rate_limit.map(|r| r.period.as_secs_f64()))
            .bind(queue.polling_interval.as_secs_f64())
            .bind(Timestamp::now().as_epoch_ms())
            .bind(owner.as_deref())
            .execute(&mut *tx)
            .await?;

            // Read back, because both conflict clauses decline silently: neither says whether the
            // row it found belongs to this application or to a peer that registered in between.
            resolve_owning_application(
                &mut tx,
                queues_table,
                "name",
                queue.name,
                application_name,
                "Queue",
            )
            .await?;

            tx.commit().await?;
            Ok(existed.is_none())
        })
        .await
    }

    async fn start_queued_workflows(
        &self,
        queue: &QueueRecord,
        executor_id: &str,
        application_version: &str,
        partition_key: Option<&str>,
        local_running_count: i64,
        partition_local_running_count: i64,
    ) -> Result<Vec<String>, Error> {
        // Refused rather than matched, because no row can hold it: `NewWorkflow` and
        // `ForkOptions` both reject an empty partition key on the way in. Accepting it here would
        // select nothing and report an empty partition, which reads as "no work" rather than as
        // the mistake it is.
        //
        // TODO(dbos-team): UPSTREAM item 5 — all four implementations differ, and no
        // two arrived at their answer the same way. Go guards every partition clause with
        // `len(input.QueuePartitionKey) > 0`, so empty *is* its absent value. Java normalises it
        // (`QueuesDAO.java:36`, `if (partitionKey != null && partitionKey.isEmpty())
        // partitionKey = null`). Python tests `is not None` and TypeScript `!== undefined`, so
        // both let an empty key through to match rows that cannot exist — the silently-empty
        // result nobody would choose deliberately. Rust refuses it, which is the only answer
        // consistent with rejecting the same value on the way in; whether the others should
        // reject it too, or Rust should normalise like Go and Java, is a cross-SDK decision
        // rather than one to take here.
        if partition_key == Some("") {
            return Err(Error::InvalidInput {
                field: "partition_key".into(),
                detail: "must be absent rather than empty".to_owned(),
            });
        }

        let workflow_table = self.tables.workflow_status.as_str();
        let pool = &self.pool;
        let application_name = self.application_name.as_deref();

        // Every count below has to match what the selection would take, or a limit is enforced
        // against a population this executor cannot dequeue from.
        let owner_predicate =
            "($1::text IS NULL OR application_name = $1 OR application_name IS NULL)";
        // No partition asked for means every partition, so a null parameter drops the clause
        // rather than matching rows whose key is null. Only the *per-partition* limits and the
        // selection wear it: a queue-wide limit counts the whole queue whichever partition this
        // call is sweeping, which is what makes the two scopes independent.
        let partition_predicate = "($2::text IS NULL OR queue_partition_key = $2)";
        // **Read through `resolved_limits`, never off the columns.** A row a peer wrote with the
        // deprecated flag holds its per-partition numbers in the queue-wide columns, and enforcing
        // those queue-wide would admit one workflow for the whole queue instead of one per key.
        let limits = queue.resolved_limits();
        // Whether any limit here is a budget peer executors spend from as well. Worker
        // concurrency is not one: it is answered from this process's own running count.
        let has_shared_budget = limits.concurrency.is_some()
            || limits.partition_concurrency.is_some()
            || limits.rate_limit.is_some()
            || limits.partition_rate_limit.is_some();
        // Whether that budget is shared across partitions too, which is a second hazard and needs
        // a stronger answer. Two calls sweeping different keys read the same queue-wide total and
        // then write disjoint rows, so no snapshot conflict fires under repeatable read and each
        // spends the whole budget — write skew, which only serialisable catches.
        let has_write_skew = partition_key.is_some()
            && (limits.concurrency.is_some() || limits.rate_limit.is_some());
        // Marks the rows a limited queue started, which is what the windows below count and why
        // cancelling clears it. A limit at either scope makes a start countable: flagging only
        // queue-wide starts would leave a per-partition window counting nothing, and the limit it
        // measures unenforceable.
        let rate_limited = limits.rate_limit.is_some() || limits.partition_rate_limit.is_some();
        // The window's width, which the database subtracts from its own clock — see
        // [`NOW_MS_SQL`]. Fixed for the run, since only the instant it is subtracted from moves.
        //
        // Saturating: a period longer than `i64` milliseconds can hold opens the window before
        // the epoch, which counts every start there has ever been — the right answer for a limit
        // whose window never closes.
        let period_ms = |limit: Option<RateLimit>| {
            limit.map_or(0, |limit| {
                i64::try_from(limit.period.as_millis()).unwrap_or(i64::MAX)
            })
        };
        let rate_limit_period_ms = period_ms(limits.rate_limit);
        let partition_rate_limit_period_ms = period_ms(limits.partition_rate_limit);

        with_retry(&self.retry, "start_queued_workflows", move || async move {
            let mut tx = pool.begin().await?;

            // Read committed otherwise: with no shared budget nothing here reads a total, so a
            // stronger isolation would buy a retry rate and nothing else.
            if has_shared_budget {
                let isolation = if has_write_skew {
                    "SERIALIZABLE"
                } else {
                    "REPEATABLE READ"
                };
                sqlx::raw_sql(AssertSqlSafe(format!(
                    "SET TRANSACTION ISOLATION LEVEL {isolation}"
                )))
                .execute(&mut *tx)
                .await?;
            }

            // How many this executor may take under every limit the queue carries: the tightest
            // of them, and `None` when it carries none. Deliberately not a capturing closure, so
            // the early returns between the groups below can read `max_tasks` while it is live.
            let narrow = |current: Option<i64>, available: i64| -> Option<i64> {
                Some(current.map_or(available, |taken: i64| taken.min(available)))
            };
            let mut max_tasks: Option<i64> = None;

            // Worker concurrency first, because it costs no query. Answered from what this
            // process is already running rather than from the database, which cannot see a
            // running workflow that has not written a step yet.
            if let Some(worker_concurrency) = limits.worker_concurrency {
                max_tasks = narrow(
                    max_tasks,
                    (i64::from(worker_concurrency) - local_running_count).max(0),
                );
            }
            if let Some(worker_concurrency) = limits.partition_worker_concurrency
                && partition_key.is_some()
            {
                max_tasks = narrow(
                    max_tasks,
                    (i64::from(worker_concurrency) - partition_local_running_count).max(0),
                );
            }
            if max_tasks == Some(0) {
                tx.commit().await?;
                return Ok(Vec::new());
            }

            // Then the rate limits, read as slots left in a rolling window rather than as a yes
            // or no: bounding the claim by what is left means a backlogged queue locks only the
            // rows it can actually start, which matters because the lock below may be `NOWAIT`.
            //
            // Twice over when a partition is named: the queue-wide window counts the whole queue,
            // the per-partition one only this key, and the tighter of the two governs.
            if let Some(limit) = limits.rate_limit {
                let recent_starts: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT count(*) FROM {workflow_table} \
                     WHERE queue_name = $2 AND rate_limited = TRUE \
                       AND status NOT IN ('ENQUEUED', 'DELAYED') \
                       AND started_at_epoch_ms > {NOW_MS_SQL} - $3 \
                       AND {owner_predicate}"
                )))
                .bind(application_name)
                .bind(&queue.name)
                .bind(rate_limit_period_ms)
                .fetch_one(&mut *tx)
                .await?;
                max_tasks = narrow(max_tasks, (i64::from(limit.limit) - recent_starts).max(0));
            }
            if let (Some(limit), Some(key)) = (limits.partition_rate_limit, partition_key) {
                let partition_starts: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT count(*) FROM {workflow_table} \
                     WHERE queue_name = $3 AND rate_limited = TRUE \
                       AND status NOT IN ('ENQUEUED', 'DELAYED') \
                       AND started_at_epoch_ms > {NOW_MS_SQL} - $4 \
                       AND queue_partition_key = $2 \
                       AND {owner_predicate}"
                )))
                .bind(application_name)
                .bind(key)
                .bind(&queue.name)
                .bind(partition_rate_limit_period_ms)
                .fetch_one(&mut *tx)
                .await?;
                max_tasks = narrow(
                    max_tasks,
                    (i64::from(limit.limit) - partition_starts).max(0),
                );
            }
            if max_tasks == Some(0) {
                tx.commit().await?;
                return Ok(Vec::new());
            }

            // Then the concurrency limits, each a count minus what is already pending at that
            // limit's scope. Last because each is a query, and a call the cheaper limits have
            // already closed never reaches them.
            if let Some(concurrency) = limits.concurrency {
                // Unscoped whichever partition is being swept: a queue-wide limit governs the
                // queue, and counting only this key would let every other key spend it again.
                let running: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT count(*) FROM {workflow_table} \
                     WHERE queue_name = $2 AND status = 'PENDING' AND {owner_predicate}"
                )))
                .bind(application_name)
                .bind(&queue.name)
                .fetch_one(&mut *tx)
                .await?;
                if running > i64::from(concurrency) {
                    // Reported rather than corrected: the excess is already running, and the
                    // limit can only govern what has yet to start.
                    tracing::warn!(
                        queue = %queue.name,
                        running,
                        concurrency,
                        "pending workflows exceed the queue's global concurrency limit"
                    );
                }
                max_tasks = narrow(max_tasks, (i64::from(concurrency) - running).max(0));
            }
            if let (Some(concurrency), Some(key)) = (limits.partition_concurrency, partition_key) {
                // Its own query rather than one grouped with the queue-wide count: this predicate
                // rides `idx_workflow_status_partition_dequeue_v2`, which a queue-wide scan loses.
                let running: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT count(*) FROM {workflow_table} \
                     WHERE queue_name = $3 AND status = 'PENDING' \
                       AND queue_partition_key = $2 AND {owner_predicate}"
                )))
                .bind(application_name)
                .bind(key)
                .bind(&queue.name)
                .fetch_one(&mut *tx)
                .await?;
                if running > i64::from(concurrency) {
                    tracing::warn!(
                        queue = %queue.name,
                        partition = %key,
                        running,
                        concurrency,
                        "pending workflows exceed the partition's concurrency limit"
                    );
                }
                max_tasks = narrow(max_tasks, (i64::from(concurrency) - running).max(0));
            }
            if max_tasks == Some(0) {
                tx.commit().await?;
                return Ok(Vec::new());
            }

            let is_latest = self
                .is_latest_application_version(&mut tx, application_version)
                .await?;
            let version_predicate = version_predicate(is_latest, 3);

            // TODO(dbos-team): UPSTREAM item 7. `SKIP LOCKED` under-delivers on CockroachDB;
            // settle before changing it.
            //
            // CockroachDB resolves write intents asynchronously after a commit, and `SKIP LOCKED`
            // skips a row whose intent is still unresolved rather than waiting. A workflow
            // enqueued moments ago is passed over, so the dequeue comes back short and that work
            // waits for the next poll. Measured on a single node, 15 rounds of three workflows:
            //
            //   FOR UPDATE SKIP LOCKED                     11/15 rounds short
            //   ... with a point read of each row first     5/15
            //   ... with a locking read over the queue      1/15
            //   FOR UPDATE                                  0/15
            //
            // **Recommendation: use a plain `FOR UPDATE` on CockroachDB**, keeping `SKIP LOCKED`
            // on PostgreSQL, in this statement and in the partitioned sweep's lock step. It waits
            // for the intent instead of skipping it, and measured clean. The cost is that
            // concurrent dequeues on one queue serialise on CockroachDB — the trade this needs
            // agreement on, because nothing else recovers the missing rows.
            //
            // No barrier outside this statement works: a row is readable while still being
            // skippable, so reading it first only narrows the window. A point-read barrier was
            // tried in the tests and CI kept failing, at a lower rate.
            //
            // Not Rust-specific, and nobody else varies the SQL. Python passes its two flags
            // straight through; TypeScript concatenates the mode; Java concatenates the literal
            // in `QueuesDAO` and keeps its CockroachDB handling in `MigrationManager`. Go alone
            // has the seam — `type CockroachDialect struct{ PostgresDialect }` overrides only
            // `Name` and `SupportsListenNotify`, inheriting `LockSkipLocked`, so its fix is one
            // line. Java's suite runs on CockroachDB and does not catch this: all fifteen of its
            // dequeue assertions check a limit being enforced (`assertEquals(0, ...)` or
            // `assertEquals(2, ...)` against four enqueued) rather than a count being complete.
            // See `UPSTREAM.md`.
            //
            // Until then the affected integration tests are skipped on CockroachDB.
            //
            // `SKIP LOCKED` steps over rows a peer is already claiming, which is what makes an
            // unlimited queue scale across executors. `NOWAIT` instead whenever a shared budget
            // is in play — a rate limit as much as a concurrency limit, since both are totals:
            // stepping over locked rows would leave this call counting a population it cannot
            // see, and let a peer spend the same budget against its own pre-claim snapshot.
            let lock = if has_shared_budget {
                "FOR UPDATE NOWAIT"
            } else {
                "FOR UPDATE SKIP LOCKED"
            };
            let limit = match max_tasks {
                Some(n) => format!(" LIMIT {n}"),
                None => String::new(),
            };
            let candidates: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT workflow_uuid FROM {workflow_table} \
                 WHERE queue_name = $4 AND status = 'ENQUEUED' \
                   AND {version_predicate} AND {owner_predicate} AND {partition_predicate} \
                 ORDER BY priority ASC, created_at ASC{limit} {lock}"
            )))
            .bind(application_name)
            .bind(partition_key)
            .bind(application_version)
            .bind(&queue.name)
            .fetch_all(&mut *tx)
            .await?;

            // One statement for the batch rather than one per workflow: every limit above has
            // already bounded `max_tasks`, so nothing is left to stop this part-way through.
            //
            // Guarded on `ENQUEUED` and on ownership together: a peer that won the race has
            // already moved the row, and re-dispatching it would run the workflow twice.
            // `COALESCE` claims an unclaimed row, which is what drains work a nameless client
            // enqueued; a nameless dequeuer leaves ownership untouched. `RETURNING` then reports
            // exactly the rows this statement flipped, so one a peer won is simply absent.
            let started: Vec<String> = if candidates.is_empty() {
                Vec::new()
            } else {
                let flipped: HashSet<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} \
                     SET status = 'PENDING', executor_id = $2, application_version = $3, \
                         started_at_epoch_ms = {NOW_MS_SQL}, rate_limited = $4, \
                         updated_at = {NOW_MS_SQL}, \
                         application_name = COALESCE(application_name, $1), \
                         workflow_deadline_epoch_ms = CASE \
                             WHEN workflow_timeout_ms IS NOT NULL \
                              AND workflow_deadline_epoch_ms IS NULL \
                             THEN {NOW_MS_SQL} + workflow_timeout_ms \
                             ELSE workflow_deadline_epoch_ms \
                         END \
                     WHERE workflow_uuid = ANY($5::text[]) AND status = 'ENQUEUED' \
                       AND {owner_predicate} \
                     RETURNING workflow_uuid"
                )))
                .bind(application_name)
                .bind(executor_id)
                .bind(application_version)
                .bind(rate_limited)
                .bind(candidates.as_slice())
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .collect();
                // Reported in dequeue order, which `RETURNING` does not promise: the caller
                // dispatches in the order it is handed, and priority is the point of the sort.
                candidates
                    .into_iter()
                    .filter(|id| flipped.contains(id))
                    .collect()
            };

            tx.commit().await?;
            if !started.is_empty() {
                tracing::debug!(queue = %queue.name, started = started.len(), "dequeued");
            }
            Ok(started)
        })
        .await
    }

    async fn get_queue_partitions(&self, queue_name: &str) -> Result<Vec<String>, Error> {
        let workflow_table = self.tables.workflow_status.as_str();
        let pool = &self.pool;
        let application_name = self.application_name.as_deref();
        // The rows this application could dequeue from this queue. Shared by both halves of the
        // recursion below, which have to agree: an anchor that started from a different
        // population than the step continued through would skip or repeat partitions silently.
        let eligible_predicate = "queue_name = $1 AND status = 'ENQUEUED' \
                                  AND ($2::text IS NULL OR application_name = $2 \
                                       OR application_name IS NULL)";

        with_retry(&self.retry, "get_queue_partitions", move || async move {
            // A loose index scan, not `SELECT DISTINCT`. Neither backend can skip to the next
            // distinct value inside a plain distinct, so it degenerates into reading every
            // enqueued row; each step of this recursion is one seek on
            // `idx_workflow_status_partition_dequeue_v2`, so the cost follows the number of
            // partitions rather than the depth of the backlog.
            let partitions: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "WITH RECURSIVE partitions AS ( \
                   (SELECT MIN(queue_partition_key) AS pk FROM {workflow_table} \
                     WHERE {eligible_predicate} AND queue_partition_key IS NOT NULL) \
                   UNION ALL \
                   (SELECT (SELECT MIN(queue_partition_key) FROM {workflow_table} \
                             WHERE {eligible_predicate} AND queue_partition_key > partitions.pk) \
                    FROM partitions WHERE partitions.pk IS NOT NULL) \
                 ) \
                 SELECT pk FROM partitions WHERE pk IS NOT NULL"
            )))
            .bind(queue_name)
            .bind(application_name)
            .fetch_all(pool)
            .await?;
            Ok(partitions)
        })
        .await
    }

    async fn start_queued_partitioned_workflows(
        &self,
        queue: &QueueRecord,
        executor_id: &str,
        application_version: &str,
        max_tasks: Option<i64>,
    ) -> Result<Vec<String>, Error> {
        // The sweep admits one row per partition and counts nothing, so it is only correct where
        // "one at a time per key" is the whole of what the queue means. Either rate limit, or a
        // queue-wide concurrency, would need the counting this deliberately does without.
        //
        // `partition_worker_concurrency` is not in the list, and does not need to be: it is at
        // least 1, and a partition already capped at one workflow across the fleet cannot exceed
        // one in this process. It could never bind here, so allowing it costs nothing.
        let limits = queue.resolved_limits();
        if limits.partition_concurrency != Some(1)
            || limits.concurrency.is_some()
            || limits.rate_limit.is_some()
            || limits.partition_rate_limit.is_some()
        {
            return Err(Error::InvalidInput {
                field: "queue".into(),
                detail: format!(
                    "a partitioned sweep needs partition concurrency 1 and no other limit, but \
                     {:?} has partition_concurrency={:?} concurrency={:?} rate_limited={} \
                     partition_rate_limited={}",
                    queue.name,
                    limits.partition_concurrency,
                    limits.concurrency,
                    limits.rate_limit.is_some(),
                    limits.partition_rate_limit.is_some(),
                ),
            });
        }
        // Nothing to take is not a sweep worth making.
        if max_tasks == Some(0) {
            return Ok(Vec::new());
        }
        // This worker's own budget bounds the sweep alongside the cap, so a process near its
        // worker concurrency does not claim heads it must immediately sit on.
        let cap = i64::from(PARTITIONED_DEQUEUE_SWEEP_CAP);
        let sweep_limit = max_tasks.map_or(cap, |budget| budget.min(cap));
        // **Random when the budget is what binds, key order when it is not.** An unbounded sweep
        // reaches every partition, so ordering by key is free and makes the claim deterministic.
        // A sweep the caller's budget cuts short does not reach every partition, and taking the
        // lowest keys every time would leave the tail of a large partition set permanently unserved
        // under sustained load. TypeScript switches on the same condition; the walk in `dequeue`
        // shuffles for the same reason.
        let sweep_order = if sweep_limit < cap {
            "random()"
        } else {
            "partitions.pk ASC"
        };

        let workflow_table = self.tables.workflow_status.as_str();
        let pool = &self.pool;
        let application_name = self.application_name.as_deref();

        with_retry(
            &self.retry,
            "start_queued_partitioned_workflows",
            move || async move {
                let mut tx = pool.begin().await?;

                let is_latest = self
                    .is_latest_application_version(&mut tx, application_version)
                    .await?;
                // Both statements below place the version at `$3`.
                let version_predicate = version_predicate(is_latest, 3);

                // Candidate query: $1 queue, $2 application, $3 version. `eligible` is the
                // shared predicate all three of its uses must agree on — see
                // `get_queue_partitions`.
                let eligible_predicate = "queue_name = $1 AND status = 'ENQUEUED' \
                                          AND ($2::text IS NULL OR application_name = $2 \
                                               OR application_name IS NULL)";

                // The same loose index scan `get_queue_partitions` uses, then one head per
                // partition. `workflow_uuid` breaks ties in the head order so every worker ranks
                // a partition identically — which is what lets the guarded flip below admit at
                // most one row per partition without anyone counting.
                //
                // **Partitions are chosen before their heads are looked up.** The `LIMIT` sits in
                // `chosen`, so a sweep the budget cuts short probes only the partitions it will
                // actually claim from rather than probing every one and discarding the excess.
                //
                // **So the limit bounds partitions probed, not heads returned**, and the two stop
                // agreeing when a chosen partition holds no row this executor may take: the
                // version predicate is in the `LATERAL`, not in `chosen`, so such a partition
                // spends a slot and produces nothing. During a rolling deploy a worker can
                // therefore come back short of its budget and run below its worker concurrency.
                // It is bounded and it heals — `sweep_order` is `random()` in exactly the case
                // the budget binds, so no partition is masked twice running, and a short sweep
                // reports no contention, so the poller scales *back* towards the queue's interval
                // rather than backing off.
                //
                // TODO(dbos-team): UPSTREAM item 21. Python and TypeScript place the `LIMIT` and
                // the version predicate exactly here too, so this is upstream behaviour rather
                // than a port's slip, and the fix worth having — `application_version` in
                // `idx_workflow_status_partition_dequeue_v2` — is a shared migration in any case.
                //
                // `LATERAL` rather than a correlated scalar subquery: it plans as a tight nested
                // loop instead of a slower per-row subplan. `workflow_uuid` totalizes the head
                // order, so every worker picks the same head under a `created_at` tie, and the
                // index's trailing `workflow_uuid` keeps the probe a pure top-1 — **for a
                // partition whose head is eligible.** Where the version predicate rejects every
                // row the probe is not a top-1 at all: `application_version` is not in the index,
                // so it walks that partition's entries and heap-checks each to return nothing.
                // That cost is paid whatever the `LIMIT` bounds, and it is why the index, rather
                // than the `LIMIT`, is what item 21 proposes moving. The `NOT EXISTS` on
                // `PENDING` is unscoped by design — a mutual-exclusion probe must block on any
                // owner's row.
                //
                // **No `--` comments inside this string.** The `\` continuations strip the
                // newlines that would end them, so a `--` comments out the whole rest of the
                // statement — including the `) head ON TRUE` alias, which fails as a missing
                // FROM-clause entry rather than as a syntax error.
                let candidates: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "WITH RECURSIVE partitions AS ( \
                       (SELECT MIN(queue_partition_key) AS pk FROM {workflow_table} \
                         WHERE {eligible_predicate} AND queue_partition_key IS NOT NULL) \
                       UNION ALL \
                       (SELECT (SELECT MIN(queue_partition_key) FROM {workflow_table} \
                                 WHERE {eligible_predicate} AND queue_partition_key > partitions.pk) \
                        FROM partitions WHERE partitions.pk IS NOT NULL) \
                     ) \
                     , chosen AS ( \
                       SELECT partitions.pk FROM partitions \
                        WHERE partitions.pk IS NOT NULL \
                          AND NOT EXISTS ( \
                            SELECT 1 FROM {workflow_table} \
                             WHERE queue_name = $1 AND status = 'PENDING' \
                               AND queue_partition_key IS NOT NULL \
                               AND queue_partition_key = partitions.pk \
                          ) \
                        ORDER BY {sweep_order} LIMIT $4 \
                     ) \
                     SELECT head.workflow_uuid FROM chosen \
                     JOIN LATERAL ( \
                       SELECT workflow_uuid FROM {workflow_table} \
                        WHERE {eligible_predicate} AND queue_partition_key = chosen.pk \
                          AND {version_predicate} \
                        ORDER BY priority ASC, created_at ASC, workflow_uuid ASC LIMIT 1 \
                     ) head ON TRUE \
                     ORDER BY chosen.pk ASC"
                )))
                .bind(&queue.name)
                .bind(application_name)
                .bind(application_version)
                .bind(sweep_limit)
                .fetch_all(&mut *tx)
                .await?;
                if candidates.is_empty() {
                    tx.commit().await?;
                    return Ok(Vec::new());
                }

                // Re-checks queue, partition and version alongside status, so a row that
                // `resume_workflows` moved to another queue mid-sweep is dropped rather than
                // hijacked. $1 ids, $2 queue, $3 version, $4 application.
                let claim_predicate = format!(
                    "workflow_uuid = ANY($1::text[]) AND status = 'ENQUEUED' \
                       AND queue_name = $2 AND queue_partition_key IS NOT NULL \
                       AND {version_predicate} \
                       AND ($4::text IS NULL OR application_name = $4 \
                            OR application_name IS NULL)"
                );

                // TODO(dbos-team): UPSTREAM item 7. This `SKIP LOCKED` under-delivers on CockroachDB
                // for the reason given
                // on `start_queued_workflows` — a head enqueued moments ago is skipped and its
                // partition idles until the next sweep. **Recommendation: plain `FOR UPDATE` on
                // CockroachDB**, applied here as well as there. Settle with the wider DBOS team
                // first; see `UPSTREAM.md`.
                //
                // Locks the fixed candidate set rather than re-selecting with a `LIMIT`, whose
                // `SKIP LOCKED` could slide past a locked head and admit a partition's second
                // row ahead of its first.
                let locked: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT workflow_uuid FROM {workflow_table} WHERE {claim_predicate} \
                     FOR UPDATE SKIP LOCKED"
                )))
                .bind(&candidates)
                .bind(&queue.name)
                .bind(application_version)
                .bind(application_name)
                .fetch_all(&mut *tx)
                .await?;
                let locked: HashSet<&str> = locked.iter().map(String::as_str).collect();
                // Partition order, so submission follows the sweep rather than the lock order.
                let claiming: Vec<&str> = candidates
                    .iter()
                    .map(String::as_str)
                    .filter(|id| locked.contains(id))
                    .collect();
                if claiming.is_empty() {
                    tx.commit().await?;
                    return Ok(Vec::new());
                }

                // `RETURNING` reports exactly the rows this statement flipped, which is what
                // makes the guard the admission control rather than a check before one.
                let flipped: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "UPDATE {workflow_table} \
                     SET status = 'PENDING', executor_id = $5, application_version = $3, \
                         started_at_epoch_ms = {NOW_MS_SQL}, rate_limited = FALSE, \
                         updated_at = {NOW_MS_SQL}, \
                         application_name = COALESCE(application_name, $4), \
                         workflow_deadline_epoch_ms = CASE \
                             WHEN workflow_timeout_ms IS NOT NULL \
                              AND workflow_deadline_epoch_ms IS NULL \
                             THEN {NOW_MS_SQL} + workflow_timeout_ms \
                             ELSE workflow_deadline_epoch_ms \
                         END \
                     WHERE {claim_predicate} RETURNING workflow_uuid"
                )))
                .bind(&claiming)
                .bind(&queue.name)
                .bind(application_version)
                .bind(application_name)
                .bind(executor_id)
                .fetch_all(&mut *tx)
                .await?;

                tx.commit().await?;
                let flipped: HashSet<&str> = flipped.iter().map(String::as_str).collect();
                let started: Vec<String> = claiming
                    .into_iter()
                    .filter(|id| flipped.contains(id))
                    .map(str::to_owned)
                    .collect();
                if !started.is_empty() {
                    tracing::debug!(
                        queue = %queue.name,
                        started = started.len(),
                        "dequeued a partition sweep"
                    );
                }
                Ok(started)
            },
        )
        .await
    }

    async fn get_deduplication_key_holder(
        &self,
        queue_name: &str,
        deduplication_id: &str,
    ) -> Result<Option<String>, Error> {
        let workflow_table = self.tables.workflow_status.as_str();
        let pool = &self.pool;

        with_retry(
            &self.retry,
            "get_deduplication_key_holder",
            move || async move {
                // No status filter, and none is needed: every terminal transition clears the key, so
                // a row still carrying one is by definition an active holder. The references filter
                // on nothing either.
                let holder: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT workflow_uuid FROM {workflow_table} \
                     WHERE queue_name = $1 AND deduplication_id = $2"
                )))
                .bind(queue_name)
                .bind(deduplication_id)
                .fetch_optional(pool)
                .await?;
                tracing::debug!(
                    queue_name,
                    deduplication_id,
                    ?holder,
                    "read the key's holder"
                );
                Ok(holder)
            },
        )
        .await
    }

    async fn get_queue(&self, name: &str) -> Result<Option<QueueRecord>, Error> {
        let queues_table = self.tables.queues.as_str();
        let pool = &self.pool;

        with_retry(&self.retry, "get_queue", move || async move {
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT {QUEUE_COLUMNS} FROM {queues_table} WHERE name = $1"
            )))
            .bind(name)
            .fetch_optional(pool)
            .await?;
            row.as_ref().map(queue_from_row).transpose()
        })
        .await
    }

    async fn list_queues(
        &self,
        applications: &Applications<'_>,
    ) -> Result<Vec<QueueRecord>, Error> {
        let queues_table = self.tables.queues.as_str();
        let pool = &self.pool;
        let application_name = self.application_name.as_deref();

        with_retry(&self.retry, "list_queues", move || async move {
            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
            q.push(QUEUE_COLUMNS).push(" FROM ").push(queues_table);
            // A search, so an unset scope means this application's own plus the unclaimed.
            match applications {
                Applications::Any => {}
                Applications::Named(names) if names.is_empty() => {}
                Applications::Named(names) => {
                    q.push(" WHERE (application_name = ANY(")
                        .push_bind(&names[..])
                        .push(") OR application_name IS NULL)");
                }
                Applications::Unset => {
                    if let Some(name) = application_name {
                        q.push(" WHERE (application_name = ")
                            .push_bind(name)
                            .push(" OR application_name IS NULL)");
                    }
                }
            }
            q.push(" ORDER BY name");
            let rows = q.build().fetch_all(pool).await?;
            rows.iter().map(queue_from_row).collect()
        })
        .await
    }

    async fn update_queue(
        &self,
        name: &str,
        update: &QueueUpdate,
        validate: &(
             dyn for<'r, 's> Fn(&'r QueueRecord, &'s QueueRecord) -> Result<(), Error> + Send + Sync
         ),
    ) -> Result<QueueRecord, Error> {
        let queues_table = self.tables.queues.as_str();
        let pool = &self.pool;

        with_retry(&self.retry, "update_queue", move || async move {
            let mut tx = pool.begin().await?;

            // `FOR UPDATE` rather than a stricter isolation level: the row is held for the rest of
            // the transaction, so a peer planning against the same queue waits here instead of
            // racing to the `UPDATE` and having its whole attempt thrown away. Go takes the other
            // route — repeatable read, and a retry on the serialization failure that follows.
            let row = sqlx::query(AssertSqlSafe(format!(
                "SELECT {QUEUE_COLUMNS} FROM {queues_table} WHERE name = $1 FOR UPDATE"
            )))
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(stored) = row.as_ref().map(queue_from_row).transpose()? else {
                return Err(Error::NotRegistered {
                    kind: "Queue".into(),
                    name: name.to_owned(),
                });
            };

            // Nothing to change leaves the row alone — including its `updated_at`, which would
            // otherwise record a write that changed nothing. Unvalidated on purpose: a caller who
            // asked for nothing is not asking to have the stored row judged.
            if update.is_empty() {
                tx.commit().await?;
                return Ok(stored);
            }

            // The caller's verdict on the row as it would be, delivered while this transaction
            // still holds it. Rolling back is what makes a refusal mean something: nothing was
            // written, and nothing else moved the row while it was being judged.
            let merged = update.apply_to(&stored);
            validate(&stored, &merged)?;

            let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("UPDATE ");
            q.push(queues_table).push(" SET ");
            let mut set = q.separated(", ");

            macro_rules! assign {
                ($field:expr, $column:literal) => {
                    if let Some(value) = $field {
                        set.push(concat!($column, " = "));
                        set.push_bind_unseparated(value);
                    }
                };
            }
            assign!(update.concurrency.set(), "concurrency");
            assign!(update.worker_concurrency.set(), "worker_concurrency");
            assign!(update.priority_enabled.set(), "priority_enabled");
            // **The flag and the limits are two spellings of one fact**, so the column is never
            // written disagreeing with them. `apply_to` has already resolved which of the three
            // cases this update is — the flag named outright, the flag rewritten to match a
            // moved per-partition limit, or neither touched — so the value to store is the
            // merged row's, and the only question left here is whether to assign at all.
            //
            // Assigning nothing when neither is touched is what lets a row a peer wrote with the
            // deprecated flag and no limits keep what it says.
            if !(update.partition_queue.is_leave()
                && update.partition_concurrency.is_leave()
                && update.partition_worker_concurrency.is_leave()
                && update.partition_rate_limit.is_leave())
            {
                set.push("partition_queue = ");
                set.push_bind_unseparated(merged.partition_queue);
            }
            assign!(update.partition_concurrency.set(), "partition_concurrency");
            assign!(
                update.partition_worker_concurrency.set(),
                "partition_worker_concurrency"
            );
            assign!(
                update.polling_interval.set().map(|d| d.as_secs_f64()),
                "polling_interval_sec"
            );
            // One field, two columns — which is the point of pairing them: an update can set both
            // or clear both, and cannot leave half a limit behind.
            if let Some(limit) = update.rate_limit.set() {
                set.push("rate_limit_max = ");
                set.push_bind_unseparated(limit.map(|l| l.limit));
                set.push("rate_limit_period_sec = ");
                set.push_bind_unseparated(limit.map(|l| l.period.as_secs_f64()));
            }
            if let Some(limit) = update.partition_rate_limit.set() {
                set.push("partition_rate_limit_max = ");
                set.push_bind_unseparated(limit.map(|l| l.limit));
                set.push("partition_rate_limit_period_sec = ");
                set.push_bind_unseparated(limit.map(|l| l.period.as_secs_f64()));
            }
            set.push("updated_at = ");
            set.push_bind_unseparated(Timestamp::now().as_epoch_ms());

            q.push(" WHERE name = ").push_bind(name);
            q.push(" RETURNING ").push(QUEUE_COLUMNS);
            let row = q.build().fetch_one(&mut *tx).await?;
            let written = queue_from_row(&row)?;

            tx.commit().await?;
            Ok(written)
        })
        .await
    }

    async fn debounce_delayed_workflow(
        &self,
        request: &DebounceRequest<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Debounce, Error> {
        request.validate()?;
        let workflow_table = self.tables.workflow_status.as_str();
        let application_name = request
            .application_name
            .or(self.application_name.as_deref());

        // Read once per attempt, so a retry after a lost commit acknowledgement records the times
        // of the attempt that actually landed.
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(
            &self.retry,
            "debounce_delayed_workflow",
            move || async move {
                // A replay reports what the first run decided, which the wrapper handles: bouncing
                // again would push a delay the first run already pushed, against a workflow that
                // may since have started.
                self.run_transactional_step(
                    caller,
                    step_names::DEBOUNCE,
                    timing,
                    |mut tx| async move {
                        // The cap is what stops a steady stream of requests postponing the workflow
                        // forever: past the deadline, the delay stops moving. `CASE` rather than the
                        // `LEAST` this backend has, so the statement stays diffable against the four
                        // references, which all spell the cap out this way.
                        //
                        // `is_debounced` and the workflow's identity are both in the guard. Without the
                        // identity, two unrelated workflows whose keys happen to concatenate the same way
                        // — or two configured instances of one class — would overwrite each other's
                        // inputs; without `is_debounced`, an ordinary deduplicated enqueue would be
                        // silently rescheduled.
                        //
                        // `application_name` is claimed for the target the way its dequeue would: left
                        // unclaimed, every peer coalesces onto the one workflow and the last inputs win.
                        //
                        // These stay Rust comments. A `\` continuation strips the newline, so a `--`
                        // comment inside the string would comment out the rest of the statement.
                        let bounced: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                            "UPDATE {workflow_table} \
                             SET delay_until_epoch_ms = CASE \
                                     WHEN debounce_deadline_epoch_ms IS NOT NULL \
                                      AND debounce_deadline_epoch_ms < $4 \
                                     THEN debounce_deadline_epoch_ms \
                                     ELSE $4 \
                                 END, \
                                 inputs = $5, serialization = $6, \
                                 updated_at = {NOW_MS_SQL}, \
                                 application_name = COALESCE(application_name, $7) \
                             WHERE name = $1 AND queue_name = $2 AND deduplication_id = $3 \
                               AND class_name IS NOT DISTINCT FROM $8 \
                               AND config_name IS NOT DISTINCT FROM $9 \
                               AND status = 'DELAYED' AND is_debounced = TRUE \
                               AND ($7::text IS NULL OR application_name = $7 \
                                    OR application_name IS NULL) \
                             RETURNING workflow_uuid"
                        )))
                        .bind(request.workflow_name)
                        .bind(request.queue_name)
                        .bind(request.deduplication_id)
                        .bind(request.delay_until.as_epoch_ms())
                        .bind(request.inputs)
                        .bind(request.serialization)
                        .bind(application_name)
                        // `IS NOT DISTINCT FROM`, so an absent class or instance matches the NULL the
                        // enqueue stored rather than matching nothing, as `=` would.
                        .bind(request.class_name)
                        .bind(request.config_name)
                        .fetch_optional(&mut *tx)
                        .await?;

                        if let Some(workflow_id) = bounced {
                            return Ok((tx, Debounce::Bounced { workflow_id }));
                        }

                        // Deliberately unscoped: whatever blocked the update above is what the caller needs
                        // described, and a peer's workflow is the most useful case to be able to name.
                        type HolderRow = (
                            String,
                            bool,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        );
                        let holder: Option<HolderRow> = sqlx::query_as(AssertSqlSafe(format!(
                            "SELECT workflow_uuid, is_debounced, name, class_name, config_name, \
                                    application_name \
                             FROM {workflow_table} \
                             WHERE queue_name = $1 AND deduplication_id = $2"
                        )))
                        .bind(request.queue_name)
                        .bind(request.deduplication_id)
                        .fetch_optional(&mut *tx)
                        .await?;

                        let outcome = match holder {
                            None => Debounce::Unheld,
                            Some((
                                workflow_id,
                                is_debounced,
                                workflow_name,
                                class_name,
                                config_name,
                                application_name,
                            )) => Debounce::Held(DebounceHolder {
                                workflow_id,
                                is_debounced,
                                workflow_name,
                                class_name,
                                config_name,
                                application_name,
                            }),
                        };
                        // Returned even when nothing bounced, so the wrapper records it: the step
                        // consumed its id either way, and a replay that re-ran it would report a holder
                        // that has since changed.
                        Ok((tx, outcome))
                    },
                )
                .await
            },
        )
        .await
    }

    async fn delete_queue(&self, name: &str) -> Result<(), Error> {
        let queues_table = self.tables.queues.as_str();
        let pool = &self.pool;

        with_retry(&self.retry, "delete_queue", move || async move {
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {queues_table} WHERE name = $1"
            )))
            .bind(name)
            .execute(pool)
            .await?;
            Ok(())
        })
        .await
    }

    async fn create_schedule(
        &self,
        schedule: &NewSchedule<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let application_name = schedule
            .application_name
            .or(self.application_name.as_deref());
        // Generated once, outside the retry: a retry after a lost commit acknowledgement must
        // find its own row rather than insert a second one under a fresh id.
        let schedule_id = schedule
            .schedule_id
            .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
        let schedule_id = schedule_id.as_str();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "create_schedule", move || async move {
            self.run_transactional_step(
                caller,
                step_names::CREATE_SCHEDULE,
                timing,
                |mut tx| async move {
                    // A peer holding the name is a collision this layer cannot resolve; this
                    // application holding it is one the caller can, so the two are different errors.
                    let owner = resolve_owning_application(
                        &mut tx,
                        schedules_table,
                        "schedule_name",
                        schedule.schedule_name,
                        application_name,
                        "Schedule",
                    )
                    .await?;

                    // A plain insert, as all four references issue: the unique index refuses a name
                    // already taken, and there is nothing to do afterwards but say which index it
                    // was. Python and TypeScript catch the violation and report the name; the
                    // constraint tells us whether the id collided instead, which they cannot.
                    let inserted = sqlx::query(AssertSqlSafe(format!(
                        "INSERT INTO {schedules_table} \
                         (schedule_id, schedule_name, workflow_name, workflow_class_name, \
                          schedule, status, context, last_fired_at, automatic_backfill, \
                          cron_timezone, queue_name, application_name) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
                    )))
                    .bind(schedule_id)
                    .bind(schedule.schedule_name)
                    .bind(schedule.workflow_name)
                    .bind(schedule.workflow_class_name)
                    .bind(schedule.schedule)
                    .bind(schedule.status.as_str())
                    .bind(schedule.context)
                    .bind(schedule.last_fired_at.map(Timestamp::to_iso8601))
                    .bind(schedule.automatic_backfill)
                    .bind(schedule.cron_timezone)
                    .bind(schedule.queue_name)
                    .bind(owner.as_deref())
                    .execute(&mut *tx)
                    .await;

                    match inserted {
                        Ok(_) => {
                            tracing::debug!(
                                schedule_name = schedule.schedule_name,
                                schedule_id,
                                "registered a schedule"
                            );
                            Ok((tx, ()))
                        }
                        Err(error) if is_unique_violation(&error) => {
                            let kind = match error.as_database_error().and_then(|e| e.constraint())
                            {
                                Some(c) if c.ends_with("_pkey") => "Schedule id",
                                _ => "Schedule",
                            };
                            Err(Error::AlreadyRegistered {
                                kind: kind.into(),
                                name: match kind {
                                    "Schedule id" => schedule_id.to_owned(),
                                    _ => schedule.schedule_name.to_owned(),
                                },
                            })
                        }
                        Err(error) => Err(error.into()),
                    }
                },
            )
            .await
        })
        .await
    }

    async fn upsert_schedule(
        &self,
        schedule: &NewSchedule<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error> {
        let schedule_id = schedule
            .schedule_id
            .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
        let schedule_id = schedule_id.as_str();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "upsert_schedule", move || async move {
            self.run_transactional_step(
                caller,
                step_names::UPSERT_SCHEDULE,
                timing,
                |mut tx| async move {
                    self.upsert_schedule_on(&mut tx, schedule, schedule_id)
                        .await?;
                    Ok((tx, ()))
                },
            )
            .await
        })
        .await
    }

    async fn get_schedule(
        &self,
        name: &str,
        caller: Option<(&str, i32)>,
    ) -> Result<Option<ScheduleRecord>, Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "get_schedule", move || async move {
            // A read is a step too: a workflow that branches on a schedule has to see the same
            // schedule on replay, whatever an operator changed in between.
            self.run_transactional_step(
                caller,
                step_names::GET_SCHEDULE,
                timing,
                |mut tx| async move {
                    let row = sqlx::query(AssertSqlSafe(format!(
                        "SELECT {SCHEDULE_COLUMNS} FROM {schedules_table} WHERE schedule_name = $1"
                    )))
                    .bind(name)
                    .fetch_optional(&mut *tx)
                    .await?;
                    let schedule = row.as_ref().map(schedule_from_row).transpose()?;
                    Ok((tx, schedule))
                },
            )
            .await
        })
        .await
    }

    async fn apply_schedules(&self, schedules: &[NewSchedule<'_>]) -> Result<(), Error> {
        let pool = &self.pool;
        // Identities for the rows this call may create, generated once for the same reason
        // `create_schedule` generates its one outside the retry.
        let ids: Vec<String> = schedules
            .iter()
            .map(|s| {
                s.schedule_id
                    .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned)
            })
            .collect();
        let ids = ids.as_slice();

        with_retry(&self.retry, "apply_schedules", move || async move {
            let mut tx = pool.begin().await?;
            for (schedule, schedule_id) in schedules.iter().zip(ids) {
                self.upsert_schedule_on(&mut tx, schedule, schedule_id)
                    .await?;
            }
            tx.commit().await?;
            tracing::debug!(count = schedules.len(), "applied schedules");
            Ok(())
        })
        .await
    }

    async fn list_schedules(
        &self,
        filter: &ScheduleFilter<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<Vec<ScheduleRecord>, Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let application_name = self.application_name.as_deref();
        let statuses: Vec<&str> = filter.statuses.iter().map(|s| s.as_str()).collect();
        let statuses = statuses.as_slice();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "list_schedules", move || async move {
            self.run_transactional_step(
                caller,
                step_names::LIST_SCHEDULES,
                timing,
                |mut tx| async move {
                    let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
                    q.push(SCHEDULE_COLUMNS)
                        .push(" FROM ")
                        .push(schedules_table)
                        .push(" WHERE TRUE");
                    if !statuses.is_empty() {
                        q.push(" AND status = ANY(").push_bind(statuses).push(")");
                    }
                    if !filter.workflow_names.is_empty() {
                        q.push(" AND workflow_name = ANY(")
                            .push_bind(&filter.workflow_names[..])
                            .push(")");
                    }
                    // A prefix match, so the pattern is built rather than bound whole: the
                    // caller's string is data, and `%` or `_` inside it must match itself.
                    if !filter.schedule_name_prefixes.is_empty() {
                        q.push(" AND (");
                        for (i, prefix) in filter.schedule_name_prefixes.iter().enumerate() {
                            if i > 0 {
                                q.push(" OR ");
                            }
                            q.push("schedule_name LIKE ")
                                .push_bind(format!("{}%", escape_like(prefix)));
                        }
                        q.push(")");
                    }
                    // A search, so an unset scope means this application's own plus the unclaimed.
                    match &filter.applications {
                        Applications::Any => {}
                        Applications::Named(names) if names.is_empty() => {}
                        Applications::Named(names) => {
                            q.push(" AND (application_name = ANY(")
                                .push_bind(&names[..])
                                .push(") OR application_name IS NULL)");
                        }
                        Applications::Unset => {
                            if let Some(name) = application_name {
                                q.push(" AND (application_name = ")
                                    .push_bind(name)
                                    .push(" OR application_name IS NULL)");
                            }
                        }
                    }
                    q.push(" ORDER BY schedule_name");
                    let rows = q.build().fetch_all(&mut *tx).await?;
                    let schedules: Vec<ScheduleRecord> = rows
                        .iter()
                        .map(schedule_from_row)
                        .collect::<Result<_, _>>()?;
                    Ok((tx, schedules))
                },
            )
            .await
        })
        .await
    }

    async fn update_schedule(
        &self,
        name: &str,
        update: &ScheduleUpdate<'_>,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "update_schedule", move || async move {
            self.run_transactional_step(
                caller,
                step_names::UPDATE_SCHEDULE,
                timing,
                |mut tx| async move {
                    // An empty update still has to say whether the schedule exists, so it becomes
                    // a read rather than an early return: silence would report a typo as success.
                    let changed = if update.is_empty() {
                        // include explict cast for CRDB compat
                        sqlx::query_scalar::<_, i32>(AssertSqlSafe(format!(
                            "SELECT 1::int4 FROM {schedules_table} WHERE schedule_name = $1"
                        )))
                        .bind(name)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some()
                    } else {
                        let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("UPDATE ");
                        q.push(schedules_table).push(" SET ");
                        let mut separated = q.separated(", ");
                        if let Change::Set(schedule) = update.schedule {
                            separated
                                .push("schedule = ")
                                .push_bind_unseparated(schedule);
                        }
                        if let Change::Set(context) = update.context {
                            separated.push("context = ").push_bind_unseparated(context);
                        }
                        if let Change::Set(backfill) = update.automatic_backfill {
                            separated
                                .push("automatic_backfill = ")
                                .push_bind_unseparated(backfill);
                        }
                        if let Change::Set(timezone) = update.cron_timezone {
                            separated
                                .push("cron_timezone = ")
                                .push_bind_unseparated(timezone);
                        }
                        if let Change::Set(queue_name) = update.queue_name {
                            separated
                                .push("queue_name = ")
                                .push_bind_unseparated(queue_name);
                        }
                        q.push(" WHERE schedule_name = ").push_bind(name);
                        q.build().execute(&mut *tx).await?.rows_affected() > 0
                    };
                    if !changed {
                        return Err(Error::NotRegistered {
                            kind: "Schedule".into(),
                            name: name.to_owned(),
                        });
                    }
                    Ok((tx, ()))
                },
            )
            .await
        })
        .await
    }

    async fn set_schedule_status(
        &self,
        name: &str,
        status: ScheduleStatus,
        caller: Option<(&str, i32)>,
    ) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        // Pause and resume are two API calls in the references and two step names with them, so a
        // replay of one is not mistaken for the other.
        let step_name = match status {
            ScheduleStatus::Active => step_names::RESUME_SCHEDULE,
            ScheduleStatus::Paused => step_names::PAUSE_SCHEDULE,
        };
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "set_schedule_status", move || async move {
            self.run_transactional_step(caller, step_name, timing, |mut tx| async move {
                let updated = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {schedules_table} SET status = $2 WHERE schedule_name = $1"
                )))
                .bind(name)
                .bind(status.as_str())
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if updated == 0 {
                    return Err(Error::NotRegistered {
                        kind: "Schedule".into(),
                        name: name.to_owned(),
                    });
                }
                tracing::debug!(
                    schedule_name = name,
                    status = status.as_str(),
                    "set a schedule's status"
                );
                Ok((tx, ()))
            })
            .await
        })
        .await
    }

    async fn update_schedule_last_fired_at(
        &self,
        name: &str,
        last_fired_at: Timestamp,
    ) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let pool = &self.pool;
        // Formatted once, outside the retry: every attempt records the same firing.
        let last_fired_at = last_fired_at.to_iso8601();
        let last_fired_at = last_fired_at.as_str();

        with_retry(
            &self.retry,
            "update_schedule_last_fired_at",
            move || async move {
                sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {schedules_table} SET last_fired_at = $2 WHERE schedule_name = $1"
                )))
                .bind(name)
                .bind(last_fired_at)
                .execute(pool)
                .await?;
                Ok(())
            },
        )
        .await
    }

    async fn delete_schedule(&self, name: &str, caller: Option<(&str, i32)>) -> Result<(), Error> {
        let schedules_table = self.tables.workflow_schedules.as_str();
        let timing = StepTiming {
            started_at: Timestamp::now(),
            completed_at: Timestamp::now(),
        };

        with_retry(&self.retry, "delete_schedule", move || async move {
            self.run_transactional_step(
                caller,
                step_names::DELETE_SCHEDULE,
                timing,
                |mut tx| async move {
                    sqlx::query(AssertSqlSafe(format!(
                        "DELETE FROM {schedules_table} WHERE schedule_name = $1"
                    )))
                    .bind(name)
                    .execute(&mut *tx)
                    .await?;
                    Ok((tx, ()))
                },
            )
            .await
        })
        .await
    }

    async fn rename_application(
        &self,
        source: RenameFrom<'_>,
        new_name: &str,
        batching: RenameBatching,
    ) -> Result<ApplicationRowCounts, Error> {
        if !is_valid_application_name(new_name) {
            return Err(Error::InvalidInput {
                field: "new_name".into(),
                detail: "must be 3 to 30 characters of lowercase letters, digits, dashes and \
                         underscores"
                    .to_owned(),
            });
        }
        if source.application() == Some(new_name) {
            return Err(Error::InvalidInput {
                field: "new_name".into(),
                detail: format!("{new_name:?} already holds that name"),
            });
        }
        if let RenameBatching::Batched(0) = batching {
            return Err(Error::InvalidInput {
                field: "batching".into(),
                detail: "a batch must hold at least one workflow".to_owned(),
            });
        }

        let workflow_table = self.tables.workflow_status.as_str();
        let steps_table = self.tables.operation_outputs.as_str();
        let queues_table = self.tables.queues.as_str();
        let schedules_table = self.tables.workflow_schedules.as_str();
        let versions_table = self.tables.application_versions.as_str();
        let pool = &self.pool;
        // `$1` is the new name, so the source lands on `$2`.
        let predicate = rename_source_predicate(source, 2);
        let (predicate, renamed_from) = (predicate.as_str(), source.application());

        // The atomic half. A rename that committed the queue but not the version registry would
        // leave the application dequeuing work whose version row it can no longer see, so these
        // four move together or not at all. In-flight means the statuses a dequeue or a sweep can
        // still act on; everything terminal is only read about.
        let counts = with_retry(&self.retry, "rename_application", move || async move {
            let mut tx = pool.begin().await?;
            let mut moved = [0u64; 4];
            for (i, table) in [
                queues_table,
                schedules_table,
                versions_table,
                workflow_table,
            ]
            .into_iter()
            .enumerate()
            {
                // Only the workflow table needs narrowing; the other three have no status.
                let in_flight = if table == workflow_table {
                    " AND status IN ('PENDING', 'ENQUEUED', 'DELAYED')"
                } else {
                    ""
                };
                moved[i] = sqlx::query(AssertSqlSafe(format!(
                    "UPDATE {table} SET application_name = $1 WHERE {predicate}{in_flight}"
                )))
                .bind(new_name)
                .bind(renamed_from)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            }
            tx.commit().await?;
            Ok(moved)
        })
        .await?;

        // The long tail, which runs outside that transaction and may take several of its own.
        // Terminal workflows and their steps scope only observability and garbage collection, so
        // nothing misreads a database in which they have not yet caught up.
        let terminal = self
            .rename_application_in_batches(workflow_table, source, new_name, batching)
            .await?;
        let step_rows = self
            .rename_application_in_batches(steps_table, source, new_name, batching)
            .await?;

        let counts = ApplicationRowCounts {
            queues: counts[0],
            schedules: counts[1],
            versions: counts[2],
            workflows: counts[3] + terminal,
            steps: step_rows,
        };
        tracing::debug!(
            queues = counts.queues,
            schedules = counts.schedules,
            versions = counts.versions,
            workflows = counts.workflows,
            steps = counts.steps,
            "renamed application"
        );
        Ok(counts)
    }

    async fn record_child_workflow(
        &self,
        parent_workflow_id: &str,
        child_workflow_id: &str,
        step_id: i32,
        step_name: &str,
        started_at: Option<Timestamp>,
    ) -> Result<(), Error> {
        // Python fails loudly here rather than "silently wedging the parent on recovery": a
        // parent that replays and finds an empty child id has no workflow to attach to.
        if child_workflow_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "child_workflow_id".into(),
                detail: "must not be empty".to_owned(),
            });
        }
        let steps_table = &self.tables.operation_outputs;
        // Spans the launch only — the parent does not wait for the child here, so the step is
        // complete as soon as the child exists. Stamped only when the caller offered a start,
        // since half a pair measures nothing. Java passes both null here.
        let completed_at = started_at.map(|_| Timestamp::now());
        let (steps_table, pool) = (steps_table.as_str(), &self.pool);
        let application_name = self.application_name.as_deref();

        with_retry(&self.retry, "record_child_workflow", move || async move {
            // Same `DO UPDATE`-to-itself trick as `record_step`, but the returned value
            // compared is the **child id**, not the completion time. A retry stamps a new clock
            // reading and would fail a timestamp comparison, while the child id it is trying to
            // record is by definition the same one. Python states the rule exactly: "Same child
            // means an idempotent db_retry; a different child means nondeterminism."
            let stored: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(format!(
                "INSERT INTO {steps_table} (workflow_uuid, function_id, function_name, \
                 child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, \
                 application_name) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (workflow_uuid, function_id) DO UPDATE \
                 SET child_workflow_id = {steps_table}.child_workflow_id \
                 RETURNING child_workflow_id"
            )))
            .bind(parent_workflow_id)
            .bind(step_id)
            .bind(step_name)
            .bind(child_workflow_id)
            .bind(started_at.map(Timestamp::as_epoch_ms))
            .bind(completed_at.map(Timestamp::as_epoch_ms))
            // The launch is the parent's step, so it is stamped with the parent's application —
            // the child's own rows carry whatever application ends up running it.
            .bind(application_name)
            .fetch_optional(pool)
            .await?;

            if let Some(stored) = stored
                && stored.as_deref() != Some(child_workflow_id)
            {
                return Err(Error::StepAlreadyRecorded {
                    workflow_id: parent_workflow_id.to_owned(),
                    step_id,
                });
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{polling_limit, split_database};
    use tokio::sync::Semaphore;

    #[test]
    fn the_default_polling_cap_is_half_the_pool() {
        assert_eq!(polling_limit(None, 10), 5);
        assert_eq!(polling_limit(None, 21), 10);
    }

    /// Half of one is zero, which as a cap would block every poll forever rather than allow a
    /// small number of them.
    #[test]
    fn a_pool_too_small_to_halve_still_admits_one_poll() {
        assert_eq!(polling_limit(None, 1), 1);
        assert_eq!(polling_limit(None, 0), 1);
    }

    #[test]
    fn a_configured_polling_cap_is_taken_as_given() {
        assert_eq!(polling_limit(Some(3), 10), 3);
        // Above the pool size is the caller's business: this caps polling, the pool caps
        // connections. Neither reference rejects it either.
        assert_eq!(polling_limit(Some(100), 10), 100);
    }

    /// Off is a permit count nothing will exhaust, not an absent semaphore.
    #[test]
    fn zero_switches_the_polling_cap_off() {
        assert_eq!(polling_limit(Some(0), 10), Semaphore::MAX_PERMITS);
    }

    #[test]
    fn splits_a_url_into_its_maintenance_form_and_database() {
        assert_eq!(
            split_database("postgresql://u:p@host:5432/dbos_sys"),
            Some((
                "postgresql://u:p@host:5432/postgres".to_owned(),
                "dbos_sys".to_owned()
            )),
        );
    }

    /// Query parameters survive, since they often carry the TLS mode needed to connect at all.
    #[test]
    fn query_parameters_are_kept_on_the_maintenance_url() {
        assert_eq!(
            split_database("postgresql://h/db?sslmode=require"),
            Some((
                "postgresql://h/postgres?sslmode=require".to_owned(),
                "db".to_owned()
            )),
        );
    }

    /// Nothing named means nothing to create.
    #[test]
    fn a_url_without_a_database_has_nothing_to_split() {
        assert_eq!(split_database("postgresql://host:5432/"), None);
        assert_eq!(split_database("postgresql://host:5432"), None);
    }
}
