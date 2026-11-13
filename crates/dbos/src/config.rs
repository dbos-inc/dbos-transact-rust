//! How to reach and configure a [`DBOS`](crate::DBOS) instance.

use std::time::Duration;

use crate::serialization::Serializer;
use crate::sysdb::DEFAULT_SCHEMA;

/// Environment variable naming the system database, which [`Config::from_env`] and
/// [`ClientConfig::from_env`](crate::ClientConfig::from_env) read.
pub const SYSTEM_DATABASE_URL_ENV: &str = "DBOS_SYSTEM_DATABASE_URL";

/// Everything a [`DBOS`](crate::DBOS) instance needs.
///
/// Constructed with [`Config::new`] or [`Config::from_env`] and adjusted by functional update:
///
/// ```no_run
/// # use dbos::Config;
/// let config = Config { max_connections: 20, ..Config::from_env("my-app") };
/// ```
///
/// **Deliberately not `#[non_exhaustive]`.** That attribute forbids struct-literal construction
/// from other crates entirely, functional update included, which is exactly the form above. Adding
/// a field stays source-compatible for every caller who wrote `..Config::new(..)`, so the
/// convention is documented rather than enforced.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    /// Names this application among those sharing the system database.
    ///
    /// Load-bearing rather than cosmetic: it is the ownership key stamped on every row this
    /// instance writes, it mixes into the application version, and Conductor addresses an
    /// application by it. Three to 256 characters of lowercase letters, digits, dashes and
    /// underscores, checked at [`launch`](crate::DBOS::launch).
    pub app_name: String,

    /// Connection URL for the system database.
    pub database_url: String,

    /// Maximum pooled connections.
    pub max_connections: u32,

    /// Schema holding the DBOS tables.
    ///
    /// Deployment-wide: every application sharing a database has to agree on it.
    pub schema: String,

    /// Identifies this process among the executors sharing the database.
    ///
    /// Resolved like [`app_version`](Self::app_version) and with the same precedence: this wins
    /// over [`EXECUTOR_ID_ENV`](crate::EXECUTOR_ID_ENV) off DBOS Cloud and loses to it on, and
    /// when neither says anything the executor is `"local"`, the id other SDKs sharing the
    /// database also default to.
    pub executor_id: Option<String>,

    /// The version of the application's code.
    ///
    /// **Nothing computes one.** Falling back to a hash of the application's code does not work
    /// here: in Rust the only thing to hash is the executable, and a Rust build is not
    /// reproducible, so the version would move under a rebuild that changed nothing — stranding the
    /// previous run's `PENDING` rows with an executor that no longer exists, after hashing tens of
    /// megabytes at every launch to get there. A version has to come from somewhere, so
    /// [`launch`](crate::DBOS::launch) fails when neither this nor `DBOS__APPVERSION` gives it one.
    ///
    /// `None` here therefore means "whatever the environment says", not "work it out". The
    /// precedence is resolved at launch: this wins over
    /// [`APP_VERSION_ENV`](crate::APP_VERSION_ENV), except on DBOS Cloud
    /// ([`CLOUD_ENV`](crate::CLOUD_ENV)), where the deployment's variable wins over whatever the
    /// application was built believing.
    ///
    /// What to put here: `env!("CARGO_PKG_VERSION")` for an application that ships as a crate, or
    /// the commit sha for a deployment that ships per commit. A workflow is only recovered by an
    /// executor running the version that started it, which is what makes a rolling deploy safe —
    /// so the value should change exactly when the code changed in a way that matters.
    ///
    /// Setting this at all is what takes the choice away from `DBOS__APPVERSION`: the variable
    /// applies only where the field is `None`. On DBOS Cloud the deployment wins either way.
    pub app_version: Option<String>,

    /// How payloads are encoded, by default: the built-in `rust_serde`, or the application's own
    /// [`CustomSerializer`](crate::CustomSerializer).
    pub serializer: Serializer,

    /// Whether to use LISTEN/NOTIFY rather than polling.
    ///
    /// CockroachDB has no LISTEN/NOTIFY and forces polling whatever this says.
    pub use_listen_notify: bool,

    /// Whether [`launch`](crate::DBOS::launch) brings the schema up to date.
    ///
    /// Off suits an application whose database is migrated by a deployment step instead.
    pub migrate: bool,

    /// How many polling reads may run at once: half the pool by default; see
    /// [`PollingConcurrency`](crate::PollingConcurrency).
    pub polling_concurrency: crate::PollingConcurrency,

    /// How often a caller waiting on a workflow it does not own asks whether it has finished.
    ///
    /// `None` is one second. Workflow completion has no wakeup anywhere — no channel carries it
    /// and no trigger publishes it — so looking is the only way to learn a status changed.
    ///
    /// Zero is rejected rather than treated as "as fast as possible": it is a busy loop against
    /// the database, never something a caller means.
    pub outcome_poll_interval: Option<Duration>,

    /// Which queues this process dequeues from. `None` is all of them.
    ///
    /// **Configuration rather than a runtime call.** The supervisor intersects this with what it
    /// reads from the `queues` table on every sweep, so the dynamic half — a queue registered
    /// later, or deleted — comes for free without a second way to change the set.
    ///
    /// What it is for: splitting one application's queues across differently-shaped processes.
    /// A worker fleet sized for slow media jobs and one sized for fast API calls share a database
    /// and a code base, and neither should drain the other's backlog.
    ///
    /// `Some(vec![])` listens to **nothing**, and is not the same as `None`. It is a real setting
    /// — a process that enqueues and never runs anything. An empty list never means "all": the
    /// option carries that meaning, so the two cannot be confused.
    ///
    /// [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE) is always dequeued from, whatever this
    /// says. It is where `resume`, `fork` and recovery put work, so filtering it out would strand
    /// them with no way to notice.
    pub listen_queues: Option<Vec<String>>,

    /// How long a written key waits for company before a wakeup is pushed for it.
    ///
    /// `None` is ten milliseconds. `Some(Duration::ZERO)` is meaningful and *not* rejected — it
    /// turns coalescing off, which is a push per write. That is the one duration here where zero
    /// is a setting rather than a mistake.
    pub notification_coalesce: Option<Duration>,

    /// How long a system-database connection may sit idle inside an open transaction before the
    /// server ends its session.
    ///
    /// Sixty seconds by default; see [`IdleTransactionTimeout`](crate::IdleTransactionTimeout).
    pub idle_transaction_timeout: crate::IdleTransactionTimeout,

    /// How often the scheduler re-reads the schedules table for schedules created, changed,
    /// paused or deleted since its last look.
    ///
    /// `None` is thirty seconds. For the first minute after launch the scheduler looks at least
    /// once a second whatever this says, so a schedule created at startup starts promptly. Zero is
    /// rejected: it would re-read the table continuously.
    pub scheduler_polling_interval: Option<Duration>,

    /// The key this application connects to DBOS Conductor with, or `None` not to connect.
    ///
    /// Setting it changes launch in two ways besides connecting. The executor id becomes a fresh
    /// UUID for each launch, replacing [`executor_id`](Self::executor_id) and `DBOS__VMID` with a
    /// warning, since Conductor must be able to tell one process from the next. And launch no
    /// longer recovers this executor's abandoned workflows: Conductor decides which executors are
    /// gone and recovers their work itself.
    ///
    /// An empty or blank key is no key. On DBOS Cloud this is not read: the deployment supplies its
    /// own Conductor settings.
    pub conductor_key: Option<String>,

    /// Where Conductor is, as a websocket URL. `None` is DBOS's hosted Conductor,
    /// `wss://cloud.dbos.dev/conductor/v1alpha1`, or the same path on the domain `DBOS_DOMAIN`
    /// names.
    ///
    /// Read only when [`conductor_key`](Self::conductor_key) is set. An empty or blank URL is no
    /// URL.
    pub conductor_url: Option<String>,

    /// What this executor reports about itself to Conductor, shown beside it in the Console.
    pub conductor_executor_metadata: Option<serde_json::Map<String, serde_json::Value>>,

    /// Whether Conductor is sent only workflow metadata, never workflow data: no inputs, outputs,
    /// events, messages or stream values, and error kinds without their messages.
    pub conductor_metadata_only_mode: bool,
}

/// Hand-written rather than derived, so that neither the database URL's passwords nor the Conductor
/// key is ever printed.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            app_name,
            database_url,
            max_connections,
            schema,
            executor_id,
            app_version,
            serializer,
            use_listen_notify,
            migrate,
            polling_concurrency,
            outcome_poll_interval,
            listen_queues,
            notification_coalesce,
            idle_transaction_timeout,
            scheduler_polling_interval,
            conductor_key,
            conductor_url,
            conductor_executor_metadata,
            conductor_metadata_only_mode,
        } = self;
        // Says whether the key is there, which is what someone reading a log needs, and nothing
        // more.
        let redacted = |value: Option<&str>| match value {
            Some(v) if !v.is_empty() => "<redacted>",
            _ => "<unset>",
        };
        f.debug_struct("Config")
            .field("app_name", app_name)
            .field("database_url", &crate::sysdb::masked_url(database_url))
            .field("max_connections", max_connections)
            .field("schema", schema)
            .field("executor_id", executor_id)
            .field("app_version", app_version)
            .field("serializer", serializer)
            .field("use_listen_notify", use_listen_notify)
            .field("migrate", migrate)
            .field("polling_concurrency", polling_concurrency)
            .field("outcome_poll_interval", outcome_poll_interval)
            .field("listen_queues", listen_queues)
            .field("notification_coalesce", notification_coalesce)
            .field("idle_transaction_timeout", idle_transaction_timeout)
            .field("scheduler_polling_interval", scheduler_polling_interval)
            .field("conductor_key", &redacted(conductor_key.as_deref()))
            .field("conductor_url", conductor_url)
            .field("conductor_executor_metadata", conductor_executor_metadata)
            .field("conductor_metadata_only_mode", conductor_metadata_only_mode)
            .finish()
    }
}

impl Config {
    /// A configuration with the defaults, for an application named `app_name`, reaching the
    /// database at `database_url`.
    ///
    /// Leaves [`app_version`](Self::app_version) unset, which means launch takes it from
    /// `DBOS__APPVERSION` and fails if that is unset too.
    pub fn new(app_name: impl Into<String>, database_url: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            app_version: None,
            database_url: database_url.into(),
            max_connections: 10,
            schema: DEFAULT_SCHEMA.to_owned(),
            executor_id: None,
            serializer: Serializer::default(),
            use_listen_notify: true,
            migrate: true,
            polling_concurrency: crate::PollingConcurrency::Default,
            outcome_poll_interval: None,
            listen_queues: None,
            notification_coalesce: None,
            idle_transaction_timeout: crate::IdleTransactionTimeout::Default,
            scheduler_polling_interval: None,
            conductor_key: None,
            conductor_url: None,
            conductor_executor_metadata: None,
            conductor_metadata_only_mode: false,
        }
    }

    /// [`Config::new`], taking the database URL from [`SYSTEM_DATABASE_URL_ENV`].
    ///
    /// That variable and no other. The identity variables — `DBOS__APPVERSION`, `DBOS__VMID`,
    /// `DBOS__CLOUD` and `DBOS__APPID` — carry two underscores because they are DBOS Cloud's to
    /// set, and `DBOS_APP_NAME` is a deployment's too; an end user *may* set them, but a
    /// configuration is not the layer that reads them. Launch is, so that a `Config` written by
    /// hand and one from here resolve to the same identity.
    ///
    /// A missing or empty `DBOS_SYSTEM_DATABASE_URL` leaves [`database_url`](Self::database_url)
    /// empty rather than failing here, so that a caller who sets it afterwards is not forced
    /// through an error path. [`launch`](crate::DBOS::launch) is where an empty URL is reported,
    /// which is also where it would have failed anyway.
    pub fn from_env(app_name: impl Into<String>) -> Self {
        Self::new(
            app_name,
            std::env::var(SYSTEM_DATABASE_URL_ENV).unwrap_or_default(),
        )
    }

    /// Checks what can be checked before anything is connected.
    ///
    /// Not the identity — the name, version and executor id are resolved against the environment
    /// at launch, and [`identity`](crate::identity) is what validates the values that resolution
    /// arrives at rather than the ones the configuration happened to carry.
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.database_url.is_empty() {
            return Err(crate::Error::Config(format!(
                "no database URL: set `database_url`, or the {SYSTEM_DATABASE_URL_ENV} environment \
                 variable if the configuration came from `Config::from_env`"
            )));
        }
        if self.schema.is_empty() {
            return Err(crate::Error::Config("`schema` cannot be empty".to_owned()));
        }
        if self.max_connections == 0 {
            return Err(crate::Error::Config(
                "`max_connections` cannot be zero".to_owned(),
            ));
        }
        // Unlike `notification_coalesce`, where zero turns coalescing off and is a setting, a zero
        // poll interval is a busy loop against the database rather than a faster answer.
        if self.outcome_poll_interval == Some(Duration::ZERO) {
            return Err(crate::Error::Config(
                "`outcome_poll_interval` cannot be zero".to_owned(),
            ));
        }
        if self.scheduler_polling_interval == Some(Duration::ZERO) {
            return Err(crate::Error::Config(
                "`scheduler_polling_interval` cannot be zero".to_owned(),
            ));
        }
        self.idle_transaction_timeout
            .validate()
            .map_err(|error| crate::Error::Config(error.to_string()))?;
        self.serializer.validate()
    }

    /// How often an adopting caller looks, resolved.
    pub(crate) fn outcome_poll_interval(&self) -> Duration {
        self.outcome_poll_interval
            .unwrap_or(DEFAULT_OUTCOME_POLL_INTERVAL)
    }

    /// How often the scheduler re-reads the schedules table, resolved.
    pub(crate) fn scheduler_polling_interval(&self) -> Duration {
        self.scheduler_polling_interval
            .unwrap_or(DEFAULT_SCHEDULER_POLLING_INTERVAL)
    }
}

/// How often the scheduler re-reads the schedules table when nothing else is configured.
pub(crate) const DEFAULT_SCHEDULER_POLLING_INTERVAL: Duration = Duration::from_secs(30);

/// How often a workflow's outcome is polled when nothing else is configured.
pub(crate) const DEFAULT_OUTCOME_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;

    /// The name is public contract, read by both `from_env` constructors, so it is pinned as a
    /// literal rather than compared with itself.
    #[test]
    fn the_system_database_url_is_read_from_dbos_system_database_url() {
        assert_eq!(SYSTEM_DATABASE_URL_ENV, "DBOS_SYSTEM_DATABASE_URL");
    }

    #[test]
    fn validation_reports_a_missing_url_by_the_name_of_the_variable_that_sets_it() {
        let err = Config::new("app", "").validate().unwrap_err();
        assert!(err.to_string().contains(SYSTEM_DATABASE_URL_ENV), "{err}");
    }

    #[test]
    fn a_zero_outcome_poll_interval_is_a_busy_loop_and_is_refused() {
        let config = |outcome_poll_interval| Config {
            outcome_poll_interval,
            ..Config::new("app", "postgres://x")
        };
        assert_eq!(
            config(None).outcome_poll_interval(),
            Duration::from_secs(1),
            "the default polling interval"
        );
        assert_eq!(
            config(Some(Duration::from_millis(250))).outcome_poll_interval(),
            Duration::from_millis(250)
        );

        let err = config(Some(Duration::ZERO)).validate().unwrap_err();
        assert!(
            err.to_string().contains("outcome_poll_interval"),
            "unlike `notification_coalesce`, zero here is not a setting: {err}"
        );
    }

    /// The server takes a 32-bit count of milliseconds, so anything longer is refused here rather
    /// than failing every connection, and a zero timeout is refused rather than read as the
    /// server's setting, which has its own variant.
    #[test]
    fn an_idle_transaction_timeout_out_of_the_servers_range_is_refused() {
        use crate::IdleTransactionTimeout::{After, ServerSetting};
        let config = |idle_transaction_timeout| Config {
            idle_transaction_timeout,
            app_version: Some("v1".to_owned()),
            ..Config::new("app", "postgres://x")
        };
        assert!(config(ServerSetting).validate().is_ok());
        assert!(
            config(After(crate::IdleTransactionTimeout::MAX))
                .validate()
                .is_ok()
        );
        for refused in [Duration::ZERO, Duration::from_millis(i32::MAX as u64 + 1)] {
            let err = config(After(refused)).validate().unwrap_err();
            assert!(
                err.to_string().contains("idle_transaction_timeout"),
                "{err}"
            );
        }
    }

    #[test]
    fn debug_masks_the_database_password_and_hides_the_conductor_key() {
        let config = Config {
            conductor_key: Some("conductor-secret".to_owned()),
            ..Config::new("app", "postgres://user:db-secret@host/db")
        };
        let printed = format!("{config:?}");
        assert!(!printed.contains("db-secret"), "{printed}");
        assert!(!printed.contains("conductor-secret"), "{printed}");
        // Where the connection goes is kept: it is what a log reader needs.
        assert!(printed.contains("postgres://user:***@host/db"), "{printed}");
    }

    #[test]
    fn functional_update_is_the_documented_way_to_adjust_a_config() {
        let config = Config {
            max_connections: 20,
            ..Config::new("app", "postgres://x")
        };
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.app_name, "app");
    }
}
