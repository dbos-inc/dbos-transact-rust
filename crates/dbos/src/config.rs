//! How to reach and configure a [`DBOS`](crate::DBOS) instance.

use std::time::Duration;

use crate::sysdb::DEFAULT_SCHEMA;

/// Environment variable naming the system database.
///
/// The same name every implementation reads.
pub const DATABASE_URL_ENV: &str = "DBOS_DATABASE_URL";

/// How payloads are encoded on their way into the system database.
///
/// One variant for now. A workflow is only ever replayed by the SDK that wrote it — workflows
/// cross languages by enqueue, not by execution — so there is no wire form to conform to here.
/// What matters is that the choice is *recorded* in the `serialization` column of every row, so a
/// later reader knows how to read what it finds.
///
/// # How this grows
///
/// The other implementations end up with two built-ins — a native one and a portable one — plus
/// whatever a user supplies, and Python's dispatch shows the model: the two built-ins are always
/// available *by name*, and a third-party serializer is available only when it is the one this
/// process is configured with. A row written by some other custom serializer is an error, not a
/// guess (`Serialization {name} is not available`). So this becomes:
///
/// ```text
/// enum Serializer { Native, Portable, Custom(Arc<dyn CustomSerializer>) }
/// ```
///
/// which is why `#[non_exhaustive]` is here from the start: adding those variants is then
/// source-compatible, because a downstream `match` already needs a wildcard arm.
///
/// The part that needs care is the *trait*, not the enum. A serializer wants
/// `fn serialize<T: Serialize>(&self, value: &T)`, and a trait with a generic method cannot be made
/// into a `dyn` object at all — verified, not assumed: rustc rejects it with "because method
/// `serialize` has generic type parameters". The way out is `erased-serde`, which is built for
/// exactly this: the trait takes `&dyn erased_serde::Serialize` and hands back a
/// `Box<dyn erased_serde::Deserializer>`, and the generic half lives in a free function the call
/// site monomorphizes. The built-in two stay ordinary variants and pay none of it.
///
/// Two things here exist to keep that door open, and both would be breaking changes to add later:
/// this is `Clone` and not `Copy`, because an `Arc` is not `Copy`; and [`name`](Self::name)
/// borrows from `&self` rather than returning `&'static str`, because a custom serializer's name
/// belongs to the serializer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Serializer {
    /// `serde` with this crate's own derives, written as `rust_serde`.
    #[default]
    RustSerde,
}

impl Serializer {
    /// The name written to the `serialization` column.
    ///
    /// Python writes `py_pickle` or `portable_json` here and TypeScript `portable_json`; the value
    /// is a serializer's own name rather than a shared enumeration.
    pub fn name(&self) -> &str {
        match self {
            Serializer::RustSerde => "rust_serde",
        }
    }
}

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
///
/// The rest of the configuration surface — patching, the scheduler, Conductor — arrives with the
/// phase that reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// when neither says anything the executor is `"local"` — the default every implementation
    /// shares.
    pub executor_id: Option<String>,

    /// The version of the application's code.
    ///
    /// **Nothing computes one.** The other implementations fall back to a hash of the
    /// application's code, and that is the mistake this does not repeat: in Rust the only thing to
    /// hash is the executable, and a Rust build is not reproducible, so the version would move
    /// under a rebuild that changed nothing — stranding the previous run's `PENDING` rows with an
    /// executor that no longer exists, after hashing tens of megabytes at every launch to get
    /// there. A version has to come from somewhere, so [`launch`](crate::DBOS::launch) fails when
    /// neither this nor `DBOS__APPVERSION` gives it one.
    ///
    /// `None` here therefore means "whatever the environment says", not "work it out". The
    /// precedence is Java's, resolved at launch: this wins over
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

    /// How payloads are encoded.
    pub serializer: Serializer,

    /// Whether to use LISTEN/NOTIFY rather than polling.
    ///
    /// CockroachDB has no LISTEN/NOTIFY and forces polling whatever this says.
    pub use_listen_notify: bool,

    /// Whether [`launch`](crate::DBOS::launch) brings the schema up to date.
    ///
    /// Off suits an application whose database is migrated by a deployment step instead.
    pub migrate: bool,

    /// How many polling reads may run at once.
    ///
    /// `None` is half the pool and at least one; `Some(0)` switches the cap off.
    pub polling_concurrency: Option<u32>,

    /// How often a caller waiting on a workflow it does not own asks whether it has finished.
    ///
    /// `None` is one second, the interval every implementation polls at. Workflow completion has
    /// no wakeup anywhere — no channel carries it and no trigger publishes it — so looking is the
    /// only way to learn a status changed.
    ///
    /// Zero is rejected rather than treated as "as fast as possible": it is a busy loop against
    /// the database, never something a caller means.
    pub outcome_poll_interval: Option<Duration>,

    /// Which queues this process dequeues from. `None` is all of them.
    ///
    /// **Configuration rather than a runtime call.** Go takes a replace-the-set API, Python a
    /// pre-launch call, TypeScript and Java configuration, and Rust follows the latter pair. The
    /// supervisor intersects this with what it reads from the `queues` table on every sweep, so
    /// the dynamic half — a queue registered later, or deleted — comes for free without a second
    /// way to change the set.
    ///
    /// What it is for: splitting one application's queues across differently-shaped processes.
    /// A worker fleet sized for slow media jobs and one sized for fast API calls share a database
    /// and a code base, and neither should drain the other's backlog.
    ///
    /// `Some(vec![])` listens to **nothing**, and is not the same as `None`. It is a real setting
    /// — a process that enqueues and never runs anything — and is the one place this differs from
    /// Go, whose empty set means "all". An empty *slice* is unambiguous in Rust where a nil-versus-
    /// empty distinction in Go is a trap, so the option carries the meaning instead.
    ///
    /// [`INTERNAL_QUEUE`](crate::sysdb::INTERNAL_QUEUE) is always dequeued from, whatever this
    /// says. It is where `resume`, `fork` and recovery put work, so filtering it out would strand
    /// them with no way to notice.
    pub listen_queues: Option<Vec<String>>,

    /// How long a written key waits for company before a wakeup is pushed for it.
    ///
    /// `None` is ten milliseconds, as in Python, TypeScript and Go. `Some(Duration::ZERO)` is
    /// meaningful and *not* rejected — it turns coalescing off, which is a push per write. That is
    /// the one duration here where zero is a setting rather than a mistake.
    pub notification_coalesce: Option<Duration>,
}

impl Config {
    /// A configuration with the defaults every implementation shares, for an application named
    /// `app_name`, reaching the database at `database_url`.
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
            polling_concurrency: None,
            outcome_poll_interval: None,
            listen_queues: None,
            notification_coalesce: None,
        }
    }

    /// [`Config::new`], taking the database URL from [`DATABASE_URL_ENV`].
    ///
    /// That variable and no other. The identity variables — `DBOS__APPVERSION`, `DBOS__VMID`,
    /// `DBOS__CLOUD` and `DBOS__APPID` — carry two underscores because they are DBOS Cloud's to
    /// set, and `DBOS_APP_NAME` is a deployment's too; an end user *may* set them, but a
    /// configuration is not the layer that reads them. Launch is, as in Java, so that a `Config`
    /// written by hand and one from here resolve to the same identity.
    ///
    /// A missing or empty `DBOS_DATABASE_URL` leaves [`database_url`](Self::database_url) empty
    /// rather than failing here, so that a caller who sets it afterwards is not forced through an
    /// error path. [`launch`](crate::DBOS::launch) is where an empty URL is reported, which is also
    /// where it would have failed anyway.
    pub fn from_env(app_name: impl Into<String>) -> Self {
        Self::new(
            app_name,
            std::env::var(DATABASE_URL_ENV).unwrap_or_default(),
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
                "no database URL: set `database_url`, or the {DATABASE_URL_ENV} environment \
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
        Ok(())
    }

    /// How often an adopting caller looks, resolved.
    pub(crate) fn outcome_poll_interval(&self) -> Duration {
        self.outcome_poll_interval
            .unwrap_or(DEFAULT_OUTCOME_POLL_INTERVAL)
    }
}

/// The interval every implementation polls a workflow's outcome at.
pub(crate) const DEFAULT_OUTCOME_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_serializer_names_itself_as_the_column_records_it() {
        assert_eq!(Serializer::default().name(), "rust_serde");
    }

    #[test]
    fn validation_reports_a_missing_url_by_the_name_of_the_variable_that_sets_it() {
        let err = Config::new("app", "").validate().unwrap_err();
        assert!(err.to_string().contains(DATABASE_URL_ENV), "{err}");
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
            "the interval every implementation polls at"
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
