//! How to reach and configure a [`DBOS`](crate::DBOS) instance.

use std::time::Duration;

use crate::sysdb::DEFAULT_SCHEMA;

/// Environment variable naming the system database.
///
/// The same name every implementation reads.
pub const DATABASE_URL_ENV: &str = "DBOS_DATABASE_URL";

/// Environment variable overriding the application version.
///
/// Two underscores, matching Python, TypeScript, Go and Java — the odd spelling is a cross-SDK
/// constant, not a typo to tidy.
pub const APP_VERSION_ENV: &str = "DBOS__APPVERSION";

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
/// This carries what the lifecycle uses. The rest of the configuration surface — patching, queues,
/// the scheduler, Conductor — arrives with the phase that reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Names this application among those sharing the system database.
    ///
    /// Load-bearing rather than cosmetic: it is the ownership key stamped on every row this
    /// instance writes, it mixes into the application version, and Conductor addresses an
    /// application by it. Three to thirty characters of lowercase letters, digits, dashes and
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
    /// `None` is `"local"`, matching Go and Python. Unlike them, no environment variable is read
    /// for it — `DBOS__VMID` is a deployment's way of naming a VM, and reading it here would make
    /// the value depend on where the process happens to run.
    pub executor_id: Option<String>,

    /// The version of the application's code.
    ///
    /// `None` computes it: the SHA-256 of the running executable, with [`app_name`](Self::app_name)
    /// mixed in so two applications shipping the same binary do not share a version.
    ///
    /// A workflow is only recovered by an executor running the version that started it, which is
    /// what makes a rolling deploy safe. The consequence in development is worth knowing: rebuild
    /// between a crash and a restart and the new binary hashes differently, so the previous run's
    /// `PENDING` rows are left for an executor that no longer exists. Set this — or
    /// `DBOS__APPVERSION`, which [`from_env`](Self::from_env) reads — to pin it while developing.
    pub application_version: Option<String>,

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

    /// How many recovered workflows may be claimed and run at once.
    ///
    /// `None` is [`max_connections`](Self::max_connections) and at least one; `Some(0)` switches
    /// the cap off.
    ///
    /// Recovery is the one submitter with no natural backpressure: a process that died holding ten
    /// thousand `PENDING` workflows would otherwise claim and spawn all ten thousand on the next
    /// launch, every one of them contending for a pool of ten. The pool size is the default
    /// because a recovered workflow that is making progress is one that will want a connection to
    /// checkpoint with, and queueing beyond that only moves the wait into the pool — where it is
    /// shared with the control plane rather than held here.
    ///
    /// The permit is taken before the row is claimed, not after, so an executor never re-stamps
    /// more workflows than it is prepared to run.
    pub recovery_concurrency: Option<usize>,

    /// How often a caller waiting on a workflow it does not own asks whether it has finished.
    ///
    /// `None` is one second, the interval every implementation polls at. Workflow completion has
    /// no wakeup anywhere — no channel carries it and no trigger publishes it — so looking is the
    /// only way to learn a status changed.
    ///
    /// Zero is rejected rather than treated as "as fast as possible": it is a busy loop against
    /// the database, never something a caller means.
    pub outcome_poll_interval: Option<Duration>,

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
    pub fn new(app_name: impl Into<String>, database_url: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            database_url: database_url.into(),
            max_connections: 10,
            schema: DEFAULT_SCHEMA.to_owned(),
            executor_id: None,
            application_version: None,
            serializer: Serializer::default(),
            use_listen_notify: true,
            migrate: true,
            polling_concurrency: None,
            recovery_concurrency: None,
            outcome_poll_interval: None,
            notification_coalesce: None,
        }
    }

    /// [`Config::new`], taking the database URL from `DBOS_DATABASE_URL` and the application
    /// version from `DBOS__APPVERSION`.
    ///
    /// A missing or empty `DBOS_DATABASE_URL` leaves [`database_url`](Self::database_url) empty
    /// rather than failing here, so that a caller who sets it afterwards is not forced through an
    /// error path. [`launch`](crate::DBOS::launch) is where an empty URL is reported, which is also
    /// where it would have failed anyway.
    pub fn from_env(app_name: impl Into<String>) -> Self {
        let url = std::env::var(DATABASE_URL_ENV).unwrap_or_default();
        let version = std::env::var(APP_VERSION_ENV)
            .ok()
            .filter(|v| !v.is_empty());
        Self {
            application_version: version,
            ..Self::new(app_name, url)
        }
    }

    /// Checks what can be checked before anything is connected.
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.database_url.is_empty() {
            return Err(crate::Error::Config(format!(
                "no database URL: set `database_url`, or the {DATABASE_URL_ENV} environment \
                 variable if the configuration came from `Config::from_env`"
            )));
        }
        validate_app_name(&self.app_name)?;
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

    /// How many recovered workflows may run at once, resolved.
    ///
    /// The shape `polling_concurrency` already uses: `Some(0)` is uncapped, and the default is
    /// derived from the pool rather than fixed.
    pub(crate) fn recovery_limit(&self) -> usize {
        match self.recovery_concurrency {
            Some(0) => tokio::sync::Semaphore::MAX_PERMITS,
            Some(n) => n,
            None => usize::max(self.max_connections as usize, 1),
        }
    }

    /// How often an adopting caller looks, resolved.
    pub(crate) fn outcome_poll_interval(&self) -> Duration {
        self.outcome_poll_interval
            .unwrap_or(DEFAULT_OUTCOME_POLL_INTERVAL)
    }
}

/// The interval every implementation polls a workflow's outcome at.
const DEFAULT_OUTCOME_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The rule the other implementations share: 3–30 characters of lowercase letters, digits, dashes
/// and underscores.
///
/// Checked rather than trusted because the name is an ownership key: a row stamped with a name no
/// other executor spells the same way is a row nothing claims.
fn validate_app_name(name: &str) -> crate::Result<()> {
    let bad = |why: &str| Err(crate::Error::Config(format!("`app_name` {why}: {name:?}")));
    match name.chars().count() {
        0 => return bad("cannot be empty"),
        1..=2 => return bad("must be at least 3 characters"),
        31.. => return bad("must be at most 30 characters"),
        _ => {}
    }
    if let Some(c) = name
        .chars()
        .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '-' | '_'))
    {
        return bad(&format!(
            "may contain only lowercase letters, digits, dashes and underscores, but has {c:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_serializer_names_itself_as_the_column_records_it() {
        assert_eq!(Serializer::default().name(), "rust_serde");
    }

    #[test]
    fn an_app_name_is_held_to_the_rule_every_implementation_shares() {
        for ok in ["abc", "my-app", "my_app_2", &"a".repeat(30)] {
            assert!(validate_app_name(ok).is_ok(), "{ok:?} should be accepted");
        }
        for bad in [
            "",
            "ab",
            &"a".repeat(31),
            "My-App",
            "my app",
            "my.app",
            "café",
        ] {
            assert!(
                validate_app_name(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn validation_reports_a_missing_url_by_the_name_of_the_variable_that_sets_it() {
        let err = Config::new("app", "").validate().unwrap_err();
        assert!(err.to_string().contains(DATABASE_URL_ENV), "{err}");
    }

    #[test]
    fn the_recovery_cap_defaults_to_the_pool_and_zero_switches_it_off() {
        let config = |recovery_concurrency| Config {
            max_connections: 8,
            recovery_concurrency,
            ..Config::new("app", "postgres://x")
        };
        assert_eq!(config(None).recovery_limit(), 8, "the pool by default");
        assert_eq!(config(Some(3)).recovery_limit(), 3);
        assert_eq!(
            config(Some(0)).recovery_limit(),
            tokio::sync::Semaphore::MAX_PERMITS,
            "zero is uncapped, as it is for `polling_concurrency`"
        );
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
