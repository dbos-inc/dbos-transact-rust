//! The seam the closed-source enterprise crate plugs into.
//!
//! **`dbos` never depends on the enterprise crate.** The dependency would be a cycle, which Cargo
//! rejects, and it would carry closed-licence code into an MIT crate. The application depends on
//! both and registers the enterprise crate on its configuration instead:
//!
//! ```ignore
//! let config = dbos::Config {
//!     enterprise: Some(dbos_enterprise::enterprise()),
//!     conductor_key: Some(key),
//!     ..dbos::Config::from_env("my-app")
//! };
//! ```
//!
//! That is Rust's counterpart to Go's blank import, Java's `ServiceLoader` and Python's import of
//! `dbos_enterprise`. It is explicit rather than link-time registration (`inventory`, `linkme`)
//! because LTO and static linking can drop a registration nothing names, and a Conductor connection
//! that silently never happens is worse than an error at launch.
//!
//! **The contract is [`__enterprise`](crate::__enterprise), and it carries no semver promise.**
//! It is public because another crate has to see it, and hidden because applications have no use
//! for it. The enterprise crate pins `dbos = "=0.7.N"`, so it is always built against the exact
//! version whose contract it was written for, and that is what lets the contract change in a patch
//! release.

use std::sync::Arc;

use crate::client::Client;
use crate::config::Config;
use crate::connection::Connection;
use crate::identity::ConductorTarget;
use crate::sysdb::SystemDatabase;
use crate::{Error, Result};

/// The crate an application adds for the enterprise features, as a launch error names it.
const ENTERPRISE_CRATE: &str = "dbos-enterprise";

/// An enterprise crate, registered on [`Config::enterprise`](crate::Config::enterprise).
///
/// The enterprise crate builds this; an application only passes it along:
///
/// ```ignore
/// let config = dbos::Config {
///     enterprise: Some(dbos_enterprise::enterprise()),
///     ..dbos::Config::from_env("my-app")
/// };
/// ```
///
/// Two registrations are equal only if one is a clone of the other, which is all that comparing
/// two configurations needs.
#[derive(Clone)]
pub struct Enterprise(Arc<dyn EnterpriseProvider>);

impl Enterprise {
    /// Wraps the enterprise crate's implementation of the contract. Called by that crate, not by
    /// applications.
    #[doc(hidden)]
    pub fn new(provider: impl EnterpriseProvider) -> Self {
        Self(Arc::new(provider))
    }

    /// What the registered crate provides.
    pub(crate) fn provider(&self) -> &dyn EnterpriseProvider {
        &*self.0
    }
}

impl PartialEq for Enterprise {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Enterprise {}

impl std::fmt::Debug for Enterprise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Enterprise").finish_non_exhaustive()
    }
}

/// What an enterprise crate provides: one factory per feature.
///
/// Only the Conductor client so far. Auto-upgrade, suspension, external storage and the sharded
/// system database join it as they are ported.
pub trait EnterpriseProvider: Send + Sync + 'static {
    /// Builds the Conductor client, without connecting.
    ///
    /// Called during launch once the system database is connected and migrated, and only when
    /// Conductor is configured. It must neither connect nor spawn: launch can still fail after
    /// it, and a failed launch then has only [`ConductorConnection::stop`] to call.
    /// [`ConductorConnection::start`] is where the connection begins.
    ///
    /// An error fails the launch.
    fn conductor(
        &self,
        engine: ConductorEngine,
        options: ConductorOptions,
    ) -> Result<Box<dyn ConductorConnection>>;
}

/// The Conductor client's link to a running executor.
///
/// Launch calls [`start`](Self::start) as its last act, once the executor is installed and polling
/// its queues, so a recovery Conductor asks for runs at once. Shutdown calls
/// [`stop`](Self::stop) after running workflows have drained, so the executor stays visible to
/// Conductor for as long as it runs workflows, and before the system database closes, since the
/// client reads through it.
///
/// A launch that fails after the client was built calls `stop` without having called `start`.
#[async_trait::async_trait]
pub trait ConductorConnection: Send + 'static {
    /// Starts connecting, on tasks spawned through [`dbos::__rt`](crate::__rt), and returns.
    ///
    /// Reconnecting after a dropped connection is the client's business, so there is nothing to
    /// report here: Conductor being unreachable does not stop an application.
    fn start(&mut self);

    /// Closes the connection and waits for the client's tasks to end.
    async fn stop(&mut self);
}

/// The Conductor settings a launch resolved, for the client to connect with.
#[derive(Clone)]
pub struct ConductorOptions {
    /// The websocket URL, ending in the protocol version (`/conductor/v1alpha1`). The client adds
    /// the application name and the key.
    pub url: String,
    /// The key the client authenticates with. Never printed.
    pub key: String,
    /// The application name Conductor files this executor under.
    pub app_name: String,
    /// What the executor reports about itself in `executor_info`.
    pub executor_metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Whether to send Conductor workflow metadata only, never workflow data.
    pub metadata_only_mode: bool,
}

impl std::fmt::Debug for ConductorOptions {
    /// Hand-written so that the key is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            url,
            key,
            app_name,
            executor_metadata,
            metadata_only_mode,
        } = self;
        f.debug_struct("ConductorOptions")
            .field("url", url)
            .field("key", &crate::identity::redacted(Some(key)))
            .field("app_name", app_name)
            .field("executor_metadata", executor_metadata)
            .field("metadata_only_mode", metadata_only_mode)
            .finish()
    }
}

/// The running executor, as the Conductor client sees it.
///
/// It holds the executor's connection and identity rather than the executor itself. The executor
/// owns the Conductor connection, which owns this, so holding the executor would make a cycle.
pub struct ConductorEngine {
    conn: Arc<Connection>,
    executor_id: String,
    app_name: String,
    app_version: String,
}

impl ConductorEngine {
    pub(crate) fn new(
        conn: &Arc<Connection>,
        executor_id: &str,
        app_name: &str,
        app_version: &str,
    ) -> Self {
        Self {
            conn: Arc::clone(conn),
            executor_id: executor_id.to_owned(),
            app_name: app_name.to_owned(),
            app_version: app_version.to_owned(),
        }
    }

    /// A client over this executor's own connection, for the management operations Conductor
    /// asks for.
    ///
    /// It shares the executor's pool, so its [`Client::close`] does nothing: the pool closes when
    /// the executor shuts down.
    pub fn client(&self) -> Client {
        Client::sharing(Arc::clone(&self.conn))
    }

    /// The system database, for the reads and writes no public operation covers.
    pub fn sysdb(&self) -> &dyn SystemDatabase {
        self.conn.sysdb()
    }

    /// This executor's id, as Conductor knows it.
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// The application name rows are stamped with.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// The application version this executor runs.
    pub fn app_version(&self) -> &str {
        &self.app_version
    }

    /// This `dbos` crate's version, which `executor_info` reports.
    pub fn dbos_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// Returns the given executors' abandoned workflows on this application version to their
    /// queues, and the ids of the workflows it moved.
    ///
    /// What Conductor's `recovery` message asks for. Launch does the same for its own executor
    /// when no Conductor is configured.
    pub async fn recover(&self, executor_ids: &[&str]) -> Result<Vec<String>> {
        crate::recovery::reenqueue(self.conn.sysdb(), executor_ids, &self.app_version).await
    }
}

impl std::fmt::Debug for ConductorEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConductorEngine")
            .field("executor_id", &self.executor_id)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .finish_non_exhaustive()
    }
}

/// What launch connects to Conductor with, given where identity resolution said to connect.
///
/// Fails when Conductor is configured and no enterprise crate is registered, naming the crate and
/// the line that registers it. Launch calls this before connecting to the system database, so a
/// missing registration costs nothing to find out.
pub(crate) fn conductor_plan(
    config: &Config,
    target: Option<ConductorTarget>,
) -> Result<Option<(Enterprise, ConductorOptions)>> {
    let Some(ConductorTarget { url, key, app_name }) = target else {
        return Ok(None);
    };
    let Some(enterprise) = &config.enterprise else {
        return Err(Error::Config(format!(
            "connecting to DBOS Conductor needs the {ENTERPRISE_CRATE} crate: add it to \
             Cargo.toml and register it with `enterprise: Some({}::enterprise())` in the Config",
            ENTERPRISE_CRATE.replace('-', "_"),
        )));
    };
    Ok(Some((
        enterprise.clone(),
        ConductorOptions {
            url,
            key,
            app_name,
            executor_metadata: config.conductor_executor_metadata.clone(),
            metadata_only_mode: config.conductor_metadata_only_mode,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Nothing;

    impl EnterpriseProvider for Nothing {
        fn conductor(
            &self,
            _: ConductorEngine,
            _: ConductorOptions,
        ) -> Result<Box<dyn ConductorConnection>> {
            Err(crate::Error::Config("no Conductor here".to_owned()))
        }
    }

    #[test]
    fn a_registration_equals_its_clones_and_nothing_else() {
        let registered = Enterprise::new(Nothing);
        assert_eq!(registered, registered.clone());
        assert_ne!(registered, Enterprise::new(Nothing));
    }

    fn target() -> ConductorTarget {
        ConductorTarget {
            url: "wss://conductor.example/conductor/v1alpha1".to_owned(),
            key: "key".to_owned(),
            app_name: "app".to_owned(),
        }
    }

    #[test]
    fn without_conductor_nothing_is_needed() {
        let config = Config::new("app", "postgres://localhost/app");
        assert!(conductor_plan(&config, None).unwrap().is_none());
    }

    #[test]
    fn conductor_without_a_registration_names_the_crate_and_the_line() {
        let config = Config::new("app", "postgres://localhost/app");
        let err = conductor_plan(&config, Some(target())).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        let message = err.to_string();
        assert!(message.contains("the dbos-enterprise crate"), "{message}");
        assert!(
            message.contains("enterprise: Some(dbos_enterprise::enterprise())"),
            "{message}"
        );
    }

    #[test]
    fn conductor_with_a_registration_carries_the_target_and_the_config() {
        let mut metadata = serde_json::Map::new();
        metadata.insert("region".to_owned(), "eu".into());
        let enterprise = Enterprise::new(Nothing);
        let config = Config {
            enterprise: Some(enterprise.clone()),
            conductor_executor_metadata: Some(metadata.clone()),
            conductor_metadata_only_mode: true,
            ..Config::new("app", "postgres://localhost/app")
        };
        let (registered, options) = conductor_plan(&config, Some(target())).unwrap().unwrap();
        assert_eq!(registered, enterprise);
        assert_eq!(options.url, "wss://conductor.example/conductor/v1alpha1");
        assert_eq!(options.key, "key");
        assert_eq!(options.app_name, "app");
        assert_eq!(options.executor_metadata, Some(metadata));
        assert!(options.metadata_only_mode);
    }

    #[test]
    fn conductor_options_never_print_the_key() {
        let options = ConductorOptions {
            url: "wss://cloud.dbos.dev/conductor/v1alpha1".to_owned(),
            key: "s3cret-key".to_owned(),
            app_name: "app".to_owned(),
            executor_metadata: None,
            metadata_only_mode: false,
        };
        let printed = format!("{options:?}");
        assert!(!printed.contains("s3cret-key"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }
}
