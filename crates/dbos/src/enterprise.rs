//! The seam the closed-source enterprise crate plugs into.
//!
//! `dbos` can't depend on that crate: it would be a cycle, and closed-licence code in an MIT
//! crate. The application registers it on [`Config::enterprise`](crate::Config::enterprise)
//! instead. Registration is explicit rather than link-time (`inventory`, `linkme`), which LTO can
//! drop silently.
//!
//! The contract is [`__enterprise`](crate::__enterprise), with no semver promise: the enterprise
//! crate pins `dbos = "=0.7.N"`.

use std::sync::Arc;

use crate::config::Config;
use crate::connection::Connection;
use crate::identity::ConductorTarget;
use crate::sysdb::SystemDatabase;
use crate::{Error, Result};

/// The enterprise crate's name, for the launch error.
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
/// Equal only to its own clones.
#[derive(Clone)]
pub struct Enterprise(Arc<dyn EnterpriseProvider>);

impl Enterprise {
    /// Called by the enterprise crate, not by applications.
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

/// What an enterprise crate provides, one factory per feature. Only Conductor so far.
pub trait EnterpriseProvider: Send + Sync + 'static {
    /// Builds the Conductor client without connecting or spawning, which is
    /// [`ConductorConnection::start`]'s job. Called during launch, after the system database is
    /// migrated. An error fails the launch.
    fn conductor(
        &self,
        engine: ConductorEngine,
        options: ConductorOptions,
    ) -> Result<Box<dyn ConductorConnection>>;
}

/// The Conductor client, as launch and shutdown drive it.
///
/// Launch calls [`start`](Self::start) last, once the executor is polling its queues. Shutdown
/// calls [`stop`](Self::stop) after the drain and before the system database closes, and waits
/// at most five seconds.
#[async_trait::async_trait]
pub trait ConductorConnection: Send + 'static {
    /// Starts connecting, on tasks spawned through [`dbos::__rt`](crate::__rt). Reconnecting is
    /// the client's job: an unreachable Conductor doesn't stop the application.
    fn start(&mut self);

    /// Closes the connection and waits for the client's tasks to end.
    async fn stop(&mut self);
}

/// The Conductor settings a launch resolved, for the client to connect with.
#[derive(Clone)]
pub struct ConductorOptions {
    /// The websocket URL, ending in `/conductor/v1alpha1`.
    pub url: String,
    /// The key the client authenticates with.
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
/// Holds the executor's connection, not the executor: the executor owns the client, so that would
/// be a cycle.
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

    /// The executor's system database.
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

    /// Re-enqueues the named executors' `PENDING` workflows on this application version, returning
    /// their ids. What Conductor's `recovery` message asks for.
    ///
    /// **Only for executors that are gone.** Each matching workflow loses its owner and goes back
    /// to a queue, so naming a running executor, this one included, hands work it is still running
    /// to another runner.
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

/// The enterprise crate and options launch connects to Conductor with, if `target` is set. Fails
/// when no enterprise crate is registered.
pub(crate) fn resolve_conductor(
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
        assert!(resolve_conductor(&config, None).unwrap().is_none());
    }

    #[test]
    fn conductor_without_a_registration_names_the_crate_and_the_line() {
        let config = Config::new("app", "postgres://localhost/app");
        let err = resolve_conductor(&config, Some(target())).unwrap_err();
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
        let (registered, options) = resolve_conductor(&config, Some(target())).unwrap().unwrap();
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
