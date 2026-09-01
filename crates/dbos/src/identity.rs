//! Who this executor is: what the configuration says, and what the deployment says.
//!
//! One place resolves the application name, the application version and the executor id, because
//! the three answer to the same rule and the rule is not obvious. It is Java's
//! `DBOSExecutor` constructor, ported: the environment is read first, the configuration overrides
//! it, and on DBOS Cloud the override does not happen — the deployment outranks the build there,
//! since the application was built without knowing which deployment would run it.
//!
//! Resolution happens at launch rather than while a [`Config`] is being built, so that a
//! configuration written by hand and one from [`Config::from_env`] arrive at the same identity.
//! It is also the last moment where an unanswerable question is still cheap to report: nothing is
//! connected yet.
//!
//! The variables themselves are named here rather than in [`config`](crate::config), which reads
//! only `DBOS_DATABASE_URL`. The `DBOS__` spelling marks them as DBOS Cloud's to set — an end user
//! may, but the layer that reads them is this one.
//!
//! The one departure from Java is the version. Java hashes the application's code when nothing
//! supplies one; nothing here does, and a missing version is an error — see
//! [`Config::app_version`].

use crate::config::Config;
use crate::{Error, Result};

/// Environment variable carrying the application version.
///
/// Two underscores, matching Python, TypeScript, Go and Java — the odd spelling is a cross-SDK
/// constant, not a typo to tidy.
pub const APP_VERSION_ENV: &str = "DBOS__APPVERSION";

/// Environment variable saying this process is running on DBOS Cloud.
///
/// Read as Java reads it — `Boolean.parseBoolean`, so a case-insensitive `true` and nothing else.
/// It decides which side of every identity question wins: on DBOS Cloud the deployment is the
/// authority, and off it the application is.
pub const CLOUD_ENV: &str = "DBOS__CLOUD";

/// Environment variable carrying the DBOS Cloud application id.
///
/// Read only from the environment: the id is a deployment's, never an application's to choose, so
/// there is no configuration field beside it.
pub const APP_ID_ENV: &str = "DBOS__APPID";

/// Environment variable naming the application on DBOS Cloud.
///
/// One underscore, unlike the rest — the cross-SDK spelling again, not a typo. It is read only
/// when [`CLOUD_ENV`] says this is DBOS Cloud, where it replaces [`Config::app_name`].
pub const CLOUD_APP_NAME_ENV: &str = "DBOS_APP_NAME";

/// Environment variable identifying the VM this process runs on.
///
/// A deployment's way of naming an executor, and what [`Config::executor_id`] falls back to.
pub const EXECUTOR_ID_ENV: &str = "DBOS__VMID";

/// The default every implementation shares for a process nothing named.
const DEFAULT_EXECUTOR_ID: &str = "local";

/// What the environment says about this process.
///
/// Taken as a snapshot so that [`resolve`] is a pure function of a `Config` and this: the process
/// environment is global mutable state, and `std::env::set_var` is `unsafe` — which this crate
/// forbids — so a test that could only reach the rule through the real environment could not
/// exercise it at all.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Environment {
    /// `DBOS__CLOUD`, which decides who wins every question below.
    pub cloud: bool,
    /// `DBOS__APPID`, empty when unset.
    pub app_id: String,
    /// `DBOS_APP_NAME`, the application's name on DBOS Cloud.
    pub app_name: Option<String>,
    /// `DBOS__APPVERSION`.
    pub app_version: Option<String>,
    /// `DBOS__VMID`, a deployment's way of naming the VM this process runs on.
    pub executor_id: Option<String>,
}

impl Environment {
    /// Reads the environment this process was started with.
    pub(crate) fn read() -> Self {
        // An empty variable is an unset one throughout: a deployment that exports a name it has
        // not filled in yet means "nothing", and Java's later `isEmpty` checks reach the same
        // conclusion by a longer route.
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self {
            // `Boolean.parseBoolean` in Java: a case-insensitive `true`, and anything else —
            // `1`, `yes`, a typo — is false.
            cloud: std::env::var(CLOUD_ENV).is_ok_and(|v| v.eq_ignore_ascii_case("true")),
            app_id: var(APP_ID_ENV).unwrap_or_default(),
            app_name: var(CLOUD_APP_NAME_ENV),
            app_version: var(APP_VERSION_ENV),
            executor_id: var(EXECUTOR_ID_ENV),
        }
    }
}

/// The identity an executor runs under, once the configuration and the environment have been
/// reconciled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    pub app_name: String,
    pub app_version: String,
    pub executor_id: String,
    pub app_id: String,
}

/// Reconciles what the application was configured with against what the deployment says.
pub(crate) fn resolve(config: &Config, env: &Environment) -> Result<Identity> {
    // The environment first, then the configuration on top — except on DBOS Cloud, where the
    // deployment is the authority and the configuration is not consulted at all.
    let (app_name, app_version, executor_id) = if env.cloud {
        (
            env.app_name.clone().unwrap_or_default(),
            env.app_version.clone(),
            env.executor_id.clone(),
        )
    } else {
        (
            config.app_name.clone(),
            config
                .app_version
                .clone()
                .or_else(|| env.app_version.clone()),
            config
                .executor_id
                .clone()
                .or_else(|| env.executor_id.clone()),
        )
    };

    if app_name.is_empty() {
        return Err(Error::Config(if env.cloud {
            format!("{CLOUD_APP_NAME_ENV} must be set when {CLOUD_ENV} is true")
        } else {
            "`app_name` cannot be empty".to_owned()
        }));
    }
    validate_app_name(&app_name)?;

    // Where Java would hash the application's code.
    let Some(app_version) = app_version else {
        return Err(Error::Config(format!(
            "no application version: set `app_version`, or the {APP_VERSION_ENV} environment \
             variable. Nothing computes one — a Rust build is not reproducible, so a computed \
             version would change under a rebuild that changed no code and leave the previous \
             run's workflows unrecoverable"
        )));
    };

    Ok(Identity {
        app_name,
        app_version,
        executor_id: executor_id.unwrap_or_else(|| DEFAULT_EXECUTOR_ID.to_owned()),
        app_id: env.app_id.clone(),
    })
}

/// The rule the other implementations share: 3–256 characters of lowercase letters, digits, dashes
/// and underscores.
///
/// Checked rather than trusted because the name is an ownership key: a row stamped with a name no
/// other executor spells the same way is a row nothing claims.
fn validate_app_name(name: &str) -> Result<()> {
    let bad = |why: &str| Err(Error::Config(format!("`app_name` {why}: {name:?}")));
    match name.chars().count() {
        0 => return bad("cannot be empty"),
        1..=2 => return bad("must be at least 3 characters"),
        257.. => return bad("must be at most 256 characters"),
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

    fn config() -> Config {
        Config {
            app_version: Some("config-v1".to_owned()),
            executor_id: Some("config-executor".to_owned()),
            ..Config::new("config-app", "postgres://x")
        }
    }

    fn deployed() -> Environment {
        Environment {
            cloud: false,
            app_id: "app-id-7".to_owned(),
            app_name: Some("env-app".to_owned()),
            app_version: Some("env-v9".to_owned()),
            executor_id: Some("env-executor".to_owned()),
        }
    }

    #[test]
    fn the_configuration_outranks_the_environment_off_dbos_cloud() {
        let resolved = resolve(&config(), &deployed()).unwrap();
        assert_eq!(resolved.app_name, "config-app");
        assert_eq!(resolved.app_version, "config-v1");
        assert_eq!(resolved.executor_id, "config-executor");
        // Never a configuration field: the id belongs to the deployment either way.
        assert_eq!(resolved.app_id, "app-id-7");
    }

    #[test]
    fn the_environment_fills_in_what_the_configuration_leaves_unset() {
        let config = Config::new("config-app", "postgres://x");
        let resolved = resolve(&config, &deployed()).unwrap();
        assert_eq!(resolved.app_version, "env-v9");
        assert_eq!(resolved.executor_id, "env-executor");
        // `DBOS_APP_NAME` is not a fallback: off DBOS Cloud the application names itself.
        assert_eq!(resolved.app_name, "config-app");
    }

    #[test]
    fn dbos_cloud_outranks_the_configuration() {
        let env = Environment {
            cloud: true,
            ..deployed()
        };
        let resolved = resolve(&config(), &env).unwrap();
        assert_eq!(resolved.app_name, "env-app");
        assert_eq!(resolved.app_version, "env-v9");
        assert_eq!(resolved.executor_id, "env-executor");
    }

    #[test]
    fn an_unnamed_process_is_local() {
        let config = Config {
            executor_id: None,
            ..config()
        };
        let env = Environment {
            executor_id: None,
            ..Environment::default()
        };
        assert_eq!(resolve(&config, &env).unwrap().executor_id, "local");
    }

    #[test]
    fn a_version_nobody_supplied_is_an_error_naming_both_ways_to_supply_one() {
        let config = Config::new("config-app", "postgres://x");
        let err = resolve(&config, &Environment::default()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("app_version"), "{message}");
        assert!(message.contains(APP_VERSION_ENV), "{message}");
    }

    #[test]
    fn dbos_cloud_without_an_application_name_says_which_variable_is_missing() {
        let env = Environment {
            cloud: true,
            app_name: None,
            ..deployed()
        };
        let err = resolve(&config(), &env).unwrap_err();
        assert!(err.to_string().contains(CLOUD_APP_NAME_ENV), "{err}");
    }

    #[test]
    fn an_app_name_is_held_to_the_rule_every_implementation_shares() {
        for ok in ["abc", "my-app", "my_app_2", &"a".repeat(256)] {
            assert!(validate_app_name(ok).is_ok(), "{ok:?} should be accepted");
        }
        for bad in [
            "",
            "ab",
            &"a".repeat(257),
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
    fn the_resolved_name_is_the_one_that_is_checked() {
        // The name DBOS Cloud supplies is what every row is stamped with, so it is what the rule
        // applies to — and the configuration's own name is not, since nothing there reaches a row.
        let env = Environment {
            cloud: true,
            app_name: Some("Not A Name".to_owned()),
            ..deployed()
        };
        let err = resolve(&config(), &env).unwrap_err();
        assert!(err.to_string().contains("Not A Name"), "{err}");
    }
}
