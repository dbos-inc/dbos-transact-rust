//! Who this executor is: what the configuration says, and what the deployment says.
//!
//! One place resolves the application name, the application version and the executor id, because
//! the three answer to the same rule and the rule is not obvious: the environment is read first,
//! the configuration overrides it, and on DBOS Cloud the override does not happen — the deployment
//! outranks the build there, since the application was built without knowing which deployment would
//! run it.
//!
//! Resolution happens at launch rather than while a [`Config`] is being built, so that a
//! configuration written by hand and one from [`Config::from_env`] arrive at the same identity.
//! It is also the last moment where an unanswerable question is still cheap to report: nothing is
//! connected yet.
//!
//! The variables themselves are named here rather than in [`config`](crate::config), which reads
//! only `DBOS_SYSTEM_DATABASE_URL`. The `DBOS__` spelling marks them as DBOS Cloud's to set — an
//! end user may, but the layer that reads them is this one.
//!
//! Nothing computes a version when none is supplied; a missing version is an error — see
//! [`Config::app_version`].

use crate::config::Config;
use crate::{Error, Result};

/// Environment variable carrying the application version.
///
/// Two underscores — the odd spelling is a constant shared by every DBOS SDK, not a typo to tidy.
pub const APP_VERSION_ENV: &str = "DBOS__APPVERSION";

/// Environment variable saying this process is running on DBOS Cloud.
///
/// A case-insensitive `true`, and nothing else.
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

/// The executor id for a process nothing named, shared with the other SDKs.
const DEFAULT_EXECUTOR_ID: &str = "local";

/// Environment variable naming the application to DBOS Conductor on DBOS Cloud.
///
/// One of three DBOS Cloud sets together; Conductor is used there only when all three are.
const CLOUD_CONDUCTOR_APP_NAME_ENV: &str = "DBOS__CONDUCTOR_APP_NAME";

/// Environment variable carrying the Conductor key on DBOS Cloud.
const CLOUD_CONDUCTOR_KEY_ENV: &str = "DBOS__CONDUCTOR_KEY";

/// Environment variable carrying the Conductor URL on DBOS Cloud.
const CLOUD_CONDUCTOR_URL_ENV: &str = "DBOS__CONDUCTOR_URL";

/// Environment variable naming the domain of DBOS's hosted Conductor, for an application that
/// gives a Conductor key and no URL.
const DBOS_DOMAIN_ENV: &str = "DBOS_DOMAIN";

/// The domain of DBOS's hosted Conductor when [`DBOS_DOMAIN_ENV`] names none.
const DEFAULT_DBOS_DOMAIN: &str = "cloud.dbos.dev";

/// The hosted Conductor's websocket URL on `domain`.
pub(crate) fn hosted_conductor_url(domain: &str) -> String {
    format!("wss://{domain}/conductor/v1alpha1")
}

/// What the environment says about this process.
///
/// Taken as a snapshot so that [`resolve`] is a pure function of a `Config` and this: the process
/// environment is global mutable state, and `std::env::set_var` is `unsafe` — which this crate
/// forbids — so a test that could only reach the rule through the real environment could not
/// exercise it at all.
#[derive(Default, Clone, PartialEq, Eq)]
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
    /// `DBOS__CONDUCTOR_APP_NAME`, the name DBOS Cloud registers the application under in
    /// Conductor.
    pub conductor_app_name: Option<String>,
    /// `DBOS__CONDUCTOR_KEY`.
    pub conductor_key: Option<String>,
    /// `DBOS__CONDUCTOR_URL`.
    pub conductor_url: Option<String>,
    /// `DBOS_DOMAIN`, the hosted Conductor's domain.
    pub dbos_domain: Option<String>,
}

impl std::fmt::Debug for Environment {
    /// Hand-written so that DBOS Cloud's Conductor key is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            cloud,
            app_id,
            app_name,
            app_version,
            executor_id,
            conductor_app_name,
            conductor_key,
            conductor_url,
            dbos_domain,
        } = self;
        f.debug_struct("Environment")
            .field("cloud", cloud)
            .field("app_id", app_id)
            .field("app_name", app_name)
            .field("app_version", app_version)
            .field("executor_id", executor_id)
            .field("conductor_app_name", conductor_app_name)
            .field("conductor_key", &redacted(conductor_key.as_deref()))
            .field("conductor_url", conductor_url)
            .field("dbos_domain", dbos_domain)
            .finish()
    }
}

impl Environment {
    /// Reads the environment this process was started with.
    pub(crate) fn read() -> Self {
        // An empty variable is an unset one throughout: a deployment that exports a name it has
        // not filled in yet means "nothing", so an empty `DBOS__VMID` falls back to the default.
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self {
            // A case-insensitive `true`, and anything else — `1`, `yes`, a typo — is false.
            cloud: std::env::var(CLOUD_ENV).is_ok_and(|v| v.eq_ignore_ascii_case("true")),
            app_id: var(APP_ID_ENV).unwrap_or_default(),
            app_name: var(CLOUD_APP_NAME_ENV),
            app_version: var(APP_VERSION_ENV),
            executor_id: var(EXECUTOR_ID_ENV),
            conductor_app_name: var(CLOUD_CONDUCTOR_APP_NAME_ENV),
            conductor_key: var(CLOUD_CONDUCTOR_KEY_ENV),
            conductor_url: var(CLOUD_CONDUCTOR_URL_ENV),
            dbos_domain: var(DBOS_DOMAIN_ENV),
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
    /// The Conductor this executor connects to, or `None` to connect to none.
    pub conductor: Option<ConductorTarget>,
    /// Whether launch recovers this executor's own abandoned workflows. Not on DBOS Cloud or with
    /// a Conductor key, where Conductor recovers the executors it sees go.
    pub self_recovery: bool,
    /// An executor id the configuration or the environment named, replaced because a Conductor key
    /// is set. Reported so that launch can say so: the id an operator chose is not the one rows are
    /// stamped with.
    pub discarded_executor_id: Option<String>,
}

/// Where and as whom an executor connects to DBOS Conductor.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConductorTarget {
    /// The websocket URL.
    pub url: String,
    /// The Conductor key, a credential.
    pub key: String,
    /// The name Conductor addresses the application by.
    pub app_name: String,
}

impl std::fmt::Debug for ConductorTarget {
    /// Hand-written so that the key is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { url, key, app_name } = self;
        f.debug_struct("ConductorTarget")
            .field("url", url)
            .field("key", &redacted(Some(key)))
            .field("app_name", app_name)
            .finish()
    }
}

/// A configured string without its surrounding whitespace, unless nothing is left, which
/// configures nothing.
///
/// Trimmed because these values come from environment variables and secret files, where a
/// trailing newline is easy to pick up and, in a key, fails authentication with no hint why.
pub(crate) fn non_blank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

/// How a `Debug` impl prints a secret: whether it is set, by the rule [`non_blank`] applies, and
/// nothing more.
pub(crate) fn redacted(secret: Option<&str>) -> &'static str {
    if non_blank(secret).is_some() {
        "<redacted>"
    } else {
        "<unset>"
    }
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

    let Some(app_version) = app_version else {
        return Err(Error::Config(format!(
            "no application version: set `app_version`, or the {APP_VERSION_ENV} environment \
             variable. Nothing computes one — a Rust build is not reproducible, so a computed \
             version would change under a rebuild that changed no code and leave the previous \
             run's workflows unrecoverable"
        )));
    };

    // On DBOS Cloud the deployment says whether there is a Conductor, through three variables that
    // mean nothing apart. Off it, a Conductor key is what connects, and the configuration's own
    // key and URL are the only ones read.
    let conductor_key = non_blank(config.conductor_key.as_deref());
    let conductor = if env.cloud {
        match (
            non_blank(env.conductor_app_name.as_deref()),
            non_blank(env.conductor_key.as_deref()),
            non_blank(env.conductor_url.as_deref()),
        ) {
            (Some(app_name), Some(key), Some(url)) => Some(ConductorTarget {
                url: url.to_owned(),
                key: key.to_owned(),
                app_name: app_name.to_owned(),
            }),
            _ => None,
        }
    } else {
        conductor_key.map(|key| ConductorTarget {
            url: non_blank(config.conductor_url.as_deref()).map_or_else(
                || {
                    hosted_conductor_url(
                        non_blank(env.dbos_domain.as_deref()).unwrap_or(DEFAULT_DBOS_DOMAIN),
                    )
                },
                str::to_owned,
            ),
            key: key.to_owned(),
            app_name: app_name.clone(),
        })
    };

    // Conductor tells executors apart by id, and an id that outlives the process would let a new
    // executor answer for the old one's workflows. So off DBOS Cloud, where nothing guarantees an
    // id names one process, a Conductor key means a fresh id per launch. On DBOS Cloud the id is
    // the VM's, which is what Conductor expects there.
    let (executor_id, discarded_executor_id) = if !env.cloud && conductor_key.is_some() {
        (uuid::Uuid::new_v4().to_string(), executor_id)
    } else {
        (
            executor_id.unwrap_or_else(|| DEFAULT_EXECUTOR_ID.to_owned()),
            None,
        )
    };

    Ok(Identity {
        app_name,
        app_version,
        executor_id,
        app_id: env.app_id.clone(),
        conductor,
        self_recovery: !(env.cloud || conductor_key.is_some()),
        discarded_executor_id,
    })
}

/// The rule shared with the other SDKs: 3–256 characters of lowercase letters, digits, dashes and
/// underscores.
///
/// Checked rather than trusted because the name is an ownership key: a row stamped with a name no
/// other executor spells the same way is a row nothing claims.
pub(crate) fn validate_app_name(name: &str) -> Result<()> {
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

/// Refuses a caller's `app_name` no app could have, as [`Error::InvalidArgument`] of `operation`
/// naming `app_name`, before the name reaches a column.
///
/// For a name an enqueue or a debounce writes on a workflow it hands to another application: a
/// row stamped with a name no executor spells is one nothing dequeues.
pub(crate) fn refuse_app_name(operation: &'static str, app_name: Option<&str>) -> Result<()> {
    match app_name.map(validate_app_name) {
        Some(Err(Error::Config(detail))) => Err(Error::InvalidArgument {
            operation: operation.into(),
            detail,
        }),
        Some(Err(other)) => Err(other),
        Some(Ok(())) | None => Ok(()),
    }
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
            ..Environment::default()
        }
    }

    /// DBOS Cloud, with all three of its Conductor variables set.
    fn cloud_with_conductor() -> Environment {
        Environment {
            cloud: true,
            conductor_app_name: Some("cloud-conductor-app".to_owned()),
            conductor_key: Some("cloud-key".to_owned()),
            conductor_url: Some("wss://conductor.example/v1alpha1".to_owned()),
            ..deployed()
        }
    }

    fn with_key(config: Config) -> Config {
        Config {
            conductor_key: Some("config-key".to_owned()),
            ..config
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
    fn an_app_name_is_held_to_the_shared_rule() {
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

    #[test]
    fn without_a_conductor_key_nothing_connects_and_launch_recovers() {
        let config = Config {
            // A URL alone configures nothing: the key is what connects.
            conductor_url: Some("wss://conductor.example/v1alpha1".to_owned()),
            ..config()
        };
        let resolved = resolve(&config, &deployed()).unwrap();
        assert_eq!(resolved.conductor, None);
        assert!(resolved.self_recovery);
        assert_eq!(resolved.executor_id, "config-executor");
        assert_eq!(resolved.discarded_executor_id, None);
    }

    #[test]
    fn a_conductor_key_connects_to_the_hosted_conductor_under_the_application_name() {
        let resolved = resolve(&with_key(config()), &deployed()).unwrap();
        let conductor = resolved.conductor.expect("a key connects");
        assert_eq!(conductor.url, "wss://cloud.dbos.dev/conductor/v1alpha1");
        assert_eq!(conductor.key, "config-key");
        assert_eq!(conductor.app_name, "config-app");
        assert!(!resolved.self_recovery, "Conductor drives recovery");
    }

    #[test]
    fn dbos_domain_moves_the_hosted_conductor_and_a_configured_url_replaces_it() {
        let env = Environment {
            dbos_domain: Some("staging.dbos.dev".to_owned()),
            ..deployed()
        };
        let resolved = resolve(&with_key(config()), &env).unwrap();
        assert_eq!(
            resolved.conductor.unwrap().url,
            "wss://staging.dbos.dev/conductor/v1alpha1"
        );

        let config = Config {
            conductor_url: Some("ws://localhost:8090/".to_owned()),
            ..with_key(config())
        };
        let resolved = resolve(&config, &env).unwrap();
        assert_eq!(resolved.conductor.unwrap().url, "ws://localhost:8090/");
    }

    #[test]
    fn a_conductor_key_replaces_the_executor_id_with_a_fresh_uuid_per_launch() {
        let first = resolve(&with_key(config()), &deployed()).unwrap();
        let second = resolve(&with_key(config()), &deployed()).unwrap();
        for resolved in [&first, &second] {
            uuid::Uuid::parse_str(&resolved.executor_id).expect("a UUID");
            assert_eq!(
                resolved.discarded_executor_id.as_deref(),
                Some("config-executor"),
                "the configured id is reported, so launch can warn"
            );
        }
        assert_ne!(first.executor_id, second.executor_id);
    }

    #[test]
    fn replacing_an_id_nobody_chose_reports_nothing() {
        let config = Config {
            executor_id: None,
            ..with_key(config())
        };
        let env = Environment {
            executor_id: None,
            ..deployed()
        };
        let resolved = resolve(&config, &env).unwrap();
        uuid::Uuid::parse_str(&resolved.executor_id).expect("a UUID");
        assert_eq!(resolved.discarded_executor_id, None);

        // `DBOS__VMID` off DBOS Cloud is a choice too, and is reported like the configuration's.
        let config = Config {
            executor_id: None,
            ..with_key(Config::new("config-app", "postgres://x"))
        };
        let resolved = resolve(&config, &deployed()).unwrap();
        assert_eq!(
            resolved.discarded_executor_id.as_deref(),
            Some("env-executor")
        );
    }

    #[test]
    fn a_blank_conductor_key_or_url_configures_nothing() {
        let config = Config {
            conductor_key: Some("  ".to_owned()),
            ..config()
        };
        let resolved = resolve(&config, &deployed()).unwrap();
        assert_eq!(resolved.conductor, None);
        assert!(resolved.self_recovery);
        assert_eq!(resolved.executor_id, "config-executor");

        let config = Config {
            conductor_url: Some(String::new()),
            ..with_key(config)
        };
        let resolved = resolve(&config, &deployed()).unwrap();
        assert_eq!(
            resolved.conductor.unwrap().url,
            "wss://cloud.dbos.dev/conductor/v1alpha1"
        );
    }

    #[test]
    fn dbos_cloud_connects_with_its_own_three_variables_and_keeps_the_vm_id() {
        // The configuration's key is not consulted on DBOS Cloud.
        let resolved = resolve(&with_key(config()), &cloud_with_conductor()).unwrap();
        let conductor = resolved.conductor.expect("all three are set");
        assert_eq!(conductor.url, "wss://conductor.example/v1alpha1");
        assert_eq!(conductor.key, "cloud-key");
        assert_eq!(conductor.app_name, "cloud-conductor-app");
        assert_eq!(resolved.executor_id, "env-executor");
        assert_eq!(resolved.discarded_executor_id, None);
        assert!(!resolved.self_recovery);
    }

    #[test]
    fn dbos_cloud_without_all_three_conductor_variables_connects_to_none() {
        let partial = [
            Environment {
                conductor_url: None,
                ..cloud_with_conductor()
            },
            Environment {
                conductor_key: Some(" ".to_owned()),
                ..cloud_with_conductor()
            },
            Environment {
                cloud: true,
                ..deployed()
            },
        ];
        for env in partial {
            let resolved = resolve(&with_key(config()), &env).unwrap();
            assert_eq!(resolved.conductor, None, "{env:?}");
            assert_eq!(resolved.executor_id, "env-executor");
            assert!(!resolved.self_recovery, "DBOS Cloud never self-recovers");
        }
    }

    #[test]
    fn a_conductor_key_and_url_lose_their_surrounding_whitespace() {
        let config = Config {
            conductor_key: Some("config-key\n".to_owned()),
            conductor_url: Some(" wss://conductor.example/conductor/v1alpha1 ".to_owned()),
            ..config()
        };
        let target = resolve(&config, &deployed()).unwrap().conductor.unwrap();
        assert_eq!(target.key, "config-key");
        assert_eq!(target.url, "wss://conductor.example/conductor/v1alpha1");
    }

    #[test]
    fn the_environment_never_prints_the_cloud_conductor_key() {
        let printed = format!("{:?}", cloud_with_conductor());
        assert!(!printed.contains("cloud-key"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }

    #[test]
    fn a_blank_secret_prints_as_unset() {
        assert_eq!(redacted(Some(" \n")), "<unset>");
        assert_eq!(redacted(None), "<unset>");
        assert_eq!(redacted(Some("key")), "<redacted>");
    }

    #[test]
    fn a_conductor_target_never_prints_its_key() {
        let resolved = resolve(&with_key(config()), &deployed()).unwrap();
        let printed = format!("{resolved:?}");
        assert!(!printed.contains("config-key"), "{printed}");
        assert!(printed.contains("cloud.dbos.dev"), "{printed}");
    }
}
