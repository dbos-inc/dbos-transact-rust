//! Workflow identity, type erasure, and the typed handle registration hands back.
//!
//! Registration turns a typed `async fn` into a JSON-in/JSON-out closure the executor can call
//! without knowing its types, which is what recovery and queue dequeue need — both start a
//! workflow from a database row, where the argument is a string and the function is a name.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{DBOS, Error, Result};

/// A boxed future, spelled here rather than pulled from `futures` for one type alias.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A registered workflow with its types erased: encoded argument in, encoded result out.
///
/// That *is* an FFI shape, and deliberately so (§9.2) — a host language marshals strings and never
/// a Rust type. No context parameter appears in it, because the context is ambient.
pub(crate) type ErasedWorkflow =
    Arc<dyn Fn(Option<String>) -> BoxFuture<'static, Result<Option<String>>> + Send + Sync>;

/// What identifies a workflow, as the `workflow_status` row stores it.
///
/// Three columns rather than one name, because that is what Conductor and Console resolve against
/// and what a configured instance needs. The references model it three different ways — Python
/// keys on the name alone with instances in a separate map, Go on a derived FQN qualified by the
/// config name, Java on the whole `{name}/{class}/{instance}` string — but all three *store* these
/// three columns, so keying on them directly is the shape that reads back from any of them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkflowKey {
    /// The workflow's name. Required.
    pub name: String,
    /// The class a method-workflow belongs to; `None` for a free function.
    pub class_name: Option<String>,
    /// The configured instance this registration is bound to; `None` unless instance-bound.
    pub config_name: Option<String>,
}

impl WorkflowKey {
    /// A free function's key.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            class_name: None,
            config_name: None,
        }
    }

    /// Rebuilds a key from a status row, treating `NULL` and `""` alike.
    ///
    /// Recovery is not why. A workflow belongs to one language from start to finish, so a row this
    /// executor recovers was written by this executor's own SDK and spells absence the one way
    /// Rust spells it. The boundary where a row arrives from *elsewhere* is **enqueue**: an
    /// application in another language can enqueue work for this one, and it writes the status row
    /// in its own spelling before any Rust executor sees it.
    ///
    /// Both spellings are in the wild, and Java's own code is the evidence rather than an
    /// assumption of ours — `WorkflowDAO` normalizes `null` and `""` to the same thing when it
    /// compares an init against an existing row (`WorkflowDAO.java:120`), which is a defence
    /// nobody writes against a distinction that cannot occur.
    #[allow(
        dead_code,
        reason = "recovery reads rows; that is the next commit but one"
    )]
    pub(crate) fn from_row(
        name: impl Into<String>,
        class_name: Option<&str>,
        config_name: Option<&str>,
    ) -> Self {
        let absent = |v: Option<&str>| v.filter(|s| !s.is_empty()).map(str::to_owned);
        Self {
            name: name.into(),
            class_name: absent(class_name),
            config_name: absent(config_name),
        }
    }

    /// A configured instance's key.
    ///
    /// A config name never appears without a class name — true in every reference, and enforced
    /// here rather than left to produce a key nothing can resolve.
    pub fn instance(
        name: impl Into<String>,
        class_name: impl Into<String>,
        config_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            class_name: Some(class_name.into()),
            config_name: Some(config_name.into()),
        }
    }
}

impl std::fmt::Display for WorkflowKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.class_name, &self.config_name) {
            (Some(class), Some(config)) => write!(f, "{}/{class}/{config}", self.name),
            (Some(class), None) => write!(f, "{}/{class}", self.name),
            _ => f.write_str(&self.name),
        }
    }
}

/// Every workflow this instance knows about.
///
/// Held by the instance, not the executor: registration happens before launch, and `launch` takes
/// a [`snapshot`](Self::snapshot) the executor keeps for its lifetime. Recovery reading a registry
/// that could still change underneath it is the thing the snapshot rules out.
#[derive(Default)]
pub(crate) struct Registry {
    workflows: RwLock<HashMap<WorkflowKey, ErasedWorkflow>>,
}

/// A registry frozen at launch.
pub(crate) type Snapshot = Arc<HashMap<WorkflowKey, ErasedWorkflow>>;

impl Registry {
    /// Registers `workflow` under `key`, refusing a second registration of the same identity.
    ///
    /// Uniqueness is on the whole triple, which is Java's model and the strictest of the three:
    /// a name that resolves to two functions is a workflow that recovers as the wrong one.
    fn insert(&self, key: WorkflowKey, workflow: ErasedWorkflow) -> Result<()> {
        let mut workflows = self
            .workflows
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if workflows.contains_key(&key) {
            return Err(Error::AlreadyRegistered {
                key: key.to_string(),
            });
        }
        workflows.insert(key, workflow);
        Ok(())
    }

    /// Freezes the registry for an executor to hold.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let workflows = self
            .workflows
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::new(workflows.clone())
    }
}

/// A workflow function this crate can register.
///
/// Implemented for `async fn()` and `async fn(P)`, and nothing else. `Marker` distinguishes the two
/// impls for the coherence checker, which cannot see that a type is never both `Fn()` and `Fn(P)`;
/// it is inferred at every call site and never written down. This is axum's `Handler` trick, used
/// for two rungs rather than sixteen — axum needs a ladder because each of its parameters is an
/// independently extracted value, whereas a workflow's arguments all collapse into the one
/// serialized payload the database stores.
pub trait WorkflowFn<P, R, Marker>: Send + Sync + 'static {
    /// Calls the workflow.
    fn call(&self, input: P) -> BoxFuture<'static, Result<R>>;
}

/// Marker for `async fn() -> Result<R>`.
#[doc(hidden)]
pub struct NoArgs;
/// Marker for `async fn(P) -> Result<R>`.
#[doc(hidden)]
pub struct OneArg;

impl<F, Fut, R> WorkflowFn<(), R, NoArgs> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    fn call(&self, (): ()) -> BoxFuture<'static, Result<R>> {
        Box::pin(self())
    }
}

impl<F, Fut, P, R> WorkflowFn<P, R, OneArg> for F
where
    F: Fn(P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    fn call(&self, input: P) -> BoxFuture<'static, Result<R>> {
        Box::pin(self(input))
    }
}

/// A registered workflow, with its argument and result types kept.
///
/// This is what registration returns and what a call site holds. The registry stores only the
/// erased form; the types live here, so a caller gets them checked once at registration rather
/// than at every invocation by string.
pub struct WorkflowRef<P, R> {
    dbos: DBOS,
    key: Arc<WorkflowKey>,
    /// `fn(P) -> R` rather than `(P, R)`: it makes this `Send`, `Sync` and `Unpin` whatever `P`
    /// and `R` are, since the reference does not hold either — it only names them.
    types: PhantomData<fn(P) -> R>,
}

impl<P, R> WorkflowRef<P, R> {
    /// The identity this workflow was registered under.
    pub fn key(&self) -> &WorkflowKey {
        &self.key
    }

    /// The workflow's name.
    pub fn name(&self) -> &str {
        &self.key.name
    }

    /// The instance this workflow is registered with.
    #[allow(dead_code, reason = "read by `run` and `start`")]
    pub(crate) fn dbos(&self) -> &DBOS {
        &self.dbos
    }
}

/// Hand-written: the derive would demand `P: Clone, R: Clone`, which a reference that holds
/// neither has no business requiring.
impl<P, R> Clone for WorkflowRef<P, R> {
    fn clone(&self) -> Self {
        Self {
            dbos: self.dbos.clone(),
            key: Arc::clone(&self.key),
            types: PhantomData,
        }
    }
}

impl<P, R> std::fmt::Debug for WorkflowRef<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowRef")
            .field("key", &self.key.to_string())
            .finish_non_exhaustive()
    }
}

impl DBOS {
    /// Registers a workflow, returning a typed handle to it.
    ///
    /// Only before [`launch`](DBOS::launch): the executor holds a snapshot of the registry, so a
    /// registration afterwards would be invisible to recovery and to dequeue — which is worse than
    /// an error, because the workflow would appear to work until the process restarted.
    ///
    /// The function takes its own argument, or none. It never takes a context.
    pub fn register_workflow<P, R, M, F>(
        &self,
        name: &str,
        workflow: F,
    ) -> Result<WorkflowRef<P, R>>
    where
        F: WorkflowFn<P, R, M>,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        self.register_workflow_as(WorkflowKey::new(name), workflow)
    }

    /// [`register_workflow`](DBOS::register_workflow) under a full identity triple.
    pub fn register_workflow_as<P, R, M, F>(
        &self,
        key: WorkflowKey,
        workflow: F,
    ) -> Result<WorkflowRef<P, R>>
    where
        F: WorkflowFn<P, R, M>,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.is_launched() {
            return Err(Error::AlreadyLaunched {
                operation: "register_workflow",
            });
        }

        let workflow = Arc::new(workflow);
        let erased: ErasedWorkflow = Arc::new(move |input: Option<String>| {
            let workflow = Arc::clone(&workflow);
            Box::pin(async move {
                let input = decode::<P>(input.as_deref())?;
                let output = workflow.call(input).await?;
                encode(&output)
            })
        });

        self.registry().insert(key.clone(), erased)?;
        Ok(WorkflowRef {
            dbos: self.clone(),
            key: Arc::new(key),
            types: PhantomData,
        })
    }
}

/// Decodes a workflow argument.
///
/// An absent argument reads as JSON `null`, which is what a zero-argument workflow's `()` decodes
/// from — so the two arities share one erased signature rather than needing two.
fn decode<P: DeserializeOwned>(input: Option<&str>) -> Result<P> {
    serde_json::from_str(input.unwrap_or("null")).map_err(|source| Error::Deserialization {
        what: "argument",
        source,
    })
}

/// Encodes a workflow result.
fn encode<R: Serialize>(output: &R) -> Result<Option<String>> {
    serde_json::to_string(output)
        .map(Some)
        .map_err(|source| Error::Serialization {
            what: "result",
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn dbos() -> DBOS {
        DBOS::new(Config::new("registry-test", "postgres://unused"))
    }

    async fn takes_nothing() -> Result<String> {
        Ok("nothing".to_owned())
    }

    async fn takes_one(n: u32) -> Result<u32> {
        Ok(n * 2)
    }

    #[test]
    fn both_arities_register_without_a_turbofish() {
        let dbos = dbos();
        let nothing = dbos
            .register_workflow("takes_nothing", takes_nothing)
            .unwrap();
        let one = dbos.register_workflow("takes_one", takes_one).unwrap();
        assert_eq!(nothing.name(), "takes_nothing");
        assert_eq!(one.name(), "takes_one");
        // Closures too, which is what a macro will generate into.
        dbos.register_workflow("closure", |s: String| async move { Ok(s.len()) })
            .unwrap();
    }

    #[tokio::test]
    async fn an_erased_workflow_round_trips_through_json() {
        let dbos = dbos();
        dbos.register_workflow("double", takes_one).unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = snapshot
            .get(&WorkflowKey::new("double"))
            .expect("registered");
        assert_eq!(
            erased(Some("21".to_owned())).await.unwrap(),
            Some("42".to_owned())
        );
    }

    #[tokio::test]
    async fn a_zero_argument_workflow_is_called_with_no_input_at_all() {
        let dbos = dbos();
        dbos.register_workflow("nothing", takes_nothing).unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = snapshot
            .get(&WorkflowKey::new("nothing"))
            .expect("registered");
        // The row for a zero-argument workflow has a NULL input; it must not need a `"null"`.
        assert_eq!(erased(None).await.unwrap(), Some("\"nothing\"".to_owned()));
    }

    #[tokio::test]
    async fn a_malformed_argument_is_reported_rather_than_panicking() {
        let dbos = dbos();
        dbos.register_workflow("double", takes_one).unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = snapshot
            .get(&WorkflowKey::new("double"))
            .expect("registered");
        let err = erased(Some("\"not a number\"".to_owned()))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Deserialization {
                    what: "argument",
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn one_identity_registers_once() {
        let dbos = dbos();
        dbos.register_workflow("same", takes_nothing).unwrap();
        let err = dbos.register_workflow("same", takes_nothing).unwrap_err();
        assert!(matches!(err, Error::AlreadyRegistered { .. }), "{err}");
    }

    #[test]
    fn a_name_and_an_instance_of_it_are_different_identities() {
        let dbos = dbos();
        dbos.register_workflow("shared", takes_nothing).unwrap();
        dbos.register_workflow_as(
            WorkflowKey::instance("shared", "Checkout", "eu"),
            takes_nothing,
        )
        .unwrap();
        assert_eq!(dbos.registry().snapshot().len(), 2);
    }

    #[test]
    fn a_row_written_with_empty_strings_resolves_to_the_same_key_as_one_written_with_nulls() {
        // Java writes `""` where Python writes NULL, and both must find this registration.
        let free = WorkflowKey::new("checkout");
        assert_eq!(WorkflowKey::from_row("checkout", None, None), free);
        assert_eq!(WorkflowKey::from_row("checkout", Some(""), Some("")), free);
    }

    #[test]
    fn a_key_displays_as_the_triple_the_references_spell() {
        assert_eq!(WorkflowKey::new("checkout").to_string(), "checkout");
        assert_eq!(
            WorkflowKey::instance("checkout", "Checkout", "eu").to_string(),
            "checkout/Checkout/eu"
        );
    }
}
