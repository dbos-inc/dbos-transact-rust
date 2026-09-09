//! Workflow identity, type erasure, and the typed handle registration hands back.
//!
//! Registration turns a typed `async fn` into a JSON-in/JSON-out closure the executor can call
//! without knowing its types, which is what recovery and queue dequeue need — both start a
//! workflow from a database row, where the argument is a string and the function is a name.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{DurableError, EngineOnly, Failure};
use crate::serialization::{decode, encode};
use crate::{DBOS, Error, Result};

/// A boxed future, spelled here rather than pulled from `futures` for one type alias.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A registered workflow with its types erased: encoded argument in, encoded outcome out.
///
/// That *is* an FFI shape, and deliberately so — a host language marshals strings and never
/// a Rust type. No context parameter appears in it, because the context is ambient. The error is
/// erased along with the value, which is what lets a caller keep a typed error while recovery,
/// holding only a row, keeps none.
pub(crate) type ErasedWorkflow = Arc<
    dyn Fn(Option<String>) -> BoxFuture<'static, std::result::Result<Option<String>, Failure>>
        + Send
        + Sync,
>;

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
    /// compares an init against an existing row (`WorkflowDAO.java`), which is a defence nobody
    /// writes against a distinction that cannot occur.
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
    state: RwLock<State>,
}

/// The map and whether it may still change, under one lock.
///
/// **One lock, deliberately.** "Are we launched?" and "what is registered?" have to be answered by
/// the same critical section or the answer to the first can go stale before the second is used: a
/// registration that read "not launched" and then inserted after [`snapshot`](Registry::snapshot)
/// had already run would be accepted, handed back a [`WorkflowRef`], and left out of the map the
/// executor actually runs — which surfaces much later as a workflow that starts, fails to resolve,
/// and sits `PENDING` until it parks. Reading the executor slot in [`DBOS`] cannot close that
/// window, because the slot is not what registration races.
#[derive(Default)]
struct State {
    workflows: HashMap<WorkflowKey, ErasedWorkflow>,
    /// Set when a snapshot is taken, cleared when the executor it was taken for goes away.
    frozen: bool,
}

/// A registry frozen at launch.
pub(crate) type Snapshot = Arc<HashMap<WorkflowKey, ErasedWorkflow>>;

impl Registry {
    /// Registers `workflow` under `key`, refusing a second registration of the same identity.
    ///
    /// Uniqueness is on the whole triple, which is Java's model and the strictest of the three:
    /// a name that resolves to two functions is a workflow that recovers as the wrong one.
    ///
    /// Refuses outright once frozen. The operation is named here rather than passed in because
    /// registration is the only thing that inserts.
    fn insert(&self, key: WorkflowKey, workflow: ErasedWorkflow) -> Result<()> {
        let mut state = self.write();
        if state.frozen {
            return Err(Error::AlreadyLaunched {
                operation: "register_workflow".into(),
            });
        }
        match state.workflows.entry(key) {
            Entry::Occupied(taken) => Err(Error::AlreadyRegistered {
                key: taken.key().to_string(),
            }),
            Entry::Vacant(slot) => {
                slot.insert(workflow);
                Ok(())
            }
        }
    }

    /// Freezes the registry and hands back what it holds, for an executor to keep.
    ///
    /// Taking the copy and closing the door are one act, which is the whole point of the shared
    /// lock: between them there is no moment for a registration to slip through.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let mut state = self.write();
        state.frozen = true;
        Arc::new(state.workflows.clone())
    }

    /// Reopens the registry, for an instance with no executor holding a snapshot.
    ///
    /// Called when a launch fails and when one is shut down — both under the lifecycle lock, so
    /// this cannot race the [`snapshot`](Self::snapshot) it undoes. Without the first of those a
    /// failed launch would leave the instance permanently unregistrable, which is a worse failure
    /// than the one that caused it.
    pub(crate) fn thaw(&self) {
        self.write().frozen = false;
    }

    /// A poisoned lock here cannot mean torn state: every section under it is infallible, so a
    /// panic elsewhere leaves the map and the flag whole.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A registered workflow, with its argument and result types kept.
///
/// This is what registration returns and what a call site holds. The registry stores only the
/// erased form; the types live here, so a caller gets them checked once at registration rather
/// than at every invocation by string.
pub struct WorkflowRef<P, R, E = EngineOnly> {
    dbos: DBOS,
    key: Arc<WorkflowKey>,
    /// `E` is the *application's* error type, the one inside [`Error::Application`], so a workflow
    /// that only fails in DBOS's own terms leaves it at [`EngineOnly`].
    ///
    /// `fn(P) -> (R, E)` rather than a tuple: it makes this `Send`, `Sync` and `Unpin` whatever the
    /// parameters are, since the reference does not hold any of them — it only names them.
    types: PhantomData<fn(P) -> (R, E)>,
}

impl<P, R, E> WorkflowRef<P, R, E> {
    /// The identity this workflow was registered under.
    pub fn key(&self) -> &WorkflowKey {
        &self.key
    }

    /// The workflow's name.
    pub fn name(&self) -> &str {
        &self.key.name
    }

    /// The instance this workflow is registered with.
    pub(crate) fn dbos(&self) -> &DBOS {
        &self.dbos
    }
}

/// Hand-written: the derive would demand `P: Clone, R: Clone`, which a reference that holds
/// neither has no business requiring.
impl<P, R, E> Clone for WorkflowRef<P, R, E> {
    fn clone(&self) -> Self {
        Self {
            dbos: self.dbos.clone(),
            key: Arc::clone(&self.key),
            types: PhantomData,
        }
    }
}

impl<P, R, E> std::fmt::Debug for WorkflowRef<P, R, E> {
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
    /// A workflow takes exactly one argument and never a context. One that needs nothing takes
    /// `_: ()`; one that needs several takes a struct or a tuple.
    ///
    /// **Do not capture the [`DBOS`] instance in `workflow`.** The registry lives on the instance,
    /// so the closure is stored inside the very `Arc` a captured handle points at — a cycle, and
    /// the instance, its executor and its connection pool are then never freed. Nothing a workflow
    /// body needs requires one: [`step`](crate::step()), [`set_event`](crate::set_event) and
    /// [`get_event`](crate::get_event) all read the ambient context.
    ///
    /// A [`WorkflowRef`] holds an instance too, so capturing one — to start a child workflow —
    /// has the same effect. That is a known gap rather than a rule anyone can follow around:
    /// until starting a child reads the ambient context the way a step does, such an application
    /// leaks one instance, which for a process that launches once is a bounded cost.
    pub fn register_workflow<P, R, E, F, Fut>(
        &self,
        name: &str,
        workflow: F,
    ) -> Result<WorkflowRef<P, R, E>>
    where
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        E: DurableError,
    {
        self.register_workflow_as(WorkflowKey::new(name), workflow)
    }

    /// [`register_workflow`](DBOS::register_workflow) under a full identity triple.
    pub fn register_workflow_as<P, R, E, F, Fut>(
        &self,
        key: WorkflowKey,
        workflow: F,
    ) -> Result<WorkflowRef<P, R, E>>
    where
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        E: DurableError,
    {
        let workflow = Arc::new(workflow);
        let erased: ErasedWorkflow = Arc::new(move |input: Option<String>| {
            let workflow = Arc::clone(&workflow);
            Box::pin(async move {
                let input = decode::<P, EngineOnly>(input.as_deref(), "argument")
                    .map_err(Failure::Control)?;
                match workflow(input).await {
                    Ok(output) => encode(&output, "result")
                        .map(Some)
                        .map_err(Failure::Control),
                    // The whole `Error<E>` is what gets recorded, application variant and all, so
                    // the column is self-describing: a reader knows the envelope without having to
                    // guess whether the payload is one of ours or one of the workflow's.
                    Err(error) => Err(match error.control() {
                        Some(control) => Failure::Control(control),
                        None => {
                            Failure::Recorded(encode(&error, "error").map_err(Failure::Control)?)
                        }
                    }),
                }
            })
        });

        self.registry().insert(key.clone(), erased)?;
        tracing::debug!(workflow = %key, "registered a workflow");
        Ok(WorkflowRef {
            dbos: self.clone(),
            key: Arc::new(key),
            types: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::Config;

    fn dbos() -> DBOS {
        DBOS::new(Config::new("registry-test", "postgres://unused"))
    }

    async fn takes_nothing(_: ()) -> Result<String> {
        Ok("nothing".to_owned())
    }

    async fn takes_one(n: u32) -> Result<u32> {
        Ok(n * 2)
    }

    #[test]
    fn registration_infers_the_argument_and_result_types() {
        let dbos = dbos();
        let nothing = dbos
            .register_workflow("takes_nothing", takes_nothing)
            .unwrap();
        let one = dbos.register_workflow("takes_one", takes_one).unwrap();
        assert_eq!(nothing.name(), "takes_nothing");
        assert_eq!(one.name(), "takes_one");
        // Closures too.
        dbos.register_workflow(
            "closure",
            |s: String| async move { Ok::<_, Error>(s.len()) },
        )
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
                Failure::Control(Error::Deserialization {
                    what: Cow::Borrowed("argument"),
                    ..
                })
            ),
            "{err:?}"
        );
    }

    /// The window the shared lock closes: once a snapshot exists, nothing may be added to the map
    /// it was taken from, because the executor holding it would never see the addition.
    #[test]
    fn a_snapshot_closes_the_registry_and_releasing_it_reopens() {
        let dbos = dbos();
        dbos.register_workflow("before", takes_nothing).unwrap();

        let snapshot = dbos.registry().snapshot();
        assert_eq!(snapshot.len(), 1);

        let err = dbos.register_workflow("after", takes_nothing).unwrap_err();
        assert!(
            matches!(
                err,
                Error::AlreadyLaunched {
                    operation: Cow::Borrowed("register_workflow")
                }
            ),
            "{err}"
        );
        assert_eq!(
            dbos.registry().snapshot().len(),
            1,
            "the refused registration is not in the map either"
        );

        dbos.registry().thaw();
        dbos.register_workflow("after", takes_nothing)
            .expect("releasing the snapshot reopens registration");
        assert_eq!(dbos.registry().snapshot().len(), 2);
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
        // Some SDKs spell absence `""` where Python writes NULL, and both must find this
        // registration.
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
