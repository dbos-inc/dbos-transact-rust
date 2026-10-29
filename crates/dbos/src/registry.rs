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

use crate::error::{DurableError, EncodedError, EngineOnly, Failure};
use crate::serialization::{PayloadEncoding, SerializationFormat, decode_input, encode};
pub(crate) use crate::sysdb::types::WorkflowKey;
use crate::{DBOS, Error, Result};

/// A boxed future, spelled here rather than pulled from `futures` for one type alias.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A registered workflow with its types erased: encoded argument in, encoded outcome out.
///
/// That *is* an FFI shape, and deliberately so — a host language marshals strings and never
/// a Rust type. No context parameter appears in it, because the context is ambient. The error is
/// erased along with the value, which is what lets a caller keep a typed error while recovery,
/// holding only a row, keeps none.
///
/// The [`PayloadEncoding`] is the row's: how the input was encoded, and so how the outcome is
/// encoded back. It comes from the row rather than from the registration, so a workflow another
/// language enqueued portably runs portably here whatever this registration would have chosen.
pub(crate) type ErasedWorkflow = Arc<
    dyn Fn(
            Option<String>,
            PayloadEncoding,
        ) -> BoxFuture<'static, std::result::Result<Option<String>, Failure>>
        + Send
        + Sync,
>;

/// A registration as the registry keeps it: the erased body, and what it was registered with.
#[derive(Clone)]
pub(crate) struct RegisteredWorkflow {
    /// The body, with its types erased.
    pub(crate) body: ErasedWorkflow,
    /// How a start of this workflow encodes when the start does not choose, as
    /// [`WorkflowOptions::serialization`] said. Kept here for the starts that hold no
    /// [`WorkflowRef`] — a schedule's runs — so they encode as one that did would.
    pub(crate) serialization: SerializationFormat,
}

/// How a workflow is registered, beyond its name and its body.
///
/// Built by functional update from [`Default`], as [`Config`](crate::Config) is:
///
/// ```no_run
/// # use dbos::{SerializationFormat, WorkflowOptions};
/// let options = WorkflowOptions {
///     serialization: SerializationFormat::Portable,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkflowOptions {
    /// How a start of this workflow encodes its input, output and error, unless the start
    /// chooses otherwise.
    ///
    /// [`SerializationFormat::Portable`] for a workflow other languages start or await;
    /// [`SerializationFormat::Default`] takes the configured [`Serializer`](crate::Serializer), and
    /// [`SerializationFormat::Native`] is `rust_serde` whatever is configured. Whatever this says,
    /// a start another language enqueued portably runs portably: the row's tag decides how its
    /// input is read.
    pub serialization: SerializationFormat,
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
    workflows: HashMap<WorkflowKey, RegisteredWorkflow>,
    /// Set when a snapshot is taken, cleared when the executor it was taken for goes away.
    frozen: bool,
}

/// [`Error::InvalidArgument`] for a key nothing can be registered as: one with an empty name, or
/// with a config but no class, since a configured instance belongs to a class. Either can be
/// read from a row, which is why [`WorkflowKey`] holds them at all.
fn refuse_unregistrable(key: &WorkflowKey) -> Result<()> {
    let detail = if key.name().is_empty() {
        "a workflow needs a name"
    } else if key.config_name().is_some() && key.class_name().is_none() {
        "a configured instance needs a class"
    } else {
        return Ok(());
    };
    Err(Error::InvalidArgument {
        operation: "register a workflow".into(),
        detail: format!("{detail}, got `{key}`"),
    })
}

/// A registry frozen at launch.
pub(crate) type Snapshot = Arc<HashMap<WorkflowKey, RegisteredWorkflow>>;

impl Registry {
    /// Registers `workflow` under `key`, refusing a second registration of the same identity.
    ///
    /// Uniqueness is on the whole triple, which is Java's model and the strictest of the three:
    /// a name that resolves to two functions is a workflow that recovers as the wrong one.
    ///
    /// Refuses outright once frozen. The operation is named here rather than passed in because
    /// registration is the only thing that inserts.
    fn insert(&self, key: WorkflowKey, workflow: RegisteredWorkflow) -> Result<()> {
        let mut state = self.write();
        if state.frozen {
            return Err(Error::AlreadyLaunched {
                operation: "register_workflow".into(),
            });
        }
        match state.workflows.entry(key) {
            Entry::Occupied(taken) => Err(Error::AlreadyRegistered {
                key: taken.key().clone(),
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
    /// How a start writes this workflow unless it chooses otherwise.
    serialization: SerializationFormat,
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
        self.key.name()
    }

    /// The instance this workflow is registered with.
    pub(crate) fn dbos(&self) -> &DBOS {
        &self.dbos
    }

    /// How a start writes this workflow when it does not choose.
    pub(crate) fn serialization(&self) -> SerializationFormat {
        self.serialization
    }
}

/// Hand-written: the derive would demand `P: Clone, R: Clone`, which a reference that holds
/// neither has no business requiring.
impl<P, R, E> Clone for WorkflowRef<P, R, E> {
    fn clone(&self) -> Self {
        Self {
            dbos: self.dbos.clone(),
            key: Arc::clone(&self.key),
            serialization: self.serialization,
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
    /// Registers a workflow under `key`, returning a typed handle to it.
    ///
    /// The key is the workflow's identity: a name alone for a free function, a
    /// [`WorkflowKey::method`] for a class-bound one, or a [`WorkflowKey::instance`] for a
    /// configured instance. Its name, class and config are what a [`Client`](crate::Client)
    /// enqueues by and what dequeue resolves a row against.
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
        key: impl Into<WorkflowKey>,
        workflow: F,
    ) -> Result<WorkflowRef<P, R, E>>
    where
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        E: DurableError,
    {
        self.register_workflow_with(key, WorkflowOptions::default(), workflow)
    }

    /// [`register_workflow`](DBOS::register_workflow), with [`WorkflowOptions`].
    ///
    /// For a workflow other languages start or await, register it
    /// [portable](crate::SerializationFormat::Portable):
    ///
    /// ```no_run
    /// # use dbos::{DBOS, SerializationFormat, WorkflowOptions};
    /// # fn example(dbos: &DBOS) -> dbos::Result<()> {
    /// let greet = dbos.register_workflow_with(
    ///     "greet",
    ///     WorkflowOptions { serialization: SerializationFormat::Portable, ..Default::default() },
    ///     |name: String| async move { Ok::<_, dbos::Error>(format!("hello, {name}")) },
    /// )?;
    /// # Ok(()) }
    /// ```
    pub fn register_workflow_with<P, R, E, F, Fut>(
        &self,
        key: impl Into<WorkflowKey>,
        options: WorkflowOptions,
        workflow: F,
    ) -> Result<WorkflowRef<P, R, E>>
    where
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        E: DurableError,
    {
        let key = key.into();
        refuse_unregistrable(&key)?;
        let workflow = Arc::new(workflow);
        let erased: ErasedWorkflow =
            Arc::new(move |input: Option<String>, encoding: PayloadEncoding| {
                let workflow = Arc::clone(&workflow);
                Box::pin(async move {
                    let input = decode_input::<P, EngineOnly>(input.as_deref(), &encoding)
                        .map_err(Failure::Control)?;
                    match workflow(input).await {
                        // A plain value, in any encoding: only an input and an error have an
                        // envelope.
                        //
                        // A value the encoding refuses is the workflow's failure, not the
                        // engine's: the body ran to its end, side effects and all, so running it
                        // again is not a retry of anything that went wrong in the engine. The
                        // refusal is recorded as the workflow's error, and the row ends rather
                        // than staying for recovery to re-run.
                        Ok(output) => match encode::<R, E>(&output, &encoding, "result") {
                            Ok(encoded) => Ok(Some(encoded)),
                            Err(refused) => {
                                // A type the encoding cannot write is a bug in the workflow, and
                                // the recorded error may be read long after, by someone else.
                                tracing::warn!(
                                    error = %refused,
                                    "the workflow's result could not be encoded; it is recorded \
                                     as the workflow's error"
                                );
                                Err(Failure::Recorded(EncodedError::of(&refused, encoding)))
                            }
                        },
                        // Natively or in a custom serializer the whole `Error<E>` is what gets
                        // recorded, application variant and all, so the column is
                        // self-describing: a reader knows the envelope without having to guess
                        // whether the payload is one of ours or one of the workflow's. Portably it
                        // is the envelope every SDK reads. An error the encoding refuses is
                        // recorded as a refusal it can encode, for the same reason as a result.
                        Err(error) => Err(match error.control() {
                            Some(control) => Failure::Control(control),
                            None => Failure::Recorded(EncodedError::of(&error, encoding)),
                        }),
                    }
                })
            });

        self.registry().insert(
            key.clone(),
            RegisteredWorkflow {
                body: erased,
                serialization: options.serialization,
            },
        )?;
        tracing::debug!(workflow = %key, "registered a workflow");
        Ok(WorkflowRef {
            dbos: self.clone(),
            key: Arc::new(key),
            serialization: options.serialization,
            types: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::Config;
    use crate::sysdb::types::WorkflowRecord;

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
        let erased = &snapshot
            .get(&WorkflowKey::new("double"))
            .expect("registered")
            .body;
        assert_eq!(
            erased(Some("21".to_owned()), PayloadEncoding::Native)
                .await
                .unwrap(),
            Some("42".to_owned())
        );
    }

    #[tokio::test]
    async fn a_zero_argument_workflow_is_called_with_no_input_at_all() {
        let dbos = dbos();
        dbos.register_workflow("nothing", takes_nothing).unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = &snapshot
            .get(&WorkflowKey::new("nothing"))
            .expect("registered")
            .body;
        // The row for a zero-argument workflow has a NULL input; it must not need a `"null"`.
        assert_eq!(
            erased(None, PayloadEncoding::Native).await.unwrap(),
            Some("\"nothing\"".to_owned())
        );
    }

    #[tokio::test]
    async fn a_malformed_argument_is_reported_rather_than_panicking() {
        let dbos = dbos();
        dbos.register_workflow("double", takes_one).unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = &snapshot
            .get(&WorkflowKey::new("double"))
            .expect("registered")
            .body;
        let err = erased(Some("\"not a number\"".to_owned()), PayloadEncoding::Native)
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

    /// A result the encoding refuses is the workflow's own failure, recorded as the refusal, not a
    /// control signal that would leave the row for recovery to run again.
    #[tokio::test]
    async fn a_result_the_encoding_refuses_is_recorded_as_the_workflows_error() {
        let dbos = dbos();
        dbos.register_workflow("not_a_number", |()| async { Ok::<_, Error>(f64::NAN) })
            .unwrap();

        let snapshot = dbos.registry().snapshot();
        let erased = &snapshot
            .get(&WorkflowKey::new("not_a_number"))
            .expect("registered")
            .body;
        let err = erased(None, PayloadEncoding::Portable).await.unwrap_err();
        let Failure::Recorded(recorded) = err else {
            panic!("expected a recorded failure, got {err:?}")
        };
        let read = recorded.decode::<EngineOnly>("wf-1");
        assert!(
            matches!(&read, Error::Serialization { what, .. } if what == "result"),
            "{read:?}"
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

    /// An empty name, or a config with no class, names nothing a workflow could be registered as.
    #[test]
    fn a_key_naming_nothing_is_refused() {
        let dbos = dbos();
        for (key, part) in [
            (WorkflowKey::new(""), "needs a name"),
            (
                WorkflowKey::instance("unclassed", "", "eu"),
                "needs a class",
            ),
        ] {
            match dbos.register_workflow(key, takes_one) {
                Err(Error::InvalidArgument { detail, .. }) => {
                    assert!(detail.contains(part), "{detail}");
                }
                Err(other) => panic!("expected InvalidArgument, got {other}"),
                Ok(_) => panic!("registered a key naming nothing"),
            }
        }
        assert!(dbos.registry().snapshot().is_empty());
    }

    #[test]
    fn one_identity_registers_once() {
        let dbos = dbos();
        let key = WorkflowKey::instance("same", "Checkout", "eu");
        dbos.register_workflow(&key, takes_nothing).unwrap();
        let err = dbos.register_workflow(&key, takes_nothing).unwrap_err();
        match err {
            Error::AlreadyRegistered { key: taken } => assert_eq!(taken, key),
            other => panic!("expected AlreadyRegistered, got {other}"),
        }
    }

    #[test]
    fn a_name_and_an_instance_of_it_are_different_identities() {
        let dbos = dbos();
        dbos.register_workflow("shared", takes_nothing).unwrap();
        dbos.register_workflow(
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
    fn a_name_converts_to_a_free_functions_key() {
        let free = WorkflowKey::new("checkout");
        let name = "checkout".to_owned();
        assert_eq!(WorkflowKey::from("checkout"), free);
        assert_eq!(WorkflowKey::from(&name), free);
        assert_eq!(WorkflowKey::from(name), free);

        let instance = WorkflowKey::instance("checkout", "Checkout", "eu");
        assert_eq!(WorkflowKey::from(&instance), instance);
    }

    #[test]
    fn a_record_yields_its_key_and_a_nameless_record_none() {
        let record = |row: serde_json::Value| -> WorkflowRecord {
            let mut fields = serde_json::json!({ "workflow_id": "w", "status": "PENDING" });
            fields
                .as_object_mut()
                .unwrap()
                .extend(row.as_object().unwrap().clone());
            serde_json::from_value(fields).unwrap()
        };
        let instance = record(serde_json::json!({
            "name": "checkout", "class_name": "Checkout", "config_name": "eu",
        }));
        assert_eq!(
            instance.key(),
            Some(WorkflowKey::instance("checkout", "Checkout", "eu"))
        );
        let blank = record(serde_json::json!({
            "name": "checkout", "class_name": "", "config_name": "",
        }));
        assert_eq!(blank.key(), Some(WorkflowKey::new("checkout")));
        assert_eq!(record(serde_json::json!({})).key(), None);
        assert_eq!(record(serde_json::json!({ "name": "" })).key(), None);
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
