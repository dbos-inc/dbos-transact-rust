//! Enqueueing a workflow by name: one this process need not have registered.
//!
//! The way to hand work to code that lives elsewhere — another service, or another language,
//! sharing the system database. The workflow is named by its [`QualifiedWorkflowName`] rather than
//! started through a [`WorkflowRef`](crate::WorkflowRef), recorded `ENQUEUED`, and run by whichever
//! executor registered that name and next polls the queue.
//!
//! Both surfaces offer it, with the same [`EnqueueOptions`]. [`Client::enqueue`] is a
//! connection's: it records against no parent, so a replayed workflow body that calls it enqueues
//! again. [`DBOS::enqueue`] is an application's: from inside a workflow the enqueued workflow is
//! its child, recorded at the parent's step, and a replay hands back the same handle.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::checkpoint::{PendingStep, StepPlacement};
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::serialization::{PayloadEncoding, PortableWorkflowArgs, encode, encode_input};
use crate::sysdb::types::{NewWorkflow, Timestamp, WorkflowIdReusePolicy};
use crate::workflow::{
    Debounced, ENQUEUE_OPERATION, Enqueue, OwnedEnqueue, OwnedStart, PendingStart, RowVersion,
    Submitted, Timeout, create, encode_attributes, init_or_join, new_row, placed_workflow_id,
    refused_start, validate_start,
};
use crate::{Client, DBOS, Executor, QualifiedWorkflowName};

/// What an enqueue by name may say, beyond the input: [`Client::enqueue_with`]'s, and
/// [`DBOS::enqueue_with`]'s.
///
/// [`queue`](Self::queue) is not optional, which is the difference from
/// [`StartOptions`](crate::StartOptions): an enqueue by name runs nothing itself, so every one
/// names a queue. That is also why there is no [`Default`] — a default would have to invent a queue
/// name — and why the shape is [`EnqueueOptions::new`] plus functional update:
///
/// ```no_run
/// # use dbos::{Enqueue, EnqueueOptions};
/// let options = EnqueueOptions {
///     workflow_id: Some("order-42"),
///     ..EnqueueOptions::new("orders")
/// };
/// ```
///
/// Asking the queue for something is [`EnqueueOptions::on`], which takes the whole [`Enqueue`]
/// rather than leaving the `queue` field to be overwritten afterwards — the queue is named once,
/// where a `..EnqueueOptions::new(..)` base under a `queue:` field would name it twice and honour
/// only the second:
///
/// ```no_run
/// # use dbos::{Enqueue, EnqueueOptions};
/// let options = EnqueueOptions {
///     workflow_id: Some("order-42"),
///     ..EnqueueOptions::on(Enqueue {
///         priority: Some(1),
///         ..Enqueue::new("orders")
///     })
/// };
/// ```
///
/// The queue-shaped options — deduplication, priority, partition, delay — live on [`Enqueue`],
/// where the runtime's enqueue keeps them too, so the two surfaces do not spell the same four
/// things differently.
#[derive(Debug, Clone)]
pub struct EnqueueOptions<'a> {
    /// The queue to leave the workflow on, and what to ask of it.
    pub queue: Enqueue<'a>,

    /// The workflow's id, in place of a generated one.
    ///
    /// An idempotency key, as everywhere else: enqueueing the same id twice records one workflow,
    /// and the second call hands back a handle to it rather than failing, unless
    /// [`workflow_id_reuse_policy`](Self::workflow_id_reuse_policy) says to refuse. The natural use
    /// from a client is a request id, so a retried HTTP request enqueues one workflow.
    pub workflow_id: Option<&'a str>,

    /// What the enqueue does when the id already names another caller's workflow: join it (the
    /// default), or refuse with [`WorkflowIdInUse`](crate::sysdb::Error::WorkflowIdInUse).
    /// [`Reject`](WorkflowIdReusePolicy::Reject) needs a [`workflow_id`](Self::workflow_id), and
    /// is refused as [`Error::InvalidArgument`] without one.
    pub workflow_id_reuse_policy: WorkflowIdReusePolicy,

    /// The application this workflow belongs to, in place of the caller's own.
    ///
    /// For a tool that hands work to an application it is not — the name decides which
    /// application's executors may dequeue it. Left unset, [`DBOS::enqueue_with`] writes the
    /// workflow for this application, and [`Client::enqueue_with`] for the client's
    /// [`ClientConfig::app_name`](crate::ClientConfig::app_name); with both unset, a client writes an unclaimed workflow, which
    /// any application may run.
    pub app_name: Option<&'a str>,

    /// The version of the application's code that must run this workflow.
    ///
    /// **`None` records no version, which is the right default for a client**, and is not quite the
    /// same as *any* version: an unversioned row is dequeued by an executor running the **latest
    /// registered** version, and by no other. That is what keeps a rolling deploy from handing new
    /// work to the code being replaced, and it is the same rule an application's own unversioned
    /// rows follow.
    ///
    /// The case worth knowing is a rollback. A version's timestamp is what makes it the latest, and
    /// re-registering one that already exists does not move it — so redeploying an earlier version
    /// leaves the version that was rolled back from still holding the title, and unversioned work
    /// waits for executors that are gone. Promoting the version being rolled back to, which is what
    /// an operator's rollback does, is what releases it.
    ///
    /// Naming a version instead pins the row: only an executor running exactly that code will
    /// dequeue it, so a stale value here is a workflow that waits forever. Name one when a
    /// deployment is being pinned deliberately — draining work to a version that is still up during
    /// a rollback, say — and otherwise let the fleet take it.
    pub app_version: Option<&'a str>,

    /// How long the whole workflow may take, once it starts.
    ///
    /// A budget rather than a deadline, and the clock starts on **dequeue**: a workflow given five
    /// minutes that waits an hour in the queue still gets its five minutes.
    ///
    /// Left unset, a workflow enqueued by [`DBOS::enqueue_with`] from inside another inherits that
    /// workflow's deadline, as a child start does; outside a workflow, and from a client, there is
    /// no deadline to inherit, and unset is no timeout. A child that should outlive its parent's
    /// deadline is given a timeout of its own.
    pub timeout: Option<Duration>,

    /// Caller-supplied attributes, stored as JSON on the workflow's row.
    ///
    /// Searchable metadata: a tenant, a request id, a trace context. Plain JSON, never the
    /// configured [`Serializer`](crate::Serializer) — the column is read by containment and by every other
    /// implementation, so a workflow's own payload encoding has no say in it.
    pub attributes: Option<&'a serde_json::Map<String, serde_json::Value>>,
}

impl<'a> EnqueueOptions<'a> {
    /// Refuses an enqueue of `workflow` no row could hold: a workflow with no name, which nothing
    /// could ever dequeue, and what every start checks — see [`validate_start`].
    ///
    /// The name is checked here because a client takes it from its caller. A
    /// [`QualifiedWorkflowName`] can hold an empty one, since a row can; an executor's starts take
    /// theirs from a registration, which refuses it.
    pub(crate) fn validate(&self, workflow: &QualifiedWorkflowName) -> Result<()> {
        if workflow.name().is_empty() {
            return Err(Error::InvalidArgument {
                operation: ENQUEUE_OPERATION.into(),
                detail: format!("queue `{}`: `workflow` must not be empty", self.queue.name),
            });
        }
        validate_start(
            Some(&self.queue),
            self.timeout,
            self.workflow_id,
            self.workflow_id_reuse_policy,
        )
    }

    /// A plain enqueue onto `queue`, asking for nothing else.
    pub fn new(queue: &'a str) -> Self {
        Self::on(Enqueue::new(queue))
    }

    /// The same, for an enqueue that has something to ask of the queue itself — a deduplication
    /// id, a priority, a partition key, a delay.
    ///
    /// **The base to build on whenever [`queue`](Self::queue) is not the default one**, because
    /// it is the only shape that names the queue once: `EnqueueOptions { queue: .., ..
    /// EnqueueOptions::new(..) }` names it twice, and the functional update overwrites whatever
    /// `new` was given, so a rename that misses one of the two silently enqueues onto the stale
    /// queue.
    pub fn on(queue: Enqueue<'a>) -> Self {
        Self {
            queue,
            workflow_id: None,
            workflow_id_reuse_policy: WorkflowIdReusePolicy::ReturnExisting,
            app_name: None,
            app_version: None,
            timeout: None,
            attributes: None,
        }
    }
}

impl<'a> From<Enqueue<'a>> for EnqueueOptions<'a> {
    fn from(queue: Enqueue<'a>) -> Self {
        Self::on(queue)
    }
}

impl Client {
    /// Enqueues a workflow, onto a queue, and hands back a handle to it.
    ///
    /// **The client's signature operation.** The workflow is recorded `ENQUEUED` and nothing runs
    /// it here; whichever executor next polls that queue claims it, and only that executor needs to
    /// have the code. The handle is a polling one, because the process that asks is by definition
    /// not the one that runs it.
    ///
    /// `workflow` is the [`QualifiedWorkflowName`] it was registered under. A free function's name
    /// alone converts into one; a workflow registered under a class, or a class and a configured
    /// instance, is named by [`QualifiedWorkflowName::method`] or
    /// [`QualifiedWorkflowName::instance`], because the registration is identified by all three
    /// parts.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// let handle: dbos::WorkflowHandle<String> =
    ///     client.enqueue("send_email", "email-queue", "hello").await?;
    /// let sent = handle.result().await?;
    ///
    /// let billing = dbos::QualifiedWorkflowName::instance("charge", "Billing", "eu");
    /// let charged: dbos::WorkflowHandle<u64> = client.enqueue(billing, "billing", 42).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The type parameters are the caller's assertion, not a checked fact: there is no registry
    /// here to look the name up in, so `R` is what the caller knows the workflow returns and a
    /// mismatch surfaces when the result is decoded. That is the trade every implementation's
    /// client makes — Java calls its return `WorkflowHandle<T, E>` on the same terms — and it is
    /// the price of naming code this process does not have.
    ///
    /// **Called from inside a workflow body this is not a checkpoint**, unlike
    /// [`DBOS::enqueue`], which records the launch against the parent and re-derives the same
    /// child on a replay. A client knows nothing of the ambient workflow (it is a connection, not
    /// an execution), so a replayed body enqueues again. Reach for [`DBOS::enqueue`] when the
    /// caller is a workflow, and for this when it is not.
    pub async fn enqueue<P, R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        queue: &str,
        input: P,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        self.enqueue_with(workflow, input, EnqueueOptions::new(queue))
            .await
    }

    /// [`enqueue`](Self::enqueue), with something to say about how.
    ///
    /// See [`EnqueueOptions`] for what may be said, and [`Enqueue`] for the queue-shaped half of
    /// it — a deduplication id, a priority, a partition key, a delay, and how the input is encoded.
    ///
    /// **For a workflow in another language, enqueue it
    /// [portable](crate::SerializationFormat::Portable)**: the input then goes into the envelope
    /// every SDK reads, a tuple of two or more as that many positional arguments, `()` as none, and
    /// anything else as the one argument. A client has no registration to defer to, so
    /// [`SerializationFormat::Default`](crate::SerializationFormat::Default) is this client's
    /// [`serializer`](crate::ClientConfig::serializer) here. A callee that takes named arguments needs
    /// [`enqueue_portable`](Self::enqueue_portable).
    pub async fn enqueue_with<P, R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        input: P,
        options: EnqueueOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>>
    where
        P: Serialize,
    {
        let workflow = workflow.into();
        // Refused before the input is encoded, as before anything is written: an enqueue no queue
        // could honour should not spend the encoding either.
        options.validate(&workflow)?;
        let encoding = PayloadEncoding::resolve(
            options.queue.serialization,
            self.connection().serializer().encoding(),
        );
        let input = encode_input(&input, &encoding)?;
        self.enqueue_encoded(&workflow, &input, encoding, options, None)
            .await
    }

    /// Enqueues a workflow with its arguments as written: the portable envelope, as given.
    ///
    /// For a workflow in another language that takes arguments by name, which no Rust value is
    /// written as: [`enqueue_with`](Self::enqueue_with) writes a tuple's elements by position.
    /// Always portable, whatever [`Enqueue::serialization`] says.
    ///
    /// ```no_run
    /// # async fn f(client: &dbos::Client) -> dbos::Result<()> {
    /// use dbos::{EnqueueOptions, PortableWorkflowArgs};
    /// let args = PortableWorkflowArgs {
    ///     positional_args: vec![serde_json::json!("ada@example.com")],
    ///     named_args: serde_json::Map::from_iter([("urgent".to_owned(), serde_json::json!(true))]),
    /// };
    /// let handle: dbos::WorkflowHandle<String> = client
    ///     .enqueue_portable("send_email", args, EnqueueOptions::new("email-queue"))
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn enqueue_portable<R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        args: PortableWorkflowArgs,
        options: EnqueueOptions<'_>,
    ) -> Result<WorkflowHandle<R, E>> {
        let workflow = workflow.into();
        options.validate(&workflow)?;
        let input = encode(&args, &PayloadEncoding::Portable, "argument")?;
        self.enqueue_encoded(&workflow, &input, PayloadEncoding::Portable, options, None)
            .await
    }

    /// The enqueue both public forms share, from an input already encoded in `encoding`.
    ///
    /// The queue options are validated by the caller, before it encodes the input: an enqueue no
    /// queue could honour should cost a round trip, not a row, and not an encoding either.
    ///
    /// `debounce` marks the row a debounced one, which only a
    /// [`DebouncerClient`](crate::DebouncerClient) asks for.
    pub(crate) async fn enqueue_encoded<R, E>(
        &self,
        workflow: &QualifiedWorkflowName,
        input: &str,
        encoding: PayloadEncoding,
        options: EnqueueOptions<'_>,
        debounce: Option<Debounced>,
    ) -> Result<WorkflowHandle<R, E>> {
        let attributes = encode_attributes(options.attributes)?;
        // Generated once, outside the retry below: a second attempt under a fresh id would enqueue
        // a *second* workflow if the first insert had in fact landed.
        let generated;
        let workflow_id = match options.workflow_id {
            Some(id) => id,
            None => {
                generated = uuid::Uuid::new_v4().to_string();
                &generated
            }
        };

        let new = NewWorkflow {
            name: Some(workflow.name()),
            class_name: workflow.class_name(),
            config_name: workflow.config_name(),
            input: Some(input),
            serialization: Some(encoding.name()),
            // **Nobody claims it.** A client runs nothing, and a queued row's executor is stamped
            // by whichever one dequeues it — writing a claimant here would name a process that is
            // never going to run it.
            executor_id: None,
            // The enqueue's own, then this client's. Naming another application deliberately is
            // what lets a tool hand work to an application it is not; naming none at all writes an
            // unclaimed row, which any application may run.
            application_name: options.app_name.or_else(|| self.connection().app_name()),
            // **Only if the caller said so.** A version pins the row to executors running exactly
            // that code, and a client has no version of its own to offer — see
            // [`EnqueueOptions::app_version`].
            application_version: options.app_version,
            // A budget, never a deadline: a queued workflow's deadline is computed on dequeue, so
            // the wait in the queue is not part of the budget. `start_with` writes the pair the
            // same way for the same reason.
            timeout: options.timeout,
            deadline: None,
            attributes: attributes.as_deref(),
            is_debounced: debounce.is_some(),
            debounce_timeout: debounce.and_then(|debounce| debounce.timeout),
            // Always `Some`: a client cannot run a workflow, so every enqueue it makes names a
            // queue — which is the difference from `start_with`, where the same base takes `None`.
            ..new_row(workflow_id, Some(&options.queue))
        };

        // A client has no step counter of its own, so an enqueue it makes records against no
        // parent — the line drawn for every other call a client makes.
        match init_or_join(
            self.connection(),
            &new,
            options.queue.duplication_policy,
            options.workflow_id_reuse_policy,
            None,
        )
        .await
        .map_err(|error| refused_start(Some(options.queue.name), error))?
        {
            Submitted::Created(_) => {
                tracing::debug!(
                    workflow_id,
                    %workflow,
                    queue = options.queue.name,
                    "the workflow is enqueued"
                );
                // This call wrote the row, so a later absence is a deletion: the handle fails
                // fast rather than polling for a row that will never reappear.
                Ok(WorkflowHandle::polling(
                    Arc::clone(self.connection()),
                    workflow_id.to_owned(),
                    true,
                ))
            }
            // The key was held and this caller asked to join whoever holds it, so the handle names
            // that workflow rather than the id this call offered. A client has no parent, so there
            // is nothing to record about the join — which is the whole of the difference from
            // `start_with`.
            Submitted::Joined(holder) => Ok(WorkflowHandle::polling(
                Arc::clone(self.connection()),
                holder,
                true,
            )),
        }
    }
}

/// The client's enqueue, from an application: a workflow named rather than registered, recorded as
/// a child when a workflow does it.
impl DBOS {
    /// Enqueues a workflow by name, onto a queue, and hands back a handle to it.
    ///
    /// **For a workflow this process need not have**: implemented by another service, or in
    /// another language, sharing the system database. The name is not looked up in this process's
    /// registrations, and the row carries what [`Client::enqueue`]'s does, so whichever executor
    /// has the code dequeues it. It differs in naming this executor as the one that wrote it, and,
    /// inside a workflow, in recording the parent and carrying its deadline. A workflow this
    /// process registered is started through its [`WorkflowRef`](crate::WorkflowRef) instead,
    /// which can also run it here.
    ///
    /// **Called from inside a workflow, the enqueued workflow is its child**, as a
    /// [`start`](crate::WorkflowRef::start_with) makes one: its id is `{parent_id}-{step_id}`
    /// unless [`workflow_id`](EnqueueOptions::workflow_id) names one, the launch is recorded at the
    /// parent's step, and a replay hands back the same handle without enqueueing again. From
    /// inside a step it is refused, as a start is. Outside a workflow it is [`Client::enqueue`]
    /// through this application's own connection.
    ///
    /// The type parameters are the caller's assertion, as they are for [`Client::enqueue`].
    ///
    /// ```no_run
    /// # async fn f(dbos: &dbos::DBOS) -> dbos::Result<()> {
    /// let handle: dbos::WorkflowHandle<String> =
    ///     dbos.enqueue("send_email", "email-queue", "hello").await?;
    /// # Ok(()) }
    /// ```
    pub fn enqueue<'a, P, R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        queue: &'a str,
        input: P,
    ) -> PendingStart<'a, R, E>
    where
        P: Serialize,
        R: 'a,
        E: 'a,
    {
        self.enqueue_with(workflow, input, EnqueueOptions::new(queue))
    }

    /// [`enqueue`](Self::enqueue), with something to say about how.
    ///
    /// The options mean what they mean to [`Client::enqueue_with`], with two differences that come
    /// of an application making the call:
    ///
    /// - **The workflow belongs to this application** unless
    ///   [`app_name`](EnqueueOptions::app_name) names another.
    /// - **[`timeout`](EnqueueOptions::timeout) left unset inherits** a parent's deadline when a
    ///   workflow makes the call, as [`Timeout::Inherit`] does for a child start. Outside a
    ///   workflow there is none to inherit.
    ///
    /// [`app_version`](EnqueueOptions::app_version) left unset writes no version, as a client's
    /// enqueue does, so the application's latest version dequeues it: this process may not have
    /// the code at all, so its own version says nothing about who can run it.
    pub fn enqueue_with<'a, P, R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        input: P,
        options: EnqueueOptions<'a>,
    ) -> PendingStart<'a, R, E>
    where
        P: Serialize,
        R: 'a,
        E: 'a,
    {
        self.enqueue_encoded(workflow.into(), options, |executor, options| {
            let encoding = PayloadEncoding::resolve(
                options.queue.serialization,
                executor.serializer().encoding(),
            );
            Ok((encode_input(&input, &encoding)?, encoding))
        })
    }

    /// [`Client::enqueue_portable`] from an application: the portable envelope, as given.
    ///
    /// For a workflow in another language that takes arguments by name. Recorded as a child from
    /// inside a workflow, as [`enqueue`](Self::enqueue) is.
    pub fn enqueue_portable<'a, R, E>(
        &self,
        workflow: impl Into<QualifiedWorkflowName>,
        args: PortableWorkflowArgs,
        options: EnqueueOptions<'a>,
    ) -> PendingStart<'a, R, E>
    where
        R: 'a,
        E: 'a,
    {
        self.enqueue_encoded(workflow.into(), options, |_, _| {
            Ok((
                encode(&args, &PayloadEncoding::Portable, "argument")?,
                PayloadEncoding::Portable,
            ))
        })
    }

    /// The enqueue every form shares, given how to encode its input.
    ///
    /// **Everything that can be refused is refused before the step id is taken**: an unlaunched
    /// instance, options no queue could honour, an input or attributes that cannot be encoded, and
    /// a caller inside a step. A call that never happens must not move the parent's counter.
    fn enqueue_encoded<'a, R, E, F>(
        &self,
        workflow: QualifiedWorkflowName,
        options: EnqueueOptions<'a>,
        encode_input: F,
    ) -> PendingStart<'a, R, E>
    where
        R: 'a,
        E: 'a,
        F: FnOnce(&Executor, &EnqueueOptions<'a>) -> Result<(String, PayloadEncoding)>,
    {
        // Recorded at the parent's step under the workflow's bare name, as a start is.
        let name: Arc<str> = Arc::from(workflow.name());
        let built = (|| {
            let executor = self.executor(ENQUEUE_OPERATION)?;
            options.validate(&workflow)?;
            let (input, encoding) = encode_input(&executor, &options)?;
            let attributes = encode_attributes(options.attributes)?;
            let placement = StepPlacement::of(executor.connection(), ENQUEUE_OPERATION)?;
            if matches!(placement, StepPlacement::InsideStep { .. }) {
                return Err(Error::InsideStep {
                    operation: ENQUEUE_OPERATION.into(),
                });
            }
            Ok(((executor, input, encoding, attributes), placement))
        })();
        // A start rather than a bare step, so it can't be raced by `select_step!`: a dropped
        // branch could leave the child created or not depending on where the drop landed, and a
        // replay would then depend on it too. See `PendingWorkflow`.
        PendingStart::from_step(PendingStep::placed(
            name,
            built,
            move |(executor, input, encoding, attributes), placement| async move {
                let request = OwnedStart {
                    workflow_id: placed_workflow_id(options.workflow_id, &placement),
                    workflow_id_reuse_policy: options.workflow_id_reuse_policy,
                    timeout: options.timeout.map_or(Timeout::Inherit, Timeout::Explicit),
                    queue: Some(OwnedEnqueue::from(&options.queue)),
                    input: Some(input),
                    encoding,
                    attributes,
                    started_at: Timestamp::now(),
                    debounce: None,
                    app_name: options.app_name.map(str::to_owned),
                    app_version: options.app_version.map_or(RowVersion::Latest, |version| {
                        RowVersion::Named(version.to_owned())
                    }),
                };
                create::<R, E>(executor, placement, workflow, request)
                    .await
                    .map_err(|error| refused_start(Some(options.queue.name), error))
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::DuplicationPolicy;

    #[test]
    fn an_enqueue_asks_for_a_queue_and_nothing_else() {
        let options = EnqueueOptions::new("work");
        assert_eq!(options.queue, Enqueue::new("work"));
        assert_eq!(options.workflow_id, None);
        assert_eq!(options.app_version, None);
        assert_eq!(options.timeout, None);
        assert_eq!(
            options.queue.duplication_policy,
            DuplicationPolicy::Reject,
            "a caller who did not think about deduplication should hear that a key was taken"
        );
    }

    /// The queue is named once, and it is the one that was asked for.
    ///
    /// `on` exists so that an enqueue with something to say about the queue does not have to name
    /// it twice — once in the [`Enqueue`] and once in an `EnqueueOptions::new` base, where only
    /// the second is honoured.
    #[test]
    fn options_over_a_queue_keep_the_queue_they_were_given() {
        let queue = Enqueue {
            priority: Some(3),
            ..Enqueue::new("billing")
        };
        let options = EnqueueOptions {
            workflow_id: Some("order-42"),
            ..EnqueueOptions::on(queue.clone())
        };
        assert_eq!(options.queue, queue);
        assert_eq!(options.queue.name, "billing");
        assert_eq!(options.workflow_id, Some("order-42"));
        assert_eq!(EnqueueOptions::from(queue.clone()).queue, queue);
    }
}
