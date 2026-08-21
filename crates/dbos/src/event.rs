//! Workflow events: a key/value a workflow publishes and anyone may read.
//!
//! Two surfaces, split by where the caller stands. [`set_event`] is a free function like
//! [`step`](crate::step), callable only from inside a workflow — publishing is something a
//! workflow does, and the checkpoint that makes it replay-safe needs the ambient step-id
//! sequence. [`DBOS::get_event`] is an instance method, because the natural reader is outside any
//! workflow — an HTTP handler polling for progress — though a workflow that reads is checkpointed
//! correctly too.
//!
//! Both are thin: `sysdb` owns the transactional write, the replay skip, the blocking read, and
//! the cross-SDK step names (`DBOS.setEvent`, `DBOS.getEvent`). What the engine adds is the step
//! ids from the ambient context, the payload encoding, and the guards on where each call may
//! stand.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::DBOS;
use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::GetEventCaller;

/// Publishes a key/value on the current workflow, for anyone to read.
///
/// Setting the same key again replaces the value, which is what makes it a progress channel: the
/// starter app publishes the number of completed steps under one key after each step, and a
/// reader polling [`DBOS::get_event`] sees the latest.
///
/// The write is checkpointed under a step id, so a replay does not publish again — `sysdb` sees
/// the recorded step and skips, in the same transaction that would have written. The error is the
/// *workflow's* channel, like [`step`](crate::step)'s, so `?` needs no conversion.
///
/// Only from inside a workflow, and not from inside a step. Outside a workflow there is no step
/// sequence to checkpoint against, and unlike a step there is no plain version of a durable write
/// to degrade to; inside a step, allocating an id would shift every later step onto the wrong
/// replay slot.
pub async fn set_event<T, E>(key: &str, value: &T) -> Result<(), E>
where
    T: Serialize,
    E: DurableError,
{
    let Some(ctx) = Ctx::current() else {
        return Err(Error::NotInWorkflow {
            operation: "set_event".into(),
        });
    };
    if ctx.in_step() {
        return Err(Error::InsideStep {
            operation: "set_event".into(),
        });
    }

    let encoded = encode(value, "event value")?;
    let step_id = ctx.next_step_id();
    let executor = ctx.executor();
    executor
        .sysdb()
        .set_event(
            ctx.workflow_id(),
            step_id,
            key,
            &encoded,
            Some(executor.serializer().name()),
        )
        .await
        .map_err(Error::SystemDatabase)?;
    Ok(())
}

impl DBOS {
    /// Reads a key a workflow published, waiting up to `timeout` for it to appear.
    ///
    /// `Ok(None)` means the key was not there when the deadline passed — absence is a value, not
    /// an error, and `Duration::ZERO` makes this a poll: look once, do not wait.
    ///
    /// From outside a workflow — the natural caller, an HTTP handler asking how far a workflow
    /// has got — this is a passthrough. From inside one, the read is checkpointed as two steps
    /// (the read and its deadline), so a replay returns what the first run saw, including a
    /// timeout's `None`, instead of waiting again; a workflow with its own error type carries the
    /// result over with [`Error::lift`]. From inside a *step*, it reads plainly with no
    /// checkpoint, the step's own checkpoint standing for everything its body did — the same leaf
    /// rule as a nested step.
    pub async fn get_event<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let executor = self.executor("get_event")?;

        let ctx = Ctx::current().filter(|ctx| !ctx.in_step());
        // Field order is the contract: the read's id first, the deadline's second, matching what
        // every SDK records and what a replay looks up.
        let caller = ctx.as_ref().map(|ctx| GetEventCaller {
            workflow_id: ctx.workflow_id(),
            step_id: ctx.next_step_id(),
            timeout_step_id: ctx.next_step_id(),
        });

        match executor
            .sysdb()
            .get_event(workflow_id, key, timeout, caller)
            .await
            .map_err(Error::SystemDatabase)?
        {
            None => Ok(None),
            Some(found) => decode(Some(&found.value), "event value").map(Some),
        }
    }
}
