//! Steps: the checkpoints that make a workflow resumable.

use std::future::Future;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use crate::context::Ctx;
use crate::error::{DurableError, Error, Result};
use crate::serialization::{decode, encode};
use crate::sysdb::types::{Outcome, StepTiming, Timestamp};

/// Runs `body` once per workflow, recording what it returned.
///
/// On a first run the body executes and its result is written to the database. On a replay the
/// recorded result is returned **without entering the body at all** — which is the whole of durable
/// execution, and why a step may do things that must not happen twice.
///
/// ```no_run
/// # async fn f() -> dbos::Result<()> {
/// let charge = dbos::step("charge_card", || async { dbos::Result::Ok(42) }).await?;
/// # Ok(()) }
/// ```
///
/// The error type is a type parameter so that a recorded failure can be decoded back into it. It
/// is the *workflow's* error type rather than one of the step's own, which is what lets an
/// infallible body write a bare `Ok(..)`: there is nothing to infer, because the surrounding `?`
/// already fixed it. A step failing with some other error type converts at the boundary, with
/// `.map_err(MyError::from)?`.
///
/// Outside a workflow the body simply runs, undurably. That makes a function built from steps
/// ordinarily callable and ordinarily testable, and it is what Python does. Inside another step the
/// same applies: a step is a leaf, so a nested one is a plain call rather than a second checkpoint.
///
/// **A workflow body must await each step before starting the next.** The flag that makes a step a
/// leaf lives on the workflow rather than on the call stack, so two steps in flight at once — under
/// `tokio::join!`, `select!`, or any other concurrent combinator — see each other's. Two ways that
/// goes wrong, and both are silent: a step that starts while another's body is running takes the
/// plain path above and is *not* checkpointed, so a replay runs it again; and a sibling finishing
/// clears the flag for a step still inside its body, so a step nested in that one allocates an id
/// after all. Step ids then fall out of poll order, and a replay that interleaves differently meets
/// a recorded step under the wrong name — which is a system-database error, so the workflow records
/// nothing, stays `PENDING`, and is recovered until it parks.
///
/// That is a known gap rather than a rule with a workaround. Concurrent steps are a later change:
/// the flag has to become per-call-stack — a nested [`Ctx`](crate::Ctx) scope around the body, so
/// that nesting is exact and siblings cannot see each other — and wants a count of live steps, so
/// that genuine concurrency is refused loudly rather than degrading to a plain call. Until then,
/// sequential is the contract.
///
/// The name is explicit and it matters: it is checked on replay, so a step whose name changed is
/// reported rather than silently matched against the recorded result of whatever used to be there.
///
/// **The body is `FnMut` rather than `FnOnce` because a step may be attempted more than once.**
/// Retries call it again, and a bound that permits exactly one call cannot express that. The cost
/// to a caller is nothing in the ordinary case — a closure written inline at the call site is
/// `FnMut` unless it moves a captured value out — and a body that genuinely consumes what it
/// captured fails to compile here rather than at its second attempt, which is where the mistake
/// should be reported.
pub async fn step<T, E, F, Fut>(name: &str, body: F) -> Result<T, E>
where
    T: Serialize + DeserializeOwned,
    E: DurableError,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut body = body;
    let Some(ctx) = Ctx::current().filter(|ctx| !ctx.in_step()) else {
        tracing::debug!(
            step_name = name,
            "the step body runs plainly, not as a checkpoint: it is outside a workflow, or \
             inside another step"
        );
        return body().await;
    };

    let step_id = ctx.next_step_id();
    let executor = ctx.executor();
    let workflow_id = ctx.workflow_id();

    let recorded = executor
        .sysdb()
        .check_step(workflow_id, step_id, name)
        .await
        .map_err(Error::SystemDatabase)?;
    if let Some(recorded) = recorded {
        tracing::debug!(
            step_id,
            step_name = name,
            "the step replays from its checkpoint; the body does not run"
        );
        return match recorded.error {
            Some(error) => Err(revive(&error, name)),
            None => decode(recorded.output.as_deref(), "step result"),
        };
    }

    // The span nests inside the workflow's, so anything the body logs carries both ids.
    let span = tracing::info_span!("step", step_id, step_name = name);
    let started_at = Timestamp::now();
    let outcome = ctx.in_step_scope(body()).instrument(span).await;

    // Built once and held, not rebuilt per attempt: `record_step` compares the stored completion
    // time against this one to tell its own retried write from another execution's, and a fresh
    // timestamp on a retry would read as somebody else.
    let timing = Some(StepTiming {
        started_at,
        completed_at: Timestamp::now(),
    });
    let serialization = Some(executor.serializer().name());

    match &outcome {
        Ok(value) => {
            let output = encode(value, "step result")?;
            executor
                .sysdb()
                .record_step(
                    workflow_id,
                    step_id,
                    name,
                    Outcome::Output(Some(&output)),
                    serialization,
                    timing,
                )
                .await
                .map_err(Error::SystemDatabase)?;
            tracing::debug!(
                step_id,
                step_name = name,
                "the step ran; its output is recorded"
            );
        }
        Err(error) => {
            // A control error is not the step's result. A cancelled workflow that recorded its
            // cancellation as a step failure would replay as *permanently* failed, having lost the
            // fact that it was interrupted rather than wrong. Debug rather than warn: the signal
            // propagates into the workflow's channel, and the recording layer above warns once,
            // with the durable consequence.
            if error.control().is_some() {
                tracing::debug!(
                    step_id,
                    step_name = name,
                    "a control signal ended the step; nothing is checkpointed"
                );
                return outcome;
            }
            // The error itself, encoded, not a description of it: a replay gives back the error
            // that failed exactly as it gives back the value that succeeded.
            let encoded = encode(error, "step error")?;
            executor
                .sysdb()
                .record_step(
                    workflow_id,
                    step_id,
                    name,
                    Outcome::Error(&encoded),
                    serialization,
                    timing,
                )
                .await
                .map_err(Error::SystemDatabase)?;
            tracing::debug!(
                step_id,
                step_name = name,
                "the step failed; its error is recorded"
            );
        }
    }
    outcome
}

/// Rebuilds the error a recorded step failed with.
///
/// The same error, not a description of it: an application failure comes back as its own variant
/// with its own fields, and an engine failure as the variant it was. The only payloads that do not
/// survive are the `serde_json::Error` sources, which arrive absent rather than different.
///
/// Falls back to a plain message when the column does not hold one of ours, which is what a row
/// written by another SDK looks like — its serializer chose its own shape, and the `serialization`
/// column says so. A readable message beats a decode failure standing in for somebody else's error.
fn revive<E: DurableError>(recorded: &str, step: &str) -> Error<E> {
    serde_json::from_str(recorded).unwrap_or_else(|_| Error::StepFailed {
        step: step.to_owned(),
        message: recorded.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::sysdb::types::{NewWorkflow, Submission};
    use crate::{Config, DBOS};

    /// A launched instance and a workflow row to hang steps off.
    ///
    /// The row has to exist because a step re-stamps its workflow's executor id, and there is no
    /// `run` here on purpose: entering the same workflow id under two contexts is precisely what a
    /// replay is, and it is the only way to test one before recovery exists to do it for real.
    async fn workflow(id: &str) -> (DBOS, dbos_test_support::TestDatabase) {
        let db = dbos_test_support::test_database().await;
        let dbos = DBOS::new(Config {
            migrate: false,
            ..Config::new("step-test", db.url())
        });
        dbos.launch().await.expect("launch failed");
        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .init_workflow(&NewWorkflow::new(id), None, Submission::Fresh)
            .await
            .expect("could not create the workflow row");
        (dbos, db)
    }

    fn ctx(dbos: &DBOS, id: &str) -> Ctx {
        Ctx::new(dbos.executor("test").expect("launched"), id)
    }

    #[tokio::test]
    async fn a_replayed_step_returns_its_recorded_result_without_entering_the_body() {
        let (dbos, _db) = workflow("wf-replay").await;
        let entered = AtomicU32::new(0);
        // Borrows `entered`, so the closure is `Copy` and both runs can use the same body.
        let body = || async {
            entered.fetch_add(1, Ordering::SeqCst);
            Ok::<_, crate::Error>(7u32)
        };

        // First run: the body executes and the result is recorded.
        let first = Ctx::scope(ctx(&dbos, "wf-replay"), step("compute", body)).await;
        assert_eq!(first.unwrap(), 7);
        assert_eq!(entered.load(Ordering::SeqCst), 1);

        // Replay: a fresh context over the same workflow id, step ids starting again from zero.
        let again = Ctx::scope(ctx(&dbos, "wf-replay"), step("compute", body)).await;
        assert_eq!(again.unwrap(), 7, "the recorded result");
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "the body must not run a second time: that is the whole of durable execution"
        );

        dbos.shutdown().await;
    }

    #[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, PartialEq)]
    #[error("the card was declined after {attempts} attempts")]
    struct CardDeclined {
        attempts: u32,
    }

    #[tokio::test]
    async fn a_replayed_step_that_failed_reports_what_it_recorded() {
        let (dbos, _db) = workflow("wf-failed-step").await;

        let failing = || async { Err::<(), _>(CardDeclined { attempts: 3 }.into()) };

        let first = Ctx::scope(ctx(&dbos, "wf-failed-step"), step("boom", failing)).await;
        let first = first.unwrap_err();
        let Error::Application(error) = &first else {
            panic!("expected an application error, got {first}")
        };
        assert_eq!(error, &CardDeclined { attempts: 3 });

        // Replay gives back the *same error*, decoded, rather than a sentence about it — the same
        // fidelity a successful step's output gets, fields and all.
        let again = Ctx::scope(ctx(&dbos, "wf-failed-step"), step("boom", failing)).await;
        let again = again.unwrap_err();
        let Error::Application(replayed) = &again else {
            panic!("expected an application error, got {again}")
        };
        assert_eq!(replayed, error, "the failure survives the round trip whole");
        assert_eq!(again.to_string(), first.to_string());

        dbos.shutdown().await;
    }

    /// A database failure is not the step's result either, so nothing is checkpointed.
    ///
    /// The step did not fail; the database did. Recording a failure here would replay as
    /// *permanently* failed, and the next attempt would never re-enter a body that never ran.
    #[tokio::test]
    async fn a_database_failure_is_not_the_steps_result() {
        use crate::sysdb::{BackendError, BackendErrorKind};

        let (dbos, _db) = workflow("wf-blip").await;

        let failed = Ctx::scope(
            ctx(&dbos, "wf-blip"),
            step("charge", || async {
                Err::<(), crate::Error>(Error::SystemDatabase(crate::sysdb::Error::Backend(
                    BackendError {
                        message: "connection reset by peer".to_owned(),
                        sqlstate: None,
                        kind: BackendErrorKind::Connection,
                    },
                )))
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(failed, Error::SystemDatabase(_)), "{failed}");

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-blip", true, None, None)
            .await
            .expect("read failed");
        assert!(
            steps.is_empty(),
            "a database failure must not be checkpointed as the step's outcome: {steps:?}"
        );

        dbos.shutdown().await;
    }

    /// The one case full fidelity cannot cover: a row this SDK did not write.
    ///
    /// A portable invocation leaves an error encoded in somebody else's shape, and no type
    /// parameter reconstructs a type that was never Rust's. The replay degrades to the message
    /// rather than failing to decode, so the workflow reports what happened instead of reporting
    /// that it could not read what happened.
    #[tokio::test]
    async fn a_step_recorded_in_a_foreign_shape_degrades_to_its_message() {
        let (dbos, _db) = workflow("wf-foreign").await;

        dbos.executor("test")
            .expect("launched")
            .sysdb()
            .record_step(
                "wf-foreign",
                0,
                "charge",
                Outcome::Error(r#"{"pythonModule":"app.errors","type":"CardDeclined"}"#),
                Some("pickle"),
                None,
            )
            .await
            .expect("could not record the step");

        let replayed = Ctx::scope(
            ctx(&dbos, "wf-foreign"),
            step("charge", || async {
                Err::<(), _>(CardDeclined { attempts: 1 }.into())
            }),
        )
        .await
        .unwrap_err();

        match &replayed {
            Error::StepFailed { step, message } => {
                assert_eq!(step, "charge");
                assert!(message.contains("CardDeclined"), "{message}");
            }
            other => panic!("expected a degraded step failure, got {other:?}"),
        }

        dbos.shutdown().await;
    }

    /// A failure that is one of *ours* replays as itself, variant and payload intact — not
    /// flattened into an application error carrying its message.
    #[tokio::test]
    async fn a_replayed_step_gives_back_the_same_variant_it_recorded() {
        let (dbos, _db) = workflow("wf-variant").await;

        let failing = || async {
            Err::<(), crate::Error>(Error::NotRegistered {
                key: "checkout/Checkout/eu".to_owned(),
            })
        };
        let first = Ctx::scope(ctx(&dbos, "wf-variant"), step("boom", failing)).await;
        let first = first.unwrap_err();

        let again = Ctx::scope(ctx(&dbos, "wf-variant"), step("boom", failing)).await;
        let again = again.unwrap_err();

        match &again {
            Error::NotRegistered { key } => assert_eq!(key, "checkout/Checkout/eu"),
            other => panic!("the variant did not survive the round trip: {other:?}"),
        }
        assert_eq!(again.to_string(), first.to_string());

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_step_whose_name_changed_is_reported_rather_than_silently_matched() {
        let (dbos, _db) = workflow("wf-renamed").await;

        let first = Ctx::scope(
            ctx(&dbos, "wf-renamed"),
            step("old_name", || async { Ok::<_, crate::Error>(1u32) }),
        )
        .await;
        assert_eq!(first.unwrap(), 1);

        // Step 0 is recorded under a different name, so its result is not this step's result.
        let renamed = Ctx::scope(
            ctx(&dbos, "wf-renamed"),
            step("new_name", || async { Ok::<_, crate::Error>(1u32) }),
        )
        .await;
        assert!(
            renamed.is_err(),
            "a changed step name must not match a recorded result"
        );

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn steps_are_recorded_in_order_under_the_ids_they_allocated() {
        let (dbos, _db) = workflow("wf-order").await;

        Ctx::scope(ctx(&dbos, "wf-order"), async {
            step("one", || async { Ok::<_, crate::Error>(1u32) })
                .await
                .unwrap();
            step("two", || async { Ok::<_, crate::Error>(2u32) })
                .await
                .unwrap();
            step("three", || async { Ok::<_, crate::Error>(3u32) })
                .await
                .unwrap();
        })
        .await;

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-order", true, None, None)
            .await
            .expect("read failed");
        let seen: Vec<(i32, &str)> = steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect();
        assert_eq!(seen, [(0, "one"), (1, "two"), (2, "three")]);
        assert_eq!(steps[2].output.as_deref(), Some("3"));

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_nested_step_is_a_plain_call_and_allocates_no_id() {
        let (dbos, _db) = workflow("wf-nested").await;

        Ctx::scope(ctx(&dbos, "wf-nested"), async {
            step("outer", || async {
                // Go #420's rule: a step is a leaf. Were this to allocate an id, every step after
                // it would replay against the wrong slot.
                let inner = step("inner", || async { Ok::<_, crate::Error>(1u32) })
                    .await
                    .unwrap();
                Ok::<_, crate::Error>(inner + 1)
            })
            .await
            .unwrap();
            step("after", || async { Ok::<_, crate::Error>(9u32) })
                .await
                .unwrap();
        })
        .await;

        let steps = dbos
            .executor("test")
            .unwrap()
            .sysdb()
            .list_workflow_steps("wf-nested", true, None, None)
            .await
            .expect("read failed");
        let seen: Vec<(i32, &str)> = steps
            .iter()
            .map(|s| (s.step_id, s.step_name.as_str()))
            .collect();
        assert_eq!(
            seen,
            [(0, "outer"), (1, "after")],
            "`inner` is not a checkpoint"
        );

        dbos.shutdown().await;
    }

    #[tokio::test]
    async fn a_step_outside_a_workflow_runs_plainly() {
        let entered = Arc::new(AtomicU32::new(0));
        let value = step("standalone", || {
            let entered = Arc::clone(&entered);
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                Ok::<_, crate::Error>(3u32)
            }
        })
        .await
        .expect("a step with no workflow around it just runs");

        assert_eq!(value, 3);
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert!(Ctx::current().is_none());
    }
}
