//! Retrying system database operations that failed for reasons that may pass.
//!
//! Every implementation wraps its sysdb calls in this: Python a `@db_retry` decorator on each
//! sysdb method, Java a `dbRetry(...)` call on each, Go a `Retry` helper on every statement. The
//! bargain is stated most plainly in Python's docstring — if DBOS loses its database connection,
//! *everything pauses until the connection is recovered, trading off availability for
//! correctness*.
//!
//! That trade is the point. A workflow engine whose durable state is briefly unreachable has
//! two choices: block, or report a failure that the caller will treat as a real outcome and act
//! on. Blocking is the only one that keeps the guarantee.
//!
//! # What is retried
//!
//! Classification lives with the backend, in [`BackendErrorKind`], because the evidence is
//! backend-specific. This module only acts on the verdict:
//!
//! - [`BackendErrorKind::Transient`] — always retried, whatever the policy says.
//! - [`BackendErrorKind::Connection`] — retried unless the policy opts out.
//! - [`BackendErrorKind::Permanent`], and every non-backend error, are returned immediately.
//!
//! [`Error::ConflictingWorkflow`] and [`Error::MaxRecoveryAttemptsExceeded`] fall in the last
//! group, and must: they are answers, not failures, and retrying them would loop forever.
//!
//! # What must not go inside the closure
//!
//! Anything that identifies *this* attempt. The retried region runs more than once, so a value
//! generated inside it differs between runs — and if a commit acknowledgement was lost, the
//! second run will not recognise the first run's write. Java's comment on the owner identity is
//! exactly this: "generated outside of the DB retry loop, in case commit acks get lost".

use std::future::Future;
use std::time::Duration;

use super::{BackendErrorKind, Error};

/// How long to wait between attempts, and what to give up on.
///
/// The defaults are Python's and Java's, which agree: one second, doubling to a minute, and no
/// attempt limit. Go starts at 100ms and caps at 30s, but retries statements rather than whole
/// operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Wait before the second attempt.
    pub initial_backoff: Duration,
    /// Ceiling the backoff doubles up to.
    pub max_backoff: Duration,
    /// Whether to block on connection failures rather than reporting them.
    ///
    /// `true` is the trade described above. Setting it `false` opts out — Python exposes the
    /// same switch as `retry_connection_errors=False` — and suits a caller that would rather
    /// handle the failure itself than wait. It does not affect contention, which is always
    /// retried.
    pub retry_connection_errors: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            retry_connection_errors: true,
        }
    }
}

impl RetryPolicy {
    /// Whether this failure should be retried.
    fn should_retry(&self, error: &Error) -> bool {
        match error {
            Error::Backend(e) => match e.kind {
                BackendErrorKind::Transient => true,
                BackendErrorKind::Connection => self.retry_connection_errors,
                BackendErrorKind::Permanent => false,
            },
            _ => false,
        }
    }
}

/// Runs `work` until it succeeds or fails for a reason that will not pass.
///
/// There is no attempt limit, matching every other implementation. The bound in practice is the
/// caller: dropping this future stops the loop at the next await point, so a shutdown or a
/// timeout cancels it without needing a count.
///
/// `work` is `FnMut() -> Fut` rather than an async closure, because the returned future must be
/// `Send` — every caller is behind `#[async_trait]` — and `AsyncFnMut` has no way to say that
/// on stable. The practical consequence is that the future may not borrow from the closure, so
/// call sites capture shared references (which are `Copy`) into a `move ||` and a `move` block.
pub(crate) async fn with_retry<T, F, Fut>(
    policy: &RetryPolicy,
    operation: &str,
    mut work: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>> + Send,
{
    let mut backoff = policy.initial_backoff;
    let mut attempt: u32 = 0;
    loop {
        match work().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if !policy.should_retry(&error) {
                    return Err(error);
                }
                let delay = jitter(backoff);
                tracing::warn!(
                    operation,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "system database operation failed; retrying"
                );
                tokio::time::sleep(delay).await;
                backoff = (backoff * 2).min(policy.max_backoff);
            }
        }
    }
}

/// Spreads the backoff over `[0.5, 1.5)` of its nominal value.
///
/// Without this, every process that lost the same database comes back at the same instant and
/// knocks it over again. Python and Java use this exact range; Go uses a much tighter one.
///
/// The randomness comes from a v4 UUID rather than a random-number generator, because `uuid` is
/// already a dependency and drawing one number per retry does not justify another. Both reach
/// the same operating-system entropy source.
fn jitter(backoff: Duration) -> Duration {
    let bits = uuid::Uuid::new_v4().as_u128() as u32;
    // `[0.5, 1.5)`, from a uniform `[0, 1)`.
    let factor = 0.5 + f64::from(bits) / f64::from(u32::MAX).next_up();
    backoff.mul_f64(factor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sysdb::BackendError;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn backend(kind: BackendErrorKind) -> Error {
        Error::Backend(BackendError {
            message: "boom".to_owned(),
            sqlstate: None,
            kind,
        })
    }

    /// Backoff long enough to be visible if it were ever waited on, short enough that a test
    /// which does wait on it still finishes.
    fn fast() -> RetryPolicy {
        RetryPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
            ..RetryPolicy::default()
        }
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_until_it_succeeds() {
        let calls = AtomicU32::new(0);
        let calls = &calls;
        let result = with_retry(&fast(), "test", move || async move {
            let seen = calls.fetch_add(1, Ordering::Relaxed) + 1;
            if seen < 3 {
                Err(backend(BackendErrorKind::Transient))
            } else {
                Ok(seen)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "should have failed twice before succeeding"
        );
    }

    #[tokio::test]
    async fn a_permanent_failure_is_returned_at_once() {
        let calls = AtomicU32::new(0);
        let calls = &calls;
        let result: Result<(), _> = with_retry(&fast(), "test", move || async move {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(backend(BackendErrorKind::Permanent))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "a rejected statement must not be repeated"
        );
    }

    /// A conflicting workflow is an answer, not a failure. Retrying it would never terminate.
    #[tokio::test]
    async fn a_semantic_error_is_not_retried() {
        let calls = AtomicU32::new(0);
        let calls = &calls;
        let result: Result<(), _> = with_retry(&fast(), "test", move || async move {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(Error::ConflictingWorkflow {
                workflow_id: "wf-1".to_owned(),
                detail: "different function".to_owned(),
            })
        })
        .await;
        assert!(matches!(result, Err(Error::ConflictingWorkflow { .. })));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// Opting out covers connection failures only; contention still has to be retried, or the
    /// write is simply lost.
    #[tokio::test]
    async fn opting_out_covers_connection_errors_but_not_contention() {
        let policy = RetryPolicy {
            retry_connection_errors: false,
            ..fast()
        };

        let calls = AtomicU32::new(0);
        let calls = &calls;
        let result: Result<(), _> = with_retry(&policy, "test", move || async move {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(backend(BackendErrorKind::Connection))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "the opt-out should have applied"
        );

        let calls = AtomicU32::new(0);
        let calls = &calls;
        let result = with_retry(&policy, "test", move || async move {
            let seen = calls.fetch_add(1, Ordering::Relaxed) + 1;
            if seen < 2 {
                Err(backend(BackendErrorKind::Transient))
            } else {
                Ok(())
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "contention ignores the opt-out"
        );
    }

    /// The delay stays inside the advertised band, and never exceeds the ceiling.
    #[test]
    fn jitter_stays_within_half_and_one_and_a_half() {
        for _ in 0..1_000 {
            let d = jitter(Duration::from_millis(1_000));
            assert!(
                (500..1_500).contains(&d.as_millis()),
                "jittered delay {d:?} left the [0.5, 1.5) band",
            );
        }
    }
}
