//! Named points in production code where a test can run something or make a call fail.
//!
//! **For this workspace's tests, and not public API.** It is compiled only with the `test-hooks`
//! feature, which this crate's own tests turn on through a self dev-dependency, and with it off
//! every point is a macro that expands to nothing, so an application build carries no registry and
//! no check. There is no semver promise on anything here.
//!
//! A *point* is a named place in the code, such as the moment after a step's commit. A test
//! registers a hook for a point and a *scope*, and the code calls [`hit`] there:
//!
//! - **Run something at the point**, through [`on`]: an async closure, so it can make a management
//!   call, run SQL, or wait on a `Notify` to hold a transaction open while the test does something
//!   else. This is how a race is made to happen on purpose.
//! - **Make the call fail once**, through [`fail_once`]. A point after a commit that returns a
//!   retryable error is a *lost acknowledgement*: the write landed, the caller was told it did not,
//!   and the retry has to find its own work already there.
//!
//! **Scoped, so tests that share a binary do not share a hook.** The scope is what the point is
//! about, usually a workflow id or a queue name, which tests already make unique. A point hit under
//! a scope nobody registered returns at once. Registering the same point and scope twice panics,
//! which is how two tests colliding on one name are caught rather than interleaved.
//!
//! **Cleared by the guard.** Registering returns a [`Guard`] that removes the hook when it drops,
//! including when the test panics, so a failed test cannot leave one behind for the next.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::sysdb::Error;

/// Where each point is in the code. A hook runs *at* the point, so what it can see is what is
/// true there.
pub mod points {
    /// In `init_workflow`, **after** the transaction that inserts the workflow has committed, and
    /// before the call returns. Scope: the workflow id.
    ///
    /// Failing here with a retryable error loses the acknowledgement of a commit that landed: the
    /// retry presents the same creator token, finds its own row, and must not be refused.
    pub const INIT_WORKFLOW_COMMIT: &str = "init_workflow_commit";

    /// In `record_step`, **after** the transaction that records the step has committed, and before
    /// the call returns. Scope: the workflow id.
    ///
    /// Failing here loses the acknowledgement of a recorded step: the retry records the same step
    /// again and must find it identical rather than call it nondeterminism.
    pub const STEP_COMMIT: &str = "step_commit";
}

type HookFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;
type HookFn = Arc<dyn Fn() -> HookFuture + Send + Sync>;

struct Entry {
    hook: HookFn,
    fired: Arc<AtomicUsize>,
    /// Told apart from a later registration of the same key, so an old guard dropping cannot
    /// remove a newer hook.
    id: usize,
}

static REGISTRY: Mutex<Option<HashMap<(&'static str, String), Entry>>> = Mutex::new(None);
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
/// How many hooks are registered, read without the lock: the common case in a test binary is a
/// point hit while none is, and that should cost one load.
static REGISTERED: AtomicUsize = AtomicUsize::new(0);

fn registry() -> std::sync::MutexGuard<'static, Option<HashMap<(&'static str, String), Entry>>> {
    // A hook that panicked poisons nothing worth keeping: the map is only ever inserted into,
    // removed from and cloned out of.
    REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Removes its hook when dropped.
#[must_use = "the hook is removed as soon as the guard drops"]
pub struct Guard {
    key: (&'static str, String),
    id: usize,
    fired: Arc<AtomicUsize>,
}

impl Guard {
    /// How many times the point has been hit under this scope while the hook was set.
    ///
    /// What a test asserts to know the point was reached at all, as Java's tests check that the
    /// one-shot exception was taken.
    pub fn fired(&self) -> usize {
        self.fired.load(Ordering::SeqCst)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut registry = registry();
        if let Some(map) = registry.as_mut()
            && map.get(&self.key).is_some_and(|entry| entry.id == self.id)
        {
            map.remove(&self.key);
            REGISTERED.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Runs `hook` each time `point` is hit under `scope`, until the returned guard drops.
///
/// The hook's error, if it returns one, is what the point returns, so it becomes the error of the
/// call under test.
///
/// # Panics
///
/// If a hook is already set for this point and scope.
pub fn on<F, Fut>(point: &'static str, scope: impl Into<String>, hook: F) -> Guard
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + 'static,
{
    let hook: HookFn = Arc::new(move || Box::pin(hook()));
    register(point, scope.into(), hook)
}

/// Makes `point` return `error` the first time it is hit under `scope`, and nothing after.
///
/// With a retryable backend error at a point after a commit, this is a lost acknowledgement.
pub fn fail_once(point: &'static str, scope: impl Into<String>, error: Error) -> Guard {
    let error = Arc::new(Mutex::new(Some(error)));
    on(point, scope, move || {
        let error = Arc::clone(&error);
        async move {
            let taken = error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            taken.map_or(Ok(()), Err)
        }
    })
}

fn register(point: &'static str, scope: String, hook: HookFn) -> Guard {
    let key = (point, scope);
    let fired = Arc::new(AtomicUsize::new(0));
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let mut registry = registry();
    let map = registry.get_or_insert_with(HashMap::new);
    assert!(
        !map.contains_key(&key),
        "a hook is already set for point `{}` under scope `{}`: two tests are sharing a name",
        key.0,
        key.1
    );
    map.insert(
        key.clone(),
        Entry {
            hook,
            fired: Arc::clone(&fired),
            id,
        },
    );
    REGISTERED.fetch_add(1, Ordering::SeqCst);
    Guard { key, id, fired }
}

/// Called by the code at a point. Returns at once when no hook is set for it under `scope`.
pub async fn hit(point: &'static str, scope: &str) -> Result<(), Error> {
    if REGISTERED.load(Ordering::SeqCst) == 0 {
        return Ok(());
    }
    // Cloned out so the lock is not held across the hook, which may take as long as a test wants.
    let found = registry()
        .as_ref()
        .and_then(|map| map.get(&(point, scope.to_owned())))
        .map(|entry| (Arc::clone(&entry.hook), Arc::clone(&entry.fired)));
    let Some((hook, fired)) = found else {
        return Ok(());
    };
    fired.fetch_add(1, Ordering::SeqCst);
    hook().await
}
