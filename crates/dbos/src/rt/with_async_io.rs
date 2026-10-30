//! Timers for the runtimes without their own: smol and `async-global-executor` both use
//! `async-io`'s, as `sqlx` does on them.

use std::time::Instant;

pub(super) async fn sleep_until(deadline: Instant) {
    async_io::Timer::at(deadline).await;
}
