//! Periodic return of free allocator space to the kernel.
//!
//! Ingest churns a large volume of short-lived allocations of widely varying
//! sizes across the runtime's worker threads. glibc serves those from per-thread
//! arenas and, once an arena has grown, keeps its free space for reuse rather
//! than handing it back. The node's resident set therefore settles at the
//! high-water mark of past traffic rather than at what it currently holds: a
//! node at tip with a 110 MB store was measured holding 881 MB, of which 647 MB
//! was free space glibc had not returned.
//!
//! `malloc_trim` walks each arena and releases the free pages it can. It takes
//! each arena's lock in turn, so it is not free, but the walk is measured in
//! microseconds against minutes of interval.
//!
//! Only glibc has this problem and this remedy. Everywhere else the loop is
//! not spawned.

use std::time::Duration;

use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::error::NodeError;

/// How often to hand free arena space back
///
/// Long enough that the walk is irrelevant to throughput, short enough that a
/// burst of traffic does not leave hundreds of megabytes stranded for hours.
const TRIM_INTERVAL: Duration = Duration::from_secs(300);

/// Release free allocator space until cancelled
pub async fn run(cancel: CancellationToken) -> Result<(), NodeError> {
    // An early return reads as a service failure and stops the node
    if !TRIM_SUPPORTED {
        debug!("allocator trim not supported on this platform, skipping");
        cancel.cancelled().await;
        return Ok(());
    }

    let mut ticker = interval(TRIM_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The first tick fires immediately and there is nothing to release yet.
    ticker.tick().await;

    loop {
        select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                let released = trim();
                debug!(released, "allocator trim");
            }
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
const TRIM_SUPPORTED: bool = true;

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
const TRIM_SUPPORTED: bool = false;

/// Ask the allocator to return free arena space. True when it released some.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim() -> bool {
    // Takes each arena lock in turn and releases what it can; no allocation of
    // its own, so it cannot fail.
    unsafe { libc::malloc_trim(0) == 1 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // the loop runs until shutdown on every platform, then stops cleanly
    #[tokio::test]
    async fn runs_until_cancelled() {
        let cancel = CancellationToken::new();
        let mut task = tokio::spawn(run(cancel.clone()));

        let early = tokio::time::timeout(Duration::from_millis(50), &mut task).await;
        assert!(early.is_err(), "the loop returned before shutdown");

        cancel.cancel();
        assert!(task.await.unwrap().is_ok());
    }
}
