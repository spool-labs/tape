//! Thread D - Erasure Recovery
//!
//! Scans for missing slices and repairs them using bandwidth-optimal
//! Clay code repair from helper nodes in the same spool group.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_store::ops::SpoolOps;
use tape_store::types::{Pubkey as StorePubkey, SpoolStatus};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::context::NodeContext;

use super::error::RecoveryError;
use super::helpers::resolve_group_helpers;
use super::repair::repair_batch;
use super::scan::{collect_recovering_spools, is_spool_recovery_complete, run_scan};

/// Recovery polling interval.
const REPAIR_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Run the recovery worker loop.
pub async fn run<S: Store>(
    ctx: Arc<NodeContext<S>>,
    cancel: CancellationToken,
) -> Result<(), RecoveryError> {
    info!("Recovery thread starting");

    let mut interval = tokio::time::interval(REPAIR_POLL_INTERVAL);
    let mut failures: HashMap<(SpoolIndex, StorePubkey), u32> = HashMap::new();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Recovery thread shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = run_cycle(&ctx, &mut failures).await {
                    warn!(error = %e, "recovery cycle failed");
                }
            }
        }
    }

    Ok(())
}

/// Single recovery cycle: transition → scan → repair → finalize.
async fn run_cycle<S: Store>(
    ctx: &NodeContext<S>,
    failures: &mut HashMap<(SpoolIndex, StorePubkey), u32>,
) -> Result<(), RecoveryError> {
    let store = &ctx.storage.store;

    // 1. Transition eligible spools to ActiveRecover
    let our_spools = ctx.control_plane.get_our_spools();
    for &spool in &our_spools {
        let status = store.get_spool_status(spool)?;
        match status {
            Some(SpoolStatus::Active) | Some(SpoolStatus::LockedToMove) => continue,
            Some(SpoolStatus::ActiveRecover) => continue,
            None | Some(SpoolStatus::None) | Some(SpoolStatus::ActiveSync) => {
                store.set_spool_status(spool, SpoolStatus::ActiveRecover)?;
                debug!(spool, "transitioned to ActiveRecover");
            }
        }
    }

    // 2. Collect all recovering spools
    let recovering = collect_recovering_spools(store)?;
    if recovering.is_empty() {
        return Ok(());
    }

    // 3. Single-pass scan (runs to completion, ~5s for 1M tracks)
    let scan_result = run_scan(store, &recovering)?;
    if scan_result.enqueued > 0 {
        debug!(
            enqueued = scan_result.enqueued,
            scanned = scan_result.scanned,
            "scan complete"
        );
    }

    // 4. Repair — one batch per spool, grouped by spool group for helper sharing
    let mut by_group: HashMap<u64, Vec<SpoolIndex>> = HashMap::new();
    for &(spool, group) in &recovering {
        by_group.entry(group).or_default().push(spool);
    }

    let insecure = ctx.config.insecure;

    for (group, spools) in &by_group {
        let helpers = match resolve_group_helpers(ctx, spools[0], insecure) {
            Ok(h) => h,
            Err(e) => {
                warn!(group, error = %e, "failed to resolve group helpers");
                continue;
            }
        };

        for &spool in spools {
            if let Err(e) = repair_batch(ctx, spool, &helpers, failures).await {
                warn!(spool, error = %e, "spool repair failed, continuing");
            }
        }
    }

    // 5. Finalize — mark completed spools as Active
    for &(spool, _) in &recovering {
        if is_spool_recovery_complete(store, spool)? {
            store.set_spool_status(spool, SpoolStatus::Active)?;
            store.remove_sync_progress(spool)?;
            info!(spool, "recovery complete, spool now Active");
        }
    }

    Ok(())
}
