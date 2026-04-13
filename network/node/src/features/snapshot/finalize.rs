//! Snapshot epoch finalization.
//!
//! Checks whether all spool groups are certified for an epoch and,
//! if so, submits `FinalizeSnapshotEpoch`. Races with other nodes
//! are handled gracefully via `is_already_done()`.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::snapshot::info::SnapshotStatus;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tracing::{debug, info, warn};

use crate::chain::submit_finalize_snapshot_epoch;
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::core::error::NodeError;

/// Submits epoch finalization if all groups are certified.
///
/// Returns early if the epoch is not yet fully certified or is already
/// finalized. Submission failures are logged, not propagated.
pub async fn try_finalize_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    let epoch_info = context
        .store
        .get_snapshot_info(epoch)
        .map_err(|e| NodeError::Store(format!("get_snapshot_info({epoch}): {e}")))?;

    let Some(info) = epoch_info else {
        return Ok(());
    };

    if info.status == SnapshotStatus::Finalized {
        return Ok(());
    }

    if info.certified_groups.count_ones() < SPOOL_GROUP_COUNT {
        return Ok(());
    }

    let result = submit_finalize_snapshot_epoch(context, epoch).await;

    match classify_tx(result) {
        TxOutcome::Confirmed(txid) => {
            info!(epoch = epoch.0, ?txid, "snapshot epoch finalized");
        }
        TxOutcome::Program(error) if error.is_already_done() => {
            debug!(epoch = epoch.0, "snapshot epoch already finalized");
        }
        TxOutcome::Program(error) => {
            warn!(epoch = epoch.0, ?error, "finalize program error");
        }
        TxOutcome::Transport(error) => {
            warn!(epoch = epoch.0, ?error, "finalize transport error");
        }
    }

    Ok(())
}
