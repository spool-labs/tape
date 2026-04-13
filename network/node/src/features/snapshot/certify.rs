//! Snapshot group certification submission.
//!
//! For each spool group this node is responsible for, collects peer
//! signatures and submits `CertifySnapshotGroup` on-chain. Races with
//! other nodes are handled gracefully via `is_already_done()`.

use std::collections::BTreeSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::snapshot::info::SnapshotGroupStatus;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tracing::{debug, info, warn};

use crate::chain::submit_certify_snapshot_group;
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::core::error::NodeError;
use crate::features::snapshot::signing::collect_group_signatures;

/// Certifies all snapshot groups this node is responsible for.
///
/// Skips groups that are already certified on-chain or that lack local
/// build artifacts. Submission failures are logged, not propagated.
pub async fn certify_snapshot_groups<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    let my_groups = local_groups(context);
    if my_groups.is_empty() {
        return Ok(());
    }

    for group in my_groups {
        let snapshot = context
            .store
            .get_snapshot_info(epoch)
            .map_err(|e| NodeError::Store(format!("get_snapshot_info({epoch}): {e}")))?;

        let Some(snapshot) = snapshot else {
            continue;
        };
        let snapshot_group = *snapshot.group(group);

        match snapshot_group.status {
            SnapshotGroupStatus::CertifiedOnChain | SnapshotGroupStatus::Missing => continue,
            SnapshotGroupStatus::Built => {}
        }

        let blob_hash = snapshot_group.blob.get_hash();
        let cert = match collect_group_signatures(context, epoch, group, blob_hash).await? {
            Some(cert) => cert,
            None => {
                debug!(epoch = epoch.0, group = group.0, "no supermajority, skipping group");
                continue;
            }
        };

        let result = submit_certify_snapshot_group(
            context,
            epoch,
            group,
            &snapshot_group.blob,
            &cert,
        )
        .await;

        match classify_tx(result) {
            TxOutcome::Confirmed(txid) => {
                info!(epoch = epoch.0, group = group.0, ?txid, "snapshot group certified");
            }
            TxOutcome::Program(error) if error.is_already_done() => {
                debug!(epoch = epoch.0, group = group.0, "snapshot group already sealed");
            }
            TxOutcome::Program(error) => {
                warn!(epoch = epoch.0, group = group.0, ?error, "certify program error");
            }
            TxOutcome::Transport(error) => {
                warn!(epoch = epoch.0, group = group.0, ?error, "certify transport error");
            }
        }
    }

    Ok(())
}

/// Returns the deduplicated sorted set of spool groups this node owns.
fn local_groups<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> BTreeSet<SpoolGroup> {
    context
        .my_spools()
        .into_iter()
        .map(SpoolGroup::of)
        .collect()
}
