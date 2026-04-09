//! Peer BLS signature collection for snapshot group certification.
//!
//! Iterates group peers, requests their BLS signatures for a snapshot
//! chunk blob hash, and aggregates them until a spool-weighted
//! supermajority is reached.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytemuck::Zeroable;
use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{CommitteeBitmap, EpochNumber, NodeId};
use tape_crypto::hash::Hash;
use tape_protocol::api::SignSnapshotReq;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tracing::{debug, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Aggregated BLS signatures for a single snapshot group.
pub struct CollectedSignatures {
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
    pub signing_epoch: EpochNumber,
}

/// Collects BLS signatures from group peers for a snapshot chunk.
///
/// Returns `Some` when a spool-weighted supermajority is reached,
/// `None` if too few peers respond or the threshold cannot be met.
pub async fn collect_group_signatures<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    blob_hash: Hash,
) -> Result<Option<CollectedSignatures>, NodeError> {
    let state = context.state();
    let signing_epoch = state.epoch;

    let epoch_info = context
        .store
        .get_epoch_info(epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_info({epoch}): {e}")))?
        .ok_or_else(|| NodeError::Store(format!("no epoch info for {epoch}")))?;

    let message = SnapshotMessage::new(
        epoch,
        signing_epoch,
        group,
        blob_hash,
        epoch_info.parent_epoch,
    );
    let message_bytes = message.to_bytes();

    // Build deduplicated peer list: NodeId -> member_index.
    // A node may own multiple spools in the same group; we only ask once.
    let peers = state.group_peers(group);
    let mut peer_members: BTreeMap<NodeId, usize> = BTreeMap::new();
    for (_, node_id) in &peers {
        if !peer_members.contains_key(node_id) {
            if let Some((member_index, _)) = state.find_member(*node_id) {
                peer_members.insert(*node_id, member_index);
            }
        }
    }

    let mut bitmap = CommitteeBitmap::zeroed();
    let mut partials: Vec<BlsSignature> = Vec::new();

    // Sign locally first (free, no network call).
    let my_node_id = context.node_id();
    if let Some(&member_index) = peer_members.get(&my_node_id) {
        match context.bls_sign(&message_bytes) {
            Ok(signature) => {
                bitmap.set(member_index);
                partials.push(signature);

                let weight = state.spools.group_weight(group, &bitmap);
                if is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
                    return aggregate_and_return(&partials, bitmap, signing_epoch);
                }
            }
            Err(error) => {
                warn!(epoch = epoch.0, group = group.0, ?error, "local bls sign failed");
            }
        }
    }

    // Collect from remote peers.
    let request = SignSnapshotReq {
        epoch,
        group,
        blob_hash,
    };

    for (&node_id, &member_index) in &peer_members {
        if node_id == my_node_id {
            continue;
        }

        match context.api.sign_snapshot(node_id, &request).await {
            Ok(response) => {
                if response.epoch != signing_epoch {
                    debug!(
                        epoch = epoch.0,
                        group = group.0,
                        peer = node_id.0,
                        expected = signing_epoch.0,
                        got = response.epoch.0,
                        "epoch mismatch, skipping signature"
                    );
                    continue;
                }

                bitmap.set(member_index);
                partials.push(response.signature);

                let weight = state.spools.group_weight(group, &bitmap);
                if is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
                    return aggregate_and_return(&partials, bitmap, signing_epoch);
                }
            }
            Err(error) => {
                debug!(
                    epoch = epoch.0,
                    group = group.0,
                    peer = node_id.0,
                    ?error,
                    "peer signature request failed"
                );
            }
        }
    }

    debug!(
        epoch = epoch.0,
        group = group.0,
        collected = partials.len(),
        "supermajority not reached"
    );
    Ok(None)
}

fn aggregate_and_return(
    partials: &[BlsSignature],
    bitmap: CommitteeBitmap,
    signing_epoch: EpochNumber,
) -> Result<Option<CollectedSignatures>, NodeError> {
    let signature = BlsSignature::aggregate(partials)
        .map_err(|e| NodeError::Store(format!("bls aggregate: {e:?}")))?;

    Ok(Some(CollectedSignatures {
        bitmap,
        signature,
        signing_epoch,
    }))
}
