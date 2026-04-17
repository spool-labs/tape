//! Shared helpers for snapshot group membership, partial verification, and
//! store-backed quorum aggregation.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::min_correct;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::cert::{SnapshotSignMessage, SnapshotWriteMessage};
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber, NodeId, SpoolGroupBitmap};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::SnapshotOps;

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub struct GroupPeer {
    pub node_id: NodeId,
    pub bitmap_index: u16,
    pub pubkey: BlsPubkey,
}

pub struct AggregatedQuorum {
    pub bitmap: SpoolGroupBitmap,
    pub signature: BlsSignature,
}

pub fn quorum_threshold() -> usize {
    min_correct(SPOOL_GROUP_SIZE as u64) as usize
}

pub fn is_current_snapshot_epoch(state: &ProtocolState, snapshot_epoch: EpochNumber) -> bool {
    state.epoch.0 == snapshot_epoch.0.saturating_add(1)
}

pub fn bitmap_index_in_group(
    state: &ProtocolState,
    group: SpoolGroup,
    node_id: NodeId,
) -> Option<u16> {
    state
        .group_peers(group)
        .into_iter()
        .find(|(_, peer_id)| *peer_id == node_id)
        .and_then(|(spool, _)| group.slice_of(spool))
        .map(|idx| idx as u16)
}

pub fn group_peer_by_index(
    state: &ProtocolState,
    group: SpoolGroup,
    bitmap_index: u16,
) -> Option<GroupPeer> {
    let spool = group.spool_at(bitmap_index as usize);
    let node_id = state.spool_owner(spool)?;
    let (_, member) = state.find_member(node_id)?;
    Some(GroupPeer {
        node_id,
        bitmap_index,
        pubkey: member.key,
    })
}

pub fn group_peers(
    state: &ProtocolState,
    group: SpoolGroup,
) -> Vec<GroupPeer> {
    state
        .group_peers(group)
        .into_iter()
        .filter_map(|(spool, node_id)| {
            let bitmap_index = group.slice_of(spool)? as u16;
            let (_, member) = state.find_member(node_id)?;
            Some(GroupPeer {
                node_id,
                bitmap_index,
                pubkey: member.key,
            })
        })
        .collect()
}

pub fn verify_partial(
    pubkey: &BlsPubkey,
    message: &[u8],
    signature: &BlsSignature,
) -> bool {
    signature
        .verify_aggregate(message, std::slice::from_ref(pubkey))
        .is_ok()
}

pub fn aggregate_write_quorum<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
) -> Result<Option<AggregatedQuorum>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let Some(artifact) = ctx
        .store
        .get_snapshot_artifact(epoch, group, chunk)
        .map_err(|e| NodeError::Store(format!("get_snapshot_artifact({epoch},{group},{chunk}): {e}")))?
    else {
        return Ok(None);
    };

    let message =
        SnapshotWriteMessage::new(epoch, group, chunk, artifact.blob.get_hash()).to_bytes();
    aggregate_quorum(
        &ctx.state(),
        group,
        &message,
        ctx.store
            .iter_snapshot_write_sigs(epoch, group, chunk)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_write_sigs({epoch},{group},{chunk}): {e}")))?,
    )
}

pub fn aggregate_finalize_quorum<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
) -> Result<Option<AggregatedQuorum>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let message = SnapshotSignMessage::new(epoch, group).to_bytes();
    aggregate_quorum(
        &ctx.state(),
        group,
        &message,
        ctx.store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs({epoch},{group}): {e}")))?,
    )
}

fn aggregate_quorum(
    state: &ProtocolState,
    group: SpoolGroup,
    message: &[u8],
    partials: Vec<(u16, BlsSignature)>,
) -> Result<Option<AggregatedQuorum>, NodeError> {
    let mut valid = Vec::with_capacity(partials.len());
    let mut pubkeys = Vec::with_capacity(partials.len());

    for (bitmap_index, signature) in partials {
        let Some(peer) = group_peer_by_index(state, group, bitmap_index) else {
            continue;
        };
        if verify_partial(&peer.pubkey, message, &signature) {
            valid.push((bitmap_index as usize, signature));
            pubkeys.push(peer.pubkey);
        }
    }

    if valid.len() < quorum_threshold() {
        return Ok(None);
    }

    valid.sort_unstable_by_key(|(bitmap_index, _)| *bitmap_index);

    let indices: Vec<usize> = valid.iter().map(|(bitmap_index, _)| *bitmap_index).collect();
    let signatures: Vec<BlsSignature> = valid.iter().map(|(_, signature)| *signature).collect();
    let aggregated = BlsSignature::aggregate(&signatures)
        .map_err(|e| NodeError::Store(format!("aggregate snapshot quorum: {e:?}")))?;
    let signers: Vec<BlsPubkey> = indices
        .iter()
        .filter_map(|bitmap_index| group_peer_by_index(state, group, *bitmap_index as u16).map(|peer| peer.pubkey))
        .collect();

    aggregated
        .verify_aggregate(message, &signers)
        .map_err(|e| NodeError::Store(format!("verify aggregated snapshot quorum: {e:?}")))?;

    Ok(Some(AggregatedQuorum {
        bitmap: SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE),
        signature: aggregated,
    }))
}
