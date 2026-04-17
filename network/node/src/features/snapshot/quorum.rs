//! Shared helpers for snapshot group membership, partial verification, and
//! store-backed quorum aggregation.

use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::snapshot_tape_pda;
use tape_core::bft::min_correct;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::cert::{SnapshotSignMessage, SnapshotWriteMessage, SNAPSHOT_SIGN_MESSAGE_SIZE};
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::snapshot::chunk::snapshot_chunk_key;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber, NodeId, SpoolGroupBitmap};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::{SnapshotOps, TapeOps, TrackOps};
use tape_store::types::{SnapshotFinalizeVote, SnapshotWriteVote};

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

pub struct AggregatedWriteQuorum {
    pub value_hash: Hash,
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

pub fn local_write_value_hash<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    bitmap_index: u16,
) -> Result<Option<Hash>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let Some(vote) = ctx
        .store
        .get_snapshot_write_sig(epoch, group, chunk, bitmap_index)
        .map_err(|e| NodeError::Store(format!(
            "get_snapshot_write_sig({epoch},{group},{chunk},{bitmap_index}): {e}"
        )))?
    else {
        return Ok(None);
    };

    let message = SnapshotWriteMessage::from_bytes(&vote.message).ok_or_else(|| {
        NodeError::Store(format!(
            "invalid local snapshot write vote message for ({epoch},{group},{chunk},{bitmap_index})"
        ))
    })?;
    if message.epoch != epoch || message.group != group || message.chunk != chunk {
        return Err(NodeError::Store(format!(
            "local snapshot write vote message mismatch for ({epoch},{group},{chunk},{bitmap_index})"
        )));
    }

    Ok(Some(message.value_hash))
}

pub fn snapshot_written_hashes<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<HashMap<Hash, Hash>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
    let Some(tape) = ctx
        .store
        .get_tape(snapshot_tape)
        .map_err(|e| NodeError::Store(format!("get_tape({snapshot_tape}): {e}")))?
    else {
        return Ok(HashMap::new());
    };

    if tape.next_track_number.0 == 0 {
        return Ok(HashMap::new());
    }

    let tracks = ctx
        .store
        .iter_tracks_by_tape_from(snapshot_tape, None, tape.next_track_number.0 as usize)
        .map_err(|e| NodeError::Store(format!(
            "iter_tracks_by_tape_from({snapshot_tape},None,{}): {e}",
            tape.next_track_number.0
        )))?;

    let mut written = HashMap::with_capacity(tracks.len());
    for track in tracks {
        written.insert(track.key, track.value_hash);
    }
    Ok(written)
}

#[inline]
pub fn snapshot_chunk_hash(
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
) -> Hash {
    snapshot_chunk_key(epoch, group, chunk)
}

pub fn aggregate_write_quorum<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
) -> Result<Option<AggregatedWriteQuorum>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    aggregate_write_votes(
        &ctx.state(),
        epoch,
        group,
        chunk,
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
        message,
        ctx.store
            .iter_snapshot_finalize_sigs(epoch, group)
            .map_err(|e| NodeError::Store(format!("iter_snapshot_finalize_sigs({epoch},{group}): {e}")))?,
    )
}

fn aggregate_write_votes(
    state: &ProtocolState,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    partials: Vec<(u16, SnapshotWriteVote)>,
) -> Result<Option<AggregatedWriteQuorum>, NodeError> {
    let threshold = quorum_threshold();
    let mut buckets: HashMap<Hash, Vec<(usize, BlsSignature)>> = HashMap::new();

    for (bitmap_index, vote) in partials {
        let Some(peer) = group_peer_by_index(state, group, bitmap_index) else {
            continue;
        };
        let Some(message) = SnapshotWriteMessage::from_bytes(&vote.message) else {
            continue;
        };
        if message.epoch != epoch || message.group != group || message.chunk != chunk {
            continue;
        }
        if verify_partial(&peer.pubkey, &vote.message, &vote.signature) {
            buckets
                .entry(message.value_hash)
                .or_default()
                .push((bitmap_index as usize, vote.signature));
        }
    }

    let Some((value_hash, valid)) = buckets
        .into_iter()
        .max_by_key(|(_, signatures)| signatures.len())
    else {
        return Ok(None);
    };
    if valid.len() < threshold {
        return Ok(None);
    }

    let AggregatedQuorum { bitmap, signature } =
        aggregate_verified_partials(state, group, &SnapshotWriteMessage::new(epoch, group, chunk, value_hash).to_bytes(), valid)?;

    Ok(Some(AggregatedWriteQuorum {
        value_hash,
        bitmap,
        signature,
    }))
}

fn aggregate_quorum(
    state: &ProtocolState,
    group: SpoolGroup,
    message: [u8; SNAPSHOT_SIGN_MESSAGE_SIZE],
    partials: Vec<(u16, SnapshotFinalizeVote)>,
) -> Result<Option<AggregatedQuorum>, NodeError> {
    let mut valid = Vec::with_capacity(partials.len());

    for (bitmap_index, vote) in partials {
        let Some(peer) = group_peer_by_index(state, group, bitmap_index) else {
            continue;
        };
        if vote.message != message {
            continue;
        }
        if verify_partial(&peer.pubkey, &vote.message, &vote.signature) {
            valid.push((bitmap_index as usize, vote.signature));
        }
    }

    if valid.len() < quorum_threshold() {
        return Ok(None);
    }

    aggregate_verified_partials(state, group, &message, valid).map(Some)
}

fn aggregate_verified_partials(
    state: &ProtocolState,
    group: SpoolGroup,
    message: &[u8],
    mut valid: Vec<(usize, BlsSignature)>,
) -> Result<AggregatedQuorum, NodeError> {
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

    Ok(AggregatedQuorum {
        bitmap: SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE),
        signature: aggregated,
    })
}
