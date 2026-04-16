//! Collects a 14-of-20 BLS quorum over a given message from the members of
//! one spool group. Shared by the `write` and `finalize` flows — they only
//! differ in which message they sign, which peer API they call, and which
//! on-chain instruction they submit afterwards.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::bft::min_correct;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{NodeId, SpoolGroupBitmap};
use tape_protocol::api::ApiError;
use tape_protocol::{Api, ProtocolState};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::context::NodeContext;

/// Max concurrent peer sign requests per quorum collection.
const MAX_CONCURRENT_PEERS: usize = 8;

/// Single-peer sign response as consumed by [`collect`]. Both the write-sig
/// and finalize-sig API responses project into this minimal shape.
pub(super) struct PeerSig {
    pub node_id: NodeId,
    pub signature: BlsSignature,
}

/// Type-erased per-peer call. Given a `NodeId` and a cancellation token,
/// returns a future resolving to that peer's `PeerSig` or an `ApiError`.
pub(super) type PerPeer = Arc<
    dyn Fn(NodeId, CancellationToken) -> Pin<Box<dyn Future<Output = Result<PeerSig, ApiError>> + Send>>
        + Send
        + Sync,
>;

/// Outcome of a quorum collection, ready to feed into a submit instruction.
pub(super) struct Quorum {
    pub bitmap: SpoolGroupBitmap,
    pub signature: BlsSignature,
}

/// Seed with the local signature, fan out peer calls up to
/// [`MAX_CONCURRENT_PEERS`] at a time, verify each response against its
/// sender's BLS pubkey, and return the aggregated signature + bitmap once
/// the 14-of-20 threshold is reached.
///
/// Returns `None` if the local node is not in the group, the local sign
/// fails, the cancellation token fires, or the group cannot reach quorum.
pub(super) async fn collect<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    group: SpoolGroup,
    message: &[u8],
    per_peer: PerPeer,
    cancel: CancellationToken,
    label: &'static str,
) -> Option<Quorum>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if cancel.is_cancelled() {
        return None;
    }

    let quorum_threshold = min_correct(SPOOL_GROUP_SIZE as u64) as usize;
    let my_node_id = ctx.node_id();

    let my_sig = match ctx.bls_sign(message) {
        Ok(sig) => sig,
        Err(error) => {
            warn!(
                ?error,
                label,
                group = group.0,
                "snapshot quorum: local bls_sign failed"
            );
            return None;
        }
    };

    let state = ctx.state();
    let Some(my_local_idx) = local_index_in_group(&state, group, my_node_id) else {
        warn!(
            label,
            group = group.0,
            "snapshot quorum: local node not in group — skipping"
        );
        return None;
    };

    let mut signatures: Vec<(usize, BlsSignature)> = Vec::with_capacity(SPOOL_GROUP_SIZE);
    signatures.push((my_local_idx, my_sig));

    if signatures.len() >= quorum_threshold {
        return finalize(signatures);
    }

    let peers = other_group_peers(&state, group, my_node_id);
    let mut tasks: JoinSet<(PeerEntry, Result<PeerSig, ApiError>)> = JoinSet::new();
    let mut peers_iter = peers.into_iter();

    for peer in peers_iter.by_ref().take(MAX_CONCURRENT_PEERS) {
        spawn_peer(&mut tasks, peer, &per_peer, cancel.clone());
    }

    while let Some(joined) = tasks.join_next().await {
        if cancel.is_cancelled() {
            tasks.abort_all();
            return None;
        }

        let (peer, result) = match joined {
            Ok(pair) => pair,
            Err(error) => {
                warn!(
                    ?error,
                    label,
                    group = group.0,
                    "snapshot quorum: peer task panicked"
                );
                continue;
            }
        };

        match result {
            Ok(res) if res.node_id != peer.node_id => {
                warn!(
                    expected = peer.node_id.0,
                    got = res.node_id.0,
                    label,
                    group = group.0,
                    "snapshot quorum: peer node_id mismatch"
                );
            }
            Ok(res) if !verify_peer_sig(&peer.pubkey, message, &res.signature) => {
                warn!(
                    node_id = peer.node_id.0,
                    label,
                    group = group.0,
                    "snapshot quorum: peer signature failed verification"
                );
            }
            Ok(res) => {
                signatures.push((peer.local_idx, res.signature));
                if signatures.len() >= quorum_threshold {
                    tasks.abort_all();
                    return finalize(signatures);
                }
            }
            Err(error) => {
                debug!(
                    node_id = peer.node_id.0,
                    error = %error,
                    label,
                    group = group.0,
                    "snapshot quorum: peer sig request failed"
                );
            }
        }

        if let Some(next) = peers_iter.next() {
            spawn_peer(&mut tasks, next, &per_peer, cancel.clone());
        }
    }

    if signatures.len() < quorum_threshold {
        warn!(
            have = signatures.len(),
            need = quorum_threshold,
            label,
            group = group.0,
            "snapshot quorum: insufficient signatures"
        );
        return None;
    }

    finalize(signatures)
}

fn spawn_peer(
    tasks: &mut JoinSet<(PeerEntry, Result<PeerSig, ApiError>)>,
    peer: PeerEntry,
    per_peer: &PerPeer,
    cancel: CancellationToken,
) {
    let per_peer = per_peer.clone();
    let node_id = peer.node_id;

    tasks.spawn(async move {
        let result = per_peer(node_id, cancel).await;
        (peer, result)
    });
}

fn finalize(signatures: Vec<(usize, BlsSignature)>) -> Option<Quorum> {
    let indices: Vec<usize> = signatures.iter().map(|(i, _)| *i).collect();
    let sigs: Vec<BlsSignature> = signatures.iter().map(|(_, s)| *s).collect();
    let signature = BlsSignature::aggregate(&sigs).ok()?;
    let bitmap = SpoolGroupBitmap::from_indices(&indices, SPOOL_GROUP_SIZE);
    Some(Quorum { bitmap, signature })
}

struct PeerEntry {
    node_id: NodeId,
    local_idx: usize,
    pubkey: BlsPubkey,
}

fn local_index_in_group(
    state: &ProtocolState,
    group: SpoolGroup,
    node_id: NodeId,
) -> Option<usize> {
    state
        .group_peers(group)
        .into_iter()
        .find(|(_, peer_id)| *peer_id == node_id)
        .and_then(|(spool, _)| group.slice_of(spool))
        .map(|s| s as usize)
}

fn other_group_peers(
    state: &ProtocolState,
    group: SpoolGroup,
    me: NodeId,
) -> Vec<PeerEntry> {
    state
        .group_peers(group)
        .into_iter()
        .filter(|(_, node_id)| *node_id != me)
        .filter_map(|(spool, node_id)| {
            let local_idx = group.slice_of(spool)? as usize;
            let (_, member) = state.find_member(node_id)?;
            Some(PeerEntry {
                node_id,
                local_idx,
                pubkey: member.key,
            })
        })
        .collect()
}

fn verify_peer_sig(pubkey: &BlsPubkey, message: &[u8], signature: &BlsSignature) -> bool {
    signature
        .verify_aggregate(message, std::slice::from_ref(pubkey))
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::CommitteeMember;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::EpochNumber;

    fn state_with_node_0_in_group_0() -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(7);
        state.committee = vec![
            CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1_000)),
            CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1_000)),
        ];
        let mut spools = [1u8; SPOOL_COUNT];
        for pos in 0..SPOOL_GROUP_SIZE {
            spools[pos] = 0;
        }
        state.spools = SpoolAssignment::new(spools);
        state
    }

    #[test]
    fn local_index_finds_self() {
        let state = state_with_node_0_in_group_0();
        assert_eq!(local_index_in_group(&state, SpoolGroup(0), NodeId(0)), Some(0));
    }

    #[test]
    fn local_index_returns_none_if_absent() {
        let state = state_with_node_0_in_group_0();
        assert!(local_index_in_group(&state, SpoolGroup(0), NodeId(42)).is_none());
    }

    #[test]
    fn other_peers_excludes_self() {
        let state = state_with_node_0_in_group_0();
        // Node 0 holds every position in group 0 here, so no "other" peers.
        assert!(other_group_peers(&state, SpoolGroup(0), NodeId(0)).is_empty());
    }
}
