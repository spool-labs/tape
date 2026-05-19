//! Build the canonical next-epoch assignment candidate.

use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::{
    AssignmentGroupPayload, ASSIGNMENT_TREE_HEIGHT, hash_assignment_group_payload,
};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::Member;
use tape_core::types::{EpochNumber, SpoolCount, StorageUnits};
use tape_crypto::merkle::{create_proof_from_leaf_hashes, root_from_leaf_hashes};
use tape_crypto::{Address, Hash};
use tape_protocol::{Api, ProtocolState};
use tape_spooler::migrate_dhondt;
use tape_store::TapeStore;
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::assignment::size::group_sizes;

#[derive(Debug, Clone)]
pub struct AssignmentCandidate {
    pub voting_epoch: EpochNumber,
    pub target_epoch: EpochNumber,
    pub nonce: Hash,
    pub hash: Hash,
    pub groups: Vec<AssignmentGroupCandidate>,
}

#[derive(Debug, Clone)]
pub struct AssignmentGroupCandidate {
    pub group: GroupIndex,
    pub payload: AssignmentGroupPayload,
    pub proof: [Hash; ASSIGNMENT_TREE_HEIGHT],
}

pub async fn build_assignment<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
) -> Result<Option<AssignmentCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let store = ctx.store.clone();
    let task =
        tokio::task::spawn_blocking(move || build_assignment_blocking(store.as_ref(), &state));

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("build_assignment task join: {e}")))?,
        _ = cancel.cancelled() =>
            Ok(None),
    }
}

fn build_assignment_blocking<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
) -> Result<Option<AssignmentCandidate>, NodeError> {
    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return Ok(None);
    };
    let Some(next_committee) = state.next_committee.as_deref() else {
        return Ok(None);
    };

    let target_groups = usize::try_from(state.system.target_group_count)
        .map_err(|_| NodeError::Store("assignment target_group_count overflow".into()))?;
    if target_groups == 0 {
        return Ok(None);
    }
    if target_groups > (1usize << ASSIGNMENT_TREE_HEIGHT) {
        return Err(NodeError::Store(format!(
            "assignment target_group_count {target_groups} exceeds tree capacity"
        )));
    }
    if state.current.groups.len() > target_groups {
        return Err(NodeError::Store(format!(
            "assignment current group count {} exceeds target group count {target_groups}",
            state.current.groups.len()
        )));
    }

    let sizes = assignment_group_sizes(store, state, next_epoch.id, target_groups)?;

    let current_spools = current_spool_owners(state, target_groups)?;
    let spool_count = SpoolCount((target_groups * GROUP_SIZE) as u64);
    let assignment = migrate_dhondt(
        target_groups,
        &current_spools,
        next_committee,
        &next_epoch.nonce,
        spool_count,
    )
    .map_err(|e| NodeError::Store(format!("assignment spooler: {e:?}")))?;

    let peer_indices = peer_index_by_address(state);
    let payloads = assignment_payloads(
        next_epoch.id,
        next_committee,
        &assignment,
        &peer_indices,
        &sizes,
    )?;

    let leaves = payloads
        .iter()
        .map(hash_assignment_group_payload)
        .collect::<Vec<_>>();
    let root = root_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(&leaves);

    let mut groups = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let proof = create_proof_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(
            &leaves,
            payload.group.0 as usize,
        )
        .map_err(|e| NodeError::Store(format!("assignment proof: {e:?}")))?;
        let proof = proof
            .try_into()
            .map_err(|_| NodeError::Store("assignment proof length mismatch".into()))?;

        groups.push(AssignmentGroupCandidate {
            group: payload.group,
            payload,
            proof,
        });
    }

    Ok(Some(AssignmentCandidate {
        voting_epoch: state.epoch(),
        target_epoch: next_epoch.id,
        nonce: next_epoch.nonce,
        hash: root,
        groups,
    }))
}

fn current_spool_owners(
    state: &ProtocolState,
    target_groups: usize,
) -> Result<Vec<Option<Address>>, NodeError> {
    let mut owners = vec![None; target_groups * GROUP_SIZE];
    for group in &state.current.groups {
        let group_index = usize::try_from(group.id.0)
            .map_err(|_| NodeError::Store("assignment group index overflow".into()))?;
        if group_index >= target_groups {
            return Err(NodeError::Store(format!(
                "assignment current group {} exceeds target group count {target_groups}",
                group.id.0
            )));
        }

        let start = group_index * GROUP_SIZE;
        for (offset, spool) in group.spools.iter().enumerate() {
            if spool.node != Address::default() {
                owners[start + offset] = Some(spool.node);
            }
        }
    }
    Ok(owners)
}

fn peer_index_by_address(state: &ProtocolState) -> HashMap<Address, u64> {
    state
        .peers
        .iter()
        .enumerate()
        .map(|(index, peer)| (peer.node, index as u64))
        .collect()
}

fn assignment_payloads(
    target_epoch: EpochNumber,
    next_committee: &[Member],
    assignment: &[Address],
    peer_indices: &HashMap<Address, u64>,
    sizes: &[StorageUnits],
) -> Result<Vec<AssignmentGroupPayload>, NodeError> {
    let mut payloads = Vec::with_capacity(assignment.len() / GROUP_SIZE);

    for (group_index, chunk) in assignment.chunks_exact(GROUP_SIZE).enumerate() {
        let group = GroupIndex(group_index as u64);
        let size = *sizes.get(group_index).ok_or_else(|| {
            NodeError::Store(format!(
                "assignment size missing for target epoch {} group {}",
                target_epoch.0, group.0
            ))
        })?;

        let mut indices = [0u64; GROUP_SIZE];
        for (offset, node) in chunk.iter().enumerate() {
            if !next_committee.iter().any(|member| member.node == *node) {
                return Err(NodeError::Store(format!(
                    "assignment spooler emitted node outside next committee: {node}"
                )));
            }
            indices[offset] = *peer_indices.get(node).ok_or_else(|| {
                NodeError::Store(format!("assignment node missing from peer set: {node}"))
            })?;
        }

        payloads.push(AssignmentGroupPayload::new(group, indices, size));
    }

    Ok(payloads)
}

fn assignment_group_sizes<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    target_epoch: EpochNumber,
    target_groups: usize,
) -> Result<Vec<StorageUnits>, NodeError> {
    group_sizes(store, state.epoch(), target_epoch, target_groups)
        .map_err(|e| NodeError::Store(format!("assignment size calculation: {e}")))
}

#[allow(dead_code)]
pub fn build_assignment_with_sizes<F>(
    state: &ProtocolState,
    mut size_for_group: F,
) -> Result<AssignmentCandidate, NodeError>
where
    F: FnMut(GroupIndex) -> Result<StorageUnits, NodeError>,
{
    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return Err(NodeError::Store("assignment next epoch missing".into()));
    };
    let Some(next_committee) = state.next_committee.as_deref() else {
        return Err(NodeError::Store("assignment next committee missing".into()));
    };

    let target_groups = usize::try_from(state.system.target_group_count)
        .map_err(|_| NodeError::Store("assignment target_group_count overflow".into()))?;
    if target_groups == 0 || target_groups > (1usize << ASSIGNMENT_TREE_HEIGHT) {
        return Err(NodeError::Store(format!(
            "assignment target group count {target_groups} is invalid"
        )));
    }

    let current_spools = current_spool_owners(state, target_groups)?;
    let spool_count = SpoolCount((target_groups * GROUP_SIZE) as u64);
    let assignment = migrate_dhondt(
        target_groups,
        &current_spools,
        next_committee,
        &next_epoch.nonce,
        spool_count,
    )
    .map_err(|e| NodeError::Store(format!("assignment spooler: {e:?}")))?;

    let peer_indices = peer_index_by_address(state);
    let mut payloads = Vec::with_capacity(target_groups);
    for (group_index, chunk) in assignment.chunks_exact(GROUP_SIZE).enumerate() {
        let group = GroupIndex(group_index as u64);
        let size = size_for_group(group)?;
        let mut indices = [0u64; GROUP_SIZE];
        for (offset, node) in chunk.iter().enumerate() {
            indices[offset] = *peer_indices.get(node).ok_or_else(|| {
                NodeError::Store(format!("assignment node missing from peer set: {node}"))
            })?;
        }
        payloads.push(AssignmentGroupPayload::new(group, indices, size));
    }

    let leaves = payloads
        .iter()
        .map(hash_assignment_group_payload)
        .collect::<Vec<_>>();
    let root = root_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(&leaves);
    let mut groups = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let proof = create_proof_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(
            &leaves,
            payload.group.0 as usize,
        )
        .map_err(|e| NodeError::Store(format!("assignment proof: {e:?}")))?;
        let proof = proof
            .try_into()
            .map_err(|_| NodeError::Store("assignment proof length mismatch".into()))?;
        groups.push(AssignmentGroupCandidate {
            group: payload.group,
            payload,
            proof,
        });
    }

    Ok(AssignmentCandidate {
        voting_epoch: state.epoch(),
        target_epoch: next_epoch.id,
        nonce: next_epoch.nonce,
        hash: root,
        groups,
    })
}
