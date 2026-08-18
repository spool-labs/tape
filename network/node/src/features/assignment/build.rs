//! Build the canonical next-epoch assignment candidate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::MIN_COMMITTEE_SIZE;
use tape_core::cert::{
    AssignmentGroupPayload, ASSIGNMENT_TREE_HEIGHT, hash_assignment_group_payload,
};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::{BlacklistEntry, Member};
use tape_core::types::{EpochNumber, SpoolCount, StorageUnits};
use tape_crypto::merkle::{create_proof_from_leaf_hashes, root_from_leaf_hashes};
use tape_crypto::{Address, Hash};
use tape_protocol::{Api, ProtocolState};
use tape_spooler::migrate_dhondt;
use tape_store::TapeStore;
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::blacklist::{BlacklistEntries, blacklist_entries_for_assignment};
use crate::features::assignment::size::{ActiveTrackFootprint, group_weights};

#[derive(Debug, Clone)]
pub struct AssignmentCandidate {
    pub voting_epoch: EpochNumber,
    pub target_epoch: EpochNumber,
    pub nonce: Hash,
    pub hash: Hash,
    pub groups: Vec<GroupCandidate>,
}

#[derive(Debug, Clone)]
pub struct GroupCandidate {
    pub group: GroupIndex,
    pub payload: AssignmentGroupPayload,
    pub proof: [Hash; ASSIGNMENT_TREE_HEIGHT],
}

// Build on a blocking worker so assignment work does not pin the async runtime.
pub async fn build_assignment<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: Arc<ProtocolState>,
    cancel: &CancellationToken,
) -> Result<Option<AssignmentCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
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

// Assemble the candidate once the next epoch and committee are known.
pub fn build_assignment_blocking<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
) -> Result<Option<AssignmentCandidate>, NodeError> {
    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return Ok(None);
    };
    let Some(next_committee) = state.next_committee.as_deref() else {
        return Ok(None);
    };
    if next_committee.len() < MIN_COMMITTEE_SIZE {
        return Ok(None);
    }

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

    let weights = group_weights(store, state.epoch(), next_epoch.id, target_groups)
        .map_err(|e| NodeError::Store(format!("assignment size calculation: {e}")))?;

    let owners = current_owners(state, target_groups)?;
    let spool_count = SpoolCount((target_groups * GROUP_SIZE) as u64);
    let assignment = migrate_dhondt(
        target_groups,
        &owners,
        next_committee,
        &next_epoch.nonce,
        spool_count,
    )
    .map_err(|e| NodeError::Store(format!("assignment spooler: {e:?}")))?;

    let Some(blacklisted) = blacklist_weights(
        store,
        next_committee,
        &assignment,
        target_groups,
        state.epoch(),
        next_epoch.id,
        &weights.tracks,
    )? else {
        return Ok(None);
    };

    let payloads = group_payloads(
        next_epoch.id,
        &assignment,
        next_committee,
        &weights.sizes,
        &blacklisted,
    )?;

    let (root, groups) = group_candidates(payloads)?;

    Ok(Some(AssignmentCandidate {
        voting_epoch: state.epoch(),
        target_epoch: next_epoch.id,
        nonce: next_epoch.nonce,
        hash: root,
        groups,
    }))
}

// Flatten current group ownership into the spooler input format.
fn current_owners(
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

// Convert spooler output into payloads the program can verify.
fn group_payloads(
    target_epoch: EpochNumber,
    assignment: &[Address],
    members: &[Member],
    sizes: &[StorageUnits],
    blacklisted_by_group: &[[StorageUnits; GROUP_SIZE]],
) -> Result<Vec<AssignmentGroupPayload>, NodeError> {
    let member_indices = members
        .iter()
        .enumerate()
        .map(|(index, member)| (member.node, index as u64))
        .collect::<HashMap<_, _>>();
    let mut payloads = Vec::with_capacity(assignment.len() / GROUP_SIZE);

    for (group_index, chunk) in assignment.chunks_exact(GROUP_SIZE).enumerate() {
        let group = GroupIndex(group_index as u64);
        let size = *sizes.get(group_index).ok_or_else(|| {
            NodeError::Store(format!(
                "assignment size missing for target epoch {} group {}",
                target_epoch.0, group.0
            ))
        })?;

        let blacklisted = *blacklisted_by_group.get(group_index).ok_or_else(|| {
            NodeError::Store(format!(
                "assignment blacklisted weights missing for target epoch {} group {}",
                target_epoch.0, group.0
            ))
        })?;

        let mut indices = [0u64; GROUP_SIZE];
        for (offset, node) in chunk.iter().enumerate() {
            if blacklisted[offset] > size {
                return Err(NodeError::Store(format!(
                    "assignment blacklisted weight {} exceeds assigned size {} for node {node} group {}",
                    blacklisted[offset].0, size.0, group.0,
                )));
            }
            indices[offset] = *member_indices.get(node).ok_or_else(|| {
                NodeError::Store(format!(
                    "assignment spooler emitted node outside next committee: {node}"
                ))
            })?;
        }

        payloads.push(AssignmentGroupPayload::new(group, indices, size, blacklisted));
    }

    Ok(payloads)
}

// Hash payloads and attach a Merkle proof to each group.
fn group_candidates(
    payloads: Vec<AssignmentGroupPayload>,
) -> Result<(Hash, Vec<GroupCandidate>), NodeError> {
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

        groups.push(GroupCandidate {
            group: payload.group,
            payload,
            proof,
        });
    }

    Ok((root, groups))
}

// Sum blacklisted active track weight for each assigned spool owner.
fn blacklist_weights<Db: Store>(
    store: &TapeStore<Db>,
    next_committee: &[Member],
    assignment: &[Address],
    target_groups: usize,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
    active_tracks: &[ActiveTrackFootprint],
) -> Result<Option<Vec<[StorageUnits; GROUP_SIZE]>>, NodeError> {
    let mut entries: HashMap<Address, HashSet<BlacklistEntry>> = HashMap::new();
    for member in next_committee {
        match blacklist_entries_for_assignment(store, member.node, voting_epoch, target_epoch)? {
            BlacklistEntries::Ready(node_entries) => {
                entries.insert(member.node, node_entries);
            }
            BlacklistEntries::Pending { .. } => return Ok(None),
        }
    }

    let mut tracks = vec![Vec::<ActiveTrackFootprint>::new(); target_groups];
    for track in active_tracks {
        let group_index = usize::try_from(track.group.0)
            .map_err(|_| NodeError::Store("active track group index overflow".into()))?;
        if group_index >= target_groups {
            return Err(NodeError::Store(format!(
                "active track group {} exceeds target group count {target_groups}",
                track.group.0
            )));
        }
        tracks[group_index].push(*track);
    }

    let mut blacklisted = vec![[StorageUnits::zero(); GROUP_SIZE]; target_groups];
    for (group_index, chunk) in assignment.chunks_exact(GROUP_SIZE).enumerate() {
        for (offset, node) in chunk.iter().enumerate() {
            let Some(entries) = entries.get(node) else {
                continue;
            };

            let mut amount = StorageUnits::zero();
            for track in &tracks[group_index] {
                let track_entry = BlacklistEntry::track(track.track);
                let tape_entry = BlacklistEntry::tape(track.tape);
                if entries.contains(&track_entry) || entries.contains(&tape_entry) {
                    amount = amount.checked_add(track.footprint).ok_or_else(|| {
                        NodeError::Store(format!(
                            "blacklisted weight overflow for node {node} group {group_index}"
                        ))
                    })?;
                }
            }
            blacklisted[group_index][offset] = amount;
        }
    }

    Ok(Some(blacklisted))
}

#[cfg(test)]
mod tests {
    use core::mem::size_of;

    use bytemuck::{Zeroable, bytes_of};
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::{blacklist_pda, track_pda};
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{TapeNumber, TrackNumber};
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn member(node: Address) -> Member {
        let mut member = Member::zeroed();
        member.node = node;
        member
    }

    fn active_track(
        track: Address,
        tape: Address,
        group: GroupIndex,
        footprint: u64,
    ) -> ActiveTrackFootprint {
        ActiveTrackFootprint {
            track,
            tape,
            group,
            footprint: StorageUnits::from_bytes(footprint),
        }
    }

    fn put_blacklist_tape(
        store: &TapeStore<MemoryStore>,
        node: Address,
        end_epoch: EpochNumber,
    ) -> Address {
        let blacklist = blacklist_pda(node).0;
        store
            .put_tape(
                blacklist,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch,
                    next_track_number: TrackNumber(0),
                },
            )
            .unwrap();
        blacklist
    }

    fn put_blacklist_entry(
        store: &TapeStore<MemoryStore>,
        blacklist: Address,
        number: u64,
        epoch: EpochNumber,
        entry: BlacklistEntry,
    ) {
        let track_number = TrackNumber(number);
        let track_address = track_pda(blacklist, track_number).0;
        let entry_hash = entry.key();
        let state = CompressedTrack {
            tape: blacklist,
            key: entry_hash,
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64),
            group: GroupIndex(0),
            value_hash: entry_hash,
        };

        store.put_track(track_address, state).unwrap();
        store
            .put_track_data(track_address, BlobData::Inline(bytes_of(&entry).to_vec()))
            .unwrap();
        store
            .put_object_info(
                track_address,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: tape_core::types::SlotNumber(epoch.0),
                },
            )
            .unwrap();
    }

    #[test]
    fn dedupes_track_and_tape() {
        let store = test_store();
        let node = Address::new_unique();
        let other = Address::new_unique();
        let tape = Address::new_unique();
        let track_a = Address::new_unique();
        let track_b = Address::new_unique();
        let target_epoch = EpochNumber(10);
        let blacklist = put_blacklist_tape(&store, node, EpochNumber(20));

        put_blacklist_entry(&store, blacklist, 0, EpochNumber(8), BlacklistEntry::track(track_a));
        put_blacklist_entry(&store, blacklist, 1, EpochNumber(8), BlacklistEntry::tape(tape));

        let active_tracks = vec![
            active_track(track_a, tape, GroupIndex(0), 100),
            active_track(track_b, tape, GroupIndex(0), 200),
        ];
        let mut assignment = vec![other; GROUP_SIZE];
        assignment[0] = node;
        let committee = vec![member(node), member(other)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
        )
        .unwrap()
        .unwrap();

        assert_eq!(blacklisted[0][0], StorageUnits::from_bytes(300));
        assert_eq!(blacklisted[0][1], StorageUnits::zero());
    }

    #[test]
    fn expired_tape_zero() {
        let store = test_store();
        let node = Address::new_unique();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let target_epoch = EpochNumber(10);
        let blacklist = put_blacklist_tape(&store, node, target_epoch);

        put_blacklist_entry(&store, blacklist, 0, EpochNumber(8), BlacklistEntry::track(track));

        let active_tracks = vec![active_track(track, tape, GroupIndex(0), 100)];
        let assignment = vec![node; GROUP_SIZE];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
        )
        .unwrap()
        .unwrap();

        assert_eq!(blacklisted[0][0], StorageUnits::zero());
    }

    #[test]
    fn missing_data_defers() {
        let store = test_store();
        let node = Address::new_unique();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let target_epoch = EpochNumber(10);
        let blacklist = put_blacklist_tape(&store, node, EpochNumber(20));
        let track_number = TrackNumber(0);
        let track_address = track_pda(blacklist, track_number).0;
        let entry = BlacklistEntry::track(track);
        let state = CompressedTrack {
            tape: blacklist,
            key: entry.key(),
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64),
            group: GroupIndex(0),
            value_hash: entry.key(),
        };

        store.put_track(track_address, state).unwrap();
        store
            .put_object_info(
                track_address,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: EpochNumber(8),
                    certified_epoch: Some(EpochNumber(8)),
                    slot: tape_core::types::SlotNumber(8),
                },
            )
            .unwrap();

        let active_tracks = vec![active_track(track, tape, GroupIndex(0), 100)];
        let assignment = vec![node; GROUP_SIZE];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
        )
        .unwrap();

        assert!(blacklisted.is_none());
    }

    #[test]
    fn current_epoch_skipped() {
        let store = test_store();
        let node = Address::new_unique();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let target_epoch = EpochNumber(10);
        let blacklist = put_blacklist_tape(&store, node, EpochNumber(20));

        put_blacklist_entry(&store, blacklist, 0, EpochNumber(10), BlacklistEntry::track(track));

        let active_tracks = vec![active_track(track, tape, GroupIndex(0), 100)];
        let assignment = vec![node; GROUP_SIZE];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
        )
        .unwrap()
        .unwrap();

        assert_eq!(blacklisted[0][0], StorageUnits::zero());
    }
}
