//! Build the canonical next-epoch assignment candidate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::cert::{
    AssignmentGroupPayload, ASSIGNMENT_TREE_HEIGHT, hash_assignment_group_payload,
};
use tape_core::encoding::ErasureCode;
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::{BlacklistEntry, Member, Peer};
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::merkle::MerkleLeafTree;
use tape_crypto::{Address, Hash};
use tape_protocol::{Api, ProtocolState};
use tape_spooler::assign::{Candidate, assign_spools};
use tape_store::TapeStore;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::blacklist::{BlacklistEntries, blacklist_entries_for_assignment};
use crate::features::assignment::size::{ActiveTrackFootprint, GroupWeights, group_weights};

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
    let profile = ctx.params().profile;
    let task = tokio::task::spawn_blocking(move || {
        build_assignment_blocking(store.as_ref(), &state, profile)
    });

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("build_assignment task join: {e}")))?,
        _ = cancel.cancelled() =>
            Ok(None),
    }
}

// Assemble the candidate once the next epoch and committee are known.
//
// The spooler works on a packed layout: n holders per group with no gaps. The
// chain's arrays are MAX_GROUP_SIZE wide, with positions at or past n left
// zeroed, so owners are packed on the way in and payloads padded on the way out.
pub fn build_assignment_blocking<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    profile: ErasureCode,
) -> Result<Option<AssignmentCandidate>, NodeError> {
    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return Ok(None);
    };
    let Some(next_committee) = state.next_committee.as_deref() else {
        return Ok(None);
    };
    // A group puts each of its n spools on its own member.
    if next_committee.len() < usize::from(profile.n()) {
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

    let weights = match group_weights(store, state.epoch(), next_epoch.id, target_groups, profile)
        .map_err(|e| NodeError::Store(format!("assignment size calculation: {e}")))?
    {
        GroupWeights::Ready(weights) => weights,
        GroupWeights::Pending { track } => {
            warn!(%track, "assignment: skipping the round until the track's blob metadata lands");
            return Ok(None);
        }
    };

    let owners = current_owners(state, target_groups, profile)?;
    let candidates = assignment_candidates(next_committee, &state.peers)?;
    let assignment =
        assign_spools(&weights.sizes, &owners, &candidates, &next_epoch.nonce, profile)
            .map_err(|e| NodeError::Store(format!("assignment spooler: {e:?}")))?;

    let Some(blacklisted) = blacklist_weights(
        store,
        next_committee,
        &assignment,
        target_groups,
        state.epoch(),
        next_epoch.id,
        &weights.tracks,
        profile,
    )? else {
        return Ok(None);
    };

    let payloads = group_payloads(
        next_epoch.id,
        &assignment,
        next_committee,
        &weights.sizes,
        &blacklisted,
        profile,
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

// Pack current group ownership into the spooler input format: the first n
// spools of every group, back to back. A group's zeroed positions past n are
// left out, so the spooler has no empty slot there to fill.
fn current_owners(
    state: &ProtocolState,
    target_groups: usize,
    profile: ErasureCode,
) -> Result<Vec<Option<Address>>, NodeError> {
    let width = usize::from(profile.n());
    let mut owners = vec![None; target_groups * width];
    for group in &state.current.groups {
        let group_index = usize::try_from(group.id.0)
            .map_err(|_| NodeError::Store("assignment group index overflow".into()))?;
        if group_index >= target_groups {
            return Err(NodeError::Store(format!(
                "assignment current group {} exceeds target group count {target_groups}",
                group.id.0
            )));
        }

        let start = group_index * width;
        for (offset, spool) in group.spools.iter().take(width).enumerate() {
            if spool.node != Address::default() {
                owners[start + offset] = Some(spool.node);
            }
        }
    }
    Ok(owners)
}

// Pair each next-committee member with the capacity it set, which its peer
// entry carries from join.
fn assignment_candidates(
    members: &[Member],
    peers: &[Peer],
) -> Result<Vec<Candidate>, NodeError> {
    let capacities: HashMap<Address, StorageUnits> = peers
        .iter()
        .map(|peer| (peer.node, peer.preferences.node_capacity))
        .collect();

    let mut candidates = Vec::with_capacity(members.len());
    for member in members {
        let capacity = *capacities.get(&member.node).ok_or_else(|| {
            NodeError::Store(format!("assignment member {} has no peer entry", member.node))
        })?;
        candidates.push(Candidate {
            node: member.node,
            stake: member.stake,
            capacity,
        });
    }
    Ok(candidates)
}

// Convert spooler output into payloads the program can verify. The packed
// assignment holds n nodes per group. A payload's arrays are MAX_GROUP_SIZE
// wide, and positions at or past n stay zero.
fn group_payloads(
    target_epoch: EpochNumber,
    assignment: &[Address],
    members: &[Member],
    sizes: &[StorageUnits],
    blacklisted_by_group: &[[StorageUnits; MAX_GROUP_SIZE]],
    profile: ErasureCode,
) -> Result<Vec<AssignmentGroupPayload>, NodeError> {
    let width = usize::from(profile.n());
    let member_indices = members
        .iter()
        .enumerate()
        .map(|(index, member)| (member.node, index as u64))
        .collect::<HashMap<_, _>>();
    let mut payloads = Vec::with_capacity(assignment.len() / width);

    for (group_index, chunk) in assignment.chunks_exact(width).enumerate() {
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

        let mut indices = [0u64; MAX_GROUP_SIZE];
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
    // One fold serves the root and every group proof.
    let tree = MerkleLeafTree::new(&leaves, ASSIGNMENT_TREE_HEIGHT)
        .map_err(|e| NodeError::Store(format!("assignment tree: {e:?}")))?;
    let root = tree.root();

    let mut groups = Vec::with_capacity(payloads.len());
    for (index, payload) in payloads.into_iter().enumerate() {
        if payload.group.0 as usize != index {
            return Err(NodeError::Store(format!(
                "assignment group {} is not the {index}th leaf",
                payload.group.0
            )));
        }

        let proof = tree
            .proof_at_n::<ASSIGNMENT_TREE_HEIGHT>(index)
            .map_err(|e| NodeError::Store(format!("assignment proof: {e:?}")))?;

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
    profile: ErasureCode,
) -> Result<Option<Vec<[StorageUnits; MAX_GROUP_SIZE]>>, NodeError> {
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

    let width = usize::from(profile.n());
    let mut blacklisted = vec![[StorageUnits::zero(); MAX_GROUP_SIZE]; target_groups];
    for (group_index, chunk) in assignment.chunks_exact(width).enumerate() {
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
    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::{blacklist_pda, TrackAddress};
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{TapeNumber, TrackNumber};
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;
    use tape_api::state::Group;
    use tape_core::cert::verify_assignment_group_payload;
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};

    /// Spools in a tapenet group.
    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

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
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
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
        let entry_hash = entry.key();
        let state = CompressedTrack {
            tape: blacklist,
            key: entry_hash,
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64),
            group: GroupIndex(0),
            value_hash: entry_hash,
        };
        let track_address = state.address();

        store.put_track(track_address, state).unwrap();
        store
            .put_track_data(track_address, track_number, BlobData::Inline(bytes_of(&entry).to_vec()))
            .unwrap();
        store
            .put_object_info(
                track_address,
                track_number,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: tape_core::types::SlotNumber(epoch.0),
                },
            )
            .unwrap();
    }

    fn payload(group: u64, size: u64) -> AssignmentGroupPayload {
        AssignmentGroupPayload::new(
            GroupIndex(group),
            [group; MAX_GROUP_SIZE],
            StorageUnits::from_bytes(size),
            [StorageUnits::from_bytes(0); MAX_GROUP_SIZE],
        )
    }

    // a 3 of 4 network packs four owners a group for the spooler and pads the
    // payload it votes on back out to the chain's array width
    #[test]
    fn small_network_layout() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let nodes: Vec<Address> = (0..4).map(|_| Address::new_unique()).collect();
        let committee: Vec<Member> = nodes.iter().map(|node| member(*node)).collect();

        let mut state = ProtocolState::default();
        for id in 0..2u64 {
            let mut group = Group { id: GroupIndex(id), ..Group::zeroed() };
            for (position, node) in nodes.iter().enumerate() {
                group.spools[position].node = *node;
            }
            state.current.groups.push(group);
        }

        let owners = current_owners(&state, 2, profile).expect("owners");
        let expected: Vec<Option<Address>> = nodes.iter().chain(&nodes).copied().map(Some).collect();
        assert_eq!(owners, expected);

        let assignment: Vec<Address> = owners.into_iter().flatten().collect();
        let sizes = [StorageUnits::from_bytes(64); 2];
        let blacklisted = [[StorageUnits::zero(); MAX_GROUP_SIZE]; 2];
        let payloads =
            group_payloads(EpochNumber(5), &assignment, &committee, &sizes, &blacklisted, profile)
                .expect("payloads");

        assert_eq!(payloads.len(), 2);
        for payload in &payloads {
            assert_eq!(payload.member_indices[..4], [0, 1, 2, 3]);
            assert!(payload.member_indices[4..].iter().all(|index| *index == 0));
        }
    }

    #[test]
    fn candidates_carry_peer_capacity() {
        let node = Address::new_unique();
        let mut peer = Peer::new(node);
        peer.preferences.node_capacity = StorageUnits::tb(10);

        let candidates = assignment_candidates(&[member(node)], &[peer]).expect("candidates");

        assert_eq!(candidates[0].capacity, StorageUnits::tb(10));
    }

    #[test]
    fn candidates_need_peer_entries() {
        let result = assignment_candidates(&[member(Address::new_unique())], &[]);
        assert!(result.is_err());
    }

    // Every group proof has to verify against the root the same call returned,
    // through the same function finalize_group runs on chain rather than through
    // the primitive under it. `verify_assignment_group_payload` derives the leaf
    // and the index from the payload itself, so a proof built at the wrong index
    // fails here where a hand-passed index would have hidden it.
    #[test]
    fn group_proofs_verify_against_the_root() {
        for count in 1..=8u64 {
            let payloads: Vec<_> = (0..count).map(|g| payload(g, 1024 * (g + 1))).collect();
            let (root, groups) = group_candidates(payloads).expect("candidates build");

            assert_eq!(groups.len() as u64, count);
            for (index, candidate) in groups.iter().enumerate() {
                assert_eq!(candidate.group, GroupIndex(index as u64));
                assert!(
                    verify_assignment_group_payload(&root, &candidate.payload, &candidate.proof),
                    "group {index} of {count} failed to verify"
                );
            }

            // A proof from another group must not verify, or the check above
            // would pass on any proof at all.
            if let [first, second, ..] = groups.as_slice() {
                assert!(!verify_assignment_group_payload(
                    &root,
                    &first.payload,
                    &second.proof
                ));
            }
        }
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
        let mut assignment = vec![other; WIDTH];
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
            TAPENET_PROFILE,
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
        let assignment = vec![node; WIDTH];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
            TAPENET_PROFILE,
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
        let entry = BlacklistEntry::track(track);
        let state = CompressedTrack {
            tape: blacklist,
            key: entry.key(),
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64),
            group: GroupIndex(0),
            value_hash: entry.key(),
        };
        let track_address = state.address();

        store.put_track(track_address, state).unwrap();
        store
            .put_object_info(
                track_address,
                track_number,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: EpochNumber(8),
                    certified_epoch: Some(EpochNumber(8)),
                    slot: tape_core::types::SlotNumber(8),
                },
            )
            .unwrap();

        let active_tracks = vec![active_track(track, tape, GroupIndex(0), 100)];
        let assignment = vec![node; WIDTH];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
            TAPENET_PROFILE,
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
        let assignment = vec![node; WIDTH];
        let committee = vec![member(node)];

        let blacklisted = blacklist_weights(
            &store,
            &committee,
            &assignment,
            1,
            EpochNumber(10),
            target_epoch,
            &active_tracks,
            TAPENET_PROFILE,
        )
        .unwrap()
        .unwrap();

        assert_eq!(blacklisted[0][0], StorageUnits::zero());
    }
}
