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
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::system::{BlacklistEntry, Member};
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::merkle::MerkleLeafTree;
use tape_crypto::{Address, Hash};
use tape_protocol::{Api, ProtocolState};
use tape_spooler::migrate_sainte_lague;
use tape_store::TapeStore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

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

    // The next epoch's group count, above or below the current one, and the
    // virtual groups each of its groups will carry.
    let Some(target) = state.fold_next() else {
        return Ok(None);
    };
    let target_groups = usize::try_from(target.live())
        .map_err(|_| NodeError::Store("assignment target_group_count overflow".into()))?;
    if target_groups > (1usize << ASSIGNMENT_TREE_HEIGHT) {
        return Err(NodeError::Store(format!(
            "assignment target_group_count {target_groups} exceeds tree capacity"
        )));
    }

    let weights = match group_weights(store, state.epoch(), next_epoch.id, target, profile)
        .map_err(|e| NodeError::Store(format!("assignment size calculation: {e}")))?
    {
        GroupWeights::Ready(weights) => weights,
        GroupWeights::Pending { track } => {
            warn!(%track, "assignment: skipping the round until the track's blob metadata lands");
            return Ok(None);
        }
    };

    let owners = current_owners(state, &weights.sizes, profile);
    let assignment =
        migrate_sainte_lague(target_groups, &owners, next_committee, &next_epoch.nonce, profile)
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
    debug!(
        epoch = %next_epoch.id,
        hash = %root,
        tracks = weights.tracks.len(),
        bytes = weights.sizes.iter().map(|size| size.0).sum::<u64>(),
        blacklisted = blacklisted.iter().flatten().map(|units| units.0).sum::<u64>(),
        committee = next_committee.len(),
        "assignment: candidate built"
    );

    Ok(Some(AssignmentCandidate {
        voting_epoch: state.epoch(),
        target_epoch: next_epoch.id,
        nonce: next_epoch.nonce,
        hash: root,
        groups,
    }))
}

// Pack who holds each target group's data now into the spooler input format:
// the first n spools of every group, back to back. A group's zeroed positions
// past n are left out, so the spooler has no empty slot there to fill.
//
// The holder of target group t at position p is whoever holds stored spool
// (t, p) today. For a group that is live now that is its own owner, so a group
// that stays live keeps its nodes when the count drops. For a group that is
// new next epoch it is the owner in the group carrying it today, so a group
// split off with data starts where that data already sits and nothing is
// copied. A new group with no data has nothing to keep in place, so it starts
// empty and gets a fresh fill on nodes of its own.
fn current_owners(
    state: &ProtocolState,
    sizes: &[StorageUnits],
    profile: ErasureCode,
) -> Vec<Option<Address>> {
    let width = usize::from(profile.n());
    let live_groups = state.current.epoch.total_groups;
    let mut owners = vec![None; sizes.len() * width];

    for (target, size) in sizes.iter().enumerate() {
        let group = VirtualGroup(target as u64);
        if group.0 >= live_groups && *size == StorageUnits::zero() {
            continue;
        }

        for position in 0..width {
            owners[target * width + position] = state.spool_owner(group.spool_at(position));
        }
    }

    owners
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

    // Bucketed by the group that carries each track in the target epoch, the
    // same sets the sizes were summed over.
    let mut tracks = vec![Vec::<ActiveTrackFootprint>::new(); target_groups];
    for track in active_tracks {
        let group_index = usize::try_from(track.group.0)
            .map_err(|_| NodeError::Store("active track's group index overflow".into()))?;
        if group_index >= target_groups {
            return Err(NodeError::Store(format!(
                "active track's group {} exceeds target group count {target_groups}",
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
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{SlotNumber, TapeNumber, TrackNumber};
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;
    use crate::features::store::apply::{apply_event, DeletePolicy};
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
            group: VirtualGroup(0),
            value_hash: entry_hash,
        };
        let track_address = state.address();

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
        state.current.epoch.total_groups = 2;
        for id in 0..2u64 {
            let mut group = Group { id: GroupIndex(id), ..Group::zeroed() };
            for (position, node) in nodes.iter().enumerate() {
                group.spools[position].node = *node;
            }
            state.current.groups.push(group);
        }

        let sizes = [StorageUnits::from_bytes(64); 2];
        let owners = current_owners(&state, &sizes, profile);
        let expected: Vec<Option<Address>> = nodes.iter().chain(&nodes).copied().map(Some).collect();
        assert_eq!(owners, expected);

        let assignment: Vec<Address> = owners.into_iter().flatten().collect();
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

    const NARROW: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
    const NARROW_WIDTH: usize = 4;

    /// A 3 of 4 network of `live` groups, each held by four nodes of its own.
    fn narrow_state(live: u64) -> (ProtocolState, Vec<Vec<Address>>) {
        let mut state = ProtocolState::default();
        state.system.live_group_count = live;
        state.current.epoch.total_groups = live;

        let mut holders = Vec::new();
        for id in 0..live {
            let nodes: Vec<Address> = (0..NARROW_WIDTH).map(|_| Address::new_unique()).collect();
            let mut group = Group { id: GroupIndex(id), ..Group::zeroed() };
            for (position, node) in nodes.iter().enumerate() {
                group.spools[position].node = *node;
            }
            state.current.groups.push(group);
            holders.push(nodes);
        }

        (state, holders)
    }

    fn holders_of(owners: &[Option<Address>], group: usize) -> Vec<Option<Address>> {
        owners[group * NARROW_WIDTH..(group + 1) * NARROW_WIDTH].to_vec()
    }

    fn all(nodes: &[Address]) -> Vec<Option<Address>> {
        nodes.iter().copied().map(Some).collect()
    }

    // groups that stay live keep their nodes when the count drops, and the
    // spooler leaves them there
    #[test]
    fn shrink_keeps_survivors() {
        let (state, holders) = narrow_state(4);
        let sizes = [StorageUnits::from_bytes(64); 2];

        let owners = current_owners(&state, &sizes, NARROW);

        assert_eq!(owners.len(), 2 * NARROW_WIDTH);
        assert_eq!(holders_of(&owners, 0), all(&holders[0]));
        assert_eq!(holders_of(&owners, 1), all(&holders[1]));

        let committee: Vec<Member> = holders
            .iter()
            .flatten()
            .map(|node| Member::new(*node, tape_core::types::coin::TAPE(1_000)))
            .collect();
        let assignment = migrate_sainte_lague(2, &owners, &committee, &Hash::default(), NARROW)
            .expect("assignment");

        let kept: Vec<Address> = holders[0].iter().chain(&holders[1]).copied().collect();
        assert_eq!(assignment, kept);
    }

    // a new group with data starts on the nodes that hold that data, and one
    // with none is left open for a fresh fill
    #[test]
    fn new_group_inherits_with_bytes() {
        let (state, holders) = narrow_state(2);
        let bytes = StorageUnits::from_bytes(64);
        let sizes = [bytes, bytes, bytes, StorageUnits::zero()];

        let owners = current_owners(&state, &sizes, NARROW);

        // Group 2 is carried by group 0 while two groups are live.
        assert_eq!(holders_of(&owners, 0), all(&holders[0]));
        assert_eq!(holders_of(&owners, 1), all(&holders[1]));
        assert_eq!(holders_of(&owners, 2), all(&holders[0]));
        assert_eq!(holders_of(&owners, 3), vec![None; NARROW_WIDTH]);
    }

    /// Seat every current holder in the next committee and give the next epoch a nonce.
    fn with_next_epoch(mut state: ProtocolState, holders: &[Vec<Address>], target: u64) -> ProtocolState {
        let nodes: Vec<Address> = holders.iter().flatten().copied().collect();

        state.system.target_group_count = target;
        state.current.epoch.id = EpochNumber(10);
        state.next_epoch = Some(tape_api::state::Epoch {
            id: EpochNumber(11),
            nonce: Hash::from([9u8; 32]),
            ..Zeroable::zeroed()
        });
        state.next_committee = Some(
            nodes
                .iter()
                .map(|node| Member::new(*node, tape_core::types::coin::TAPE(1_000)))
                .collect(),
        );
        state
    }

    /// A certified inline track of `bytes` in a virtual group, old enough to count.
    fn put_counted_track(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        group: u64,
        bytes: u64,
    ) -> Address {
        let state = CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(group),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(bytes),
            group: VirtualGroup(group),
            value_hash: Hash::new_unique(),
        };
        let track = state.address();

        store.put_track(track, state).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(8),
                    certified_epoch: Some(EpochNumber(8)),
                    slot: tape_core::types::SlotNumber(8),
                },
            )
            .unwrap();

        track
    }

    fn user_tape(store: &TapeStore<MemoryStore>) -> Address {
        let tape = Address::new_unique();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(7),
                    flags: 0,
                    end_epoch: EpochNumber(50),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        tape
    }

    /// The nodes a candidate seats in a group, in position order.
    fn seated(candidate: &AssignmentCandidate, state: &ProtocolState, group: usize) -> Vec<Address> {
        let committee = state.next_committee.as_deref().expect("next committee");

        candidate.groups[group].payload.member_indices[..NARROW_WIDTH]
            .iter()
            .map(|index| committee[*index as usize].node)
            .collect()
    }

    // four groups going to two: the survivors keep their nodes, each weighs
    // what it carries, and the same inputs give the same root
    #[test]
    fn fewer_groups() {
        let store = test_store();
        let tape = user_tape(&store);
        put_counted_track(&store, tape, 0, 10);
        put_counted_track(&store, tape, 1, 100);
        put_counted_track(&store, tape, 2, 7);
        put_counted_track(&store, tape, 3, 40);

        let (state, holders) = narrow_state(4);
        let state = with_next_epoch(state, &holders, 2);

        let candidate = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");

        // Virtual group 2 folds onto group 0 and virtual group 3 onto group 1.
        assert_eq!(candidate.groups.len(), 2);
        assert_eq!(candidate.groups[0].payload.size, StorageUnits::from_bytes(17));
        assert_eq!(candidate.groups[1].payload.size, StorageUnits::from_bytes(140));
        assert_eq!(seated(&candidate, &state, 0), holders[0]);
        assert_eq!(seated(&candidate, &state, 1), holders[1]);

        let again = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");
        assert_eq!(candidate.hash, again.hash);
    }

    // a delete that lands in the voting epoch leaves the candidate as it was
    #[test]
    fn delete_in_voting_epoch() {
        let store = test_store();
        let tape = user_tape(&store);
        put_counted_track(&store, tape, 0, 10);
        let deleted = put_counted_track(&store, tape, 1, 100);

        let (state, holders) = narrow_state(2);
        let state = with_next_epoch(state, &holders, 2);

        let before = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");

        apply_event(
            &store,
            SlotNumber(100),
            None,
            &ReplayableEvent::DeleteTrack { track: deleted, epoch: EpochNumber(10) },
            NARROW,
            DeletePolicy::NextEpoch,
        )
        .expect("delete applied");

        let after = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");

        assert_eq!(after.groups[1].payload.size, StorageUnits::from_bytes(100));
        assert_eq!(before.hash, after.hash);
    }

    // a delete from an earlier epoch takes the track out of the candidate
    #[test]
    fn delete_before_voting_epoch() {
        let store = test_store();
        let tape = user_tape(&store);
        put_counted_track(&store, tape, 0, 10);
        let deleted = put_counted_track(&store, tape, 1, 100);

        let (state, holders) = narrow_state(2);
        let state = with_next_epoch(state, &holders, 2);

        apply_event(
            &store,
            SlotNumber(100),
            None,
            &ReplayableEvent::DeleteTrack { track: deleted, epoch: EpochNumber(9) },
            NARROW,
            DeletePolicy::NextEpoch,
        )
        .expect("delete applied");

        let candidate = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");

        assert_eq!(candidate.groups[1].payload.size, StorageUnits::zero());
    }

    // two groups doubling to four: each new group starts on the nodes that
    // hold its data, so nothing is copied
    #[test]
    fn doubling_copies_nothing() {
        let store = test_store();
        let tape = user_tape(&store);
        put_counted_track(&store, tape, 0, 10);
        put_counted_track(&store, tape, 1, 100);
        put_counted_track(&store, tape, 2, 7);
        put_counted_track(&store, tape, 3, 40);

        let (state, holders) = narrow_state(2);
        let state = with_next_epoch(state, &holders, 4);

        let candidate = build_assignment_blocking(&store, &state, NARROW)
            .expect("build")
            .expect("a candidate");

        assert_eq!(candidate.groups.len(), 4);
        assert_eq!(candidate.groups[0].payload.size, StorageUnits::from_bytes(10));
        assert_eq!(candidate.groups[2].payload.size, StorageUnits::from_bytes(7));
        assert_eq!(candidate.groups[3].payload.size, StorageUnits::from_bytes(40));

        // Virtual group 2 was carried by group 0 and virtual group 3 by group 1.
        assert_eq!(seated(&candidate, &state, 0), holders[0]);
        assert_eq!(seated(&candidate, &state, 1), holders[1]);
        assert_eq!(seated(&candidate, &state, 2), holders[0]);
        assert_eq!(seated(&candidate, &state, 3), holders[1]);
    }

    // a live group with no data keeps its nodes all the same
    #[test]
    fn empty_live_group_keeps_nodes() {
        let (state, holders) = narrow_state(2);
        let sizes = [StorageUnits::zero(); 2];

        let owners = current_owners(&state, &sizes, NARROW);

        assert_eq!(holders_of(&owners, 1), all(&holders[1]));
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
            group: VirtualGroup(0),
            value_hash: entry.key(),
        };
        let track_address = state.address();

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
