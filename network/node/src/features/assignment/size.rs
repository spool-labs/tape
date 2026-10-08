//! Assignment size calculation.
//!
//! The assignment root commits to per-group spool sizes. That value is
//! consensus-critical and must be derived from active track metadata at a
//! deterministic epoch cutoff.

use thiserror::Error;

use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::{GroupFold, GroupIndex};
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_slicer::{num_stripes, stripe_chunk_len, SliceMetadata};
use tape_store::ops::{
    ObjectInfoOps, RemovalOps, TapeOps, TrackDataOps, TrackOps, track_has_left,
};
use tape_store::types::ObjectInfo;
use tape_store::TapeStore;

const TRACK_SCAN_BATCH: usize = 1024;

#[derive(Debug, Error)]
pub enum AssignmentSizeError {
    #[error("store error: {0}")]
    Store(String),
    #[error("track {track} has invalid assignment metadata: {reason}")]
    InvalidTrack { track: Address, reason: String },
    #[error("assignment size overflow for group {group}")]
    Overflow { group: GroupIndex },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTrackFootprint {
    pub track: Address,
    pub tape: Address,
    /// The group that carries the track in the target epoch.
    pub group: GroupIndex,
    pub footprint: StorageUnits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentGroupWeights {
    pub sizes: Vec<StorageUnits>,
    pub tracks: Vec<ActiveTrackFootprint>,
}

/// A finished weight pass, or the track that holds it up
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupWeights {
    Ready(AssignmentGroupWeights),
    Pending { track: Address },
}

/// One track's place in a weight pass
enum TrackWeight {
    Counted(ActiveTrackFootprint),
    Excluded,
    Pending,
}

/// Compute per-spool storage size for every target group.
///
/// During Closing for epoch N, nodes are building the N+1 assignment. The
/// deterministic cutoff is the last fully completed epoch: only tracks with
/// `registered_epoch < N` and `certified_epoch < N` are counted.
///
///  Rules implemented:
///   - include only user ObjectInfo::Valid tracks with registered_epoch < voting_epoch;
///   - require certified_epoch < voting_epoch;
///   - include a track deleted in the voting epoch, and skip one deleted before it;
///   - skip invalidated/uncertified/current-epoch tracks;
///   - skip tapes that had expired by the voting epoch;
///   - return zero for empty/new groups;
///   - count a track under the group that carries it at the target epoch's
///     group count, so a group that absorbs another weighs both;
///   - report Pending while a coded track's blob metadata has yet to arrive,
///     since every peer counts that track once it lands;
///   - derive blob per-spool footprint from the BlobEncoding metadata and the
///     network's profile.
pub fn group_weights<Db: Store>(
    store: &TapeStore<Db>,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
    target: GroupFold,
    profile: ErasureCode,
) -> Result<GroupWeights, AssignmentSizeError> {
    let target_groups = usize::try_from(target.live())
        .map_err(|_| AssignmentSizeError::Store("target group count overflows usize".into()))?;
    let mut sizes = vec![StorageUnits::zero(); target_groups];
    let mut active_tracks = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), TRACK_SCAN_BATCH)
            .map_err(store_error)?;

        let mut addresses: Vec<Address> = Vec::with_capacity(tracks.len());
        for (track, _) in &tracks {
            addresses.push(*track);
        }
        // The page's object info once, rather than one read per track.
        let infos = store.get_object_infos(&addresses).map_err(store_error)?;

        for ((track, metadata), info) in tracks.iter().zip(infos) {
            let weight = active_track_footprint(
                store,
                *track,
                metadata,
                info,
                voting_epoch,
                target_epoch,
                target,
                profile,
            )?;
            let active = match weight {
                TrackWeight::Counted(active) => active,
                TrackWeight::Excluded => continue,
                TrackWeight::Pending => return Ok(GroupWeights::Pending { track: *track }),
            };

            // The fold always lands below the target count.
            let group_index = active.group.as_usize();

            sizes[group_index] = sizes[group_index]
                .checked_add(active.footprint)
                .ok_or(AssignmentSizeError::Overflow {
                    group: active.group,
                })?;
            active_tracks.push(active);
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    Ok(GroupWeights::Ready(AssignmentGroupWeights {
        sizes,
        tracks: active_tracks,
    }))
}

/// One track's contribution to a group's weight, or nothing where it makes none
///
/// The object info is handed in, since the page read the whole batch's in one
/// call. What is still read here is the parent tape and the footprint.
fn active_track_footprint<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
    info: Option<ObjectInfo>,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
    target: GroupFold,
    profile: ErasureCode,
) -> Result<TrackWeight, AssignmentSizeError> {
    if !is_active_track(store, track, metadata, info, voting_epoch, target_epoch)? {
        return Ok(TrackWeight::Excluded);
    }

    let Some(footprint) = track_footprint(store, track, metadata, profile)? else {
        return Ok(TrackWeight::Pending);
    };

    Ok(TrackWeight::Counted(ActiveTrackFootprint {
        track,
        tape: metadata.tape,
        group: target.live_group(metadata.group),
        footprint,
    }))
}

/// Whether the voting epoch's round counts a track
///
/// The challenge picks from the same set, so the two can never count
/// different tracks. The object info is handed in, and the parent tape is
/// read here.
pub fn is_active_track<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
    info: Option<ObjectInfo>,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> Result<bool, AssignmentSizeError> {
    // Only user tracks are accounted. System-owned tracks are kept live for
    // repair/GC without entering assignment sizing.
    let (track_address, registered_epoch, certified_epoch) = match info {
        Some(ObjectInfo::Valid {
            track_address,
            registered_epoch,
            certified_epoch,
            ..
        }) => (track_address, registered_epoch, certified_epoch),
        // A delete that landed in the voting epoch reaches each node at its own
        // moment, so the track counts as if it had yet to land.
        Some(ObjectInfo::Deleted {
            kind: None,
            track_address,
            registered_epoch,
            certified_epoch,
            deleted_epoch,
        }) if deleted_epoch >= voting_epoch => {
            (track_address, registered_epoch, Some(certified_epoch))
        }
        Some(
            ObjectInfo::Deleted { .. }
            | ObjectInfo::System { .. }
            | ObjectInfo::Invalid { .. }
            | ObjectInfo::Blacklisted,
        )
        | None => return Ok(false),
    };

    if track_address != track {
        return Err(invalid_track(track, "object info points at a different track"));
    }

    // A certificate landing between the two reads shows one record ahead of the
    // other; the track counts on the next pass either way
    let Some(certified_epoch) = certified_epoch else {
        return Ok(false);
    };

    if !metadata.is_certified() {
        return Ok(false);
    }

    if registered_epoch >= voting_epoch
       || certified_epoch >= voting_epoch {
        return Ok(false);
    }

    let tape = store.get_tape(metadata.tape).map_err(store_error)?;

    if track_has_left(tape.as_ref(), metadata) {
        // A close or a rewind that landed in the voting epoch reaches each node
        // at its own moment, so its tracks count as if it had yet to land. The
        // entry is only written for a tape that was live in that epoch, and it
        // gives the tree start the tape had then: a track numbered below it
        // left in an earlier epoch.
        let left_in_voting_epoch = store
            .get_removal(metadata.tape, metadata.tape_number, voting_epoch)
            .map_err(store_error)?
            .is_some_and(|tree_start| metadata.track_number >= tree_start);

        if !left_in_voting_epoch {
            return Ok(false);
        }
    } else if tape.is_some_and(|tape| tape.end_epoch < target_epoch) {
        // A tape counts when it was live in the voting epoch, which is an end at
        // or after the target epoch. No extension during the round changes that:
        // the program refuses to extend an expired tape, and an end only moves
        // later. A renew moves the tree's start too, so the expired tape's
        // tracks have left.
        return Ok(false);
    }

    Ok(true)
}

/// One track's footprint, or nothing while its blob metadata is missing
fn track_footprint<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
    profile: ErasureCode,
) -> Result<Option<StorageUnits>, AssignmentSizeError> {
    if metadata.is_inline() {
        return Ok(Some(metadata.size));
    }

    if !metadata.is_coded() {
        return Err(invalid_track(track, "unknown track kind"));
    }

    let blob = match store.get_track_data(track).map_err(store_error)? {
        Some(BlobData::Coded(blob)) => blob,
        Some(BlobData::Inline(_)) => {
            return Err(invalid_track(track, "inline blob data on a coded track"));
        }
        // The footprint needs the encoding, and a peer fetch fills it in later.
        None => return Ok(None),
    };

    if blob.size != metadata.size {
        return Err(invalid_track(track, "blob size does not match track metadata"));
    }
    if blob.get_hash() != metadata.value_hash {
        return Err(invalid_track(track, "blob hash does not match track metadata"));
    }
    if blob.commitment_root(profile) != Some(blob.commitment) {
        return Err(invalid_track(track, "blob commitment is invalid"));
    }

    blob_footprint(track, blob, profile).map(Some)
}

/// Bytes one spool holds for a coded track: its slice, metadata included.
fn blob_footprint(
    track: Address,
    blob: BlobEncoding,
    profile: ErasureCode,
) -> Result<StorageUnits, AssignmentSizeError> {
    let stripe_size = usize::try_from(blob.stripe_size.as_u64())
        .map_err(|_| invalid_track(track, "stripe size overflows usize"))?;

    if stripe_size == 0 {
        return Err(invalid_track(track, "stripe size is zero"));
    }

    let blob_len = usize::try_from(blob.size.as_u64())
        .map_err(|_| invalid_track(track, "blob size overflows usize"))?;
    let stripe_count = usize::try_from(blob.stripe_count.as_u64())
        .map_err(|_| invalid_track(track, "stripe count overflows usize"))?;

    let expected_stripe_count = num_stripes(blob_len, stripe_size);
    if stripe_count != expected_stripe_count {
        return Err(invalid_track(
            track,
            format!("stripe count {stripe_count} != expected {expected_stripe_count}"),
        ));
    }

    let chunk_size = stripe_chunk_len(profile, stripe_size, blob_len);

    let bytes = (stripe_count as u128)
        .checked_mul(chunk_size as u128)
        .and_then(|bytes| bytes.checked_add(SliceMetadata::SIZE as u128))
        .ok_or_else(|| invalid_track(track, "blob footprint overflows u128"))?;
    let bytes =
        u64::try_from(bytes).map_err(|_| invalid_track(track, "blob footprint overflows u64"))?;

    Ok(StorageUnits::from_bytes(bytes))
}

fn store_error(error: impl std::fmt::Display) -> AssignmentSizeError {
    AssignmentSizeError::Store(error.to_string())
}

fn invalid_track(track: Address, reason: impl Into<String>) -> AssignmentSizeError {
    AssignmentSizeError::InvalidTrack {
        track,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_store_memory::MemoryStore;
    use tape_core::encoding::{RSParams, RepParams, TAPENET_CLAY, TAPENET_PROFILE};
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    /// A target epoch of `groups` groups.
    fn fold(groups: u64) -> GroupFold {
        GroupFold::new(groups).expect("live groups")
    }

    fn ready_sizes(
        store: &TapeStore<MemoryStore>,
        voting_epoch: EpochNumber,
        target_epoch: EpochNumber,
        target_groups: u64,
    ) -> Option<Vec<StorageUnits>> {
        let target = fold(target_groups);

        match group_weights(store, voting_epoch, target_epoch, target, TAPENET_PROFILE)
            .expect("weights build")
        {
            GroupWeights::Ready(weights) => Some(weights.sizes),
            GroupWeights::Pending { .. } => None,
        }
    }

    // a retired group's tracks weigh on the group that carries them, and split
    // off again when the group returns
    #[test]
    fn retired_group_weighs_on_its_carrier() {
        let store = test_store();
        let tape = Address::new_unique();
        let kept = Address::new_unique();
        let retired = Address::new_unique();
        let kept_size = StorageUnits::from_bytes(100);
        let retired_size = StorageUnits::from_bytes(40);

        store.put_tape(tape, tape_info(EpochNumber(20))).unwrap();
        store.put_track(kept, raw_track(tape, VirtualGroup(3), kept_size)).unwrap();
        store.put_track(retired, raw_track(tape, VirtualGroup(7), retired_size)).unwrap();
        for track in [kept, retired] {
            store
                .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
                .unwrap();
        }

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 5)
            .expect("weights ready");

        assert_eq!(sizes.len(), 5);
        assert_eq!(sizes[3], StorageUnits::from_bytes(140));

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 10)
            .expect("weights ready");

        assert_eq!(sizes[3], kept_size);
        assert_eq!(sizes[7], retired_size);
    }

    fn tape_info(end_epoch: EpochNumber) -> TapeInfo {
        TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch,
            next_track_number: TrackNumber(0),
            tree_start: TrackNumber(0),
            authority: Address::default(),
            delegate: Address::default(),
            nonce: Hash::default(),
        }
    }

    fn raw_track(tape: Address, group: VirtualGroup, size: StorageUnits) -> CompressedTrack {
        CompressedTrack {
            tape,
            tape_number: TapeNumber(1),
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size,
            group,
            value_hash: Hash::new_unique(),
        }
    }

    fn coded_track(tape: Address, group: VirtualGroup, size: StorageUnits) -> CompressedTrack {
        let mut track = raw_track(tape, group, size);
        track.kind = TrackKind::Coded as u64;
        track
    }

    fn valid_object(track: Address, registered_epoch: EpochNumber, slot: SlotNumber) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch,
            certified_epoch: Some(registered_epoch),
            slot,
        }
    }

    #[test]
    fn counts_certified_user_tracks_before_cutoff() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let size = StorageUnits::from_bytes(123);

        store.put_tape(tape, tape_info(EpochNumber(20))).unwrap();
        store.put_track(track, raw_track(tape, VirtualGroup(2), size)).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 4)
            .expect("weights ready");

        assert_eq!(sizes[0], StorageUnits::zero());
        assert_eq!(sizes[1], StorageUnits::zero());
        assert_eq!(sizes[2], size);
        assert_eq!(sizes[3], StorageUnits::zero());
    }

    // a tape in its last epoch counts, so an extend during the round changes nothing
    #[test]
    fn extend_in_round() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let size = StorageUnits::from_bytes(64);

        store.put_tape(tape, tape_info(EpochNumber(11))).unwrap();
        store.put_track(track, raw_track(tape, VirtualGroup(1), size)).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();

        let before = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(before[1], size);

        let mut info = store.get_tape(tape).expect("tape read").expect("tape stored");
        info.end_epoch = EpochNumber(30);
        store.put_tape(tape, info).expect("tape stored");

        let after = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(after, before);
    }

    // a tape that had expired by the voting epoch is left out
    #[test]
    fn expired_tape_skipped() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();

        store.put_tape(tape, tape_info(EpochNumber(10))).unwrap();
        store
            .put_track(track, raw_track(tape, VirtualGroup(1), StorageUnits::from_bytes(64)))
            .unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(sizes[1], StorageUnits::zero());
    }

    fn deleted_object(track: Address, deleted_epoch: EpochNumber) -> ObjectInfo {
        ObjectInfo::Deleted {
            kind: None,
            track_address: track,
            registered_epoch: EpochNumber(8),
            certified_epoch: EpochNumber(8),
            deleted_epoch,
        }
    }

    // a track deleted in the voting epoch still counts, and one deleted before it is skipped
    #[test]
    fn deleted_track_by_epoch() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let size = StorageUnits::from_bytes(64);

        store.put_tape(tape, tape_info(EpochNumber(20))).unwrap();
        store.put_track(track, raw_track(tape, VirtualGroup(1), size)).unwrap();

        store
            .put_object_info(track, deleted_object(track, EpochNumber(10)))
            .unwrap();
        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(sizes[1], size);

        store
            .put_object_info(track, deleted_object(track, EpochNumber(9)))
            .unwrap();
        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(sizes[1], StorageUnits::zero());
    }

    const VOTING: EpochNumber = EpochNumber(10);
    const TARGET: EpochNumber = EpochNumber(11);
    const LEFT_GROUP: usize = 1;

    /// A certified track at `number` on a tape, registered before the voting epoch
    fn put_numbered(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        tape_number: TapeNumber,
        number: u64,
        size: StorageUnits,
    ) {
        let track = Address::new_unique();
        let state = CompressedTrack {
            tape_number,
            track_number: TrackNumber(number),
            ..raw_track(tape, VirtualGroup(LEFT_GROUP as u64), size)
        };
        store.put_track(track, state).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();
    }

    fn left_group_size(store: &TapeStore<MemoryStore>, voting_epoch: EpochNumber) -> StorageUnits {
        let sizes = ready_sizes(store, voting_epoch, voting_epoch.next(), 2).expect("weights ready");
        sizes[LEFT_GROUP]
    }

    // a tape closed in the voting epoch still weighs what it held, and weighs
    // nothing in the round after
    #[test]
    fn closed_tape_by_epoch() {
        let store = test_store();
        let tape = Address::new_unique();
        let size = StorageUnits::from_bytes(64);
        put_numbered(&store, tape, TapeNumber(1), 0, size);

        // The close deleted the tape's row and left this entry.
        store.record_removal(tape, TapeNumber(1), VOTING, TrackNumber(0)).unwrap();

        assert_eq!(left_group_size(&store, VOTING), size);
        assert_eq!(left_group_size(&store, TARGET), StorageUnits::zero());
    }

    // after a rewind in the voting epoch, the tracks the tape held when the
    // epoch began still count. A track from before an earlier rewind does not.
    #[test]
    fn rewound_tape_by_epoch() {
        let store = test_store();
        let tape = Address::new_unique();
        let earlier = StorageUnits::from_bytes(1);
        let rewound = StorageUnits::from_bytes(10);
        let held = StorageUnits::from_bytes(100);
        put_numbered(&store, tape, TapeNumber(1), 0, earlier);
        put_numbered(&store, tape, TapeNumber(1), 1, rewound);
        put_numbered(&store, tape, TapeNumber(1), 2, held);

        // An earlier rewind moved the start to 1, and this epoch's moved it to 2.
        let row = TapeInfo { tree_start: TrackNumber(2), ..tape_info(EpochNumber(20)) };
        store.put_tape(tape, row).unwrap();
        store.record_removal(tape, TapeNumber(1), VOTING, TrackNumber(1)).unwrap();

        assert_eq!(left_group_size(&store, VOTING), rewound + held);
        assert_eq!(left_group_size(&store, TARGET), held);
    }

    // a tape closed and reserved again in the voting epoch: the closed tape's
    // tracks count under its own number, whatever the new tape's row says
    #[test]
    fn reserved_again_in_epoch() {
        let store = test_store();
        let tape = Address::new_unique();
        let size = StorageUnits::from_bytes(64);
        put_numbered(&store, tape, TapeNumber(1), 0, size);

        let row = TapeInfo { id: TapeNumber(2), ..tape_info(EpochNumber(20)) };
        store.put_tape(tape, row).unwrap();
        store.record_removal(tape, TapeNumber(1), VOTING, TrackNumber(0)).unwrap();

        assert_eq!(left_group_size(&store, VOTING), size);
        assert_eq!(left_group_size(&store, TARGET), StorageUnits::zero());
    }

    // a renewed tape is live again, and the tracks it held when it expired
    // weigh nothing: a renew leaves no entry
    #[test]
    fn renewed_tape_weighs_nothing() {
        let store = test_store();
        let tape = Address::new_unique();
        put_numbered(&store, tape, TapeNumber(1), 0, StorageUnits::from_bytes(64));

        let row = TapeInfo { tree_start: TrackNumber(1), ..tape_info(EpochNumber(20)) };
        store.put_tape(tape, row).unwrap();

        assert_eq!(left_group_size(&store, VOTING), StorageUnits::zero());
    }

    // a removed blacklist entry is a system track and never enters the sizes
    #[test]
    fn deleted_system_track_skipped() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();

        store.put_tape(tape, tape_info(EpochNumber(20))).unwrap();
        store
            .put_track(track, raw_track(tape, VirtualGroup(1), StorageUnits::from_bytes(64)))
            .unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Deleted {
                    kind: Some(SystemObjectKind::Blacklist),
                    track_address: track,
                    registered_epoch: EpochNumber(8),
                    certified_epoch: EpochNumber(8),
                    deleted_epoch: EpochNumber(10),
                },
            )
            .unwrap();

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 2)
            .expect("weights ready");
        assert_eq!(sizes[1], StorageUnits::zero());
    }

    #[test]
    fn excludes_snapshot_tracks_without_object_info() {
        let store = test_store();
        let user_tape = Address::new_unique();
        let snapshot_tape = Address::new_unique();
        let user_track = Address::new_unique();
        let snapshot_track = Address::new_unique();
        let user_size = StorageUnits::from_bytes(100);

        store
            .put_tape(user_tape, tape_info(EpochNumber(20)))
            .unwrap();
        store
            .put_tape(snapshot_tape, tape_info(EpochNumber(20)))
            .unwrap();
        store
            .put_track(user_track, raw_track(user_tape, VirtualGroup(1), user_size))
            .unwrap();
        store
            .put_track(
                snapshot_track,
                raw_track(
                    snapshot_tape,
                    VirtualGroup(1),
                    StorageUnits::from_bytes(999),
                ),
            )
            .unwrap();
        store
            .put_object_info(
                user_track,
                valid_object(user_track, EpochNumber(8), SlotNumber(1)),
            )
            .unwrap();

        let sizes = ready_sizes(&store, EpochNumber(10), EpochNumber(11), 3)
            .expect("weights ready");

        assert_eq!(sizes[0], StorageUnits::zero());
        assert_eq!(sizes[1], user_size);
        assert_eq!(sizes[2], StorageUnits::zero());
    }

    // a coded track with no blob metadata holds up the whole pass
    #[test]
    fn coded_without_blob() {
        let store = test_store();
        let tape = Address::new_unique();
        let inline_track = Address::new_unique();
        let coded = Address::new_unique();
        let size = StorageUnits::from_bytes(256);

        store
            .put_tape(tape, tape_info(EpochNumber(20)))
            .expect("tape stored");
        store
            .put_track(inline_track, raw_track(tape, VirtualGroup(0), size))
            .expect("inline track stored");
        store
            .put_track(coded, coded_track(tape, VirtualGroup(1), size))
            .expect("coded track stored");
        store
            .put_object_info(
                inline_track,
                valid_object(inline_track, EpochNumber(8), SlotNumber(1)),
            )
            .expect("inline object info stored");
        store
            .put_object_info(coded, valid_object(coded, EpochNumber(8), SlotNumber(1)))
            .expect("coded object info stored");

        let weights = group_weights(&store, EpochNumber(10), EpochNumber(11), fold(2), TAPENET_PROFILE)
            .expect("weights build");

        assert_eq!(weights, GroupWeights::Pending { track: coded });
    }

    /// A blob of `len` bytes striped the way a writer on `profile` stripes it.
    fn striped_blob(profile: ErasureCode, len: usize) -> BlobEncoding {
        use tape_slicer::Slicer;

        let slicer = Slicer::try_from_profile(profile).expect("valid profile");
        let stripe_size = slicer.derived_stripe_size(len);

        let mut blob = BlobEncoding::zeroed();
        blob.size = StorageUnits::from_bytes(len as u64);
        blob.stripe_size = StorageUnits::from_bytes(stripe_size as u64);
        blob.stripe_count = StripeCount(num_stripes(len, stripe_size) as u64);
        blob
    }

    // tapenet's per-spool footprint is the Clay chunk size it always was, so
    // every node keeps agreeing on the assignment sizes
    #[test]
    fn tapenet_footprint() {
        use tape_slicer::ClayCoder;

        let coder = ClayCoder::from_params(TAPENET_CLAY);

        for len in [1usize, 700, 1_400, 99_999, 1_000_000, 1_000_001, 7_654_321, 64 * 1024 * 1024] {
            let blob = striped_blob(TAPENET_PROFILE, len);
            let stripe_size = blob.stripe_size.as_usize();
            let stripes = blob.stripe_count.0 as usize;
            let expected = stripes * coder.track_chunk_size(stripe_size, len) + SliceMetadata::SIZE;

            let footprint = blob_footprint(Address::default(), blob, TAPENET_PROFILE).expect("footprint");

            assert_eq!(footprint, StorageUnits::from_bytes(expected as u64), "len {len}");
        }
    }

    // a spool's footprint is the slice its code writes: about a third of the
    // track at 3 of 4, and the whole track for a copy
    #[test]
    fn small_profile_footprints() {
        use tape_slicer::{ErasureCoder, Slicer};

        let len = 300_000usize;
        for profile in [
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::Replication(RepParams::new(2)),
        ] {
            let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
            let slices = slicer.encode(&vec![0x5Au8; len]).expect("encode");
            let blob = striped_blob(profile, len);

            let footprint = blob_footprint(Address::default(), blob, profile).expect("footprint");

            assert_eq!(footprint, StorageUnits::from_bytes(slices[0].len() as u64), "{profile:?}");
        }
    }

    // a stored blob whose size disagrees with the track row is refused
    #[test]
    fn blob_size_mismatch() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let mut blob = BlobEncoding::zeroed();
        blob.size = StorageUnits::from_bytes(512);

        store
            .put_tape(tape, tape_info(EpochNumber(20)))
            .expect("tape stored");
        store
            .put_track(
                track,
                coded_track(tape, VirtualGroup(0), StorageUnits::from_bytes(256)),
            )
            .expect("track stored");
        store
            .put_track_data(track, BlobData::Coded(blob))
            .expect("blob stored");
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .expect("object info stored");

        let error = group_weights(&store, EpochNumber(10), EpochNumber(11), fold(1), TAPENET_PROFILE)
            .expect_err("mismatch refused");

        assert!(matches!(
            error,
            AssignmentSizeError::InvalidTrack { ref reason, .. }
                if reason == "blob size does not match track metadata"
        ));
    }
}
