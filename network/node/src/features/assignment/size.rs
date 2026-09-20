//! Assignment size calculation.
//!
//! The assignment root commits to per-group spool sizes. That value is
//! consensus-critical and must be derived from active track metadata at a
//! deterministic epoch cutoff.

use thiserror::Error;
use tracing::debug;

use tape_store_api::Store;
use tape_core::encoding::EncodingType;
use tape_core::spooler::GroupIndex;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_slicer::{num_stripes, ClayCoder, SliceMetadata};
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
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
    pub group: GroupIndex,
    pub footprint: StorageUnits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentGroupWeights {
    pub sizes: Vec<StorageUnits>,
    pub tracks: Vec<ActiveTrackFootprint>,
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
///   - skip invalidated/deleted/uncertified/current-epoch tracks;
///   - skip tapes with end_epoch <= target_epoch;
///   - return zero for empty/new groups;
///   - skip a coded track whose blob metadata has not arrived yet;
///   - derive blob per-spool footprint from BlobEncoding encoding metadata, not total logical track size.
pub fn group_sizes<Db: Store>(
    store: &TapeStore<Db>,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
    target_groups: usize,
) -> Result<Vec<StorageUnits>, AssignmentSizeError> {
    Ok(group_weights(store, voting_epoch, target_epoch, target_groups)?.sizes)
}

pub fn group_weights<Db: Store>(
    store: &TapeStore<Db>,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
    target_groups: usize,
) -> Result<AssignmentGroupWeights, AssignmentSizeError> {
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
            let Some(active) =
                active_track_footprint(store, *track, metadata, info, voting_epoch, target_epoch)?
            else {
                continue;
            };

            let group_index = usize::try_from(active.group.0).map_err(|_| {
                invalid_track(*track, format!("group index {} overflows usize", active.group.0))
            })?;

            if group_index >= target_groups {
                return Err(invalid_track(
                    *track,
                    format!(
                        "group {} exceeds target group count {target_groups}",
                        active.group.0
                    ),
                ));
            }

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

    Ok(AssignmentGroupWeights {
        sizes,
        tracks: active_tracks,
    })
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
) -> Result<Option<ActiveTrackFootprint>, AssignmentSizeError> {
    // Only ObjectInfo::Valid is accounted user data. System-owned tracks are
    // kept live for repair/GC without entering assignment sizing.
    let Some(ObjectInfo::Valid {
        track_address,
        registered_epoch,
        certified_epoch,
        ..
    }) = info
    else {
        return Ok(None);
    };

    if track_address != track {
        return Err(invalid_track(track, "object info points at a different track"));
    }

    // A certificate landing between the two reads shows one record ahead of the
    // other; the track counts on the next pass either way
    let Some(certified_epoch) = certified_epoch else {
        return Ok(None);
    };

    if !metadata.is_certified() {
        return Ok(None);
    }

    if registered_epoch >= voting_epoch 
       || certified_epoch >= voting_epoch {
        return Ok(None);
    }

    let tape = store.get_tape(metadata.tape).map_err(store_error)?;
    let Some(tape) = tape else {
        return Err(invalid_track(
            track,
            format!("missing tape metadata for tape {}", metadata.tape),
        ));
    };

    if tape.end_epoch <= target_epoch {
        return Ok(None);
    }

    let Some(footprint) = track_footprint(store, track, metadata)? else {
        return Ok(None);
    };

    Ok(Some(ActiveTrackFootprint {
        track,
        tape: metadata.tape,
        group: metadata.group,
        footprint,
    }))
}

/// One track's footprint, or nothing while its blob metadata is missing
fn track_footprint<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
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
        None => {
            debug!(track = %track, "assignment: coded track has no blob metadata yet");
            return Ok(None);
        }
    };

    if blob.size != metadata.size {
        return Err(invalid_track(track, "blob size does not match track metadata"));
    }
    if blob.get_hash() != metadata.value_hash {
        return Err(invalid_track(track, "blob hash does not match track metadata"));
    }
    if blob.commitment_root() != blob.commitment {
        return Err(invalid_track(track, "blob commitment is invalid"));
    }

    blob_footprint(track, blob).map(Some)
}

fn blob_footprint(track: Address, blob: BlobEncoding) -> Result<StorageUnits, AssignmentSizeError> {
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

    let chunk_size = match blob.profile.encoding_type() {
        Some(EncodingType::Clay) => {
            let coder = ClayCoder::from_params(blob.profile.clay_params());
            coder.track_chunk_size(stripe_size, blob_len)
        }
        Some(EncodingType::Basic) => {
            let k = blob.profile.rs_params().k() as usize;
            if k == 0 {
                return Err(invalid_track(track, "basic encoding k is zero"));
            }
            let effective_len = stripe_size.min(blob_len);
            if effective_len == 0 {
                64
            } else {
                let raw = effective_len.div_ceil(k);
                raw.div_ceil(64) * 64
            }
        }
        Some(EncodingType::Unknown) | None => {
            return Err(invalid_track(track, "unknown blob encoding profile"));
        }
    };

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
    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState, TrackVisibility};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn tape_info(end_epoch: EpochNumber) -> TapeInfo {
        TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch,
            next_track_number: TrackNumber(0),
            authority: Address::default(),
            delegate: Address::default(),
            share_nonce: Hash::default(),
        }
    }

    fn raw_track(tape: Address, group: GroupIndex, size: StorageUnits) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            visibility: TrackVisibility::Public as u64,
            size,
            group,
            value_hash: Hash::new_unique(),
        }
    }

    fn coded_track(tape: Address, group: GroupIndex, size: StorageUnits) -> CompressedTrack {
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
        store.put_track(track, raw_track(tape, GroupIndex(2), size)).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();

        let sizes = group_sizes(&store, EpochNumber(10), EpochNumber(11), 4).unwrap();

        assert_eq!(sizes[0], StorageUnits::zero());
        assert_eq!(sizes[1], StorageUnits::zero());
        assert_eq!(sizes[2], size);
        assert_eq!(sizes[3], StorageUnits::zero());
    }

    #[test]
    fn counts_tracks_after_extend() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let size = StorageUnits::from_bytes(64);

        store.put_tape(tape, tape_info(EpochNumber(11))).unwrap();
        store.put_track(track, raw_track(tape, GroupIndex(1), size)).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .unwrap();

        // Expiring at the target epoch excludes the tape from sizing.
        let sizes = group_sizes(&store, EpochNumber(10), EpochNumber(11), 2).unwrap();
        assert_eq!(sizes[1], StorageUnits::zero());

        // An applied extend moves the expiry out and the tracks count again.
        let mut info = store.get_tape(tape).unwrap().expect("tape stored");
        info.end_epoch = EpochNumber(30);
        store.put_tape(tape, info).unwrap();

        let sizes = group_sizes(&store, EpochNumber(10), EpochNumber(11), 2).unwrap();
        assert_eq!(sizes[1], size);
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
            .put_track(user_track, raw_track(user_tape, GroupIndex(1), user_size))
            .unwrap();
        store
            .put_track(
                snapshot_track,
                raw_track(
                    snapshot_tape,
                    GroupIndex(1),
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

        let sizes = group_sizes(&store, EpochNumber(10), EpochNumber(11), 3).unwrap();

        assert_eq!(sizes[0], StorageUnits::zero());
        assert_eq!(sizes[1], user_size);
        assert_eq!(sizes[2], StorageUnits::zero());
    }

    // a coded track counts nothing while its blob metadata is missing
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
            .put_track(inline_track, raw_track(tape, GroupIndex(0), size))
            .expect("inline track stored");
        store
            .put_track(coded, coded_track(tape, GroupIndex(1), size))
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

        let weights =
            group_weights(&store, EpochNumber(10), EpochNumber(11), 2).expect("weights build");

        assert_eq!(weights.sizes[0], size);
        assert_eq!(weights.sizes[1], StorageUnits::zero());
        assert_eq!(weights.tracks.len(), 1);
        assert_eq!(weights.tracks[0].track, inline_track);
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
                coded_track(tape, GroupIndex(0), StorageUnits::from_bytes(256)),
            )
            .expect("track stored");
        store
            .put_track_data(track, BlobData::Coded(blob))
            .expect("blob stored");
        store
            .put_object_info(track, valid_object(track, EpochNumber(8), SlotNumber(1)))
            .expect("object info stored");

        let error = group_weights(&store, EpochNumber(10), EpochNumber(11), 1)
            .expect_err("mismatch refused");

        assert!(matches!(
            error,
            AssignmentSizeError::InvalidTrack { ref reason, .. }
                if reason == "blob size does not match track metadata"
        ));
    }
}
