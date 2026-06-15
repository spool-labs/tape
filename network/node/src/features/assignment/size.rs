//! Assignment size calculation.
//!
//! The assignment root commits to per-group spool sizes. That value is
//! consensus-critical and must be derived from active track metadata at a
//! deterministic epoch cutoff.

use thiserror::Error;

use store::Store;
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
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, TRACK_SCAN_BATCH)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, metadata) in &tracks {
            let Some(active) =
                active_track_footprint(store, *track, metadata, voting_epoch, target_epoch)?
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

        cursor = tracks.last().map(|(track, _)| *track);
    }

    Ok(AssignmentGroupWeights {
        sizes,
        tracks: active_tracks,
    })
}

fn active_track_footprint<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> Result<Option<ActiveTrackFootprint>, AssignmentSizeError> {

    // Only ObjectInfo::Valid is accounted user data. System-owned tracks are
    // kept live for repair/GC without entering assignment sizing.
    let info = store
        .get_object_info(track)
        .map_err(store_error)?;

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

    let Some(certified_epoch) = certified_epoch else {
        if metadata.is_certified() {
            return Err(invalid_track(
                track,
                "track metadata is certified but object info is not",
            ));
        }
        return Ok(None);
    };

    if !metadata.is_certified() {
        return Err(invalid_track(
            track,
            "object info is certified but track metadata is not",
        ));
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

    let footprint = track_footprint(store, track, metadata)?;
    Ok(Some(ActiveTrackFootprint {
        track,
        tape: metadata.tape,
        group: metadata.group,
        footprint,
    }))
}

fn track_footprint<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    metadata: &CompressedTrack,
) -> Result<StorageUnits, AssignmentSizeError> {
    if metadata.is_inline() {
        return Ok(metadata.size);
    }

    if !metadata.is_coded() {
        return Err(invalid_track(track, "unknown track kind"));
    }

    let data = store.get_track_data(track).map_err(store_error)?;
    let Some(BlobData::Coded(blob)) = data else {
        return Err(invalid_track(track, "missing blob metadata"));
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

    blob_footprint(track, blob)
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
    use store_memory::MemoryStore;
    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
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
        }
    }

    fn raw_track(tape: Address, group: GroupIndex, size: StorageUnits) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size,
            group,
            value_hash: Hash::new_unique(),
        }
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
}
