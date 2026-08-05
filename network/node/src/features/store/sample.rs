//! The row a track contributes to its group's challenge sample set.
//!
//! Every path that installs a track locally has to write it, or the group
//! disagrees about what a round asks for. Replay is one such path; the snapshot
//! build and the bootstrap fetch are the others, and a track installed without a
//! row is one its owner holds the bytes of and can never be asked about.

use store::Store;
use tape_core::challenge::sample::EntryKind;
use tape_core::track::data::BlobData;
use tape_core::types::{SlotNumber, StorageUnits};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_slicer::coded_slice_len;
use tape_store::ops::SampleOps;
use tape_store::types::TrackSample;
use tape_store::TapeStore;

use crate::core::error::NodeError;

/// Slot a snapshot track's row is cut at.
///
/// A snapshot track is derived at an epoch boundary rather than written into a
/// round, and every committee member derives the same one, so there is no
/// mid-round cut to respect. Zero puts it in the set from the first round that
/// can see it.
pub const SNAPSHOT_REGISTERED_SLOT: SlotNumber = SlotNumber(0);

/// Record a track in its group's sample set.
///
/// A coded track weighs its slice length, derived from the registered encoding
/// rather than measured, so an owner that never received its slice still
/// enumerates it. An inline track is one bounded entry.
pub fn put_sample<Db: Store>(
    store: &TapeStore<Db>,
    group: tape_core::types::GroupIndex,
    track: Address,
    data: &BlobData,
    value_hash: Hash,
    registered_slot: SlotNumber,
) -> Result<(), NodeError> {
    let kind = match data {
        BlobData::Coded(blob) => EntryKind::Coded {
            slice_len: StorageUnits::from_bytes(coded_slice_len(
                blob.profile,
                blob.size.as_usize(),
                blob.stripe_size.as_usize(),
                blob.stripe_count.0 as usize,
            ) as u64),
        },
        BlobData::Inline(_) => EntryKind::Inline,
    };

    store
        .put_track_sample(
            group,
            track,
            TrackSample {
                kind,
                value_hash,
                registered_slot,
                deleted_slot: None,
            },
        )
        .map_err(|error| NodeError::Store(error.to_string()))
}
