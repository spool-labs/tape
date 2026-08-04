//! The row a coded track contributes to its group's challenge sample set.
//!
//! Every path that installs a coded track locally has to write it, or the group
//! disagrees about what a round asks for. Replay is one such path; the snapshot
//! build and the bootstrap fetch are the others, and a track installed without a
//! row is one its owner holds a slice of and can never be asked about.

use store::Store;
use tape_core::track::blob::BlobEncoding;
use tape_core::types::{SlotNumber, StorageUnits};
use tape_crypto::address::Address;
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

/// Record a coded track in its group's sample set.
pub fn put_sample<Db: Store>(
    store: &TapeStore<Db>,
    group: tape_core::types::GroupIndex,
    track: Address,
    blob: &BlobEncoding,
    registered_slot: SlotNumber,
) -> Result<(), NodeError> {
    let slice_len = coded_slice_len(
        blob.profile,
        blob.size.as_usize(),
        blob.stripe_size.as_usize(),
        blob.stripe_count.0 as usize,
    );

    store
        .put_track_sample(
            group,
            track,
            TrackSample {
                slice_len: StorageUnits::from_bytes(slice_len as u64),
                registered_slot,
                deleted_slot: None,
            },
        )
        .map_err(|error| NodeError::Store(error.to_string()))
}
