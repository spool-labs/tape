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

/// Makes snapshot tracks eligible for every round that can observe them.
pub const SNAPSHOT_REGISTERED_SLOT: SlotNumber = SlotNumber(0);

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
