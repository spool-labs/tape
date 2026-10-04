use tape_store_api::Store;
use tape_core::challenge::sample::EntryKind;
use tape_core::encoding::ErasureCode;
use tape_core::track::data::BlobData;
use tape_core::types::{SlotNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_slicer::coded_slice_len;
use tape_store::ops::SampleOps;
use tape_store::types::TrackSample;
use tape_store::TapeStore;

use crate::core::error::NodeError;

pub fn put_sample<Db: Store>(
    store: &TapeStore<Db>,
    group: tape_core::types::GroupIndex,
    track: Address,
    track_number: TrackNumber,
    data: &BlobData,
    value_hash: Hash,
    registered_slot: SlotNumber,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    let kind = match data {
        BlobData::Coded(blob) => EntryKind::Coded {
            slice_len: StorageUnits::from_bytes(coded_slice_len(
                profile,
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
            track_number,
            TrackSample {
                kind,
                value_hash,
                registered_slot,
                deleted_slot: None,
            },
        )
        .map_err(|error| NodeError::Store(error.to_string()))
}
