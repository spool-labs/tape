use tape_crypto::Hash;
use tape_crypto::hash::hash;

use crate::track::blob::BlobInfo;
use crate::track::types::{TrackKind, TrackState};
use crate::types::StorageUnits;

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};


/// Owned track payload for node-side storage.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub enum TrackData {
    Raw(Vec<u8>),
    Blob(BlobInfo),
}

impl TrackData {
    #[inline(always)]
    pub fn as_slice(&self) -> TrackDataSlice<'_> {
        match self {
            Self::Raw(bytes) => TrackDataSlice::Raw(bytes),
            Self::Blob(blob) => TrackDataSlice::Blob(*blob),
        }
    }

    #[inline(always)]
    pub fn get_hash(&self) -> Hash {
        self.as_slice().get_hash()
    }

    #[inline(always)]
    pub fn meta(&self) -> Option<TrackMeta> {
        self.as_slice().meta()
    }
}

/// Lightweight track payload view for instruction parsing and metadata derivation.
#[derive(Clone, Copy, Debug)]
pub enum TrackDataSlice<'a> {
    Raw(&'a [u8]),
    Blob(BlobInfo),
}

impl<'a> TrackDataSlice<'a> {
    #[inline(always)]
    pub fn size(self) -> StorageUnits {
        match self {
            Self::Raw(bytes) => StorageUnits::from_bytes(bytes.len() as u64),
            Self::Blob(blob) => blob.size,
        }
    }

    #[inline(always)]
    pub fn get_hash(self) -> Hash {
        match self {
            Self::Raw(bytes) => hash(bytes),
            Self::Blob(blob) => blob.get_hash(),
        }
    }

    #[inline(always)]
    pub fn meta(self) -> Option<TrackMeta> {
        match self {
            Self::Raw(bytes) => Some(TrackMeta {
                kind: TrackKind::Raw,
                size: StorageUnits::from_bytes(bytes.len() as u64),
                initial_state: TrackState::Certified,
                value_hash: hash(bytes),
            }),
            Self::Blob(blob) => {
                if blob.commitment_root() != blob.commitment {
                    return None;
                }

                Some(TrackMeta {
                    kind: TrackKind::Blob,
                    size: blob.size,
                    initial_state: TrackState::Registered,
                    value_hash: blob.get_hash(),
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackMeta {
    pub kind: TrackKind,
    pub size: StorageUnits,
    pub initial_state: TrackState,
    pub value_hash: Hash,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::SPOOL_GROUP_SIZE;

    fn sample_blob_info() -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(1024),
            root: Hash::from([0x11; 32]),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: 128,
            stripe_count: 4,
            leaves: [Hash::from([0x33; 32]); SPOOL_GROUP_SIZE],
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn track_data_blob_wincode_roundtrip() {
        let data = TrackData::Blob(sample_blob_info());
        let bytes = wincode::serialize(&data).expect("serialize");
        let recovered: TrackData = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, data);
    }
}
