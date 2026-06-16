//! Track payload containers and metadata derivation.

use tape_crypto::hash::hash;
use tape_crypto::Hash;

use crate::track::blob::BlobEncoding;
use crate::track::types::{TrackKind, TrackState};
use crate::types::{ContentType, StorageUnits};

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

/// Owned track payload for node-side storage.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub enum BlobData {
    Inline(Vec<u8>),
    Coded(BlobEncoding),
}

impl BlobData {
    #[inline(always)]
    pub fn as_slice(&self) -> BlobDataSlice<'_> {
        match self {
            Self::Inline(bytes) => BlobDataSlice::Inline(bytes),
            Self::Coded(blob) => BlobDataSlice::Coded(*blob),
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
pub enum BlobDataSlice<'source> {
    Inline(&'source [u8]),
    Coded(BlobEncoding),
}

impl<'source> BlobDataSlice<'source> {
    #[inline(always)]
    pub fn size(self) -> StorageUnits {
        match self {
            Self::Inline(bytes) => StorageUnits::from_bytes(bytes.len() as u64),
            Self::Coded(blob) => blob.size,
        }
    }

    #[inline(always)]
    pub fn get_hash(self) -> Hash {
        match self {
            Self::Inline(bytes) => hash(bytes),
            Self::Coded(blob) => blob.get_hash(),
        }
    }

    #[inline(always)]
    pub fn to_owned(self) -> BlobData {
        match self {
            Self::Inline(bytes) => BlobData::Inline(bytes.to_vec()),
            Self::Coded(blob) => BlobData::Coded(blob),
        }
    }

    #[inline(always)]
    pub fn meta(self) -> Option<TrackMeta> {
        match self {
            Self::Inline(bytes) => Some(TrackMeta {
                kind: TrackKind::Inline,
                size: StorageUnits::from_bytes(bytes.len() as u64),
                state: TrackState::Certified,
                value_hash: hash(bytes),
            }),
            Self::Coded(blob) => {
                if blob.commitment_root() != blob.commitment {
                    return None;
                }

                Some(TrackMeta {
                    kind: TrackKind::Coded,
                    size: blob.size,
                    state: TrackState::Registered,
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
    pub state: TrackState,
    pub value_hash: Hash,
}

/// Owned object write envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct TrackObjectInfo {
    pub name: Vec<u8>,
    pub content_type: ContentType,
    pub logical_size: StorageUnits,
}

impl TrackObjectInfo {
    #[inline(always)]
    pub fn as_slice(&self) -> TrackObjectInfoSlice<'_> {
        TrackObjectInfoSlice {
            name: &self.name,
            content_type: self.content_type,
            logical_size: self.logical_size,
        }
    }
}

/// Borrowed object metadata attached to a named track write.
#[derive(Clone, Copy, Debug)]
pub struct TrackObjectInfoSlice<'source> {
    pub name: &'source [u8],
    pub content_type: ContentType,
    pub logical_size: StorageUnits,
}

/// Owned track write envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct BlobInfo {
    pub object: Option<TrackObjectInfo>,
    pub data: BlobData,
}

impl BlobInfo {
    #[inline(always)]
    pub fn as_slice(&self) -> BlobInfoSlice<'_> {
        BlobInfoSlice {
            object: self.object.as_ref().map(TrackObjectInfo::as_slice),
            data: self.data.as_slice(),
        }
    }
}

/// Borrowed track write envelope.
#[derive(Clone, Copy, Debug)]
pub struct BlobInfoSlice<'source> {
    pub object: Option<TrackObjectInfoSlice<'source>>,
    pub data: BlobDataSlice<'source>,
}

impl<'source> BlobInfoSlice<'source> {
    #[inline(always)]
    pub fn to_owned(self) -> BlobInfo {
        BlobInfo {
            object: self.object.map(|object| TrackObjectInfo {
                name: object.name.to_vec(),
                content_type: object.content_type,
                logical_size: object.logical_size,
            }),
            data: self.data.to_owned(),
        }
    }

    #[inline(always)]
    pub fn name(self) -> &'source [u8] {
        self.object.map(|object| object.name).unwrap_or_default()
    }
}

#[inline(always)]
pub fn track_key(name: &[u8], data: &BlobDataSlice<'_>) -> Hash {
    if !name.is_empty() {
        return hash(name);
    }

    match data {
        BlobDataSlice::Inline(bytes) => hash(bytes),
        BlobDataSlice::Coded(blob) => blob.commitment,
    }
}

#[cfg(test)]
#[cfg(feature = "wincode")]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::GROUP_SIZE;
    use crate::types::StripeCount;

    fn sample_blob_encoding() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(1024),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x33; 32]); GROUP_SIZE],
        }
    }

    #[test]
    fn blob_data_wincode() {
        let data = BlobData::Coded(sample_blob_encoding());
        let bytes = wincode::serialize(&data).expect("serialize");
        let recovered: BlobData = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, data);
    }

    #[test]
    fn envelope_wincode() {
        let blob = BlobInfo {
            object: Some(TrackObjectInfo {
                name: b"photos/cat.jpg".to_vec(),
                content_type: ContentType::ImageJpeg,
                logical_size: StorageUnits::from_bytes(1024),
            }),
            data: BlobData::Coded(sample_blob_encoding()),
        };

        let bytes = wincode::serialize(&blob).expect("serialize");
        let recovered: BlobInfo = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, blob);
    }
}
