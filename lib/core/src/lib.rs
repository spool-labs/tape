#![allow(unexpected_cfgs)]

pub mod bft;
pub mod bls;
pub mod cert;
pub mod encoding;
pub mod erasure;
pub mod track;
pub mod spooler;
pub mod staking;
pub mod system;
pub mod types;
pub mod snapshot;
pub mod tape;
mod macros;

pub mod prelude {
    pub use crate::encoding::EncodingProfile;
    pub use crate::types::{GroupIndex, SpoolIndex};
    pub use crate::system::{
        EpochPhase, EpochState, NodeStatus, SpoolState, SpoolStatus, VoteKind,
    };
    pub use crate::track::blob::BlobEncoding;
    pub use crate::track::data::{BlobData, BlobDataSlice, BlobInfo, BlobInfoSlice};
    pub use crate::track::types::{CompressedTrack, TrackKind, TrackState};
    pub use crate::types::{
        ChunkNumber, ContentType, EpochNumber, NodeId, SlotNumber, StorageUnits, StripeCount,
        TapeNumber, TrackNumber,
    };
}
