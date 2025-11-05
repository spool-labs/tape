use bytemuck::{ Pod, Zeroable };
use num_enum::{ IntoPrimitive, TryFromPrimitive };
use tape_crypto::hash::Hash;
use crate::types::EpochNumber;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum BlobState {
    Unknown = 0,
    Registered,   // Data is being synced to the network
    Certified,    // The blob is certified and available
    Invalidated,  // The network has found an inconsistency with the blob
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlobData {
    /// The state of this blob.
    pub state: u64,

    /// The epoch when this blob was registered.
    pub registered_epoch: EpochNumber,

    /// The epoch when this blob was certified.
    pub certified_epoch: EpochNumber,

    /// The merkle root of the erasure coded data.
    pub commitment_hash: Hash,

    /// The number of parity segments.
    pub num_parity: u64,

    /// The number of data segments.
    pub num_data: u64,
}


