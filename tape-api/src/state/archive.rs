use steel::*;
use super::AccountType;
use crate::{state, types::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The unique identifier for this archive.
    pub id: ArchiveNumber,

    /// The encoding scheme used by this archive (e.g., erasure coding = 0, replication = 1).
    pub encoding: u64,

    /// The number of data shards (spools) in the encoding scheme.
    pub spool_count: u64,

    /// The total storage capacity of the archive.
    pub storage_capacity: u64,

    /// The total storage used by the archive.
    pub storage_used: u64,
}

state!(AccountType, Archive);
