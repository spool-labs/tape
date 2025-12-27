//! Dummy ID types - will be replaced by tape-core imports later

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

// We really shouldn't be using these, they already exist in tape-core, tape-crypto, and solana.
// These we're placed here as the crate was worked on before tape-core was ready to be depended on.
// The biggest issue is that the on-chain types are zero-copy POD, whereas these are
// bincode/wincode/serde. We should try to find a way to resolve this without adding wincode/serde
// to the parent crates. Not sure what the best path forward is as I'd love to use the POD types we
// have rather than redefining them here or mutating their definitions in another crate due to
// serialization requirements here.

/// Tape identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeNumber(pub u64);

/// Track identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackNumber(pub u64);

/// Epoch number for rotation cycles
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct EpochNumber(pub u64);

/// Node identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NodeId(pub u64);

/// Hash value (32 bytes)
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Hash(#[wincode(with = "wincode::containers::Pod<_>")] pub [u8; 32]);

impl Hash {
    pub const ZERO: Self = Hash([0u8; 32]);
}

/// Solana-style public key (32 bytes)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Pubkey(#[wincode(with = "wincode::containers::Pod<_>")] pub [u8; 32]);

impl Pubkey {
    pub const ZERO: Self = Pubkey([0u8; 32]);
}
