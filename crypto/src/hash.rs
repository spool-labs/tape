#![allow(unexpected_cfgs)]

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

pub const HASH_BYTES: usize = 32;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, Default, Pod, Zeroable, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Hash(pub [u8; HASH_BYTES]);

impl From<Hash> for [u8; HASH_BYTES] {
    fn from(from: Hash) -> Self {
        from.0
    }
}

impl From<[u8; HASH_BYTES]> for Hash {
    fn from(from: [u8; HASH_BYTES]) -> Self {
        Self(from)
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Hash {
    pub const LEN: usize = HASH_BYTES;

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        solana_program::pubkey::Pubkey::new_unique().to_bytes().into()
    }

    pub fn to_bytes(self) -> [u8; HASH_BYTES] {
        self.0
    }
}

#[inline(always)]
pub fn hashv(data: &[&[u8]]) -> Hash {
    let res = solana_program::blake3::hashv(data);
    Hash(res.to_bytes())
}

#[inline(always)]
pub fn hash(data: &[u8]) -> Hash {
    let res = solana_program::blake3::hash(data);
    Hash(res.to_bytes())
}
