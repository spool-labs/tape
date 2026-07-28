#![allow(unexpected_cfgs)]

use core::str::FromStr;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(not(target_os = "solana"))]
use solana_hash::{Hash as SolanaRuntimeHash, ParseHashError};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

pub const HASH_BYTES: usize = 32;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, Default, Pod, Zeroable, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
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
        SolanaRuntimeHash::new_unique().to_bytes().into()
    }

    pub fn to_bytes(self) -> [u8; HASH_BYTES] {
        self.0
    }

}

#[cfg(not(target_os = "solana"))]
impl From<SolanaRuntimeHash> for Hash {
    fn from(value: SolanaRuntimeHash) -> Self {
        Self(value.to_bytes())
    }
}

#[cfg(not(target_os = "solana"))]
impl From<Hash> for SolanaRuntimeHash {
    fn from(value: Hash) -> Self {
        SolanaRuntimeHash::new_from_array(value.0)
    }
}

#[cfg(not(target_os = "solana"))]
impl core::fmt::Display for Hash {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&SolanaRuntimeHash::new_from_array(self.0), formatter)
    }
}

#[cfg(not(target_os = "solana"))]
impl FromStr for Hash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SolanaRuntimeHash::from_str(s).map(|h| Self(h.to_bytes()))
    }
}

#[cfg(target_os = "solana")]
impl FromStr for Hash {
    type Err = solana_program::hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        solana_program::hash::Hash::from_str(s).map(|h| Self(h.to_bytes()))
    }
}

#[inline(always)]
pub fn hashv(data: &[&[u8]]) -> Hash {
    #[cfg(not(target_os = "solana"))]
    let res = solana_sha256_hasher::hashv(data);
    #[cfg(target_os = "solana")]
    let res = solana_program::hash::hashv(data);

    Hash(res.to_bytes())
}

#[inline(always)]
pub fn hash(data: &[u8]) -> Hash {
    #[cfg(not(target_os = "solana"))]
    let res = solana_sha256_hasher::hash(data);
    #[cfg(target_os = "solana")]
    let res = solana_program::hash::hash(data);

    Hash(res.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_uses_solana_hash_encoding() {
        let hash = Hash([7u8; HASH_BYTES]);
        let expected = SolanaRuntimeHash::new_from_array(hash.0).to_string();

        assert_eq!(hash.to_string(), expected);
        assert!(!hash.to_string().contains('['));
    }
}
