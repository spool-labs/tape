#![allow(unexpected_cfgs)]

use core::str::FromStr;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(not(target_os = "solana"))]
use solana_hash::{Hash as SolanaRuntimeHash, ParseHashError};
#[cfg(not(target_os = "solana"))]
use tape_sha256::{hash_messages, Message};
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
    return hashv_parts(data);
    #[cfg(target_os = "solana")]
    return Hash(solana_program::hash::hashv(data).to_bytes());
}

/// SHA-256 of the parts in order, on the kernel the CPU supports
#[cfg(not(target_os = "solana"))]
fn hashv_parts(parts: &[&[u8]]) -> Hash {
    let joined: Vec<u8>;
    let message = match parts {
        [] => Message::new(&[]),
        [body] => Message::new(body),
        [prefix, body] => Message::prefixed(prefix, body),
        [prefix, left, right] => Message::pair(prefix, left, right),
        [_, _, _, _, ..] => {
            joined = parts.concat();
            Message::new(&joined)
        }
    };
    let mut out = [[0u8; HASH_BYTES]];
    hash_messages(&[message], &mut out);
    Hash(out[0])
}

/// Name of the SHA-256 kernel this build hashes with.
///
/// Off-chain it is chosen from what the CPU reports, so a box that fell back to
/// one lane is otherwise indistinguishable.
pub fn sha256_backend() -> &'static str {
    #[cfg(not(target_os = "solana"))]
    return tape_sha256::backend();
    #[cfg(target_os = "solana")]
    return "solana-syscall";
}

/// How many messages that kernel hashes at once.
///
/// One means every message costs a pass of its own.
pub fn sha256_lane_width() -> usize {
    #[cfg(not(target_os = "solana"))]
    return tape_sha256::lane_width();
    #[cfg(target_os = "solana")]
    return 1;
}

#[inline(always)]
pub fn hash(data: &[u8]) -> Hash {
    #[cfg(not(target_os = "solana"))]
    return hashv_parts(&[data]);
    #[cfg(target_os = "solana")]
    return Hash(solana_program::hash::hash(data).to_bytes());
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

    // single buffers match the standard SHA-256 digests across padding edges
    #[test]
    fn known_vectors() {
        let vectors = [
            (0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            (1, "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"),
            (55, "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318"),
            (56, "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"),
            (64, "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"),
            (1000, "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"),
        ];
        for (len, digest) in vectors {
            let data = vec![b'a'; len];
            assert_eq!(hex::encode(hash(&data).0), digest);
            assert_eq!(hex::encode(hashv(&[&data]).0), digest);
        }
    }

    // any split into parts hashes the concatenation
    #[test]
    fn split_parts() {
        let sentence: &[u8] = b"The quick brown fox jumps over the lazy dog";
        let digest = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592";
        let cuts = [4, 10, 16, 20, 35];
        for count in 1..=5 {
            let mut parts = Vec::new();
            let mut start = 0;
            for cut in &cuts[..count - 1] {
                parts.push(&sentence[start..*cut]);
                start = *cut;
            }
            parts.push(&sentence[start..]);
            assert_eq!(parts.len(), count);
            assert_eq!(hex::encode(hashv(&parts).0), digest);
        }
        assert_eq!(hex::encode(hashv(&[b"", sentence, b""]).0), digest);
    }
}
