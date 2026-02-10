//! Wincode-compatible wrapper types for external types
//!
//! This module provides wrapper types with SchemaRead/SchemaWrite implementations
//! for types that can't be modified in their source crates.

use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// A wincode-serializable wrapper around solana Pubkey for storage operations.
///
/// This type stores pubkeys as raw 32-byte arrays and provides conversions
/// to/from solana_program::pubkey::Pubkey via `.into()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Pubkey(pub [u8; 32]);

impl Pubkey {
    pub const LEN: usize = 32;

    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        Self(solana_program::pubkey::Pubkey::new_unique().to_bytes())
    }
}

impl From<solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl From<Pubkey> for solana_program::pubkey::Pubkey {
    fn from(stored: Pubkey) -> Self {
        solana_program::pubkey::Pubkey::new_from_array(stored.0)
    }
}

impl From<&solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: &solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl std::fmt::Display for Pubkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pk: solana_program::pubkey::Pubkey = (*self).into();
        write!(f, "{pk}")
    }
}

impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl SchemaWrite for Pubkey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for Pubkey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Pubkey>) -> ReadResult<()> {
        let bytes: [u8; 32] = unsafe { reader.get_t()? };
        dst.write(Pubkey(bytes));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pubkey_roundtrip() {
        let pubkey = Pubkey::new([0xAB; 32]);
        let bytes = wincode::serialize(&pubkey).unwrap();
        let decoded: Pubkey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(pubkey, decoded);
    }

    #[test]
    fn test_pubkey_conversion() {
        let solana_pubkey = solana_program::pubkey::Pubkey::new_unique();
        let stored: Pubkey = solana_pubkey.into();
        let back: solana_program::pubkey::Pubkey = stored.into();
        assert_eq!(solana_pubkey, back);
    }
}
