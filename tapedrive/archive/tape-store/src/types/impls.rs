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

// ============================================================================
// StoredPubkey - wrapper for solana_program::Pubkey
// ============================================================================

/// A wincode-serializable wrapper around Pubkey for storage operations.
///
/// This type stores pubkeys as raw 32-byte arrays and provides conversions
/// to/from solana_program::Pubkey.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct StoredPubkey(pub [u8; 32]);

impl StoredPubkey {
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

impl From<solana_program::pubkey::Pubkey> for StoredPubkey {
    fn from(pubkey: solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl From<StoredPubkey> for solana_program::pubkey::Pubkey {
    fn from(stored: StoredPubkey) -> Self {
        solana_program::pubkey::Pubkey::new_from_array(stored.0)
    }
}

impl From<&solana_program::pubkey::Pubkey> for StoredPubkey {
    fn from(pubkey: &solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl AsRef<[u8]> for StoredPubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl SchemaWrite for StoredPubkey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for StoredPubkey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<StoredPubkey>) -> ReadResult<()> {
        let bytes: [u8; 32] = unsafe { reader.get_t()? };
        dst.write(StoredPubkey(bytes));
        Ok(())
    }
}
