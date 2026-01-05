//! Key types with big-endian encoding for proper lexicographic sorting

use crate::types::Pubkey;
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// New SliceKey structure: (spool_idx, track_address)
/// Serializes as 34 bytes: [spool_idx BE 2 bytes][track_address 32 bytes]
///
/// This key structure enables:
/// - Efficient iteration by spool: "give me all slices for spool 42"
/// - Direct lookup: "give me slice for spool 42, track X"
/// - Efficient GC: "delete slices for spools I no longer own"
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SliceKey {
    pub spool_idx: u16,
    pub track_address: Pubkey,
}

impl SliceKey {
    pub fn new(spool_idx: u16, track_address: Pubkey) -> Self {
        Self {
            spool_idx,
            track_address,
        }
    }
}

impl SchemaWrite for SliceKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(34) // 2 bytes spool_idx + 32 bytes track_address
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let spool_bytes = src.spool_idx.to_be_bytes();
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&src.track_address.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SliceKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SliceKey>) -> ReadResult<()> {
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 32] = unsafe { reader.get_t()? };
        let spool_idx = u16::from_be_bytes(spool_bytes);
        dst.write(SliceKey {
            spool_idx,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}

/// Spool index key (big-endian encoding)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpoolKey(pub u16);

impl SchemaWrite for SpoolKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(2)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SpoolKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SpoolKey>) -> ReadResult<()> {
        let bytes: [u8; 2] = unsafe { reader.get_t()? };
        let idx = u16::from_be_bytes(bytes);
        dst.write(SpoolKey(idx));
        Ok(())
    }
}

/// Key for GC index: (timestamp, spool_idx, track_address)
/// Serializes as 42 bytes: [gc_at BE 8 bytes][spool_idx BE 2 bytes][track_address 32 bytes]
///
/// Time-ordered for efficient GC sweeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GcKey {
    pub timestamp: i64,
    pub spool_idx: u16,
    pub track_address: Pubkey,
}

impl GcKey {
    pub fn new(timestamp: i64, spool_idx: u16, track_address: Pubkey) -> Self {
        Self {
            timestamp,
            spool_idx,
            track_address,
        }
    }
}

impl SchemaWrite for GcKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(42) // 8 bytes timestamp + 2 bytes spool_idx + 32 bytes track_address
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let ts_bytes = src.timestamp.to_be_bytes();
        let spool_bytes = src.spool_idx.to_be_bytes();
        writer.write_exact(&ts_bytes)?;
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&src.track_address.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for GcKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<GcKey>) -> ReadResult<()> {
        let ts_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 32] = unsafe { reader.get_t()? };
        let timestamp = i64::from_be_bytes(ts_bytes);
        let spool_idx = u16::from_be_bytes(spool_bytes);
        dst.write(GcKey {
            timestamp,
            spool_idx,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}
