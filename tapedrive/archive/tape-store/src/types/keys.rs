//! Key types with big-endian encoding for proper lexicographic sorting

use super::ids::{TapeNumber, TrackNumber};
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Key for tape lookup by ID (big-endian encoding)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TapeKey(pub TapeNumber);

impl SchemaWrite for TapeKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(8)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0 .0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for TapeKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<TapeKey>) -> ReadResult<()> {
        let bytes: [u8; 8] = unsafe { reader.get_t()? };
        let id = u64::from_be_bytes(bytes);
        dst.write(TapeKey(TapeNumber(id)));
        Ok(())
    }
}

/// Key for track lookup by ID (big-endian encoding)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackKey(pub TrackNumber);

impl SchemaWrite for TrackKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(8)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0 .0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for TrackKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<TrackKey>) -> ReadResult<()> {
        let bytes: [u8; 8] = unsafe { reader.get_t()? };
        let id = u64::from_be_bytes(bytes);
        dst.write(TrackKey(TrackNumber(id)));
        Ok(())
    }
}

/// Composite key for slice data/meta/state: (track_id, spool_idx)
/// Serializes as 10 bytes: [track_id BE 8 bytes][spool_idx BE 2 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SliceKey {
    pub track_id: TrackNumber,
    pub spool_idx: u16,
}

impl SliceKey {
    pub fn new(track_id: TrackNumber, spool_idx: u16) -> Self {
        Self { track_id, spool_idx }
    }
}

impl SchemaWrite for SliceKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(10)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let track_bytes = src.track_id.0.to_be_bytes();
        let spool_bytes = src.spool_idx.to_be_bytes();
        writer.write_exact(&track_bytes)?;
        writer.write_exact(&spool_bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SliceKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SliceKey>) -> ReadResult<()> {
        let track_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let track_id = u64::from_be_bytes(track_bytes);
        let spool_idx = u16::from_be_bytes(spool_bytes);
        dst.write(SliceKey {
            track_id: TrackNumber(track_id),
            spool_idx,
        });
        Ok(())
    }
}

/// Key for pending recovery queue: (spool_idx, track_id)
/// Serializes as 10 bytes: [spool_idx BE 2 bytes][track_id BE 8 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecoveryKey {
    pub spool_idx: u16,
    pub track_id: TrackNumber,
}

impl RecoveryKey {
    pub fn new(spool_idx: u16, track_id: TrackNumber) -> Self {
        Self { spool_idx, track_id }
    }
}

impl SchemaWrite for RecoveryKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(10)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let spool_bytes = src.spool_idx.to_be_bytes();
        let track_bytes = src.track_id.0.to_be_bytes();
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&track_bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for RecoveryKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<RecoveryKey>) -> ReadResult<()> {
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_idx = u16::from_be_bytes(spool_bytes);
        let track_id = u64::from_be_bytes(track_bytes);
        dst.write(RecoveryKey {
            spool_idx,
            track_id: TrackNumber(track_id),
        });
        Ok(())
    }
}

/// Key for GC index: (gc_at, track_id, spool_idx)
/// Serializes as 18 bytes: [gc_at BE 8 bytes][track_id BE 8 bytes][spool_idx BE 2 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GcKey {
    pub gc_at: i64,
    pub track_id: TrackNumber,
    pub spool_idx: u16,
}

impl GcKey {
    pub fn new(gc_at: i64, track_id: TrackNumber, spool_idx: u16) -> Self {
        Self {
            gc_at,
            track_id,
            spool_idx,
        }
    }
}

impl SchemaWrite for GcKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(18)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let gc_bytes = src.gc_at.to_be_bytes();
        let track_bytes = src.track_id.0.to_be_bytes();
        let spool_bytes = src.spool_idx.to_be_bytes();
        writer.write_exact(&gc_bytes)?;
        writer.write_exact(&track_bytes)?;
        writer.write_exact(&spool_bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for GcKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<GcKey>) -> ReadResult<()> {
        let gc_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let gc_at = i64::from_be_bytes(gc_bytes);
        let track_id = u64::from_be_bytes(track_bytes);
        let spool_idx = u16::from_be_bytes(spool_bytes);
        dst.write(GcKey {
            gc_at,
            track_id: TrackNumber(track_id),
            spool_idx,
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
