//! Key types with big-endian encoding for proper lexicographic sorting
//!
//! All composite keys use big-endian encoding to ensure proper ordering in RocksDB:
//! - EpochKey: epoch BE (8 bytes)
//! - UnitKey: empty (0 bytes)
//! - SpoolIndexKey: spool_id BE (2 bytes)
//! - SliceKey: (spool_id BE, track_address) (34 bytes)
//! - TrackLookupKey: (tape, track_number BE, key) (72 bytes)

use crate::types::Pubkey;
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use tape_core::types::TrackNumber;
use tape_crypto::Hash;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Key for epoch-indexed data (8 bytes)
///
/// Format: [epoch BE 8 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EpochKey(pub u64);

impl EpochKey {
    pub const SIZE: usize = 8;

    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }
}

impl SchemaWrite for EpochKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for EpochKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<EpochKey>) -> ReadResult<()> {
        let bytes: [u8; 8] = unsafe { reader.get_t()? };
        let epoch = u64::from_be_bytes(bytes);
        dst.write(EpochKey(epoch));
        Ok(())
    }
}

/// Singleton key (0 bytes) for entries that have exactly one value
///
/// Used for sync_cursor and similar singleton values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct UnitKey;

impl UnitKey {
    pub const SIZE: usize = 0;
}

impl SchemaWrite for UnitKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(_writer: &mut Writer, _src: &Self::Src) -> WriteResult<()> {
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for UnitKey {
    type Dst = Self;

    fn read(_reader: &mut Reader<'de>, dst: &mut MaybeUninit<UnitKey>) -> ReadResult<()> {
        dst.write(UnitKey);
        Ok(())
    }
}

/// Key for spool-indexed data (2 bytes)
///
/// Format: [spool_id BE 2 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpoolIndexKey(pub u16);

impl SpoolIndexKey {
    pub const SIZE: usize = 2;

    pub fn new(spool_id: u16) -> Self {
        Self(spool_id)
    }
}

impl SchemaWrite for SpoolIndexKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SpoolIndexKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SpoolIndexKey>) -> ReadResult<()> {
        let bytes: [u8; 2] = unsafe { reader.get_t()? };
        let spool_id = u16::from_be_bytes(bytes);
        dst.write(SpoolIndexKey(spool_id));
        Ok(())
    }
}

/// Key for slice data and pending recovery (34 bytes)
///
/// Format: [spool_id BE 2 bytes][track_address 32 bytes]
///
/// Spool-first ordering enables efficient prefix iteration by spool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SliceKey {
    pub spool_id: u16,
    pub track_address: Pubkey,
}

impl SliceKey {
    pub const SIZE: usize = 34;

    pub fn new(spool_id: u16, track_address: Pubkey) -> Self {
        Self {
            spool_id,
            track_address,
        }
    }

    /// Create prefix bytes for spool-based iteration
    pub fn spool_prefix(spool_id: u16) -> [u8; 2] {
        spool_id.to_be_bytes()
    }
}

/// Key for tape-local ordered track lookup (72 bytes).
///
/// Format: [tape 32 bytes][track_number BE 8 bytes][key 32 bytes]
///
/// Tape-first ordering enables efficient prefix iteration by tape, while
/// track_number ordering enables ordered tape scans for list/proof generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackLookupKey {
    pub tape: Pubkey,
    pub track_number: TrackNumber,
    pub key: Hash,
}

impl TrackLookupKey {
    pub const SIZE: usize = 72;

    pub fn new(tape: Pubkey, track_number: TrackNumber, key: Hash) -> Self {
        Self {
            tape,
            track_number,
            key,
        }
    }

    /// Create prefix bytes for tape-based iteration.
    pub fn tape_prefix(tape: Pubkey) -> [u8; 32] {
        tape.0
    }

    /// Create a start key that sorts immediately after all entries for the
    /// given track number under the tape.
    pub fn after_track_number(tape: Pubkey, track_number: TrackNumber) -> Self {
        Self {
            tape,
            track_number,
            key: Hash([u8::MAX; 32]),
        }
    }
}

impl SchemaWrite for TrackLookupKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.tape.0)?;
        writer.write_exact(&src.track_number.0.to_be_bytes())?;
        writer.write_exact(&src.key.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for TrackLookupKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<TrackLookupKey>) -> ReadResult<()> {
        let tape: [u8; 32] = unsafe { reader.get_t()? };
        let track_number: [u8; 8] = unsafe { reader.get_t()? };
        let key: [u8; 32] = unsafe { reader.get_t()? };
        dst.write(TrackLookupKey {
            tape: Pubkey(tape),
            track_number: TrackNumber(u64::from_be_bytes(track_number)),
            key: Hash(key),
        });
        Ok(())
    }
}

impl SchemaWrite for SliceKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let spool_bytes = src.spool_id.to_be_bytes();
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
        let spool_id = u16::from_be_bytes(spool_bytes);
        dst.write(SliceKey {
            spool_id,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}

/// Key for event log entries (20 bytes)
///
/// Format: [epoch BE 8 bytes][slot BE 8 bytes][seq BE 4 bytes]
///
/// This enables:
/// - Prefix scan by epoch (first 8 bytes)
/// - Ordered iteration by (slot, sequence) within an epoch
/// - Efficient deletion of all events for an epoch
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventLogKey {
    pub epoch: u64,
    pub slot: u64,
    pub seq: u32,
}

impl EventLogKey {
    pub const SIZE: usize = 20;

    pub fn new(epoch: u64, slot: u64, seq: u32) -> Self {
        Self { epoch, slot, seq }
    }

    /// Create prefix bytes for epoch-based iteration (first 8 bytes).
    pub fn epoch_prefix(epoch: u64) -> [u8; 8] {
        epoch.to_be_bytes()
    }
}

impl SchemaWrite for EventLogKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.epoch.to_be_bytes())?;
        writer.write_exact(&src.slot.to_be_bytes())?;
        writer.write_exact(&src.seq.to_be_bytes())?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for EventLogKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<EventLogKey>) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let slot_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let seq_bytes: [u8; 4] = unsafe { reader.get_t()? };
        dst.write(EventLogKey {
            epoch: u64::from_be_bytes(epoch_bytes),
            slot: u64::from_be_bytes(slot_bytes),
            seq: u32::from_be_bytes(seq_bytes),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_key_size() {
        let key = EpochKey::new(12345);
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), EpochKey::SIZE);
    }

    #[test]
    fn test_epoch_key_ordering() {
        let key1 = EpochKey::new(1);
        let key2 = EpochKey::new(256);

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2);
    }

    #[test]
    fn test_epoch_key_roundtrip() {
        let key = EpochKey::new(12345);
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: EpochKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_unit_key_size() {
        let key = UnitKey;
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), UnitKey::SIZE);
    }

    #[test]
    fn test_spool_index_key_size() {
        let key = SpoolIndexKey::new(42);
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SpoolIndexKey::SIZE);
    }

    #[test]
    fn test_spool_index_key_ordering() {
        let key1 = SpoolIndexKey::new(1);
        let key2 = SpoolIndexKey::new(256);

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "spool_id should sort numerically");
    }

    #[test]
    fn test_spool_index_key_roundtrip() {
        let key = SpoolIndexKey::new(1023);
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SpoolIndexKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_slice_key_size() {
        let key = SliceKey::new(42, Pubkey([1u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SliceKey::SIZE);
    }

    #[test]
    fn test_slice_key_ordering() {
        // Spool 1 should come before spool 100
        let key1 = SliceKey::new(1, Pubkey([255u8; 32]));
        let key2 = SliceKey::new(100, Pubkey([0u8; 32]));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "spool_id should be primary sort key");
    }

    #[test]
    fn test_slice_key_roundtrip() {
        let key = SliceKey::new(42, Pubkey([0xAB; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SliceKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_track_lookup_key_size() {
        let key = TrackLookupKey::new(
            Pubkey([0xAA; 32]),
            TrackNumber(42),
            Hash([0xBB; 32]),
        );
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), TrackLookupKey::SIZE);
    }

    #[test]
    fn test_track_lookup_key_ordering() {
        let key1 = TrackLookupKey::new(Pubkey([1u8; 32]), TrackNumber(1), Hash([0u8; 32]));
        let key2 = TrackLookupKey::new(Pubkey([1u8; 32]), TrackNumber(2), Hash([0u8; 32]));
        let key3 = TrackLookupKey::new(Pubkey([2u8; 32]), TrackNumber(0), Hash([0u8; 32]));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();
        let bytes3 = wincode::serialize(&key3).unwrap();

        assert!(bytes1 < bytes2);
        assert!(bytes2 < bytes3);
    }

    #[test]
    fn test_track_lookup_key_roundtrip() {
        let key = TrackLookupKey::new(
            Pubkey([0xCD; 32]),
            TrackNumber(77),
            Hash([0xEF; 32]),
        );
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: TrackLookupKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }
}
