//! Key types with big-endian encoding for proper lexicographic sorting
//!
//! All composite keys use big-endian encoding to ensure proper ordering in RocksDB:
//! - SpoolEpochKey: (epoch BE, spool_id BE) - epoch first for range cleanup
//! - SliceKey: (spool_id BE, track_address) - spool first for prefix iteration
//! - PendingRecoveryKey: (epoch BE, spool_id BE, slice_type, track_address)
//! - EpochKey: epoch BE
//! - UnitKey: empty key for singletons

use crate::types::{Pubkey, SliceType};
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Key for epoch-namespaced spool operations (10 bytes)
///
/// Format: [epoch BE 8 bytes][spool_id BE 2 bytes]
///
/// Epoch-first ordering enables efficient range deletion of old epoch data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpoolEpochKey {
    pub epoch: u64,
    pub spool_id: u16,
}

impl SpoolEpochKey {
    pub const SIZE: usize = 10;

    pub fn new(epoch: u64, spool_id: u16) -> Self {
        Self { epoch, spool_id }
    }

    /// Create prefix bytes for epoch-based iteration
    pub fn epoch_prefix(epoch: u64) -> [u8; 8] {
        epoch.to_be_bytes()
    }
}

impl SchemaWrite for SpoolEpochKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let epoch_bytes = src.epoch.to_be_bytes();
        let spool_bytes = src.spool_id.to_be_bytes();
        writer.write_exact(&epoch_bytes)?;
        writer.write_exact(&spool_bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SpoolEpochKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SpoolEpochKey>) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let epoch = u64::from_be_bytes(epoch_bytes);
        let spool_id = u16::from_be_bytes(spool_bytes);
        dst.write(SpoolEpochKey { epoch, spool_id });
        Ok(())
    }
}

/// Key for slice data and metadata (34 bytes)
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

/// Key for pending recovery entries (43 bytes)
///
/// Format: [epoch BE 8 bytes][spool_id BE 2 bytes][slice_type 1 byte][track_address 32 bytes]
///
/// Epoch-first for cleanup, spool-second for iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingRecoveryKey {
    pub epoch: u64,
    pub spool_id: u16,
    pub slice_type: SliceType,
    pub track_address: Pubkey,
}

impl PendingRecoveryKey {
    pub const SIZE: usize = 43;

    pub fn new(epoch: u64, spool_id: u16, slice_type: SliceType, track_address: Pubkey) -> Self {
        Self {
            epoch,
            spool_id,
            slice_type,
            track_address,
        }
    }

    /// Create prefix bytes for epoch + spool iteration
    pub fn epoch_spool_prefix(epoch: u64, spool_id: u16) -> [u8; 10] {
        let mut prefix = [0u8; 10];
        prefix[0..8].copy_from_slice(&epoch.to_be_bytes());
        prefix[8..10].copy_from_slice(&spool_id.to_be_bytes());
        prefix
    }
}

impl SchemaWrite for PendingRecoveryKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let epoch_bytes = src.epoch.to_be_bytes();
        let spool_bytes = src.spool_id.to_be_bytes();
        let slice_type_byte = src.slice_type as u8;
        writer.write_exact(&epoch_bytes)?;
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&[slice_type_byte])?;
        writer.write_exact(&src.track_address.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for PendingRecoveryKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<PendingRecoveryKey>) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let slice_type_byte: [u8; 1] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 32] = unsafe { reader.get_t()? };

        let epoch = u64::from_be_bytes(epoch_bytes);
        let spool_id = u16::from_be_bytes(spool_bytes);
        let slice_type = match slice_type_byte[0] {
            0 => SliceType::Primary,
            1 => SliceType::Recovery,
            _ => SliceType::Primary, // Default for invalid values
        };

        dst.write(PendingRecoveryKey {
            epoch,
            spool_id,
            slice_type,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spool_epoch_key_size() {
        let key = SpoolEpochKey::new(100, 42);
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SpoolEpochKey::SIZE);
    }

    #[test]
    fn test_spool_epoch_key_ordering() {
        // Epoch 1, spool 100 should come before epoch 2, spool 1
        let key1 = SpoolEpochKey::new(1, 100);
        let key2 = SpoolEpochKey::new(2, 1);

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "epoch should be primary sort key");
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
    fn test_pending_recovery_key_size() {
        let key = PendingRecoveryKey::new(100, 42, SliceType::Primary, Pubkey([1u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), PendingRecoveryKey::SIZE);
    }

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
    fn test_unit_key_size() {
        let key = UnitKey;
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), UnitKey::SIZE);
    }

    #[test]
    fn test_spool_epoch_key_roundtrip() {
        let key = SpoolEpochKey::new(12345, 678);
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SpoolEpochKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_slice_key_roundtrip() {
        let key = SliceKey::new(42, Pubkey([0xAB; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SliceKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_pending_recovery_key_roundtrip() {
        let key = PendingRecoveryKey::new(100, 42, SliceType::Recovery, Pubkey([0xCD; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: PendingRecoveryKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_epoch_spool_prefix() {
        let prefix = PendingRecoveryKey::epoch_spool_prefix(100, 42);
        assert_eq!(prefix.len(), 10);

        // Verify prefix matches start of full key
        let key = PendingRecoveryKey::new(100, 42, SliceType::Primary, Pubkey([0u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(&bytes[0..10], &prefix);
    }
}
