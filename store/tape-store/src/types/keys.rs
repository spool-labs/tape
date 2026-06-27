//! Key types with big-endian encoding for proper lexicographic sorting
//!
//! All composite keys use big-endian encoding to ensure proper ordering in RocksDB:
//! - EpochKey: epoch BE (8 bytes)
//! - UnitKey: empty (0 bytes)
//! - SpoolIndexKey: spool_id BE (2 bytes)
//! - SliceKey: (spool_id BE, track_address) (34 bytes)
//! - TrackLookupKey: (tape, track_number BE, key) (72 bytes)
//! - SnapshotArtifactKey: (epoch BE, group BE, chunk BE) (24 bytes)
//! - VoteSigKey: (voting_epoch BE, kind BE, target_epoch BE, hash, group BE, signer) (96 bytes)

use std::mem::MaybeUninit;

use serde::{Deserialize, Serialize};
use tape_core::spooler::GroupIndex;
use tape_core::system::{VoteCandidate, VoteKind};
use tape_core::types::{EpochNumber, SpoolIndex, TrackNumber};
use tape_crypto::address::Address;
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
pub struct SpoolIndexKey(pub SpoolIndex);

impl SpoolIndexKey {
    pub const SIZE: usize = 2;

    pub fn new(spool_id: SpoolIndex) -> Self {
        Self(spool_id)
    }
}

impl SchemaWrite for SpoolIndexKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = (src.0.as_u64() as u16).to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SpoolIndexKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SpoolIndexKey>) -> ReadResult<()> {
        let bytes: [u8; 2] = unsafe { reader.get_t()? };
        let spool_id = u16::from_be_bytes(bytes);
        dst.write(SpoolIndexKey(SpoolIndex(spool_id as u64)));
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
    pub spool_id: SpoolIndex,
    pub track_address: Address,
}

impl SliceKey {
    pub const SIZE: usize = 34;

    pub fn new(spool_id: SpoolIndex, track_address: Address) -> Self {
        Self {
            spool_id,
            track_address,
        }
    }

    /// Create prefix bytes for spool-based iteration
    pub fn spool_prefix(spool_id: SpoolIndex) -> [u8; 2] {
        (spool_id.as_u64() as u16).to_be_bytes()
    }

    /// Byte range `[start, end)` covering every slice key for this spool, for a
    /// range delete or scan. `end` is `None` when the spool prefix is the maximum
    /// (`u16::MAX`) and so has no 2-byte exclusive successor.
    pub fn spool_key_range(spool_id: SpoolIndex) -> ([u8; 2], Option<[u8; 2]>) {
        let spool = spool_id.as_u64() as u16;
        (spool.to_be_bytes(), spool.checked_add(1).map(u16::to_be_bytes))
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
    pub tape: Address,
    pub track_number: TrackNumber,
    pub key: Hash,
}

impl TrackLookupKey {
    pub const SIZE: usize = 72;

    pub fn new(tape: Address, track_number: TrackNumber, key: Hash) -> Self {
        Self {
            tape,
            track_number,
            key,
        }
    }

    /// Create prefix bytes for tape-based iteration.
    pub fn tape_prefix(tape: Address) -> [u8; 32] {
        tape.to_bytes()
    }

    /// Create a start key that sorts immediately after all entries for the
    /// given track number under the tape.
    pub fn after_track_number(tape: Address, track_number: TrackNumber) -> Self {
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
        writer.write_exact(src.tape.as_ref())?;
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
            tape: Address::from(tape),
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
        let spool_bytes = (src.spool_id.as_u64() as u16).to_be_bytes();
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(src.track_address.as_ref())?;
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
            spool_id: SpoolIndex(spool_id as u64),
            track_address: Address::from(track_bytes),
        });
        Ok(())
    }
}

/// Key for per-chunk snapshot build artifacts (24 bytes)
///
/// Format: [epoch BE 8 bytes][group BE 8 bytes][chunk BE 8 bytes]
///
/// Enables:
/// - Prefix scan by epoch (8 bytes)
/// - Prefix scan by (epoch, group) (16 bytes)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotArtifactKey {
    pub epoch: u64,
    pub group: u64,
    pub chunk: u64,
}

impl SnapshotArtifactKey {
    pub const SIZE: usize = 24;

    pub fn new(epoch: u64, group: u64, chunk: u64) -> Self {
        Self { epoch, group, chunk }
    }

    pub fn epoch_prefix(epoch: u64) -> [u8; 8] {
        epoch.to_be_bytes()
    }

    pub fn group_prefix(epoch: u64, group: u64) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&epoch.to_be_bytes());
        buf[8..16].copy_from_slice(&group.to_be_bytes());
        buf
    }
}

impl SchemaWrite for SnapshotArtifactKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.epoch.to_be_bytes())?;
        writer.write_exact(&src.group.to_be_bytes())?;
        writer.write_exact(&src.chunk.to_be_bytes())?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SnapshotArtifactKey {
    type Dst = Self;

    fn read(
        reader: &mut Reader<'de>,
        dst: &mut MaybeUninit<SnapshotArtifactKey>,
    ) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let group_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let chunk_bytes: [u8; 8] = unsafe { reader.get_t()? };
        dst.write(SnapshotArtifactKey {
            epoch: u64::from_be_bytes(epoch_bytes),
            group: u64::from_be_bytes(group_bytes),
            chunk: u64::from_be_bytes(chunk_bytes),
        });
        Ok(())
    }
}

/// Key for generic pushed vote signatures (96 bytes)
///
/// Format:
/// [voting_epoch BE 8 bytes][kind BE 8 bytes][target_epoch BE 8 bytes]
/// [hash 32 bytes][group BE 8 bytes][signer address 32 bytes]
///
/// Ordering supports scans by voting epoch, candidate, and candidate group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoteSigKey {
    pub candidate: VoteCandidate,
    pub group: GroupIndex,
    pub signer: Address,
}

impl VoteSigKey {
    pub const SIZE: usize = 96;
    pub const EPOCH_PREFIX_SIZE: usize = 8;
    pub const CANDIDATE_PREFIX_SIZE: usize = 56;
    pub const GROUP_PREFIX_SIZE: usize = 64;

    pub fn new(candidate: VoteCandidate, group: GroupIndex, signer: Address) -> Self {
        Self {
            candidate,
            group,
            signer,
        }
    }

    pub fn epoch_prefix(voting_epoch: EpochNumber) -> [u8; Self::EPOCH_PREFIX_SIZE] {
        voting_epoch.0.to_be_bytes()
    }

    pub fn candidate_prefix(candidate: VoteCandidate) -> [u8; Self::CANDIDATE_PREFIX_SIZE] {
        let mut buf = [0u8; Self::CANDIDATE_PREFIX_SIZE];
        let kind: u64 = candidate.kind.into();
        buf[0..8].copy_from_slice(&candidate.voting_epoch.0.to_be_bytes());
        buf[8..16].copy_from_slice(&kind.to_be_bytes());
        buf[16..24].copy_from_slice(&candidate.target_epoch.0.to_be_bytes());
        buf[24..56].copy_from_slice(&candidate.hash.0);
        buf
    }

    pub fn group_prefix(
        candidate: VoteCandidate,
        group: GroupIndex,
    ) -> [u8; Self::GROUP_PREFIX_SIZE] {
        let mut buf = [0u8; Self::GROUP_PREFIX_SIZE];
        buf[0..Self::CANDIDATE_PREFIX_SIZE].copy_from_slice(&Self::candidate_prefix(candidate));
        buf[Self::CANDIDATE_PREFIX_SIZE..Self::GROUP_PREFIX_SIZE]
            .copy_from_slice(&group.0.to_be_bytes());
        buf
    }
}

impl SchemaWrite for VoteSigKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let kind: u64 = src.candidate.kind.into();
        writer.write_exact(&src.candidate.voting_epoch.0.to_be_bytes())?;
        writer.write_exact(&kind.to_be_bytes())?;
        writer.write_exact(&src.candidate.target_epoch.0.to_be_bytes())?;
        writer.write_exact(&src.candidate.hash.0)?;
        writer.write_exact(&src.group.0.to_be_bytes())?;
        writer.write_exact(src.signer.as_ref())?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for VoteSigKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<VoteSigKey>) -> ReadResult<()> {
        let voting_epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let kind_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let target_epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let hash: [u8; 32] = unsafe { reader.get_t()? };
        let group_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let signer: [u8; 32] = unsafe { reader.get_t()? };
        let kind =
            VoteKind::try_from(u64::from_be_bytes(kind_bytes)).unwrap_or(VoteKind::Unknown);
        dst.write(VoteSigKey {
            candidate: VoteCandidate {
                kind,
                voting_epoch: EpochNumber(u64::from_be_bytes(voting_epoch_bytes)),
                target_epoch: EpochNumber(u64::from_be_bytes(target_epoch_bytes)),
                hash: Hash(hash),
            },
            group: GroupIndex(u64::from_be_bytes(group_bytes)),
            signer: Address::from(signer),
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

/// Key for the per-bucket object listing index (variable length).
///
/// Format: `[bucket 32 bytes][name raw bytes]`. The bucket is a fixed 32-byte
/// prefix so per-bucket prefix scans work; the name is written as raw trailing
/// bytes (no length prefix) so keys sort in lexicographic name order — exactly
/// S3 `ListObjects` ordering.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectListKey {
    pub bucket: Address,
    pub name: Vec<u8>,
}

impl ObjectListKey {
    pub fn new(bucket: Address, name: impl Into<Vec<u8>>) -> Self {
        Self {
            bucket,
            name: name.into(),
        }
    }

    /// Prefix bytes for per-bucket iteration (32 bytes).
    pub fn bucket_prefix(bucket: Address) -> [u8; 32] {
        bucket.to_bytes()
    }
}

impl SchemaWrite for ObjectListKey {
    type Src = Self;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        Ok(32 + src.name.len())
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(src.bucket.as_ref())?;
        writer.write_exact(&src.name)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for ObjectListKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<ObjectListKey>) -> ReadResult<()> {
        let bucket: [u8; 32] = unsafe { reader.get_t()? };
        let remaining = reader.as_slice().len();
        let name = reader.read_borrowed(remaining)?.to_vec();
        dst.write(ObjectListKey {
            bucket: Address::from(bucket),
            name,
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
        let key = SpoolIndexKey::new(SpoolIndex(42));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SpoolIndexKey::SIZE);
    }

    #[test]
    fn test_spool_index_key_ordering() {
        let key1 = SpoolIndexKey::new(SpoolIndex(1));
        let key2 = SpoolIndexKey::new(SpoolIndex(256));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "spool_id should sort numerically");
    }

    #[test]
    fn test_spool_index_key_roundtrip() {
        let key = SpoolIndexKey::new(SpoolIndex(1023));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SpoolIndexKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_slice_key_size() {
        let key = SliceKey::new(SpoolIndex(42), Address::new([1u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SliceKey::SIZE);
    }

    #[test]
    fn test_slice_key_ordering() {
        // Spool 1 should come before spool 100
        let key1 = SliceKey::new(SpoolIndex(1), Address::new([255u8; 32]));
        let key2 = SliceKey::new(SpoolIndex(100), Address::new([0u8; 32]));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "spool_id should be primary sort key");
    }

    #[test]
    fn test_slice_key_roundtrip() {
        let key = SliceKey::new(SpoolIndex(42), Address::new([0xAB; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SliceKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_track_lookup_key_size() {
        let key = TrackLookupKey::new(
            Address::new([0xAA; 32]),
            TrackNumber(42),
            Hash([0xBB; 32]),
        );
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), TrackLookupKey::SIZE);
    }

    #[test]
    fn test_track_lookup_key_ordering() {
        let key1 = TrackLookupKey::new(Address::new([1u8; 32]), TrackNumber(1), Hash([0u8; 32]));
        let key2 = TrackLookupKey::new(Address::new([1u8; 32]), TrackNumber(2), Hash([0u8; 32]));
        let key3 = TrackLookupKey::new(Address::new([2u8; 32]), TrackNumber(0), Hash([0u8; 32]));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();
        let bytes3 = wincode::serialize(&key3).unwrap();

        assert!(bytes1 < bytes2);
        assert!(bytes2 < bytes3);
    }

    #[test]
    fn test_track_lookup_key_roundtrip() {
        let key = TrackLookupKey::new(
            Address::new([0xCD; 32]),
            TrackNumber(77),
            Hash([0xEF; 32]),
        );
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: TrackLookupKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn object_list_key_roundtrip() {
        let key = ObjectListKey::new(Address::new([0x11; 32]), b"photos/2026/cat.jpg".to_vec());
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: ObjectListKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
        assert_eq!(bytes.len(), 32 + b"photos/2026/cat.jpg".len());
    }

    #[test]
    fn object_list_key_orders_by_name() {
        let bucket = Address::new([0x11; 32]);
        let a = wincode::serialize(&ObjectListKey::new(bucket, b"a".to_vec())).unwrap();
        let ab = wincode::serialize(&ObjectListKey::new(bucket, b"ab".to_vec())).unwrap();
        let b = wincode::serialize(&ObjectListKey::new(bucket, b"b".to_vec())).unwrap();
        assert!(a < ab && ab < b);

        // A different bucket dominates the ordering.
        let other =
            wincode::serialize(&ObjectListKey::new(Address::new([0x12; 32]), b"a".to_vec()))
                .unwrap();
        assert!(b < other);
    }
}
