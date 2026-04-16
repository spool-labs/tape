use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::spooler::SpoolGroup;
use crate::types::{ChunkNumber, EpochNumber};

/// Domain separation tag for snapshot certification.
pub const SNAPSHOT_WRITE_DOMAIN_TAG: &[u8; 8] = b"SNAPWRIT";

/// Size of the snapshot certification message in bytes.
/// 8 (domain) + 8 (epoch) + 8 (group) + 8 (chunk) + 32 (value_hash) = 64 bytes.
pub const SNAPSHOT_WRITE_MESSAGE_SIZE: usize = 64;

/// Message format for snapshot certification BLS signatures.
///
/// Only contains fields that aren't derivable from on-chain state:
/// - `signing_epoch` is always `current_epoch` (enforced by the advance gate)
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotWriteMessage {
    pub epoch: EpochNumber,
    pub group: SpoolGroup,
    pub chunk: ChunkNumber,
    pub value_hash: Hash,
}

impl SnapshotWriteMessage {
    pub const fn new(
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        value_hash: Hash,
    ) -> Self {
        Self {
            epoch,
            group,
            chunk,
            value_hash,
        }
    }

    pub fn to_bytes(&self) -> [u8; SNAPSHOT_WRITE_MESSAGE_SIZE] {
        let mut buf = [0u8; SNAPSHOT_WRITE_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(SNAPSHOT_WRITE_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SNAPSHOT_WRITE_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != SNAPSHOT_WRITE_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::cert::track::TrackWriteMessage;

    use super::*;

    #[test]
    fn test_message_size() {
        assert_eq!(SNAPSHOT_WRITE_MESSAGE_SIZE, 64);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(SNAPSHOT_WRITE_DOMAIN_TAG.len(), 8);
        assert_eq!(SNAPSHOT_WRITE_DOMAIN_TAG, b"SNAPWRIT");
    }

    #[test]
    fn test_message_roundtrip() {
        let msg = SnapshotWriteMessage::new(
            EpochNumber(12345),
            SpoolGroup(7),
            ChunkNumber(3),
            Hash::from([0xCD; 32]),
        );
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), SNAPSHOT_WRITE_MESSAGE_SIZE);

        let recovered = SnapshotWriteMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered, msg);
    }

    #[test]
    fn test_message_format() {
        let msg = SnapshotWriteMessage::new(
            EpochNumber(0x0102030405060708),
            SpoolGroup(0x2122232425262728),
            ChunkNumber(0x3132333435363738),
            Hash::from([0x99; 32]),
        );
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"SNAPWRIT");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..24], &[0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21]);
        assert_eq!(&bytes[24..32], &[0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31]);
        assert_eq!(&bytes[32..64], &[0x99; 32]);
    }

    #[test]
    fn test_invalid_domain_tag_rejected() {
        let mut bytes = [0u8; SNAPSHOT_WRITE_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(SnapshotWriteMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 55];
        assert!(SnapshotWriteMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 73];
        assert!(SnapshotWriteMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let value_hash = Hash::from([0xAA; 32]);
        let msg1 = SnapshotWriteMessage::new(
            EpochNumber(1),
            SpoolGroup(3),
            ChunkNumber(0),
            value_hash,
        );
        let msg2 = SnapshotWriteMessage::new(
            EpochNumber(2),
            SpoolGroup(3),
            ChunkNumber(0),
            value_hash,
        );

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_groups_produce_different_messages() {
        let value_hash = Hash::from([0xAA; 32]);
        let msg1 = SnapshotWriteMessage::new(
            EpochNumber(42),
            SpoolGroup(1),
            ChunkNumber(0),
            value_hash,
        );
        let msg2 = SnapshotWriteMessage::new(
            EpochNumber(42),
            SpoolGroup(2),
            ChunkNumber(0),
            value_hash,
        );

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_domain_separation_from_certify() {
        let snapshot_msg = SnapshotWriteMessage::new(
            EpochNumber(42),
            SpoolGroup(9),
            ChunkNumber(0),
            Hash::from([0xAA; 32]),
        );
        let certify_msg = TrackWriteMessage::new(
            EpochNumber(42),
            Hash([0xAA; 32])
        );

        assert_ne!(&snapshot_msg.to_bytes()[0..8], &certify_msg.to_bytes()[0..8]);
    }
}
