use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::spooler::SpoolGroup;
use crate::types::EpochNumber;

/// Domain separation tag for snapshot certification.
/// 8 bytes: "SNAPSHOT"
pub const SNAPSHOT_DOMAIN_TAG: &[u8; 8] = b"SNAPSHOT";

/// Size of the snapshot certification message in bytes.
/// 8 (domain) + 8 + 8 + 8 + 32 + 8 = 72 bytes.
pub const SNAPSHOT_MESSAGE_SIZE: usize = 72;

/// Message format for snapshot certification BLS signatures.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotMessage {
    pub epoch: EpochNumber,
    pub signing_epoch: EpochNumber,
    pub group: SpoolGroup,
    pub blob_hash: Hash,
    pub parent_epoch: EpochNumber,
}

impl SnapshotMessage {
    pub const fn new(
        epoch: EpochNumber,
        signing_epoch: EpochNumber,
        group: SpoolGroup,
        blob_hash: Hash,
        parent_epoch: EpochNumber,
    ) -> Self {
        Self {
            epoch,
            signing_epoch,
            group,
            blob_hash,
            parent_epoch,
        }
    }

    pub fn to_bytes(&self) -> [u8; SNAPSHOT_MESSAGE_SIZE] {
        let mut buf = [0u8; SNAPSHOT_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(SNAPSHOT_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SNAPSHOT_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != SNAPSHOT_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_size() {
        assert_eq!(SNAPSHOT_MESSAGE_SIZE, 72);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(SNAPSHOT_DOMAIN_TAG.len(), 8);
        assert_eq!(SNAPSHOT_DOMAIN_TAG, b"SNAPSHOT");
    }

    #[test]
    fn test_message_roundtrip() {
        let msg = SnapshotMessage::new(
            EpochNumber(12345),
            EpochNumber(12346),
            SpoolGroup(7),
            Hash::from([0xCD; 32]),
            EpochNumber(12344),
        );
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), SNAPSHOT_MESSAGE_SIZE);

        let recovered = SnapshotMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered, msg);
    }

    #[test]
    fn test_message_format() {
        let msg = SnapshotMessage::new(
            EpochNumber(0x0102030405060708),
            EpochNumber(0x1112131415161718),
            SpoolGroup(0x2122232425262728),
            Hash::from([0x99; 32]),
            EpochNumber(0x3132333435363738),
        );
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"SNAPSHOT");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..24], &[0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]);
        assert_eq!(&bytes[24..32], &[0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21]);
        assert_eq!(&bytes[32..64], &[0x99; 32]);
        assert_eq!(&bytes[64..72], &[0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31]);
    }

    #[test]
    fn test_invalid_domain_tag_rejected() {
        let mut bytes = [0u8; SNAPSHOT_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(SnapshotMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 47]; // Too short
        assert!(SnapshotMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 49]; // Too long
        assert!(SnapshotMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let blob_hash = Hash::from([0xAA; 32]);
        let msg1 = SnapshotMessage::new(
            EpochNumber(1),
            EpochNumber(2),
            SpoolGroup(3),
            blob_hash,
            EpochNumber(0),
        );
        let msg2 = SnapshotMessage::new(
            EpochNumber(2),
            EpochNumber(2),
            SpoolGroup(3),
            blob_hash,
            EpochNumber(0),
        );

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_group_or_parent_produce_different_messages() {
        let msg1 = SnapshotMessage::new(
            EpochNumber(42),
            EpochNumber(43),
            SpoolGroup(1),
            Hash::from([0xAA; 32]),
            EpochNumber(41),
        );
        let msg2 = SnapshotMessage::new(
            EpochNumber(42),
            EpochNumber(43),
            SpoolGroup(2),
            Hash::from([0xAA; 32]),
            EpochNumber(41),
        );
        let msg3 = SnapshotMessage::new(
            EpochNumber(42),
            EpochNumber(43),
            SpoolGroup(1),
            Hash::from([0xAA; 32]),
            EpochNumber(40),
        );

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
        assert_ne!(msg1.to_bytes(), msg3.to_bytes());
    }

    #[test]
    fn test_domain_separation_from_certify() {
        use crate::cert::track::CertifyMessage;

        let snapshot_msg = SnapshotMessage::new(
            EpochNumber(42),
            EpochNumber(43),
            SpoolGroup(9),
            Hash::from([0xAA; 32]),
            EpochNumber(41),
        );
        let certify_msg = CertifyMessage::new(EpochNumber(42), [0xAA; 32]);

        assert_ne!(&snapshot_msg.to_bytes()[0..8], &certify_msg.to_bytes()[0..8]);
    }
}
