use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::EpochNumber;

use super::{SNAPSHOT_SIGN_DOMAIN_TAG, SNAPSHOT_SIGN_FORMAT_VERSION, SNAPSHOT_SIGN_MESSAGE_SIZE};

/// Message format for snapshot certification BLS signatures.
///
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotSignMessage {
    pub epoch: EpochNumber,
    pub snapshot_hash: Hash,
    pub format_version: u64,
}

impl SnapshotSignMessage {
    pub const fn new(epoch: EpochNumber, snapshot_hash: Hash) -> Self {
        Self {
            epoch,
            snapshot_hash,
            format_version: SNAPSHOT_SIGN_FORMAT_VERSION,
        }
    }

    pub fn to_bytes(&self) -> [u8; SNAPSHOT_SIGN_MESSAGE_SIZE] {
        let mut buf = [0u8; SNAPSHOT_SIGN_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(SNAPSHOT_SIGN_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SNAPSHOT_SIGN_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != SNAPSHOT_SIGN_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::Hash;

    use super::*;

    #[test]
    fn test_message_size() {
        assert_eq!(SNAPSHOT_SIGN_MESSAGE_SIZE, 56);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(SNAPSHOT_SIGN_DOMAIN_TAG.len(), 8);
        assert_eq!(SNAPSHOT_SIGN_DOMAIN_TAG, b"SNAPSIGN");
    }

    #[test]
    fn test_message_roundtrip() {
        let msg = SnapshotSignMessage::new(EpochNumber(12345), Hash::from([7; 32]));
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), SNAPSHOT_SIGN_MESSAGE_SIZE);

        let recovered = SnapshotSignMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered, msg);
    }

    #[test]
    fn test_message_format() {
        let msg =
            SnapshotSignMessage::new(EpochNumber(0x0102030405060708), Hash::from([0x21; 32]));
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"SNAPSIGN");
        assert_eq!(
            &bytes[8..16],
            &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        );
        assert_eq!(&bytes[16..48], &[0x21; 32]);
        assert_eq!(&bytes[48..56], &SNAPSHOT_SIGN_FORMAT_VERSION.to_le_bytes());
    }

    #[test]
    fn test_invalid_domain_tag_rejected() {
        let mut bytes = [0u8; SNAPSHOT_SIGN_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(SnapshotSignMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 47];
        assert!(SnapshotSignMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 65];
        assert!(SnapshotSignMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let msg1 = SnapshotSignMessage::new(EpochNumber(1), Hash::from([3; 32]));
        let msg2 = SnapshotSignMessage::new(EpochNumber(2), Hash::from([3; 32]));

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_hashes_produce_different_messages() {
        let msg1 = SnapshotSignMessage::new(EpochNumber(42), Hash::from([1; 32]));
        let msg2 = SnapshotSignMessage::new(EpochNumber(42), Hash::from([2; 32]));

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_domain_separation_tag_is_not_assignment() {
        let msg1 = SnapshotSignMessage::new(EpochNumber(42), Hash::from([9; 32]));

        assert_ne!(&msg1.to_bytes()[0..8], b"ASSIGN\0\0");
    }
}
