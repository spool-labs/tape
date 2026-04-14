use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::EpochNumber;

/// Domain separation tag for track certification.
pub const CERTIFY_DOMAIN_TAG: &[u8; 8] = b"CERTIFY\0";

/// Size of the certification message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (track hash) = 48 bytes
pub const CERTIFY_MESSAGE_SIZE: usize = 48;

/// Message format for track certification BLS signatures.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct TrackWriteMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Current authenticated compressed-track hash.
    pub track_hash: Hash,
}

impl TrackWriteMessage {
    pub const fn new(
        epoch: EpochNumber,
        track_hash: Hash,
    ) -> Self {
        Self {
            epoch,
            track_hash,
        }
    }

    pub fn to_bytes(&self) -> [u8; CERTIFY_MESSAGE_SIZE] {
        let mut buf = [0u8; CERTIFY_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(CERTIFY_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != CERTIFY_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != CERTIFY_DOMAIN_TAG {
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
        assert_eq!(CERTIFY_MESSAGE_SIZE, 48);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(CERTIFY_DOMAIN_TAG.len(), 8);
        assert_eq!(CERTIFY_DOMAIN_TAG, b"CERTIFY\0");
    }

    #[test]
    fn test_message_roundtrip() {
        let epoch = EpochNumber(12345);
        let track_hash = Hash([0xAB; 32]);

        let msg = TrackWriteMessage::new(epoch, track_hash);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), CERTIFY_MESSAGE_SIZE);

        let recovered = TrackWriteMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.track_hash, track_hash);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let track_hash = Hash([0x42; 32]);

        let msg = TrackWriteMessage::new(epoch, track_hash);
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"CERTIFY\0");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..48], &[0x42; 32]);
    }

    #[test]
    fn test_invalid_domain_tag_rejected() {
        let mut bytes = [0u8; CERTIFY_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(TrackWriteMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 87];
        assert!(TrackWriteMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 89];
        assert!(TrackWriteMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let leaf = Hash([0x42; 32]);
        let msg1 = TrackWriteMessage::new(EpochNumber(1), leaf);
        let msg2 = TrackWriteMessage::new(EpochNumber(2), leaf);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_leaves_produce_different_messages() {
        let epoch = EpochNumber(42);
        let msg1 = TrackWriteMessage::new(epoch, Hash([0x11; 32]));
        let msg2 = TrackWriteMessage::new(epoch, Hash([0x22; 32]));

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
