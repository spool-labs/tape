use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::EpochNumber;

/// Domain separation tag for track invalidation.
pub const INVALIDATE_DOMAIN_TAG: &[u8; 8] = b"INVALID\0";

/// Size of the invalidation message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (track hash) + 32 (computed root) = 80 bytes
pub const INVALIDATE_MESSAGE_SIZE: usize = 80;

/// Message format for track invalidation BLS signatures.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct TrackInvalidateMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Current authenticated compressed-track hash.
    pub track_hash: Hash,
    /// Computed merkle root from re-encoding (differs from registered commitment).
    pub computed_root: Hash,
}

impl TrackInvalidateMessage {
    pub const fn new(
        epoch: EpochNumber,
        track_hash: Hash,
        computed_root: Hash,
    ) -> Self {
        Self {
            epoch,
            track_hash,
            computed_root,
        }
    }

    pub fn to_bytes(&self) -> [u8; INVALIDATE_MESSAGE_SIZE] {
        let mut buf = [0u8; INVALIDATE_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(INVALIDATE_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != INVALIDATE_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != INVALIDATE_DOMAIN_TAG {
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
        assert_eq!(INVALIDATE_MESSAGE_SIZE, 80);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(INVALIDATE_DOMAIN_TAG.len(), 8);
        assert_eq!(INVALIDATE_DOMAIN_TAG, b"INVALID\0");
    }

    #[test]
    fn test_message_roundtrip() {
        let epoch = EpochNumber(12345);
        let track_hash = Hash([0xAB; 32]);
        let computed_root = Hash([0xCD; 32]);

        let msg = TrackInvalidateMessage::new(epoch, track_hash, computed_root);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), INVALIDATE_MESSAGE_SIZE);

        let recovered = TrackInvalidateMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.track_hash, track_hash);
        assert_eq!(recovered.computed_root, computed_root);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let track_hash = Hash([0x42; 32]);
        let computed_root = Hash([0x99; 32]);

        let msg = TrackInvalidateMessage::new(epoch, track_hash, computed_root);
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"INVALID\0");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..48], &[0x42; 32]);
        assert_eq!(&bytes[48..80], &[0x99; 32]);
    }

    #[test]
    fn test_certify_domain_tag_rejected() {
        let mut bytes = [0u8; INVALIDATE_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"CERTIFY\0");

        assert!(TrackInvalidateMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 119];
        assert!(TrackInvalidateMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 121];
        assert!(TrackInvalidateMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let leaf = Hash([0x42; 32]);
        let root = Hash([0xAA; 32]);
        let msg1 = TrackInvalidateMessage::new(EpochNumber(1), leaf, root);
        let msg2 = TrackInvalidateMessage::new(EpochNumber(2), leaf, root);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_leaves_produce_different_messages() {
        let epoch = EpochNumber(42);
        let root = Hash([0xAA; 32]);
        let msg1 = TrackInvalidateMessage::new(epoch, Hash([0x11; 32]), root);
        let msg2 = TrackInvalidateMessage::new(epoch, Hash([0x22; 32]), root);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
