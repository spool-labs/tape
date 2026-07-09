use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::{Address, Hash};

use crate::types::EpochNumber;

/// Domain separation tag for node eviction.
pub const EVICT_DOMAIN_TAG: &[u8; 8] = b"EVICT\0\0\0";

/// Size of the eviction message in bytes.
/// 8 (domain) + 8 (target epoch) + 32 (nonce) + 32 (node) = 80 bytes
pub const EVICT_MESSAGE_SIZE: usize = 80;

/// Message format for node eviction BLS signatures.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct NodeEvictMessage {
    /// Epoch being protected (the committee the node is evicted from).
    pub target_epoch: EpochNumber,
    /// Target epoch nonce, binding the signature to one specific epoch.
    pub nonce: Hash,
    /// Node being evicted.
    pub node: Address,
}

impl NodeEvictMessage {
    pub const fn new(target_epoch: EpochNumber, nonce: Hash, node: Address) -> Self {
        Self {
            target_epoch,
            nonce,
            node,
        }
    }

    pub fn to_bytes(&self) -> [u8; EVICT_MESSAGE_SIZE] {
        let mut buf = [0u8; EVICT_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(EVICT_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != EVICT_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != EVICT_DOMAIN_TAG {
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
        assert_eq!(EVICT_MESSAGE_SIZE, 80);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(EVICT_DOMAIN_TAG.len(), 8);
        assert_eq!(EVICT_DOMAIN_TAG, b"EVICT\0\0\0");
    }

    #[test]
    fn test_message_roundtrip() {
        let target_epoch = EpochNumber(12345);
        let nonce = Hash([0xAB; 32]);
        let node = Address::new([0xCD; 32]);

        let msg = NodeEvictMessage::new(target_epoch, nonce, node);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), EVICT_MESSAGE_SIZE);

        let recovered = NodeEvictMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.target_epoch, target_epoch);
        assert_eq!(recovered.nonce, nonce);
        assert_eq!(recovered.node, node);
    }

    #[test]
    fn test_message_format() {
        let target_epoch = EpochNumber(0x0102030405060708);
        let nonce = Hash([0x42; 32]);
        let node = Address::new([0x99; 32]);

        let msg = NodeEvictMessage::new(target_epoch, nonce, node);
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"EVICT\0\0\0");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..48], &[0x42; 32]);
        assert_eq!(&bytes[48..80], &[0x99; 32]);
    }

    #[test]
    fn test_invalidate_domain_tag_rejected() {
        let mut bytes = [0u8; EVICT_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(NodeEvictMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 79];
        assert!(NodeEvictMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 81];
        assert!(NodeEvictMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let nonce = Hash([0x42; 32]);
        let node = Address::new([0xAA; 32]);
        let msg1 = NodeEvictMessage::new(EpochNumber(1), nonce, node);
        let msg2 = NodeEvictMessage::new(EpochNumber(2), nonce, node);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_nonces_produce_different_messages() {
        let target_epoch = EpochNumber(42);
        let node = Address::new([0xAA; 32]);
        let msg1 = NodeEvictMessage::new(target_epoch, Hash([0x11; 32]), node);
        let msg2 = NodeEvictMessage::new(target_epoch, Hash([0x22; 32]), node);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
