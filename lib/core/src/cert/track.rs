//! Track certification message format.
//!
//! Defines the message format for BLS signatures in the track certification flow.
//! Includes domain separation, epoch binding, and leaf binding to prevent
//! signature reuse and ensure quorum checks are evaluated against the exact
//! authenticated compressed track state.
//!
//! # Message Format
//!
//! ```text
//! +------------------+------------------+------------------+
//! | DOMAIN_TAG (8B)  | EPOCH (8B LE)    | TRACK_HASH (32B) |
//! +------------------+------------------+------------------+
//! ```
//!
//! Total: 48 bytes

use crate::types::EpochNumber;

/// Domain separation tag for track certification.
/// 8 bytes: "CERTIFY\0"
pub const CERTIFY_DOMAIN_TAG: &[u8; 8] = b"CERTIFY\0";

/// Size of the certification message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (track hash) = 48 bytes
pub const CERTIFY_MESSAGE_SIZE: usize = 48;

/// Message format for track certification BLS signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertifyMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Current authenticated compressed-track hash.
    pub track_hash: [u8; 32],
}

impl CertifyMessage {
    /// Create a new certification message.
    pub const fn new(
        epoch: EpochNumber,
        track_hash: [u8; 32],
    ) -> Self {
        Self {
            epoch,
            track_hash,
        }
    }

    /// Serialize the message to bytes for signing.
    ///
    /// Format: `DOMAIN_TAG (8) || EPOCH (8 LE) || TRACK_HASH (32)`
    pub fn to_bytes(&self) -> [u8; CERTIFY_MESSAGE_SIZE] {
        let mut buf = [0u8; CERTIFY_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(CERTIFY_DOMAIN_TAG);
        buf[8..16].copy_from_slice(&self.epoch.pack());
        buf[16..48].copy_from_slice(&self.track_hash);
        buf
    }

    /// Deserialize a message from bytes.
    ///
    /// Returns `None` if the domain tag doesn't match or length is wrong.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != CERTIFY_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != CERTIFY_DOMAIN_TAG {
            return None;
        }

        let epoch = EpochNumber::unpack(bytes[8..16].try_into().ok()?);
        let mut track_hash = [0u8; 32];
        track_hash.copy_from_slice(&bytes[16..48]);

        Some(Self {
            epoch,
            track_hash,
        })
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
        let track_hash = [0xAB; 32];

        let msg = CertifyMessage::new(epoch, track_hash);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), CERTIFY_MESSAGE_SIZE);

        let recovered = CertifyMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.track_hash, track_hash);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let track_hash = [0x42; 32];

        let msg = CertifyMessage::new(epoch, track_hash);
        let bytes = msg.to_bytes();

        assert_eq!(&bytes[0..8], b"CERTIFY\0");
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&bytes[16..48], &[0x42; 32]);
    }

    #[test]
    fn test_invalid_domain_tag_rejected() {
        let mut bytes = [0u8; CERTIFY_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"INVALID\0");

        assert!(CertifyMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 87];
        assert!(CertifyMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 89];
        assert!(CertifyMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let leaf = [0x42; 32];
        let msg1 = CertifyMessage::new(EpochNumber(1), leaf);
        let msg2 = CertifyMessage::new(EpochNumber(2), leaf);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_leaves_produce_different_messages() {
        let epoch = EpochNumber(42);
        let msg1 = CertifyMessage::new(epoch, [0x11; 32]);
        let msg2 = CertifyMessage::new(epoch, [0x22; 32]);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
