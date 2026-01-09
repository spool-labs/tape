//! Track certification message format.
//!
//! Defines the message format for BLS signatures in the track certification flow.
//! Includes domain separation and epoch binding to prevent signature reuse.
//!
//! # Message Format
//!
//! ```text
//! +------------------+------------------+------------------+
//! | DOMAIN_TAG (8B)  | EPOCH (8B LE)    | TRACK_ADDR (32B) |
//! +------------------+------------------+------------------+
//! ```
//!
//! Total: 48 bytes
//!
//! # Domain Separation
//!
//! The domain tag `CERTIFY\0` ensures signatures cannot be reused across different
//! protocols that may use the same BLS keys.
//!
//! # Epoch Binding
//!
//! Including the epoch number ensures:
//! - Signatures from previous epochs cannot be replayed
//! - Signatures are only valid for the epoch in which they were created
//! - Committee membership changes naturally invalidate old signatures
//!
//! # Example
//!
//! ```rust
//! use tape_core::cert::track::CertifyMessage;
//! use tape_core::types::EpochNumber;
//!
//! let msg = CertifyMessage::new(EpochNumber(42), [0u8; 32]);
//! let bytes = msg.to_bytes();
//! assert_eq!(bytes.len(), 48);
//! ```

use crate::types::EpochNumber;

/// Domain separation tag for track certification.
/// 8 bytes: "CERTIFY\0"
pub const CERTIFY_DOMAIN_TAG: &[u8; 8] = b"CERTIFY\0";

/// Size of the certification message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (track address) = 48 bytes
pub const CERTIFY_MESSAGE_SIZE: usize = 48;

/// Message format for track certification BLS signatures.
///
/// This struct represents the canonical message that committee members sign
/// when certifying a track. It includes:
/// - Domain separation tag to prevent cross-protocol signature reuse
/// - Epoch number to prevent replay attacks across epochs
/// - Track address to bind the signature to a specific track
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertifyMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Track's on-chain PDA address (32 bytes).
    pub track_address: [u8; 32],
}

impl CertifyMessage {
    /// Create a new certification message.
    pub const fn new(epoch: EpochNumber, track_address: [u8; 32]) -> Self {
        Self {
            epoch,
            track_address,
        }
    }

    /// Serialize the message to bytes for signing.
    ///
    /// Format: `DOMAIN_TAG (8) || EPOCH (8 LE) || TRACK_ADDRESS (32)`
    pub fn to_bytes(&self) -> [u8; CERTIFY_MESSAGE_SIZE] {
        let mut buf = [0u8; CERTIFY_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(CERTIFY_DOMAIN_TAG);
        buf[8..16].copy_from_slice(&self.epoch.0.to_le_bytes());
        buf[16..48].copy_from_slice(&self.track_address);
        buf
    }

    /// Deserialize a message from bytes.
    ///
    /// Returns `None` if the domain tag doesn't match or length is wrong.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != CERTIFY_MESSAGE_SIZE {
            return None;
        }

        // Verify domain tag
        if &bytes[0..8] != CERTIFY_DOMAIN_TAG {
            return None;
        }

        let epoch = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let mut track_address = [0u8; 32];
        track_address.copy_from_slice(&bytes[16..48]);

        Some(Self {
            epoch: EpochNumber(epoch),
            track_address,
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
        let track_address = [0xAB; 32];

        let msg = CertifyMessage::new(epoch, track_address);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), CERTIFY_MESSAGE_SIZE);

        let recovered = CertifyMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.track_address, track_address);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let track_address = [0x42; 32];

        let msg = CertifyMessage::new(epoch, track_address);
        let bytes = msg.to_bytes();

        // Check domain tag
        assert_eq!(&bytes[0..8], b"CERTIFY\0");

        // Check epoch (little-endian)
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);

        // Check track address
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
        let bytes = [0u8; 47]; // Too short
        assert!(CertifyMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 49]; // Too long
        assert!(CertifyMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let track = [0x42; 32];
        let msg1 = CertifyMessage::new(EpochNumber(1), track);
        let msg2 = CertifyMessage::new(EpochNumber(2), track);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_tracks_produce_different_messages() {
        let epoch = EpochNumber(42);
        let msg1 = CertifyMessage::new(epoch, [0x11; 32]);
        let msg2 = CertifyMessage::new(epoch, [0x22; 32]);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
