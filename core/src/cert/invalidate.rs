//! Track invalidation message format.
//!
//! Defines the message format for BLS signatures in the track invalidation flow.
//! When recovery detects that re-encoded slices produce a different merkle root
//! than the on-chain commitment, nodes sign an invalidation message to prove
//! the track was registered with invalid erasure coding.
//!
//! # Message Format
//!
//! ```text
//! +------------------+------------------+------------------+---------------------+
//! | DOMAIN_TAG (8B)  | EPOCH (8B LE)    | TRACK_ADDR (32B) | COMPUTED_ROOT (32B) |
//! +------------------+------------------+------------------+---------------------+
//! ```
//!
//! Total: 80 bytes
//!
//! # Domain Separation
//!
//! The domain tag `INVALID\0` ensures signatures cannot be reused across different
//! protocols or confused with certification signatures.
//!
//! # Computed Root
//!
//! The `computed_root` is the merkle root that the node independently computed by
//! downloading k slices, decoding, re-encoding, and computing the merkle root of
//! the resulting slices. This differs from the on-chain commitment, proving
//! the original registration was invalid.

use crate::types::EpochNumber;

/// Domain separation tag for track invalidation.
/// 8 bytes: "INVALID\0"
pub const INVALIDATE_DOMAIN_TAG: &[u8; 8] = b"INVALID\0";

/// Size of the invalidation message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (track address) + 32 (computed root) = 80 bytes
pub const INVALIDATE_MESSAGE_SIZE: usize = 80;

/// Message format for track invalidation BLS signatures.
///
/// This struct represents the canonical message that committee members sign
/// when attesting to a track inconsistency. It includes:
/// - Domain separation tag to prevent cross-protocol signature reuse
/// - Epoch number to prevent replay attacks across epochs
/// - Track address to bind the signature to a specific track
/// - Computed root (the merkle root from re-encoding) to bind to specific evidence
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidateMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Track's on-chain PDA address (32 bytes).
    pub track_address: [u8; 32],
    /// Computed merkle root from re-encoding (differs from on-chain commitment).
    pub computed_root: [u8; 32],
}

impl InvalidateMessage {
    /// Create a new invalidation message.
    pub const fn new(epoch: EpochNumber, track_address: [u8; 32], computed_root: [u8; 32]) -> Self {
        Self {
            epoch,
            track_address,
            computed_root,
        }
    }

    /// Serialize the message to bytes for signing.
    ///
    /// Format: `DOMAIN_TAG (8) || EPOCH (8 LE) || TRACK_ADDRESS (32) || COMPUTED_ROOT (32)`
    pub fn to_bytes(&self) -> [u8; INVALIDATE_MESSAGE_SIZE] {
        let mut buf = [0u8; INVALIDATE_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(INVALIDATE_DOMAIN_TAG);
        buf[8..16].copy_from_slice(&self.epoch.0.to_le_bytes());
        buf[16..48].copy_from_slice(&self.track_address);
        buf[48..80].copy_from_slice(&self.computed_root);
        buf
    }

    /// Deserialize a message from bytes.
    ///
    /// Returns `None` if the domain tag doesn't match or length is wrong.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != INVALIDATE_MESSAGE_SIZE {
            return None;
        }

        // Verify domain tag
        if &bytes[0..8] != INVALIDATE_DOMAIN_TAG {
            return None;
        }

        let epoch = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let mut track_address = [0u8; 32];
        track_address.copy_from_slice(&bytes[16..48]);
        let mut computed_root = [0u8; 32];
        computed_root.copy_from_slice(&bytes[48..80]);

        Some(Self {
            epoch: EpochNumber(epoch),
            track_address,
            computed_root,
        })
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
        let track_address = [0xAB; 32];
        let computed_root = [0xCD; 32];

        let msg = InvalidateMessage::new(epoch, track_address, computed_root);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), INVALIDATE_MESSAGE_SIZE);

        let recovered = InvalidateMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.track_address, track_address);
        assert_eq!(recovered.computed_root, computed_root);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let track_address = [0x42; 32];
        let computed_root = [0x99; 32];

        let msg = InvalidateMessage::new(epoch, track_address, computed_root);
        let bytes = msg.to_bytes();

        // Check domain tag
        assert_eq!(&bytes[0..8], b"INVALID\0");

        // Check epoch (little-endian)
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);

        // Check track address
        assert_eq!(&bytes[16..48], &[0x42; 32]);

        // Check computed root
        assert_eq!(&bytes[48..80], &[0x99; 32]);
    }

    #[test]
    fn test_certify_domain_tag_rejected() {
        let mut bytes = [0u8; INVALIDATE_MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(b"CERTIFY\0");

        assert!(InvalidateMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_wrong_length_rejected() {
        let bytes = [0u8; 79]; // Too short
        assert!(InvalidateMessage::from_bytes(&bytes).is_none());

        let bytes = [0u8; 81]; // Too long
        assert!(InvalidateMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_different_epochs_produce_different_messages() {
        let track = [0x42; 32];
        let root = [0xAA; 32];
        let msg1 = InvalidateMessage::new(EpochNumber(1), track, root);
        let msg2 = InvalidateMessage::new(EpochNumber(2), track, root);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_tracks_produce_different_messages() {
        let epoch = EpochNumber(42);
        let root = [0xAA; 32];
        let msg1 = InvalidateMessage::new(epoch, [0x11; 32], root);
        let msg2 = InvalidateMessage::new(epoch, [0x22; 32], root);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }
}
