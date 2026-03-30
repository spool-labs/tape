//! Snapshot certification message format.
//!
//! Defines the message format for BLS signatures in the snapshot certification flow.
//! Unlike track certification which binds to a track address, snapshot certification
//! uses (epoch, commitment) since all committee members deterministically produce
//! the same event log.
//!
//! # Message Format
//!
//! ```text
//! +------------------+------------------+---------------------+
//! | DOMAIN_TAG (8B)  | EPOCH (8B LE)    | COMMITMENT (32B)    |
//! +------------------+------------------+---------------------+
//! ```
//!
//! Total: 48 bytes

use crate::types::EpochNumber;

/// Domain separation tag for snapshot certification.
/// 8 bytes: "SNAPSHOT"
pub const SNAPSHOT_DOMAIN_TAG: &[u8; 8] = b"SNAPSHOT";

/// Size of the snapshot certification message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (commitment hash) = 48 bytes
pub const SNAPSHOT_MESSAGE_SIZE: usize = 48;

/// Message format for snapshot certification BLS signatures.
///
/// This struct represents the canonical message that committee members sign
/// when certifying a snapshot chunk. It includes:
/// - Domain separation tag to prevent cross-protocol signature reuse
/// - Epoch number to prevent replay attacks across epochs
/// - Commitment hash (merkle root) to bind the signature to specific data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMessage {
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Commitment hash (merkle root of erasure-coded slices).
    pub commitment_hash: [u8; 32],
}

impl SnapshotMessage {
    /// Create a new snapshot certification message.
    pub const fn new(epoch: EpochNumber, commitment_hash: [u8; 32]) -> Self {
        Self {
            epoch,
            commitment_hash,
        }
    }

    /// Serialize the message to bytes for signing.
    ///
    /// Format: `DOMAIN_TAG (8) || EPOCH (8 LE) || COMMITMENT_HASH (32)`
    pub fn to_bytes(&self) -> [u8; SNAPSHOT_MESSAGE_SIZE] {
        let mut buf = [0u8; SNAPSHOT_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(SNAPSHOT_DOMAIN_TAG);
        buf[8..16].copy_from_slice(&self.epoch.pack());
        buf[16..48].copy_from_slice(&self.commitment_hash);
        buf
    }

    /// Deserialize a message from bytes.
    ///
    /// Returns `None` if the domain tag doesn't match or length is wrong.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SNAPSHOT_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != SNAPSHOT_DOMAIN_TAG {
            return None;
        }

        let epoch = EpochNumber::unpack(bytes[8..16].try_into().ok()?);
        let mut commitment_hash = [0u8; 32];
        commitment_hash.copy_from_slice(&bytes[16..48]);

        Some(Self {
            epoch,
            commitment_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_size() {
        assert_eq!(SNAPSHOT_MESSAGE_SIZE, 48);
    }

    #[test]
    fn test_domain_tag() {
        assert_eq!(SNAPSHOT_DOMAIN_TAG.len(), 8);
        assert_eq!(SNAPSHOT_DOMAIN_TAG, b"SNAPSHOT");
    }

    #[test]
    fn test_message_roundtrip() {
        let epoch = EpochNumber(12345);
        let commitment_hash = [0xCD; 32];

        let msg = SnapshotMessage::new(epoch, commitment_hash);
        let bytes = msg.to_bytes();

        assert_eq!(bytes.len(), SNAPSHOT_MESSAGE_SIZE);

        let recovered = SnapshotMessage::from_bytes(&bytes).expect("should parse");
        assert_eq!(recovered.epoch, epoch);
        assert_eq!(recovered.commitment_hash, commitment_hash);
    }

    #[test]
    fn test_message_format() {
        let epoch = EpochNumber(0x0102030405060708);
        let commitment_hash = [0x99; 32];

        let msg = SnapshotMessage::new(epoch, commitment_hash);
        let bytes = msg.to_bytes();

        // Check domain tag
        assert_eq!(&bytes[0..8], b"SNAPSHOT");

        // Check epoch (little-endian)
        assert_eq!(&bytes[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);

        // Check commitment hash
        assert_eq!(&bytes[16..48], &[0x99; 32]);
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
        let commitment = [0xAA; 32];
        let msg1 = SnapshotMessage::new(EpochNumber(1), commitment);
        let msg2 = SnapshotMessage::new(EpochNumber(2), commitment);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_different_commitments_produce_different_messages() {
        let msg1 = SnapshotMessage::new(EpochNumber(42), [0xAA; 32]);
        let msg2 = SnapshotMessage::new(EpochNumber(42), [0xBB; 32]);

        assert_ne!(msg1.to_bytes(), msg2.to_bytes());
    }

    #[test]
    fn test_domain_separation_from_certify() {
        use crate::cert::track::CertifyMessage;

        // Same epoch and commitment, different message types should produce different bytes
        let epoch = EpochNumber(42);
        let commitment = [0xAA; 32];

        let snapshot_msg = SnapshotMessage::new(epoch, commitment);
        let certify_msg = CertifyMessage::new(epoch, commitment);

        // Domain tags differ
        assert_ne!(&snapshot_msg.to_bytes()[0..8], &certify_msg.to_bytes()[0..8]);
    }
}
