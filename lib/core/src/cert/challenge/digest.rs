use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::{Address, Hash};

use crate::types::EpochNumber;

/// Domain separation tag for an epoch view digest report.
pub const DIGEST_DOMAIN_TAG: &[u8; 8] = b"WWDIGE\0\0";

/// Size of the digest report message in bytes.
pub const DIGEST_MESSAGE_SIZE: usize = 80;

/// What a peer signs to claim a view of a settled epoch.
///
/// The signer is inside the message, so one peer's report cannot be replayed as
/// another's, and the epoch is inside it, so a report cannot be carried into an
/// epoch the signer never spoke for.
///
/// It is signed on its own rather than folded into the round attestation. The
/// attestations of a group aggregate, and they only aggregate while every
/// signer signs identical bytes; a per-signer view digest in those bytes would
/// stop a group certifying whenever two of its members read the chain a slot
/// apart.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct ChallengeDigestMessage {
    pub signer: Address,
    pub epoch: EpochNumber,
    pub digest: Hash,
}

impl ChallengeDigestMessage {
    pub const fn new(signer: Address, epoch: EpochNumber, digest: Hash) -> Self {
        Self {
            signer,
            epoch,
            digest,
        }
    }

    pub fn to_bytes(&self) -> [u8; DIGEST_MESSAGE_SIZE] {
        let mut buf = [0u8; DIGEST_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(DIGEST_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != DIGEST_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != DIGEST_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::challenge::ATTEST_DOMAIN_TAG;

    fn signer(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn message() -> ChallengeDigestMessage {
        ChallengeDigestMessage::new(signer(1), EpochNumber(7), Hash([0xAB; 32]))
    }

    #[test]
    fn fixed_size() {
        assert_eq!(DIGEST_MESSAGE_SIZE, 80);
        assert_eq!(message().to_bytes().len(), DIGEST_MESSAGE_SIZE);
    }

    #[test]
    fn round_trip() {
        let recovered = ChallengeDigestMessage::from_bytes(&message().to_bytes()).expect("parse");
        assert_eq!(recovered, message());
    }

    #[test]
    fn pinned_layout() {
        let bytes = message().to_bytes();
        assert_eq!(&bytes[0..8], DIGEST_DOMAIN_TAG);
        assert_eq!(&bytes[8..40], signer(1).as_ref());
        assert_eq!(&bytes[40..48], &7u64.to_le_bytes());
        assert_eq!(&bytes[48..80], &[0xAB; 32]);
    }

    // the signer is in the bytes, so a captured report cannot be re-presented as
    // somebody else's
    #[test]
    fn signer_is_bound() {
        let other = ChallengeDigestMessage {
            signer: signer(2),
            ..message()
        };
        assert_ne!(other.to_bytes(), message().to_bytes());
    }

    // and so is the epoch, so it cannot be carried forward
    #[test]
    fn epoch_is_bound() {
        let other = ChallengeDigestMessage {
            epoch: EpochNumber(8),
            ..message()
        };
        assert_ne!(other.to_bytes(), message().to_bytes());
    }

    #[test]
    fn digest_matters() {
        let other = ChallengeDigestMessage {
            digest: Hash([0xAC; 32]),
            ..message()
        };
        assert_ne!(other.to_bytes(), message().to_bytes());
    }

    // a signature over an attestation must not read as a digest report
    #[test]
    fn attest_tag_rejected() {
        let mut bytes = message().to_bytes();
        bytes[0..8].copy_from_slice(ATTEST_DOMAIN_TAG);
        assert!(ChallengeDigestMessage::from_bytes(&bytes).is_none());
    }
}
