use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};

/// Domain separation tag for a challenge attestation.
pub const ATTEST_DOMAIN_TAG: &[u8; 8] = b"WWATTE\0\0";

/// Size of the attestation message in bytes.
/// 8 (domain) + 8 (epoch) + 8 (group) + 8 (round) + 8 (spool) + 32 (block) = 72
pub const ATTEST_MESSAGE_SIZE: usize = 72;

/// What an observer signs once it has accepted a proof of access.
///
/// It names the round and the challenged spool, and nothing about the observer,
/// so every accepting owner signs identical bytes and their signatures aggregate.
/// The candidate block is in the message because signatures made against
/// different branches must not combine: an aggregate that mixed them would claim
/// a quorum that never agreed on one history.
///
/// It deliberately omits the sampled leaf. The leaf is in the response the owner
/// itself signed. Carrying it here would make two observers who saw the same
/// round sign different bytes if either mis-derived the sample.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct ChallengeAttestMessage {
    /// Epoch the round belongs to.
    pub epoch: EpochNumber,
    /// Group whose record this round joins.
    pub group: GroupIndex,
    /// Round within the epoch.
    pub round: RoundNumber,
    /// Spool that was challenged.
    pub spool: SpoolIndex,
    /// Hash of the candidate entropy block the round was drawn from.
    pub block: Hash,
}

impl ChallengeAttestMessage {
    pub const fn new(
        epoch: EpochNumber,
        group: GroupIndex,
        round: RoundNumber,
        spool: SpoolIndex,
        block: Hash,
    ) -> Self {
        Self {
            epoch,
            group,
            round,
            spool,
            block,
        }
    }

    pub fn to_bytes(&self) -> [u8; ATTEST_MESSAGE_SIZE] {
        let mut buf = [0u8; ATTEST_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(ATTEST_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ATTEST_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != ATTEST_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::challenge::{ChallengeRespondMessage, RESPOND_DOMAIN_TAG};

    fn message() -> ChallengeAttestMessage {
        ChallengeAttestMessage::new(
            EpochNumber(7),
            GroupIndex(3),
            RoundNumber(11),
            SpoolIndex(64),
            Hash([0xAB; 32]),
        )
    }

    // the message is always the declared size
    #[test]
    fn fixed_size() {
        assert_eq!(ATTEST_MESSAGE_SIZE, 72);
        assert_eq!(message().to_bytes().len(), ATTEST_MESSAGE_SIZE);
    }

    // a message parses back into what it was built from
    #[test]
    fn round_trip() {
        let recovered = ChallengeAttestMessage::from_bytes(&message().to_bytes()).expect("parse");
        assert_eq!(recovered, message());
    }

    // the tag and every coordinate sit at a fixed offset
    #[test]
    fn pinned_layout() {
        let bytes = message().to_bytes();
        assert_eq!(&bytes[0..8], ATTEST_DOMAIN_TAG);
        assert_eq!(&bytes[8..16], &7u64.to_le_bytes());
        assert_eq!(&bytes[16..24], &3u64.to_le_bytes());
        assert_eq!(&bytes[24..32], &11u64.to_le_bytes());
        assert_eq!(&bytes[32..40], &64u64.to_le_bytes());
        assert_eq!(&bytes[40..72], &[0xAB; 32]);
    }

    // two observers of one round sign the same bytes, which is what lets their
    // signatures aggregate
    #[test]
    fn same_bytes() {
        assert_eq!(message().to_bytes(), message().to_bytes());
    }

    // a different candidate block gives different bytes, so signatures made
    // against two branches cannot combine
    #[test]
    fn branch_differs() {
        let other = ChallengeAttestMessage {
            block: Hash([0xAC; 32]),
            ..message()
        };
        assert_ne!(other.to_bytes(), message().to_bytes());
    }

    // changing any one coordinate changes the message
    #[test]
    fn coordinates_matter() {
        let base = message().to_bytes();
        let variants = [
            ChallengeAttestMessage { epoch: EpochNumber(8), ..message() },
            ChallengeAttestMessage { group: GroupIndex(4), ..message() },
            ChallengeAttestMessage { round: RoundNumber(12), ..message() },
            ChallengeAttestMessage { spool: SpoolIndex(65), ..message() },
        ];

        for variant in variants {
            assert_ne!(variant.to_bytes(), base);
        }
    }

    // the response domain tag is refused
    #[test]
    fn response_tag() {
        let mut bytes = message().to_bytes();
        bytes[0..8].copy_from_slice(RESPOND_DOMAIN_TAG);
        assert!(ChallengeAttestMessage::from_bytes(&bytes).is_none());
    }

    // a response message does not parse as an attestation, and length alone is
    // not what separates them, which is why the tag check runs first
    #[test]
    fn response_message() {
        let respond = ChallengeRespondMessage::new(
            EpochNumber(7),
            GroupIndex(3),
            RoundNumber(11),
            SpoolIndex(64),
            Hash([0xAB; 32]),
            Hash([0xCD; 32]),
        );
        assert!(ChallengeAttestMessage::from_bytes(&respond.to_bytes()).is_none());
    }

    // a message of the wrong length is refused either way
    #[test]
    fn wrong_length() {
        assert!(ChallengeAttestMessage::from_bytes(&[0u8; ATTEST_MESSAGE_SIZE - 1]).is_none());
        assert!(ChallengeAttestMessage::from_bytes(&[0u8; ATTEST_MESSAGE_SIZE + 1]).is_none());
    }
}
