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
/// itself signed; carrying it here would make two observers who saw the same
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

    #[test]
    fn the_message_is_a_fixed_size() {
        assert_eq!(ATTEST_MESSAGE_SIZE, 72);
        assert_eq!(message().to_bytes().len(), ATTEST_MESSAGE_SIZE);
    }

    #[test]
    fn a_message_survives_a_round_trip() {
        let recovered = ChallengeAttestMessage::from_bytes(&message().to_bytes()).expect("parse");
        assert_eq!(recovered, message());
    }

    #[test]
    fn the_layout_is_pinned() {
        let bytes = message().to_bytes();
        assert_eq!(&bytes[0..8], ATTEST_DOMAIN_TAG);
        assert_eq!(&bytes[8..16], &7u64.to_le_bytes());
        assert_eq!(&bytes[16..24], &3u64.to_le_bytes());
        assert_eq!(&bytes[24..32], &11u64.to_le_bytes());
        assert_eq!(&bytes[32..40], &64u64.to_le_bytes());
        assert_eq!(&bytes[40..72], &[0xAB; 32]);
    }

    #[test]
    fn two_observers_of_one_round_sign_the_same_bytes() {
        // The property aggregation rests on: nothing observer-specific is in the
        // message, so signatures over it combine.
        assert_eq!(message().to_bytes(), message().to_bytes());
    }

    #[test]
    fn signatures_against_different_branches_cannot_combine() {
        let other = ChallengeAttestMessage {
            block: Hash([0xAC; 32]),
            ..message()
        };
        assert_ne!(other.to_bytes(), message().to_bytes());
    }

    #[test]
    fn every_coordinate_changes_the_message() {
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

    #[test]
    fn a_response_tag_is_refused() {
        let mut bytes = message().to_bytes();
        bytes[0..8].copy_from_slice(RESPOND_DOMAIN_TAG);
        assert!(ChallengeAttestMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn a_response_message_does_not_parse_as_an_attestation() {
        // Length alone separates them, which is why the tag check runs first.
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

    #[test]
    fn a_wrong_length_is_refused() {
        assert!(ChallengeAttestMessage::from_bytes(&[0u8; ATTEST_MESSAGE_SIZE - 1]).is_none());
        assert!(ChallengeAttestMessage::from_bytes(&[0u8; ATTEST_MESSAGE_SIZE + 1]).is_none());
    }
}
