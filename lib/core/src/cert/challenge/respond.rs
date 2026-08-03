use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};

/// Domain separation tag for a proof of access.
pub const RESPOND_DOMAIN_TAG: &[u8; 8] = b"WWRESP\0\0";

/// Size of the response message in bytes.
/// 8 (domain) + 8 (epoch) + 8 (group) + 8 (round) + 8 (spool) + 32 (block) + 32 (leaf) = 104
pub const RESPOND_MESSAGE_SIZE: usize = 104;

/// What a challenged owner signs when it broadcasts its proof of access.
///
/// It binds the round coordinates so a response cannot be replayed into another
/// round, the candidate block so a response built on a branch that loses is not
/// usable on the one that wins, and the sampled leaf so the signature covers what
/// was actually served rather than merely the fact of answering.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct ChallengeRespondMessage {
    /// Epoch the round belongs to.
    pub epoch: EpochNumber,
    /// Group being challenged.
    pub group: GroupIndex,
    /// Round within the epoch.
    pub round: RoundNumber,
    /// Spool whose data was sampled.
    pub spool: SpoolIndex,
    /// Hash of the candidate entropy block the round was drawn from.
    pub block: Hash,
    /// Hash of the sampled sub-leaf, binding the signature to what was served.
    pub leaf: Hash,
}

impl ChallengeRespondMessage {
    pub const fn new(
        epoch: EpochNumber,
        group: GroupIndex,
        round: RoundNumber,
        spool: SpoolIndex,
        block: Hash,
        leaf: Hash,
    ) -> Self {
        Self {
            epoch,
            group,
            round,
            spool,
            block,
            leaf,
        }
    }

    pub fn to_bytes(&self) -> [u8; RESPOND_MESSAGE_SIZE] {
        let mut buf = [0u8; RESPOND_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(RESPOND_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != RESPOND_MESSAGE_SIZE {
            return None;
        }

        if &bytes[0..8] != RESPOND_DOMAIN_TAG {
            return None;
        }

        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::challenge::ATTEST_DOMAIN_TAG;

    fn message() -> ChallengeRespondMessage {
        ChallengeRespondMessage::new(
            EpochNumber(7),
            GroupIndex(3),
            RoundNumber(11),
            SpoolIndex(64),
            Hash([0xAB; 32]),
            Hash([0xCD; 32]),
        )
    }

    // the message is always the declared size
    #[test]
    fn fixed_size() {
        assert_eq!(RESPOND_MESSAGE_SIZE, 104);
        assert_eq!(message().to_bytes().len(), RESPOND_MESSAGE_SIZE);
    }

    // a message parses back into what it was built from
    #[test]
    fn round_trip() {
        let recovered = ChallengeRespondMessage::from_bytes(&message().to_bytes()).expect("parse");
        assert_eq!(recovered, message());
    }

    // the tag and every coordinate sit at a fixed offset
    #[test]
    fn pinned_layout() {
        let bytes = message().to_bytes();
        assert_eq!(&bytes[0..8], RESPOND_DOMAIN_TAG);
        assert_eq!(&bytes[8..16], &7u64.to_le_bytes());
        assert_eq!(&bytes[16..24], &3u64.to_le_bytes());
        assert_eq!(&bytes[24..32], &11u64.to_le_bytes());
        assert_eq!(&bytes[32..40], &64u64.to_le_bytes());
        assert_eq!(&bytes[40..72], &[0xAB; 32]);
        assert_eq!(&bytes[72..104], &[0xCD; 32]);
    }

    // changing any one coordinate changes the message, so a response replays
    // into no other round, spool, epoch or branch
    #[test]
    fn coordinates_matter() {
        let base = message().to_bytes();
        let variants = [
            ChallengeRespondMessage { epoch: EpochNumber(8), ..message() },
            ChallengeRespondMessage { group: GroupIndex(4), ..message() },
            ChallengeRespondMessage { round: RoundNumber(12), ..message() },
            ChallengeRespondMessage { spool: SpoolIndex(65), ..message() },
            ChallengeRespondMessage { block: Hash([0xAC; 32]), ..message() },
            ChallengeRespondMessage { leaf: Hash([0xCE; 32]), ..message() },
        ];

        for variant in variants {
            assert_ne!(variant.to_bytes(), base);
        }
    }

    // the attestation domain tag is refused, which is what keeps a signature
    // over one message from reading as a signature over the other
    #[test]
    fn attest_tag() {
        let mut bytes = message().to_bytes();
        bytes[0..8].copy_from_slice(ATTEST_DOMAIN_TAG);
        assert!(ChallengeRespondMessage::from_bytes(&bytes).is_none());
    }

    // a message of the wrong length is refused either way
    #[test]
    fn wrong_length() {
        assert!(ChallengeRespondMessage::from_bytes(&[0u8; RESPOND_MESSAGE_SIZE - 1]).is_none());
        assert!(ChallengeRespondMessage::from_bytes(&[0u8; RESPOND_MESSAGE_SIZE + 1]).is_none());
    }
}
