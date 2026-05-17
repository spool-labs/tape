use bytemuck::{Pod, Zeroable, bytes_of, try_from_bytes};
use tape_crypto::Hash;

use crate::types::EpochNumber;

use super::{
    ASSIGNMENT_VOTE_DOMAIN_TAG, ASSIGNMENT_VOTE_FORMAT_VERSION, ASSIGNMENT_VOTE_MESSAGE_SIZE,
};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct AssignmentVoteMessage {
    /// The epoch the assignment is for (next epoch from the signer's perspective).
    pub epoch: EpochNumber,

    /// Epoch nonce recorded by `commit_epoch`.
    pub nonce: Hash,

    /// Hash over compact assignment group payloads.
    pub assignment_hash: Hash,

    /// Signed message format version.
    pub format_version: u64,
}

impl AssignmentVoteMessage {
    pub const fn new(epoch: EpochNumber, nonce: Hash, assignment_hash: Hash) -> Self {
        Self {
            epoch,
            nonce,
            assignment_hash,
            format_version: ASSIGNMENT_VOTE_FORMAT_VERSION,
        }
    }

    pub fn to_bytes(&self) -> [u8; ASSIGNMENT_VOTE_MESSAGE_SIZE] {
        let mut buf = [0u8; ASSIGNMENT_VOTE_MESSAGE_SIZE];
        buf[0..8].copy_from_slice(ASSIGNMENT_VOTE_DOMAIN_TAG);
        buf[8..].copy_from_slice(bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ASSIGNMENT_VOTE_MESSAGE_SIZE {
            return None;
        }
        if &bytes[0..8] != ASSIGNMENT_VOTE_DOMAIN_TAG {
            return None;
        }
        try_from_bytes::<Self>(&bytes[8..]).copied().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let msg =
            AssignmentVoteMessage::new(EpochNumber(42), Hash::from([7; 32]), Hash::from([9; 32]));
        let bytes = msg.to_bytes();
        let recovered = AssignmentVoteMessage::from_bytes(&bytes).expect("roundtrip");
        assert_eq!(recovered, msg);
    }
}
