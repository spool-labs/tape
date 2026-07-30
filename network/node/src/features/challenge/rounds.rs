//! What a node holds in flight for the rounds it is currently witnessing.
//!
//! One entry per challenged spool per round: the answer that owner broadcast, and
//! the attestations gathered from owners that accepted it. An entry certifies once
//! it holds a quorum, and is dropped once the round can no longer matter.
//!
//! Everything here is in memory. A certificate is standing evidence only while its
//! entropy block stands, and a node that restarts mid-round has simply missed that
//! round, which the record treats as a gap rather than a miss against anyone.

use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::bls::BlsSignature;
use tape_core::challenge::ProofOfAccess;
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;

/// One challenged spool in one round, pinned to its entropy block.
///
/// The block is part of the identity because signatures over different block
/// candidates cannot aggregate: an answer and its attestations only certify
/// together when they name the same block, so evidence gathered under one
/// candidate must never satisfy a lookup made under another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoundKey {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
}

/// Rounds this node still has something to do about.
#[derive(Default)]
pub struct RoundBuffer {
    entries: Mutex<HashMap<RoundKey, RoundEntry>>,
}

#[derive(Default)]
struct RoundEntry {
    /// The answer its owner broadcast, once one has been accepted.
    answer: Option<ProofOfAccess>,
    /// Attestations by signer, so a peer that sends twice counts once.
    attestations: HashMap<Address, BlsSignature>,
    /// Whether a certificate has already been formed and recorded.
    certified: bool,
}

impl RoundBuffer {
    /// Take in an answer, returning false when this round already had one.
    ///
    /// First accepted answer wins. A second one is either a duplicate from the
    /// relay or an owner trying to answer its own round twice, and neither should
    /// displace what the group has already started attesting to.
    pub fn accept_answer(&self, key: RoundKey, answer: ProofOfAccess) -> bool {
        let mut entries = self.entries.lock().expect("round buffer");
        let entry = entries.entry(key).or_default();
        if entry.answer.is_some() {
            return false;
        }

        entry.answer = Some(answer);
        true
    }

    /// The answer held for a round, if one has been accepted.
    pub fn answer(&self, key: RoundKey) -> Option<ProofOfAccess> {
        let entries = self.entries.lock().expect("round buffer");
        entries.get(&key)?.answer.clone()
    }

    /// Take in one signer's attestation, returning false when it is a repeat.
    pub fn accept_attestation(
        &self,
        key: RoundKey,
        signer: Address,
        signature: BlsSignature,
    ) -> bool {
        let mut entries = self.entries.lock().expect("round buffer");
        let entry = entries.entry(key).or_default();
        entry.attestations.insert(signer, signature).is_none()
    }

    /// Signers gathered for a round so far.
    pub fn signers(&self, key: RoundKey) -> Vec<Address> {
        let entries = self.entries.lock().expect("round buffer");
        let Some(entry) = entries.get(&key) else {
            return Vec::new();
        };
        let mut signers: Vec<Address> = entry.attestations.keys().copied().collect();
        signers.sort_unstable();
        signers
    }

    /// Every attestation gathered for a round, for aggregating.
    pub fn attestations(&self, key: RoundKey) -> Vec<(Address, BlsSignature)> {
        let entries = self.entries.lock().expect("round buffer");
        let Some(entry) = entries.get(&key) else {
            return Vec::new();
        };
        entry
            .attestations
            .iter()
            .map(|(signer, signature)| (*signer, *signature))
            .collect()
    }

    /// Give a round back its unclaimed state, when a claimed certificate turned
    /// out not to verify and the quorum should be allowed to re-form.
    pub fn release_certificate(&self, key: RoundKey) {
        let mut entries = self.entries.lock().expect("round buffer");
        if let Some(entry) = entries.get_mut(&key) {
            entry.certified = false;
        }
    }

    /// Claim the right to certify a round, once and only once.
    ///
    /// Returns false when the quorum is not there yet or another pass already
    /// certified it, so a round is recorded a single time however many
    /// attestations arrive after the threshold.
    pub fn claim_certificate(&self, key: RoundKey, threshold: usize) -> bool {
        let mut entries = self.entries.lock().expect("round buffer");
        let Some(entry) = entries.get_mut(&key) else {
            return false;
        };
        if entry.certified || entry.answer.is_none() || entry.attestations.len() < threshold {
            return false;
        }

        entry.certified = true;
        true
    }

    /// Whether a round has certified.
    pub fn is_certified(&self, key: RoundKey) -> bool {
        let entries = self.entries.lock().expect("round buffer");
        entries.get(&key).is_some_and(|entry| entry.certified)
    }

    /// Drop every round older than the given one, once they can no longer certify.
    pub fn retire_before(&self, epoch: EpochNumber, round: RoundNumber) {
        let mut entries = self.entries.lock().expect("round buffer");
        entries.retain(|key, _| (key.epoch, key.round) >= (epoch, round));
    }

    /// Rounds currently held, for reporting.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("round buffer").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::track::blob::SubLeafProof;
    use tape_core::types::GroupIndex;

    fn key(round: u64, spool: u64) -> RoundKey {
        RoundKey {
            epoch: EpochNumber(3),
            round: RoundNumber(round),
            spool: SpoolIndex(spool),
            block: Hash([1; 32]),
        }
    }

    fn answer(key: RoundKey) -> ProofOfAccess {
        ProofOfAccess {
            epoch: key.epoch,
            group: GroupIndex(0),
            round: key.round,
            spool: key.spool,
            block: key.block,
            track: Address::new_unique(),
            sub_leaf: 0,
            proof: SubLeafProof {
                sub_leaf: vec![0u8; 8],
                sub_proof: Vec::new(),
            },
            signature: signature(),
        }
    }

    /// Any signature will do here: the buffer stores what it is handed and does
    /// no verification, which the handlers do before anything reaches it.
    fn signature() -> BlsSignature {
        BlsPrivateKey::from_random().sign(b"round buffer").expect("sign")
    }

    #[test]
    fn the_first_answer_wins() {
        // A second answer is a relay duplicate or an owner answering twice, and
        // neither should displace what the group is already attesting to.
        let buffer = RoundBuffer::default();
        let key = key(1, 4);

        assert!(buffer.accept_answer(key, answer(key)));
        let first = buffer.answer(key).expect("held");
        assert!(!buffer.accept_answer(key, answer(key)));
        assert_eq!(buffer.answer(key), Some(first));
    }

    #[test]
    fn a_signer_counts_once_however_often_it_sends() {
        let buffer = RoundBuffer::default();
        let key = key(1, 4);
        let peer = Address::new_unique();

        assert!(buffer.accept_attestation(key, peer, signature()));
        assert!(!buffer.accept_attestation(key, peer, signature()));
        assert_eq!(buffer.signers(key), vec![peer]);
    }

    #[test]
    fn a_round_certifies_once_and_only_once() {
        let buffer = RoundBuffer::default();
        let key = key(1, 4);
        buffer.accept_answer(key, answer(key));

        for _ in 0..3 {
            buffer.accept_attestation(key, Address::new_unique(), signature());
        }
        assert!(!buffer.claim_certificate(key, 4), "certified under the threshold");

        buffer.accept_attestation(key, Address::new_unique(), signature());
        assert!(buffer.claim_certificate(key, 4));
        assert!(buffer.is_certified(key));

        // Attestations keep arriving after the threshold; the round is recorded once.
        buffer.accept_attestation(key, Address::new_unique(), signature());
        assert!(!buffer.claim_certificate(key, 4));
    }

    #[test]
    fn a_quorum_without_an_answer_does_not_certify() {
        // Attestations alone are not evidence: they attest to an answer, and this
        // node has not seen one.
        let buffer = RoundBuffer::default();
        let key = key(1, 4);

        for _ in 0..8 {
            buffer.accept_attestation(key, Address::new_unique(), signature());
        }
        assert!(!buffer.claim_certificate(key, 4));
    }

    #[test]
    fn retiring_drops_only_what_is_past() {
        let buffer = RoundBuffer::default();
        for round in 0..5 {
            let key = key(round, 4);
            buffer.accept_answer(key, answer(key));
        }
        assert_eq!(buffer.len(), 5);

        buffer.retire_before(EpochNumber(3), RoundNumber(3));
        assert_eq!(buffer.len(), 2);
        assert!(buffer.answer(key(3, 4)).is_some());
        assert!(buffer.answer(key(2, 4)).is_none());
    }

    #[test]
    fn an_earlier_epoch_retires_whatever_its_round_number() {
        // Round numbers restart each epoch, so ordering has to be on the pair or
        // a stale round from last epoch outlives this one's.
        let buffer = RoundBuffer::default();
        let old = RoundKey {
            epoch: EpochNumber(2),
            round: RoundNumber(9_000),
            spool: SpoolIndex(4),
            block: Hash([1; 32]),
        };
        buffer.accept_answer(old, answer(old));
        buffer.accept_answer(key(0, 4), answer(key(0, 4)));

        buffer.retire_before(EpochNumber(3), RoundNumber(0));
        assert!(buffer.answer(old).is_none());
        assert!(buffer.answer(key(0, 4)).is_some());
    }

    #[test]
    fn evidence_under_one_block_never_answers_for_another() {
        // Signatures over different block candidates cannot aggregate, so a
        // certificate gathered under one candidate must be invisible to a lookup
        // made under another, or a stale grid placement reads as a success.
        let buffer = RoundBuffer::default();
        let mine = key(1, 4);
        let theirs = RoundKey { block: Hash([2; 32]), ..mine };

        buffer.accept_answer(theirs, answer(theirs));
        for _ in 0..4 {
            buffer.accept_attestation(theirs, Address::new_unique(), signature());
        }
        assert!(buffer.claim_certificate(theirs, 4));

        assert!(buffer.answer(mine).is_none());
        assert!(!buffer.is_certified(mine));
    }
}
