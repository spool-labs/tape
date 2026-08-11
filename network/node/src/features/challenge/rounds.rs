use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::bls::BlsSignature;
use tape_core::challenge::ProofOfAccess;
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;

/// Identifies a challenged spool and the entropy block its signatures bind to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoundKey {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
}

#[derive(Default)]
pub struct RoundBuffer {
    entries: Mutex<HashMap<RoundKey, RoundEntry>>,
}

#[derive(Default)]
struct RoundEntry {
    answer: Option<ProofOfAccess>,
    attestations: HashMap<Address, BlsSignature>,
    certified: bool,
}

impl RoundBuffer {
    /// Accepts the first answer for a round and returns whether it was stored.
    pub fn accept_answer(&self, key: RoundKey, answer: ProofOfAccess) -> bool {
        let mut entries = self.entries.lock().expect("round buffer");
        let entry = entries.entry(key).or_default();
        if entry.answer.is_some() {
            return false;
        }

        entry.answer = Some(answer);
        true
    }

    pub fn answer(&self, key: RoundKey) -> Option<ProofOfAccess> {
        let entries = self.entries.lock().expect("round buffer");
        entries.get(&key)?.answer.clone()
    }

    /// Stores one attestation per signer and returns whether it was new.
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

    pub fn signers(&self, key: RoundKey) -> Vec<Address> {
        let entries = self.entries.lock().expect("round buffer");
        let Some(entry) = entries.get(&key) else {
            return Vec::new();
        };
        let mut signers: Vec<Address> = entry.attestations.keys().copied().collect();
        signers.sort_unstable();
        signers
    }

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

    /// Allows another certification attempt after aggregate verification fails.
    pub fn release_certificate(&self, key: RoundKey) {
        let mut entries = self.entries.lock().expect("round buffer");
        if let Some(entry) = entries.get_mut(&key) {
            entry.certified = false;
        }
    }

    /// Claims a round once its answer and quorum are present.
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

    pub fn is_certified(&self, key: RoundKey) -> bool {
        let entries = self.entries.lock().expect("round buffer");
        entries.get(&key).is_some_and(|entry| entry.certified)
    }

    pub fn discard_block(&self, block: Hash) {
        let mut entries = self.entries.lock().expect("round buffer");
        entries.retain(|key, _| key.block != block);
    }

    pub fn retire_before(&self, epoch: EpochNumber, round: RoundNumber) {
        let mut entries = self.entries.lock().expect("round buffer");
        entries.retain(|key, _| (key.epoch, key.round) >= (epoch, round));
    }

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
    use tape_core::challenge::proof::SampleProof;
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
            proof: SampleProof::Coded {
                sub_leaf: 0,
                proof: SubLeafProof {
                    sub_leaf: vec![0u8; 8],
                    sub_proof: Vec::new(),
                },
            },
            signature: signature(),
        }
    }

    /// Any signature will do here: the buffer stores what it is handed and does
    /// no verification, which the handlers do before anything reaches it.
    fn signature() -> BlsSignature {
        BlsPrivateKey::from_random().sign(b"round buffer").expect("sign")
    }

    // a second answer is a relay duplicate or an owner answering twice, and
    // neither displaces what the group is already attesting to
    #[test]
    fn first_answer() {
        let buffer = RoundBuffer::default();
        let key = key(1, 4);

        assert!(buffer.accept_answer(key, answer(key)));
        let first = buffer.answer(key).expect("held");
        assert!(!buffer.accept_answer(key, answer(key)));
        assert_eq!(buffer.answer(key), Some(first));
    }

    // a signer counts once however often it sends
    #[test]
    fn signer_once() {
        let buffer = RoundBuffer::default();
        let key = key(1, 4);
        let peer = Address::new_unique();

        assert!(buffer.accept_attestation(key, peer, signature()));
        assert!(!buffer.accept_attestation(key, peer, signature()));
        assert_eq!(buffer.signers(key), vec![peer]);
    }

    // a round is recorded once however many attestations arrive after the
    // threshold, and not at all below it
    #[test]
    fn certifies_once() {
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

        // Attestations keep arriving after the threshold. The round is recorded once.
        buffer.accept_attestation(key, Address::new_unique(), signature());
        assert!(!buffer.claim_certificate(key, 4));
    }

    // attestations alone are not evidence, because they attest to an answer
    // this node has not seen
    #[test]
    fn quorum_no_answer() {
        let buffer = RoundBuffer::default();
        let key = key(1, 4);

        for _ in 0..8 {
            buffer.accept_attestation(key, Address::new_unique(), signature());
        }
        assert!(!buffer.claim_certificate(key, 4));
    }

    // retiring drops the rounds before the cutoff and keeps the rest
    #[test]
    fn retire_past() {
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

    // a round still open in one group must keep its evidence when another
    // group opens its first round of a new epoch, or it settles as a miss its
    // owner never earned
    #[test]
    fn retire_spares_the_pending() {
        let buffer = RoundBuffer::default();
        let pending = RoundKey {
            epoch: EpochNumber(3),
            round: RoundNumber(3),
            spool: SpoolIndex(4),
            block: Hash([1; 32]),
        };
        buffer.accept_answer(pending, answer(pending));
        for _ in 0..4 {
            buffer.accept_attestation(pending, Address::new_unique(), signature());
        }
        assert!(buffer.claim_certificate(pending, 4));

        // The oldest round still open is the pending one, so retiring behind it
        // keeps it even though another group has moved to the next epoch.
        buffer.retire_before(EpochNumber(3), RoundNumber(3));
        assert!(buffer.is_certified(pending), "evidence retired before it settled");

        // Once nothing is open behind it, it goes.
        buffer.retire_before(EpochNumber(4), RoundNumber(0));
        assert!(!buffer.is_certified(pending));
    }

    // round numbers restart each epoch, so ordering is on the pair or a stale
    // round from last epoch outlives this epoch's
    #[test]
    fn retire_epoch() {
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

    // signatures over different candidate blocks cannot aggregate, so evidence
    // gathered under one is invisible to a lookup made under another
    #[test]
    fn block_scoped() {
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
