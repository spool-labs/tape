//! A challenged owner's broadcast answer, and what an observer checks about it.
//!
//! The owner derives its own sample for the round, reads the bytes, and signs the
//! round coordinates together with the leaf it served. An observer does not take
//! the coordinates on trust: it derives the same sample from its own view of the
//! group's tracks and rejects an answer to a different question. Without that a
//! challenged owner would simply pick a leaf it happened to keep.

use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::hash_leaf;

use crate::bls::{BlsPubkey, BlsSignature};
use crate::cert::challenge::ChallengeRespondMessage;
use crate::challenge::sample::Sample;
use crate::erasure::leaf_position;
use crate::track::blob::{BlobEncoding, SubLeafProof};
use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};

/// Why an observer would not accept a proof of access.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofRejection {
    /// It answers a different question than the round asked.
    WrongSample,
    /// The sub-leaf path does not reach the slice's registered root.
    BadProof,
    /// The owner's signature does not cover this response.
    BadSignature,
    /// It arrived after the round's deadline.
    Late,
}

/// One owner's answer for one round, as it goes out to the group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofOfAccess {
    /// Epoch the round belongs to.
    pub epoch: EpochNumber,
    /// Group the round was run in.
    pub group: GroupIndex,
    /// Round within the epoch.
    pub round: RoundNumber,
    /// The answering owner's spool.
    pub spool: SpoolIndex,
    /// Candidate entropy block the sample was drawn from.
    pub block: Hash,
    /// Track the sampled leaf belongs to, carried so a mismatch is diagnosable.
    pub track: Address,
    /// Index of the sampled leaf inside the slice.
    pub sub_leaf: u64,
    /// The leaf and its path to the slice root.
    pub proof: SubLeafProof,
    /// The owner's signature over the round coordinates and the leaf it served.
    pub signature: BlsSignature,
}

impl ProofOfAccess {
    /// The message an owner signs for this response.
    pub fn message(&self) -> ChallengeRespondMessage {
        ChallengeRespondMessage::new(
            self.epoch,
            self.group,
            self.round,
            self.spool,
            self.block,
            hash_leaf(&self.proof.sub_leaf),
        )
    }

    /// Check an answer against the question this observer derived for itself.
    ///
    /// `expected` is the sample this observer drew for the answering spool, and
    /// `encoding` is the track's registered encoding from this observer's own
    /// store. Nothing the sender supplied is trusted to stand for either.
    pub fn verify(
        &self,
        expected: &Sample,
        encoding: &BlobEncoding,
        signer: &BlsPubkey,
        in_time: bool,
    ) -> Result<(), ProofRejection> {
        if !in_time {
            return Err(ProofRejection::Late);
        }

        if self.track != expected.track || self.sub_leaf != expected.sub_leaf as u64 {
            return Err(ProofRejection::WrongSample);
        }

        let position = leaf_position(self.spool);
        if !encoding.verify_sub_leaf(position, expected.sub_leaf, &self.proof) {
            return Err(ProofRejection::BadProof);
        }

        self.signature
            .verify_aggregate(self.message().to_bytes(), core::slice::from_ref(signer))
            .map_err(|_| ProofRejection::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bls::BlsPrivateKey;
    use crate::cert::challenge::ChallengeRespondMessage;
    use crate::encoding::EncodingProfile;
    use crate::erasure::{GROUP_SIZE, SUB_LEAF_BYTES, SLICE_TREE_HEIGHT, slice_root};
    use crate::types::{StorageUnits, StripeCount};
    use tape_crypto::merkle::root_from_leaf_hashes;

    const SPOOL: SpoolIndex = SpoolIndex(23);
    const SUB_LEAF: usize = 2;

    /// Twenty slices whose leaves all differ, so a wrong path cannot pass by
    /// symmetry, plus the encoding a verifier holds for them.
    fn encoding_and_slices() -> (BlobEncoding, Vec<Vec<u8>>) {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let slices: Vec<Vec<u8>> = (0..GROUP_SIZE)
            .map(|_| {
                (0..SUB_LEAF_BYTES * 8)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        (state >> 24) as u8
                    })
                    .collect()
            })
            .collect();

        let leaves: [Hash; GROUP_SIZE] =
            core::array::from_fn(|index| slice_root(&slices[index]).expect("within capacity"));
        let encoding = BlobEncoding {
            size: StorageUnits::from_bytes(1),
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(1),
            stripe_count: StripeCount(1),
            leaves,
        };

        (encoding, slices)
    }

    /// One key per test, since the type only generates random ones.
    fn key() -> BlsPrivateKey {
        BlsPrivateKey::from_random()
    }

    fn sample(track: Address) -> Sample {
        Sample {
            track,
            sub_leaf: SUB_LEAF,
        }
    }

    fn signed(
        signer: &BlsPrivateKey,
        encoding: &BlobEncoding,
        slices: &[Vec<u8>],
        track: Address,
    ) -> ProofOfAccess {
        let position = leaf_position(SPOOL);
        let proof = encoding
            .prove_sub_leaf(position, SUB_LEAF, &slices[position.as_usize()])
            .expect("prove");

        let message = ChallengeRespondMessage::new(
            EpochNumber(4),
            GroupIndex(1),
            RoundNumber(6),
            SPOOL,
            Hash([0x5A; 32]),
            hash_leaf(&proof.sub_leaf),
        );

        ProofOfAccess {
            epoch: message.epoch,
            group: message.group,
            round: message.round,
            spool: message.spool,
            block: message.block,
            track,
            sub_leaf: SUB_LEAF as u64,
            proof,
            signature: signer.sign(message.to_bytes()).expect("sign"),
        }
    }

    #[test]
    fn an_honest_answer_is_accepted() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        assert_eq!(
            answer.verify(&sample(track), &encoding, &signer.public_key().expect("pubkey"), true),
            Ok(())
        );
    }

    #[test]
    fn an_answer_to_a_different_leaf_is_refused() {
        // The property that stops an owner picking a leaf it happened to keep:
        // the observer derived the question and checks the answer matches it.
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        let elsewhere = Sample {
            track,
            sub_leaf: SUB_LEAF + 1,
        };
        assert_eq!(
            answer.verify(&elsewhere, &encoding, &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::WrongSample)
        );
    }

    #[test]
    fn an_answer_about_another_track_is_refused() {
        let (encoding, slices) = encoding_and_slices();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, Address::new_unique());

        assert_eq!(
            answer.verify(&sample(Address::new_unique()), &encoding, &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::WrongSample)
        );
    }

    #[test]
    fn a_tampered_leaf_is_refused() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.proof.sub_leaf[0] ^= 0xFF;

        assert_eq!(
            answer.verify(&sample(track), &encoding, &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadProof)
        );
    }

    #[test]
    fn another_owners_signature_is_refused() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);
        let impostor = BlsPrivateKey::from_random();

        assert_eq!(
            answer.verify(&sample(track), &encoding, &impostor.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn a_response_replayed_into_another_round_is_refused() {
        // The signature covers the round coordinates, so moving an answer to a
        // different round breaks it even though the bytes are honest.
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.round = RoundNumber(7);

        assert_eq!(
            answer.verify(&sample(track), &encoding, &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn a_response_moved_to_another_branch_is_refused() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.block = Hash([0x5B; 32]);

        assert_eq!(
            answer.verify(&sample(track), &encoding, &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn a_late_answer_is_refused_before_anything_is_hashed() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        assert_eq!(
            answer.verify(&sample(track), &encoding, &signer.public_key().expect("pubkey"), false),
            Err(ProofRejection::Late)
        );
    }
}
