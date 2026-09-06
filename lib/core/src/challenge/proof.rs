//! A challenged owner's broadcast answer, and what an observer checks about it.
//!
//! The owner derives its own sample for the round, reads the bytes, and signs the
//! round coordinates together with the leaf it served. An observer does not take
//! the coordinates on trust: it derives the same sample from its own view of the
//! group's tracks and rejects an answer to a different question. Without that a
//! challenged owner would simply pick a leaf it happened to keep.

use tape_crypto::Address;
use tape_crypto::hash::{Hash, hash};
use tape_crypto::merkle::hash_leaf;

use crate::bls::{BlsPubkey, BlsSignature};
use crate::cert::challenge::ChallengeRespondMessage;
use crate::challenge::sample::{Sample, SampleLeaf};
use crate::erasure::{group_for_spool, leaf_position};
use crate::track::blob::{BlobEncoding, SubLeafProof};
use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};

/// Why an observer would not accept a proof of access.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofRejection {
    /// The named group is not the one the answering spool belongs to.
    WrongGroup,
    /// It answers a different question than the round asked.
    WrongSample,
    /// The sub-leaf path does not reach the slice's registered root.
    BadProof,
    /// The owner's signature does not cover this response.
    BadSignature,
    /// It arrived after the round's deadline.
    Late,
}

/// What an owner produces for a sample.
///
/// A coded track answers with one leaf and its path to the slice root. An inline
/// track has no slice to index: every owner keeps the whole payload, so the
/// payload is the proof, checked against the value hash the write registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SampleProof {
    /// The sampled leaf, its index, and its path to the slice root.
    Coded { sub_leaf: u64, proof: SubLeafProof },
    /// The complete replicated payload.
    Inline { payload: Vec<u8> },
}

impl SampleProof {
    /// The bytes the owner signs over, whichever shape the answer took.
    pub fn signed_leaf(&self) -> Hash {
        match self {
            SampleProof::Coded { proof, .. } => hash_leaf(&proof.sub_leaf),
            SampleProof::Inline { payload } => hash_leaf(payload),
        }
    }
}

/// What an observer checks an answer against, from its own state.
pub enum Registered<'source> {
    /// The track's registered encoding, for a coded slice.
    Coded(&'source BlobEncoding),
    /// The value hash the inline write registered.
    Inline(Hash),
}

/// One owner's answer for one round, as it goes out to the group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofOfAccess {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
    pub track: Address,
    pub proof: SampleProof,
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
            self.proof.signed_leaf(),
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
        registered: Registered<'_>,
        signer: &BlsPubkey,
        in_time: bool,
    ) -> Result<(), ProofRejection> {
        self.verify_shape(expected, registered, in_time)?;
        self.signature
            .verify_aggregate(self.message().to_bytes(), core::slice::from_ref(signer))
            .map_err(|_| ProofRejection::BadSignature)
    }

    /// Everything an answer is judged on except its signature, so a caller
    /// holding several can check their signatures together.
    pub fn verify_shape(
        &self,
        expected: &Sample,
        registered: Registered<'_>,
        in_time: bool,
    ) -> Result<(), ProofRejection> {
        if !in_time {
            return Err(ProofRejection::Late);
        }

        // The group is in the round seed, so an owner free to name it could grind
        // it until the draw landed on a leaf it kept. A spool's group is fixed by
        // the spool, and nothing the sender says stands for it.
        if self.group != group_for_spool(self.spool) {
            return Err(ProofRejection::WrongGroup);
        }

        if self.track != expected.track {
            return Err(ProofRejection::WrongSample);
        }

        match (&self.proof, expected.leaf, registered) {
            (
                SampleProof::Coded { sub_leaf, proof },
                SampleLeaf::Coded { sub_leaf: asked },
                Registered::Coded(encoding),
            ) => {
                if *sub_leaf != asked as u64 {
                    return Err(ProofRejection::WrongSample);
                }
                if !encoding.verify_sub_leaf(leaf_position(self.spool), asked, proof) {
                    return Err(ProofRejection::BadProof);
                }
            }
            (SampleProof::Inline { payload }, SampleLeaf::Inline, Registered::Inline(value_hash)) => {
                if hash(payload) != value_hash {
                    return Err(ProofRejection::BadProof);
                }
            }
            // An answer of the other shape is an answer to another question.
            _ => return Err(ProofRejection::WrongSample),
        }

        Ok(())
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

    fn key() -> BlsPrivateKey {
        BlsPrivateKey::from_random()
    }

    fn sample(track: Address) -> Sample {
        Sample {
            track,
            leaf: SampleLeaf::Coded { sub_leaf: SUB_LEAF },
        }
    }

    fn inline_sample(track: Address) -> Sample {
        Sample {
            track,
            leaf: SampleLeaf::Inline,
        }
    }

    fn signed_inline(signer: &BlsPrivateKey, track: Address, payload: Vec<u8>) -> ProofOfAccess {
        let proof = SampleProof::Inline { payload };
        let message = ChallengeRespondMessage::new(
            EpochNumber(4),
            GroupIndex(1),
            RoundNumber(6),
            SPOOL,
            Hash([0x5A; 32]),
            proof.signed_leaf(),
        );

        ProofOfAccess {
            epoch: message.epoch,
            group: message.group,
            round: message.round,
            spool: message.spool,
            block: message.block,
            track,
            proof,
            signature: signer.sign(message.to_bytes()).expect("sign"),
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
            proof: SampleProof::Coded {
                sub_leaf: SUB_LEAF as u64,
                proof,
            },
            signature: signer.sign(message.to_bytes()).expect("sign"),
        }
    }

    #[test]
    fn honest_answer() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Ok(())
        );
    }

    #[test]
    fn other_leaf() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        let elsewhere = Sample {
            track,
            leaf: SampleLeaf::Coded {
                sub_leaf: SUB_LEAF + 1,
            },
        };
        assert_eq!(
            answer.verify(&elsewhere, Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::WrongSample)
        );
    }

    #[test]
    fn other_track() {
        let (encoding, slices) = encoding_and_slices();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, Address::new_unique());

        assert_eq!(
            answer.verify(&sample(Address::new_unique()), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::WrongSample)
        );
    }

    #[test]
    fn other_group() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.group = GroupIndex(4);

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::WrongGroup)
        );
    }

    #[test]
    fn tampered_leaf() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        if let SampleProof::Coded { proof, .. } = &mut answer.proof {
            proof.sub_leaf[0] ^= 0xFF;
        }

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadProof)
        );
    }

    #[test]
    fn wrong_signer() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);
        let impostor = BlsPrivateKey::from_random();

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &impostor.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn replayed_round() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.round = RoundNumber(7);

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn moved_branch() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let mut answer = signed(&signer, &encoding, &slices, track);
        answer.block = Hash([0x5B; 32]);

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), true),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn honest_inline() {
        let track = Address::new_unique();
        let signer = key();
        let payload = b"a small object that fits in one write".to_vec();
        let answer = signed_inline(&signer, track, payload.clone());

        assert_eq!(
            answer.verify(
                &inline_sample(track),
                Registered::Inline(hash(&payload)),
                &signer.public_key().expect("pubkey"),
                true
            ),
            Ok(())
        );
    }

    #[test]
    fn tampered_payload() {
        let track = Address::new_unique();
        let signer = key();
        let payload = b"a small object that fits in one write".to_vec();
        let mut rotten = payload.clone();
        rotten[0] ^= 0xFF;
        let answer = signed_inline(&signer, track, rotten);

        assert_eq!(
            answer.verify(
                &inline_sample(track),
                Registered::Inline(hash(&payload)),
                &signer.public_key().expect("pubkey"),
                true
            ),
            Err(ProofRejection::BadProof)
        );
    }

    #[test]
    fn crossed_shapes() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let payload = b"inline".to_vec();

        let coded = signed(&signer, &encoding, &slices, track);
        assert_eq!(
            coded.verify(
                &inline_sample(track),
                Registered::Inline(hash(&payload)),
                &signer.public_key().expect("pubkey"),
                true
            ),
            Err(ProofRejection::WrongSample)
        );

        let inline = signed_inline(&signer, track, payload);
        assert_eq!(
            inline.verify(
                &sample(track),
                Registered::Coded(&encoding),
                &signer.public_key().expect("pubkey"),
                true
            ),
            Err(ProofRejection::WrongSample)
        );
    }

    #[test]
    fn replayed_inline() {
        let track = Address::new_unique();
        let signer = key();
        let payload = b"a small object".to_vec();
        let mut answer = signed_inline(&signer, track, payload.clone());
        answer.round = RoundNumber(7);

        assert_eq!(
            answer.verify(
                &inline_sample(track),
                Registered::Inline(hash(&payload)),
                &signer.public_key().expect("pubkey"),
                true
            ),
            Err(ProofRejection::BadSignature)
        );
    }

    #[test]
    fn late_answer() {
        let (encoding, slices) = encoding_and_slices();
        let track = Address::new_unique();
        let signer = key();
        let answer = signed(&signer, &encoding, &slices, track);

        assert_eq!(
            answer.verify(&sample(track), Registered::Coded(&encoding), &signer.public_key().expect("pubkey"), false),
            Err(ProofRejection::Late)
        );
    }
}
