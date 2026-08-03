//! The two-level sub-leaf commitment and a spool owner's certified track set.
//!
//! The commitment is the slicer's own, not a model of it: each coded slice is
//! split into fixed-size sample leaves under a per-slice root, and the top tree
//! over those roots is the registered track commitment. A response is one sample
//! leaf and its path to that slice's root. It carries no path from the root to
//! the commitment, because every verifier holds the track's encoding and already
//! has the root the path must reach.
//!
//! A spool owner holds one slice of each coded track plus the whole payload of
//! each inline track it is responsible for, addressed by the tape track number.
//! The round draws one entry uniformly over the flat enumeration of every
//! sub-leaf of every held slice plus one entry per inline track, which makes the
//! draw byte-weighted: a larger track spans more leaves and is proportionally
//! more likely to be inspected.

use anyhow::{anyhow, bail, Result};
use tape_core::erasure::{sub_leaf_count, SUB_LEAF_BYTES, SUB_TREE_HEIGHT};
use tape_core::track::blob::{BlobEncoding, SubLeafProof};
use tape_core::types::{SpoolIndex, TrackNumber};
use tape_crypto::hash::Hash;
use tape_crypto::merkle::hash_leaf;
use tape_slicer::{ErasureCoder, Slicer};

use crate::crypto::leaf_index;
use crate::spool::{blob_encoding, deterministic_payload};
use crate::types::GroupPosition;

/// One coded track and the encoding a verifier checks its proofs against.
pub struct CodedTrack {
    /// Sequential track number addressing this track within its tape.
    pub number: TrackNumber,
    /// The n coded slices; slice i is what spool owner i stores.
    pub slices: Vec<Vec<u8>>,
    /// The registered encoding: commitment, profile, stripe layout, slice roots.
    pub encoding: BlobEncoding,
}

impl CodedTrack {
    /// Encode a payload into a real coded track and register its encoding.
    pub fn build(number: TrackNumber, payload: &[u8]) -> Result<Self> {
        let mut slicer = Slicer::clay_default();
        let slices = slicer
            .encode(payload)
            .map_err(|error| anyhow!("clay encode failed: {error:?}"))?;

        let encoding = blob_encoding(&slicer, payload.len(), &slices)?;

        Ok(Self {
            number,
            slices,
            encoding,
        })
    }

    /// Sub-leaves in one slice; every slice of a track has the same count.
    pub fn leaves_per_slice(&self) -> usize {
        self.slices
            .first()
            .map(|slice| sub_leaf_count(slice.len()))
            .unwrap_or(0)
    }

    /// Build the proof for one sampled sub-leaf of the slice at one position.
    fn prove(&self, position: SpoolIndex, sub_leaf: usize) -> Result<SubLeafProof> {
        let slice = self
            .slices
            .get(position.as_usize())
            .ok_or_else(|| anyhow!("slice {position} missing"))?;

        self.encoding
            .prove_sub_leaf(position, sub_leaf, slice)
            .ok_or_else(|| anyhow!("sub-leaf {sub_leaf} out of range for slice {position}"))
    }
}

/// One inline track, replicated whole rather than coded.
pub struct InlineTrack {
    /// Sequential track number addressing this track within its tape.
    pub number: TrackNumber,
    /// The complete replicated payload, the whole proof of access.
    pub payload: Vec<u8>,
    /// Commitment to the replicated payload.
    pub commitment: Hash,
}

impl InlineTrack {
    /// Build an inline track and commit to its replicated payload.
    pub fn build(number: TrackNumber, payload: Vec<u8>) -> Self {
        let commitment = hash_leaf(&payload);
        Self {
            number,
            payload,
            commitment,
        }
    }
}

/// The tracks one spool owner is responsible for, coded and inline.
pub struct SpoolHoldings {
    /// Coded tracks, one held slice per owner position.
    pub coded: Vec<CodedTrack>,
    /// Inline tracks, replicated whole.
    pub inline: Vec<InlineTrack>,
}

/// One drawn sample, addressed by track number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sample {
    /// A sub-leaf of a coded slice at the owner's position.
    Coded {
        track: TrackNumber,
        slice: SpoolIndex,
        sub_leaf: usize,
    },
    /// The whole payload of an inline track.
    Inline { track: TrackNumber },
}

/// A built proof of access for one sample.
pub enum SampleProof {
    /// The sampled sub-leaf and its path to the slice root.
    Coded(SubLeafProof),
    /// The complete replicated payload of an inline track.
    Inline { payload: Vec<u8> },
}

impl SampleProof {
    /// Bytes a possession response puts on the wire.
    ///
    /// A coded response carries the sampled sub-leaf itself and not just its
    /// hash, because a hash with a path proves only that the owner cached a
    /// proof, which is the free-rider the commitment probe demonstrates. An
    /// inline response carries the whole replicated payload.
    pub fn wire_bytes(&self) -> usize {
        match self {
            SampleProof::Coded(proof) => proof.sub_leaf.len() + proof.sub_proof.len() * Hash::LEN,
            SampleProof::Inline { payload } => payload.len(),
        }
    }
}

impl SpoolHoldings {
    /// Build a deterministic set of coded and inline tracks for one spool group.
    ///
    /// The coded tracks descend in size so the flat leaf enumeration is dominated
    /// by the largest track, which is what makes the uniform sub-leaf draw
    /// byte-weighted. Inline tracks are one leaf each, keeping that weighting
    /// honest. Track numbers are sequential, coded tracks first.
    pub fn build(blob_bytes: usize, group_size: usize) -> Result<Self> {
        let sizes = [blob_bytes.max(1), (blob_bytes / 2).max(1), (blob_bytes / 4).max(1)];
        let mut coded = Vec::with_capacity(sizes.len());
        for (index, size) in sizes.iter().enumerate() {
            let number = TrackNumber(index as u64);
            let payload = deterministic_payload(*size, number.as_u64());
            let track = CodedTrack::build(number, &payload)?;
            if track.slices.len() < group_size {
                bail!(
                    "coded track has {} slices, need {}",
                    track.slices.len(),
                    group_size
                );
            }
            coded.push(track);
        }

        let inline_sizes = [200usize, 400usize];
        let mut inline = Vec::with_capacity(inline_sizes.len());
        for (index, size) in inline_sizes.iter().enumerate() {
            let number = TrackNumber((sizes.len() + index) as u64);
            let payload = deterministic_payload(*size, number.as_u64());
            inline.push(InlineTrack::build(number, payload));
        }

        Ok(Self { coded, inline })
    }

    /// Flat leaf count for a spool owner: every sub-leaf of every held slice plus
    /// one entry per inline track. The owner holds slice position of each coded
    /// track, and every slice of a track has the same sub-leaf count, so the
    /// total does not depend on the position.
    pub fn leaf_total(&self) -> usize {
        let coded: usize = self.coded.iter().map(CodedTrack::leaves_per_slice).sum();
        coded + self.inline.len()
    }

    /// Draw one sample for the owner at position from the round seed.
    ///
    /// The draw is uniform over the flat leaf enumeration, so it is byte-weighted:
    /// tracks are enumerated by number, each held slice's sub-leaves in order,
    /// then one entry per inline track, and the seed indexes into that list to
    /// land on one track, slice, and sub-leaf.
    pub fn sample(&self, position: GroupPosition, seed: &Hash) -> Sample {
        let total = self.leaf_total().max(1);
        let mut index = leaf_index(seed, total);
        for track in &self.coded {
            let leaves = track.leaves_per_slice();
            if index < leaves {
                return Sample::Coded {
                    track: track.number,
                    slice: SpoolIndex(position.as_u64()),
                    sub_leaf: index,
                };
            }
            index -= leaves;
        }
        let inline_index = index.min(self.inline.len().saturating_sub(1));
        Sample::Inline {
            track: self.inline[inline_index].number,
        }
    }

    /// Wire size of a full coded possession response.
    ///
    /// Every coded response is the same size because the sub-leaf size and the
    /// sub-tree height are fixed, so this is the figure to cost a round with. A
    /// short trailing sub-leaf at the end of a slice is the only exception.
    pub fn coded_response_bytes() -> usize {
        SUB_LEAF_BYTES + SUB_TREE_HEIGHT * Hash::LEN
    }

    /// The coded track with this number, if the owner holds it.
    fn coded_track(&self, number: TrackNumber) -> Option<&CodedTrack> {
        self.coded.iter().find(|track| track.number == number)
    }

    /// Build the proof of access for a drawn sample.
    pub fn prove(&self, sample: Sample) -> Result<SampleProof> {
        match sample {
            Sample::Coded { track, slice, sub_leaf } => {
                let coded = self
                    .coded_track(track)
                    .ok_or_else(|| anyhow!("coded track {track} missing"))?;
                Ok(SampleProof::Coded(coded.prove(slice, sub_leaf)?))
            }
            Sample::Inline { track } => {
                let inline = self
                    .inline
                    .iter()
                    .find(|candidate| candidate.number == track)
                    .ok_or_else(|| anyhow!("inline track {track} missing"))?;
                Ok(SampleProof::Inline {
                    payload: inline.payload.clone(),
                })
            }
        }
    }

    /// Registered commitment for the sampled track, what a verifier checks against.
    pub fn commitment(&self, sample: Sample) -> Option<Hash> {
        match sample {
            Sample::Coded { track, .. } => {
                self.coded_track(track).map(|coded| coded.encoding.commitment)
            }
            Sample::Inline { track } => self
                .inline
                .iter()
                .find(|candidate| candidate.number == track)
                .map(|candidate| candidate.commitment),
        }
    }

    /// Verify a proof of access against the registered encoding, for real.
    pub fn verify(&self, sample: Sample, proof: &SampleProof) -> bool {
        match (sample, proof) {
            (
                Sample::Coded {
                    track,
                    slice,
                    sub_leaf,
                },
                SampleProof::Coded(coded),
            ) => self
                .coded_track(track)
                .is_some_and(|held| held.encoding.verify_sub_leaf(slice, sub_leaf, coded)),
            (Sample::Inline { track }, SampleProof::Inline { payload }) => self
                .inline
                .iter()
                .find(|candidate| candidate.number == track)
                .is_some_and(|held| hash_leaf(payload) == held.commitment),
            (Sample::Coded { .. }, SampleProof::Inline { .. }) => false,
            (Sample::Inline { .. }, SampleProof::Coded(_)) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::GROUP_SIZE;

    // a real sub-leaf proof verifies and a flipped byte fails
    #[test]
    fn two_level_proof() {
        let holdings = SpoolHoldings::build(120_000, GROUP_SIZE).expect("build holdings");
        // Walk several positions and tracks, proving and verifying each.
        for position in [0u64, 3, 7, 19] {
            let track = &holdings.coded[0];
            let leaves = track.leaves_per_slice();
            for sub_leaf in [0usize, leaves / 2, leaves - 1] {
                let sample = Sample::Coded {
                    track: track.number,
                    slice: SpoolIndex(position),
                    sub_leaf,
                };
                let proof = holdings.prove(sample).expect("prove sample");
                assert!(holdings.verify(sample, &proof));

                // A flipped byte in the sampled sub-leaf must fail verification.
                if let SampleProof::Coded(coded) = &proof {
                    let mut tampered = coded.clone();
                    tampered.sub_leaf[0] ^= 0xFF;
                    assert!(!holdings.verify(sample, &SampleProof::Coded(tampered)));
                }
            }
        }
    }

    // a proof for one slice does not verify against another slice
    #[test]
    fn wrong_position() {
        // The path anchors at the slice's own root, so replaying one owner's
        // response for another owner's slice has to fail.
        let holdings = SpoolHoldings::build(120_000, GROUP_SIZE).expect("build holdings");
        let track = &holdings.coded[0];
        let sample = Sample::Coded {
            track: track.number,
            slice: SpoolIndex(4),
            sub_leaf: 1,
        };
        let proof = holdings.prove(sample).expect("prove sample");
        let replayed = Sample::Coded {
            track: track.number,
            slice: SpoolIndex(5),
            sub_leaf: 1,
        };
        assert!(!holdings.verify(replayed, &proof));
    }

    // an inline sample verifies against the whole replicated payload
    #[test]
    fn inline_sample() {
        let holdings = SpoolHoldings::build(120_000, GROUP_SIZE).expect("build holdings");
        let inline = &holdings.inline[0];
        let sample = Sample::Inline { track: inline.number };
        let proof = holdings.prove(sample).expect("prove sample");
        assert!(holdings.verify(sample, &proof));
    }

    // the costing figure equals what a full coded response really carries
    #[test]
    fn response_size() {
        let holdings = SpoolHoldings::build(200_000, GROUP_SIZE).expect("build holdings");
        // Every full coded response is one size, so the costing figure has to
        // equal what a real proof actually serialises to.
        let mut checked = 0;
        for round in 0..64u64 {
            let seed = tape_crypto::hash::hashv(&[b"size", &round.to_le_bytes()]);
            let sample = holdings.sample(GroupPosition::new(5), &seed);
            let proof = holdings.prove(sample).expect("prove sample");
            if let SampleProof::Coded(coded) = &proof {
                // Skip a short trailing sub-leaf, which is the one exception.
                if coded.sub_leaf.len() == SUB_LEAF_BYTES {
                    assert_eq!(proof.wire_bytes(), SpoolHoldings::coded_response_bytes());
                    checked += 1;
                }
            }
        }
        assert!(checked > 0, "no full coded sub-leaf was drawn");
    }

    // the uniform leaf draw favours the largest track
    #[test]
    fn byte_weighted_draw() {
        let holdings = SpoolHoldings::build(200_000, GROUP_SIZE).expect("build holdings");
        let mut coded_hits = [0u64; 3];
        let mut inline_hits = 0u64;
        for round in 0..4000u64 {
            let seed = tape_crypto::hash::hashv(&[b"draw", &round.to_le_bytes()]);
            match holdings.sample(GroupPosition::new(5), &seed) {
                Sample::Coded { track, .. } => coded_hits[track.as_u64() as usize] += 1,
                Sample::Inline { .. } => inline_hits += 1,
            }
        }
        // The largest track is sampled far more than the smallest, and both far
        // more than the one-leaf inline tracks.
        assert!(coded_hits[0] > coded_hits[1]);
        assert!(coded_hits[1] > coded_hits[2]);
        assert!(coded_hits[2] > inline_hits);
    }

    // the flat leaf total is every held sub-leaf plus one entry per inline track
    #[test]
    fn leaf_enumeration() {
        let holdings = SpoolHoldings::build(80_000, GROUP_SIZE).expect("build holdings");
        let first = holdings.coded[0].leaves_per_slice();
        let second = holdings.coded[1].leaves_per_slice();
        // Flat index zero is the first sub-leaf of the first track at the position.
        let total = holdings.leaf_total();
        assert_eq!(total, first + second + holdings.coded[2].leaves_per_slice() + holdings.inline.len());
    }

    // the registered commitment is the slicer's own root, not a rebuilt one
    #[test]
    fn slicer_commitment() {
        // The sim must not build its own tree: the root here has to be the one
        // the slicer would register for the same slices.
        let track = CodedTrack::build(TrackNumber(0), &deterministic_payload(150_000, 0)).expect("build track");
        assert_eq!(
            track.encoding.commitment,
            tape_slicer::blob_merkle_root(&track.slices)
        );
        for (position, slice) in track.slices.iter().enumerate() {
            assert!(track
                .encoding
                .verify_slice(SpoolIndex(position as u64), slice));
        }
    }
}
