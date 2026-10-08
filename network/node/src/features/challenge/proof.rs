//! Builds and checks the proof for a picked track

use tape_core::encoding::ErasureCode;
use tape_core::erasure::{
    leaf_position, prove_sub_leaf_windowed, sample_window_range, sub_leaf_count,
    SAMPLE_WINDOW_LEAVES, SUB_LEAF_BYTES,
};
use tape_core::track::blob::{BlobEncoding, SubLeafProof};
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::hash::hash;
use tape_slicer::coded_slice_len;
use tape_store::ops::{SliceOps, TrackDataOps};
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::challenge::error::ChallengeError;
use crate::features::challenge::pick::Pick;
use crate::features::challenge::sample::Sample;

/// A prover sends one of these for each spool in a round: a leaf of a coded
/// track's slice, or the whole payload of an inline track
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Proof {
    Coded(SubLeafProof),
    Inline(Vec<u8>),
}

/// An observer's finding for one proof
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofCheck {
    Valid,
    Invalid,
    /// The observer holds no encoding for the coded track, so it has no root to check against
    NoEncoding,
}

/// Work out which leaf of a coded track's slice a sample asks for
///
/// The slice length comes from the encoding and is the same at every
/// position, so a node that holds no slice of the track reaches the same leaf.
pub fn sampled_leaf(blob: &BlobEncoding, sample: Sample, profile: ErasureCode) -> Option<usize> {
    let size = blob.size.try_as_usize().ok()?;
    let stripe_size = blob.stripe_size.try_as_usize().ok()?;
    let stripe_count = blob.stripe_count.try_as_usize().ok()?;
    let slice_len = coded_slice_len(profile, size, stripe_size, stripe_count);

    sample.leaf_index(sub_leaf_count(slice_len))
}

/// Build the proof the holder of `spool` owes for the picked track
///
/// `spool` is the live spool being answered for. Nothing comes back when
/// this node lacks the payload, the encoding or the slice.
pub fn build_proof<Db: Store>(
    store: &TapeStore<Db>,
    pick: &Pick,
    spool: SpoolIndex,
    sample: Sample,
    profile: ErasureCode,
) -> Result<Option<Proof>, ChallengeError> {
    let data = store.get_track_data(pick.address).map_err(store_error)?;

    match data {
        Some(BlobData::Inline(payload)) => Ok(Some(Proof::Inline(payload))),
        Some(BlobData::Coded(blob)) => coded_proof(store, pick, spool, &blob, sample, profile),
        None => Ok(None),
    }
}

/// Read the sampled leaf from the track's own stored spool and build its path
fn coded_proof<Db: Store>(
    store: &TapeStore<Db>,
    pick: &Pick,
    spool: SpoolIndex,
    blob: &BlobEncoding,
    sample: Sample,
    profile: ErasureCode,
) -> Result<Option<Proof>, ChallengeError> {
    let Some(leaf) = sampled_leaf(blob, sample, profile) else {
        return Ok(None);
    };

    let stored = pick.track.group.spool_at(leaf_position(spool).as_usize());
    let range = sample_window_range(leaf);
    let Some((sidecar, window)) = store
        .slice_window(stored, pick.address, range.start, range.len())
        .map_err(store_error)?
    else {
        return Ok(None);
    };

    let start = (leaf % SAMPLE_WINDOW_LEAVES) * SUB_LEAF_BYTES;
    let end = (start + SUB_LEAF_BYTES).min(window.len());
    let Some(sub_leaf) = window.get(start..end).filter(|bytes| !bytes.is_empty()) else {
        return Ok(None);
    };
    let Some(sub_proof) = prove_sub_leaf_windowed(&sidecar, &window, leaf) else {
        return Ok(None);
    };

    Ok(Some(Proof::Coded(SubLeafProof {
        sub_leaf: sub_leaf.to_vec(),
        sub_proof,
    })))
}

/// Check a proof for the picked track, as the holder of `spool` owes it
///
/// An inline payload is checked against the track's value hash, which every
/// node holds. A coded leaf is checked against this node's copy of the
/// track's encoding.
pub fn check_proof<Db: Store>(
    store: &TapeStore<Db>,
    pick: &Pick,
    spool: SpoolIndex,
    sample: Sample,
    profile: ErasureCode,
    proof: &Proof,
) -> Result<ProofCheck, ChallengeError> {
    let is_valid = match proof {
        Proof::Inline(payload) => pick.track.is_inline() && hash(payload) == pick.track.value_hash,
        Proof::Coded(_) if !pick.track.is_coded() => false,
        Proof::Coded(sub_leaf) => {
            let data = store.get_track_data(pick.address).map_err(store_error)?;
            let Some(BlobData::Coded(blob)) = data else {
                return Ok(ProofCheck::NoEncoding);
            };

            sampled_leaf(&blob, sample, profile)
                .is_some_and(|leaf| blob.verify_sub_leaf(leaf_position(spool), leaf, sub_leaf))
        }
    };

    Ok(if is_valid { ProofCheck::Valid } else { ProofCheck::Invalid })
}

fn store_error(error: impl std::fmt::Display) -> ChallengeError {
    ChallengeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::RepParams;
    use tape_core::erasure::SAMPLE_WINDOW_BYTES;
    use tape_core::spooler::{GroupIndex, VirtualGroup};
    use tape_core::track::types::TrackState;
    use tape_core::types::{RoundNumber, TrackNumber};
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    use crate::features::challenge::fixture::{
        coded, payload, put_tape, test_store, write_coded, write_inline, Coded, EPOCH, PROFILE,
        WRITTEN,
    };

    use super::*;

    const TAPE: u8 = 50;
    const GROUP: VirtualGroup = VirtualGroup(1);
    const ROUNDS: u64 = 12;

    /// Get the live spool at `position` of the group that answers for `GROUP`
    fn spool(position: usize) -> SpoolIndex {
        GroupIndex(GROUP.0).spool_at(position)
    }

    fn sample(round: u64, position: usize) -> Sample {
        Sample::new(EPOCH, RoundNumber(round), spool(position), Hash::from([round as u8; 32]))
    }

    /// Build a sample that asks for one chosen leaf
    fn sample_for(leaf: usize) -> Sample {
        let mut bytes = [0u8; 32];
        bytes[8..16].copy_from_slice(&(leaf as u64).to_le_bytes());
        Sample(Hash::from(bytes))
    }

    fn coded_leaf(proof: &Proof) -> Option<SubLeafProof> {
        match proof {
            Proof::Coded(sub_leaf) => Some(sub_leaf.clone()),
            Proof::Inline(_) => None,
        }
    }

    fn inline_payload(proof: &Proof) -> Option<Vec<u8>> {
        match proof {
            Proof::Inline(payload) => Some(payload.clone()),
            Proof::Coded(_) => None,
        }
    }

    fn stored(
        profile: ErasureCode,
        number: u64,
        len: usize,
    ) -> (TapeStore<MemoryStore>, Coded, Pick) {
        let store = test_store();
        put_tape(&store, TAPE, EPOCH.next(), TrackNumber(0));
        let blob = coded(profile, TAPE, number, GROUP, &payload(TAPE, number, len));
        let case = write_coded(&store, &blob, WRITTEN);
        let pick = Pick { address: case.address, track: case.track };

        (store, blob, pick)
    }

    fn inline() -> (TapeStore<MemoryStore>, Pick) {
        let store = test_store();
        put_tape(&store, TAPE, EPOCH.next(), TrackNumber(0));
        let case = write_inline(&store, TAPE, 0, GROUP, TrackState::Certified, WRITTEN);

        (store, Pick { address: case.address, track: case.track })
    }

    fn built(
        store: &TapeStore<MemoryStore>,
        pick: &Pick,
        position: usize,
        sample: Sample,
        profile: ErasureCode,
    ) -> Proof {
        build_proof(store, pick, spool(position), sample, profile)
            .expect("proof builds")
            .expect("data held")
    }

    fn checked(
        store: &TapeStore<MemoryStore>,
        pick: &Pick,
        position: usize,
        sample: Sample,
        profile: ErasureCode,
        proof: &Proof,
    ) -> ProofCheck {
        check_proof(store, pick, spool(position), sample, profile, proof).expect("proof checks")
    }

    // every position's proof verifies at its own position and leaf
    #[test]
    fn coded_proof() {
        let (store, _, pick) = stored(PROFILE, 0, 40_000);

        for position in 0..usize::from(PROFILE.n()) {
            for round in 1..=ROUNDS {
                let sample = sample(round, position);
                let proof = built(&store, &pick, position, sample, PROFILE);

                assert_eq!(
                    checked(&store, &pick, position, sample, PROFILE, &proof),
                    ProofCheck::Valid
                );
            }
        }
    }

    // a proof fails at another position, at another leaf and for another track
    #[test]
    fn own_place() {
        let (store, blob, pick) = stored(PROFILE, 0, 40_000);
        let leaves = sub_leaf_count(blob.slices[0].len());
        assert!(leaves > 2);
        let proof = built(&store, &pick, 3, sample_for(1), PROFILE);

        let other = coded(PROFILE, TAPE, 1, GROUP, &payload(TAPE, 1, 40_000));
        let other_case = write_coded(&store, &other, WRITTEN);
        let other_pick = Pick { address: other_case.address, track: other_case.track };

        assert_eq!(checked(&store, &pick, 3, sample_for(1), PROFILE, &proof), ProofCheck::Valid);
        assert_eq!(checked(&store, &pick, 4, sample_for(1), PROFILE, &proof), ProofCheck::Invalid);
        assert_eq!(checked(&store, &pick, 3, sample_for(2), PROFILE, &proof), ProofCheck::Invalid);
        assert_eq!(
            checked(&store, &other_pick, 3, sample_for(1), PROFILE, &proof),
            ProofCheck::Invalid
        );
    }

    // one changed byte in the leaf or in the path fails
    #[test]
    fn changed_byte() {
        let (store, _, pick) = stored(PROFILE, 0, 40_000);
        let sample = sample(1, 0);
        let proof = coded_leaf(&built(&store, &pick, 0, sample, PROFILE)).expect("a coded proof");

        let mut leaf = proof.clone();
        leaf.sub_leaf[0] ^= 1;
        let mut path = proof.clone();
        path.sub_proof[0] = Hash::from([7u8; 32]);

        for changed in [leaf, path] {
            assert_eq!(
                checked(&store, &pick, 0, sample, PROFILE, &Proof::Coded(changed)),
                ProofCheck::Invalid
            );
        }
    }

    // the leaf count from the encoding is the leaf count of the stored slice,
    // and a short last leaf proves and verifies
    #[test]
    fn leaf_count() {
        for (number, len) in [1usize, 700, 7_000, 40_000, 99_999, 250_000].into_iter().enumerate() {
            let (store, blob, pick) = stored(PROFILE, number as u64, len);
            let leaves = sub_leaf_count(blob.slices[0].len());
            let last = sample_for(leaves - 1);

            for slice in &blob.slices {
                assert_eq!(slice.len(), blob.slices[0].len());
            }
            assert_eq!(sampled_leaf(&blob.blob, last, PROFILE), Some(leaves - 1), "len {len}");
            assert_eq!(sampled_leaf(&blob.blob, sample_for(leaves), PROFILE), Some(0), "len {len}");

            let proof = built(&store, &pick, 5, last, PROFILE);
            let sub_leaf = coded_leaf(&proof).expect("a coded proof");
            let tail = blob.slices[0].len() - (leaves - 1) * SUB_LEAF_BYTES;

            assert_eq!(sub_leaf.sub_leaf.len(), tail, "len {len}");
            assert_eq!(checked(&store, &pick, 5, last, PROFILE, &proof), ProofCheck::Valid);
        }
    }

    // a leaf past the first window is read from its own window of the slice
    #[test]
    fn later_window() {
        let copies = ErasureCode::Replication(RepParams::new(3));
        let (store, blob, pick) = stored(copies, 0, 2 * SAMPLE_WINDOW_BYTES + 5_000);
        let slice = &blob.slices[1];
        let leaves = sub_leaf_count(slice.len());
        assert!(leaves > 2 * SAMPLE_WINDOW_LEAVES);

        for leaf in [0, SAMPLE_WINDOW_LEAVES - 1, SAMPLE_WINDOW_LEAVES, leaves - 1] {
            let sample = sample_for(leaf);
            let proof = built(&store, &pick, 1, sample, copies);
            let sub_leaf = coded_leaf(&proof).expect("a coded proof");
            let start = leaf * SUB_LEAF_BYTES;
            let end = (start + SUB_LEAF_BYTES).min(slice.len());

            assert_eq!(sub_leaf.sub_leaf, slice[start..end], "leaf {leaf}");
            assert_eq!(checked(&store, &pick, 1, sample, copies, &proof), ProofCheck::Valid);
        }
    }

    // an inline track is proven by its payload, checked against the value hash
    #[test]
    fn inline_proof() {
        let (store, pick) = inline();
        let sample = sample(1, 0);
        let proof = built(&store, &pick, 0, sample, PROFILE);

        let mut changed = inline_payload(&proof).expect("an inline payload");
        changed[0] ^= 1;

        assert_eq!(checked(&store, &pick, 0, sample, PROFILE, &proof), ProofCheck::Valid);
        assert_eq!(
            checked(&store, &pick, 0, sample, PROFILE, &Proof::Inline(changed)),
            ProofCheck::Invalid
        );
        assert_eq!(
            checked(&store, &pick, 0, sample, PROFILE, &Proof::Inline(Vec::new())),
            ProofCheck::Invalid
        );
    }

    // a proof of the wrong kind for the track is invalid
    #[test]
    fn wrong_kind() {
        let (coded_store, _, coded_pick) = stored(PROFILE, 0, 40_000);
        let (inline_store, inline_pick) = inline();
        let sample = sample(1, 0);
        let leaf = built(&coded_store, &coded_pick, 0, sample, PROFILE);
        let bytes = built(&inline_store, &inline_pick, 0, sample, PROFILE);

        assert_eq!(
            checked(&coded_store, &coded_pick, 0, sample, PROFILE, &bytes),
            ProofCheck::Invalid
        );
        assert_eq!(
            checked(&inline_store, &inline_pick, 0, sample, PROFILE, &leaf),
            ProofCheck::Invalid
        );
    }

    // a node without the slice, the payload or the encoding builds no proof,
    // and an observer without the encoding cannot check one
    #[test]
    fn missing_data() {
        let (store, _, pick) = stored(PROFILE, 0, 40_000);
        let sample = sample(1, 2);
        let proof = built(&store, &pick, 2, sample, PROFILE);

        store
            .delete_slice(pick.track.group.spool_at(2), pick.address)
            .expect("slice deleted");
        assert!(build_proof(&store, &pick, spool(2), sample, PROFILE).expect("builds").is_none());
        assert_eq!(checked(&store, &pick, 2, sample, PROFILE, &proof), ProofCheck::Valid);

        store.delete_track_data(pick.address).expect("encoding deleted");
        assert!(build_proof(&store, &pick, spool(3), sample, PROFILE).expect("builds").is_none());
        assert_eq!(checked(&store, &pick, 2, sample, PROFILE, &proof), ProofCheck::NoEncoding);

        let (store, pick) = inline();
        store.delete_track_data(pick.address).expect("payload deleted");
        assert!(build_proof(&store, &pick, spool(0), sample, PROFILE).expect("builds").is_none());
    }
}
