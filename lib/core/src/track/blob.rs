//! Blob payload metadata and commitment encoding.

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::hash::hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_crypto::merkle::{compute_path, create_proof_from_leaf_hashes, hash_leaf, verify_proof_hash};

use crate::encoding::EncodingProfile;
use crate::erasure::{
    GROUP_SIZE, SLICE_TREE_HEIGHT, SUB_LEAF_BYTES, SUB_TREE_HEIGHT, slice_root, sub_leaf_hashes,
};
use crate::types::{SpoolIndex, StorageUnits, StripeCount};

#[cfg(feature = "wincode")]
use core::mem::MaybeUninit;
#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Blob payload metadata stored on nodes responsible for the track's spool group.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize))]
pub struct BlobEncoding {
    /// Original unencoded data size in bytes.
    pub size: StorageUnits,

    /// Root of the erasure-coded commitment tree.
    pub commitment: Hash,

    /// Erasure-coding profile used for the blob.
    pub profile: EncodingProfile,

    /// Stripe size in bytes.
    pub stripe_size: StorageUnits,

    /// Number of stripes.
    pub stripe_count: StripeCount,

    /// Per-slice commitment leaves, each the root of that slice's sub-leaf tree.
    pub leaves: [Hash; GROUP_SIZE],
}

/// One sampled sub-leaf with its path to the blob commitment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubLeafProof {
    /// Bytes of the sampled leaf, shorter than a full leaf only at the end of a slice.
    pub sub_leaf: Vec<u8>,

    /// Path from the sampled leaf to the root of its slice.
    pub sub_proof: Vec<Hash>,

    /// Path from that slice root to the blob commitment.
    pub top_proof: Vec<Hash>,
}

pub type PackedBlobEncoding = [u8; size_of::<BlobEncoding>()];

impl BlobEncoding {
    #[inline(always)]
    pub fn pack(&self) -> PackedBlobEncoding {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    #[inline(always)]
    pub fn unpack(data: PackedBlobEncoding) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }

    /// Recompute the commitment root from stored leaf hashes.
    pub fn commitment_root(&self) -> Hash {
        root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&self.leaves)
    }

    /// Verify a whole slice against its stored slice root, rebuilding its sub-leaf tree.
    pub fn verify_slice(&self, position: SpoolIndex, data: &[u8]) -> bool {
        let position = position.as_usize();
        if position >= self.leaves.len() {
            return false;
        }

        slice_root(data) == Some(self.leaves[position])
    }

    /// Prove one sample leaf of a slice the caller holds.
    pub fn prove_sub_leaf(
        &self,
        position: SpoolIndex,
        sub_leaf_index: usize,
        slice: &[u8],
    ) -> Option<SubLeafProof> {
        let position = position.as_usize();
        if position >= self.leaves.len() {
            return None;
        }

        let start = sub_leaf_index.checked_mul(SUB_LEAF_BYTES)?;
        if start >= slice.len() {
            return None;
        }
        let end = (start + SUB_LEAF_BYTES).min(slice.len());

        let hashes = sub_leaf_hashes(slice);
        let sub_proof =
            create_proof_from_leaf_hashes::<SUB_TREE_HEIGHT>(&hashes, sub_leaf_index).ok()?;
        let top_proof =
            create_proof_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&self.leaves, position).ok()?;

        Some(SubLeafProof {
            sub_leaf: slice[start..end].to_vec(),
            sub_proof,
            top_proof,
        })
    }

    /// Verify a sampled sub-leaf against the commitment.
    pub fn verify_sub_leaf(
        &self,
        position: SpoolIndex,
        sub_leaf_index: usize,
        proof: &SubLeafProof,
    ) -> bool {
        let position = position.as_usize();
        if position >= self.leaves.len() {
            return false;
        }
        // compute_path indexes the proof directly, so a short one must not reach it.
        if proof.sub_proof.len() != SUB_TREE_HEIGHT {
            return false;
        }
        // An empty leaf hashes to the value padding uses, so without this a prover
        // could answer at any index past the end of the slice.
        if proof.sub_leaf.is_empty() || proof.sub_leaf.len() > SUB_LEAF_BYTES {
            return false;
        }

        let leaf = hash_leaf(&proof.sub_leaf);
        let sub_path = compute_path(
            &proof.sub_proof,
            leaf,
            sub_leaf_index as u64,
            SUB_TREE_HEIGHT,
        );
        let slice_root = sub_path[SUB_TREE_HEIGHT];

        verify_proof_hash(
            slice_root,
            &self.commitment,
            &proof.top_proof,
            position as u64,
            SLICE_TREE_HEIGHT,
        )
    }

    /// Compute the canonical value hash for this blob payload.
    pub fn get_hash(&self) -> Hash {
        hash(bytemuck::bytes_of(self))
    }
}

#[cfg(feature = "wincode")]
impl SchemaWrite for BlobEncoding {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(size_of::<Self>())
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.pack())?;
        Ok(())
    }
}

#[cfg(feature = "wincode")]
impl<'de> SchemaRead<'de> for BlobEncoding {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // SAFETY: The serialized representation is exactly `PackedBlobEncoding` bytes for this
        // pod-compatible type.
        let packed: PackedBlobEncoding = unsafe { reader.get_t()? };
        dst.write(Self::unpack(packed));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::sub_leaf_count;

    fn sample_blob_encoding() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(512),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
            leaves: [Hash::from([0x33; 32]); GROUP_SIZE],
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn encoding_wincode() {
        let blob = sample_blob_encoding();
        let bytes = wincode::serialize(&blob).expect("serialize");
        let recovered: BlobEncoding = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, blob);
    }

    #[test]
    fn encoding_pack() {
        let blob = sample_blob_encoding();
        let packed = blob.pack();
        let recovered = BlobEncoding::unpack(packed);

        assert_eq!(recovered.stripe_size, StorageUnits::from_bytes(64));
        assert_eq!(recovered.stripe_count, StripeCount(2));
        assert_eq!(recovered, blob);
    }

    /// A blob whose leaves are the real sub-leaf roots of the returned slices.
    fn coded_blob() -> (BlobEncoding, Vec<Vec<u8>>) {
        let slices: Vec<Vec<u8>> = (0..GROUP_SIZE)
            .map(|i| {
                let len = SUB_LEAF_BYTES * 3 + i + 1;
                (0..len).map(|b| (b + i) as u8).collect()
            })
            .collect();

        let leaves: [Hash; GROUP_SIZE] =
            core::array::from_fn(|i| slice_root(&slices[i]).expect("slice within capacity"));

        let blob = BlobEncoding {
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves),
            leaves,
            ..sample_blob_encoding()
        };
        (blob, slices)
    }

    #[test]
    fn verify_slice_accepts_only_the_stored_slice() {
        let (blob, slices) = coded_blob();

        for (i, slice) in slices.iter().enumerate() {
            assert!(blob.verify_slice(SpoolIndex(i as u64), slice));
        }

        let mut tampered = slices[0].clone();
        tampered[SUB_LEAF_BYTES + 5] ^= 0xFF;
        assert!(!blob.verify_slice(SpoolIndex(0), &tampered));

        assert!(!blob.verify_slice(SpoolIndex(GROUP_SIZE as u64), &slices[0]));
    }

    #[test]
    fn sub_leaf_proof_round_trip() {
        let (blob, slices) = coded_blob();
        let position = SpoolIndex(3);
        let slice = &slices[3];

        for index in 0..sub_leaf_count(slice.len()) {
            let proof = blob
                .prove_sub_leaf(position, index, slice)
                .expect("proof for a held sub-leaf");
            assert_eq!(proof.sub_proof.len(), SUB_TREE_HEIGHT);
            assert_eq!(proof.top_proof.len(), SLICE_TREE_HEIGHT);
            assert!(blob.verify_sub_leaf(position, index, &proof));
        }
    }

    #[test]
    fn sub_leaf_proof_rejects_tampering() {
        let (blob, slices) = coded_blob();
        let position = SpoolIndex(3);
        let proof = blob
            .prove_sub_leaf(position, 1, &slices[3])
            .expect("proof for a held sub-leaf");

        let mut flipped = proof.clone();
        flipped.sub_leaf[0] ^= 0xFF;
        assert!(!blob.verify_sub_leaf(position, 1, &flipped));

        // The same proof replayed at a different index or slice must not verify.
        assert!(!blob.verify_sub_leaf(position, 2, &proof));
        assert!(!blob.verify_sub_leaf(SpoolIndex(4), 1, &proof));
    }

    #[test]
    fn sub_leaf_proof_rejects_malformed_paths_without_panicking() {
        let (blob, slices) = coded_blob();
        let position = SpoolIndex(3);
        let proof = blob
            .prove_sub_leaf(position, 1, &slices[3])
            .expect("proof for a held sub-leaf");

        let mut short_sub = proof.clone();
        short_sub.sub_proof.pop();
        assert!(!blob.verify_sub_leaf(position, 1, &short_sub));

        let mut short_top = proof.clone();
        short_top.top_proof.clear();
        assert!(!blob.verify_sub_leaf(position, 1, &short_top));

        let mut empty_leaf = proof.clone();
        empty_leaf.sub_leaf.clear();
        assert!(!blob.verify_sub_leaf(position, 1, &empty_leaf));

        let mut long_leaf = proof;
        long_leaf.sub_leaf = vec![0u8; SUB_LEAF_BYTES + 1];
        assert!(!blob.verify_sub_leaf(position, 1, &long_leaf));
    }

    #[test]
    fn prove_sub_leaf_rejects_an_index_past_the_slice() {
        let (blob, slices) = coded_blob();
        let past_end = sub_leaf_count(slices[0].len());

        assert!(blob.prove_sub_leaf(SpoolIndex(0), past_end, &slices[0]).is_none());
        assert!(blob.prove_sub_leaf(SpoolIndex(GROUP_SIZE as u64), 0, &slices[0]).is_none());
    }
}
