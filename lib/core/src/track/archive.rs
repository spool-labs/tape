use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::merkle::{MerkleError, MerkleTree};

use crate::track::types::{CompressedTrack, CompressedTrackProof};
use crate::types::TrackNumber;

pub const TRACK_TREE_HEIGHT: usize = 16;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct TrackArchive {
    pub num_tracks: u64,
    pub next_number: TrackNumber,
    pub tree: MerkleTree<TRACK_TREE_HEIGHT>,
}

impl TrackArchive {
    #[inline(always)]
    pub fn next_number(&self) -> TrackNumber {
        self.next_number
    }

    #[inline(always)]
    pub fn num_tracks(&self) -> u64 {
        self.num_tracks
    }

    #[inline(always)]
    pub fn verify(&self, proof: &CompressedTrackProof) -> Result<Hash, MerkleError> {
        proof.verify(&self.tree)
    }

    #[inline(always)]
    pub fn append(&mut self, track: &CompressedTrack) -> Result<(), MerkleError> {
        self.append_hash(track.track_number, track.get_hash())
    }

    pub fn append_hash(
        &mut self,
        track_number: TrackNumber,
        leaf: Hash,
    ) -> Result<(), MerkleError> {
        self.tree.ensure_initialized();

        if self.next_number.as_u64() != self.tree.next_index {
            return Err(MerkleError::InvalidIndex);
        }

        if track_number != self.next_number {
            return Err(MerkleError::InvalidIndex);
        }

        let inserted_index = self.tree.add_leaf_hash(leaf)?;
        if inserted_index != track_number.as_u64() {
            return Err(MerkleError::InvalidIndex);
        }

        self.next_number.increment();
        self.num_tracks = self.num_tracks
            .checked_add(1)
            .ok_or(MerkleError::TreeFull)?;

        Ok(())
    }

    pub fn update(
        &mut self,
        proof: &CompressedTrackProof,
        updated_track: &CompressedTrack,
    ) -> Result<(), MerkleError> {
        self.tree.ensure_initialized();

        if updated_track.track_number != proof.state.track_number
            || updated_track.tape != proof.state.tape
            || updated_track.key != proof.state.key
        {
            return Err(MerkleError::InvalidIndex);
        }

        self.tree.update_leaf_hash(
            proof.state.track_number.0,
            &proof.proof,
            proof.state.get_hash(),
            updated_track.get_hash(),
        )
    }

    pub fn remove(&mut self, proof: &CompressedTrackProof) -> Result<(), MerkleError> {
        self.tree.ensure_initialized();
        self.tree.remove_leaf_hash(
            proof.state.track_number.0,
            &proof.proof,
            proof.state.get_hash(),
        )?;

        self.num_tracks = self
            .num_tracks
            .checked_sub(1)
            .ok_or(MerkleError::InvalidIndex)?;

        Ok(())
    }
}
