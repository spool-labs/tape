use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::merkle::{MerkleError, MerkleTree};

use crate::track::types::{CompressedTrack, CompressedTrackProof};
use crate::track::TRACK_TREE_HEIGHT;
use crate::types::TrackNumber;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct TrackStore {
    pub tree: MerkleTree<TRACK_TREE_HEIGHT>,
    pub next_number: TrackNumber,
    pub live_count: u64,
}

impl TrackStore {
    #[inline(always)]
    pub fn next_number(&self) -> TrackNumber {
        self.next_number
    }

    #[inline(always)]
    pub fn live_count(&self) -> u64 {
        self.live_count
    }

    #[inline(always)]
    pub fn verify(&self, proof: &CompressedTrackProof) -> Result<Hash, MerkleError> {
        proof.verify(&self.tree)
    }

    pub fn append(&mut self, track: &CompressedTrack) -> Result<(), MerkleError> {
        self.tree.ensure_initialized();

        if self.next_number.0 != self.tree.next_index {
            return Err(MerkleError::InvalidIndex);
        }

        if track.track_number.0 != self.next_number.0 {
            return Err(MerkleError::InvalidIndex);
        }

        let inserted_index = self.tree.add_leaf_hash(track.get_hash())?;
        if inserted_index != track.track_number.0 {
            return Err(MerkleError::InvalidIndex);
        }

        self.next_number = TrackNumber(
            self.next_number
                .0
                .checked_add(1)
                .ok_or(MerkleError::TreeFull)?,
        );
        self.live_count = self
            .live_count
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
        self.live_count = self
            .live_count
            .checked_sub(1)
            .ok_or(MerkleError::InvalidIndex)?;
        Ok(())
    }
}
