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

    /// The track at position 0 of the tree has this number. The counter keeps
    /// counting when the tree is cleared, so the two differ from then on.
    #[inline(always)]
    pub fn tree_start(&self) -> TrackNumber {
        self.next_number.saturating_sub(TrackNumber(self.tree.next_index))
    }

    /// A track's position in the tree. A track numbered below the tree's start has none.
    #[inline(always)]
    pub fn position(&self, track_number: TrackNumber) -> Option<u64> {
        track_number
            .checked_sub(self.tree_start())
            .map(|position| position.as_u64())
    }

    #[inline(always)]
    pub fn verify(&self, proof: &CompressedTrackProof) -> Result<Hash, MerkleError> {
        let position = self
            .position(proof.state.track_number)
            .ok_or(MerkleError::InvalidIndex)?;
        proof.verify(&self.tree, position)
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

        // The tree takes one leaf per number, so the counter is never behind it.
        if self.next_number.as_u64() < self.tree.next_index {
            return Err(MerkleError::InvalidIndex);
        }

        if track_number != self.next_number {
            return Err(MerkleError::InvalidIndex);
        }

        self.tree.add_leaf_hash(leaf)?;

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

        // An update may change `state` and nothing else, so a track keeps its address.
        let unchanged = CompressedTrack {
            state: proof.state.state,
            ..*updated_track
        };
        if unchanged != proof.state {
            return Err(MerkleError::InvalidIndex);
        }

        let position = self
            .position(proof.state.track_number)
            .ok_or(MerkleError::InvalidIndex)?;

        self.tree.update_leaf_hash(
            position,
            &proof.proof,
            proof.state.get_hash(),
            updated_track.get_hash(),
        )
    }

    pub fn remove(&mut self, proof: &CompressedTrackProof) -> Result<(), MerkleError> {
        self.tree.ensure_initialized();

        let position = self
            .position(proof.state.track_number)
            .ok_or(MerkleError::InvalidIndex)?;

        self.tree.remove_leaf_hash(
            position,
            &proof.proof,
            proof.state.get_hash(),
        )?;

        self.num_tracks = self
            .num_tracks
            .checked_sub(1)
            .ok_or(MerkleError::InvalidIndex)?;

        Ok(())
    }

    /// Drop every track and free every position. The counter keeps counting,
    /// so no track number comes back.
    pub fn clear(&mut self) {
        self.tree = MerkleTree::zeroed();
        self.num_tracks = 0;
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, hash_leaf};

    use crate::spooler::VirtualGroup;
    use crate::types::{StorageUnits, TapeNumber};

    use super::*;

    // The counter's value when the tree of `cleared` was cleared
    const START: u64 = 41;

    fn track(number: u64) -> CompressedTrack {
        CompressedTrack {
            track_number: TrackNumber(number),
            key: Hash([number as u8; 32]),
            ..proof().state
        }
    }

    // an archive whose tree was cleared once its counter reached `START`,
    // holding these tracks
    fn cleared(tracks: &[CompressedTrack]) -> TrackArchive {
        let mut archive = TrackArchive::zeroed();
        archive.next_number = TrackNumber(START);
        for track in tracks {
            archive.append(track).expect("append");
        }
        archive
    }

    fn proof_at(leaves: &[Hash], position: usize, state: CompressedTrack) -> CompressedTrackProof {
        let proof = create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(leaves, position)
            .expect("proof")
            .try_into()
            .expect("proof length");
        CompressedTrackProof { state, proof }
    }

    fn proof() -> CompressedTrackProof {
        CompressedTrackProof {
            state: CompressedTrack {
                tape: Address::new([1u8; 32]),
                tape_number: TapeNumber(5),
                track_number: TrackNumber(0),
                key: Hash([2u8; 32]),
                kind: 1,
                state: 0,
                flags: 0,
                size: StorageUnits(1024),
                group: VirtualGroup(3),
                value_hash: Hash([4u8; 32]),
            },
            proof: [Hash::default(); TRACK_TREE_HEIGHT],
        }
    }

    // an update may change state and nothing else
    #[test]
    fn update_keeps_fixed_fields() {
        let proof = proof();
        let track = proof.state;

        let changed = [
            CompressedTrack { tape: Address::new([7u8; 32]), ..track },
            CompressedTrack { tape_number: TapeNumber(6), ..track },
            CompressedTrack { track_number: TrackNumber(1), ..track },
            CompressedTrack { key: Hash([7u8; 32]), ..track },
            CompressedTrack { kind: 0, ..track },
            CompressedTrack { flags: 1, ..track },
            CompressedTrack { size: StorageUnits(1025), ..track },
            CompressedTrack { group: VirtualGroup(4), ..track },
            CompressedTrack { value_hash: Hash([7u8; 32]), ..track },
        ];

        for updated in changed {
            let mut archive = TrackArchive::zeroed();
            let result = archive.update(&proof, &updated);
            assert!(matches!(result, Err(MerkleError::InvalidIndex)));
        }
    }

    // a tape that was never cleared has every track at its own number
    #[test]
    fn position_is_number() {
        let mut archive = TrackArchive::zeroed();
        for number in 0..3 {
            archive.append(&track(number)).expect("append");
        }

        assert_eq!(archive.tree_start(), TrackNumber(0));
        assert_eq!(archive.position(TrackNumber(2)), Some(2));
    }

    // a cleared tree fills from position 0 while the counter keeps counting
    #[test]
    fn append_after_clear() {
        let tracks = [track(START), track(START + 1)];
        let archive = cleared(&tracks);

        let mut expected = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        for track in &tracks {
            expected.add_leaf_hash(track.get_hash()).expect("add leaf");
        }

        assert_eq!(archive.tree.root(), expected.root());
        assert_eq!(archive.next_number(), TrackNumber(START + 2));
        assert_eq!(archive.num_tracks(), 2);
        assert_eq!(archive.tree_start(), TrackNumber(START));
        assert_eq!(archive.position(TrackNumber(START + 1)), Some(1));
        assert_eq!(archive.position(TrackNumber(START - 1)), None);
    }

    // an append must carry the counter's number, whatever the tree holds
    #[test]
    fn append_wrong_number() {
        let mut archive = cleared(&[]);

        assert_eq!(archive.append(&track(0)), Err(MerkleError::InvalidIndex));
        assert_eq!(archive.append(&track(START + 1)), Err(MerkleError::InvalidIndex));
    }

    // a counter behind the tree's leaf count is refused
    #[test]
    fn counter_behind_tree() {
        let mut archive = cleared(&[track(START), track(START + 1)]);
        archive.next_number = TrackNumber(1);

        assert_eq!(archive.append(&track(1)), Err(MerkleError::InvalidIndex));
    }

    // a full tree refuses a track and keeps its counter, and takes tracks
    // again once it is cleared
    #[test]
    fn full_tree() {
        let capacity = MerkleTree::<TRACK_TREE_HEIGHT>::capacity();
        let mut archive = cleared(&[track(START)]);
        archive.tree.next_index = capacity;
        archive.next_number = TrackNumber(START + capacity);

        let next = track(START + capacity);
        assert_eq!(archive.append(&next), Err(MerkleError::TreeFull));
        assert_eq!(archive.next_number(), TrackNumber(START + capacity));
        assert_eq!(archive.tree_start(), TrackNumber(START));

        archive.clear();

        archive.append(&next).expect("append");
        assert_eq!(archive.tree_start(), TrackNumber(START + capacity));
        assert_eq!(archive.position(next.track_number), Some(0));
    }

    // clearing drops the tracks and moves the tree's start up to the counter,
    // so a proof for an old track is refused
    #[test]
    fn clear_keeps_counter() {
        let tracks = [track(START), track(START + 1)];
        let mut archive = cleared(&tracks);
        let leaves: Vec<Hash> = tracks.iter().map(|track| track.get_hash()).collect();
        let old = proof_at(&leaves, 1, tracks[1]);
        archive.verify(&old).expect("verify");

        archive.clear();

        assert_eq!(archive.num_tracks(), 0);
        assert_eq!(archive.next_number(), TrackNumber(START + 2));
        assert_eq!(archive.tree_start(), TrackNumber(START + 2));
        assert_eq!(archive.tree, MerkleTree::zeroed());
        assert_eq!(archive.verify(&old), Err(MerkleError::InvalidIndex));
    }

    // verify, update and remove find a track at its position in a cleared tree
    #[test]
    fn proofs_after_clear() {
        let tracks = [track(START), track(START + 1), track(START + 2)];
        let mut archive = cleared(&tracks);
        let mut leaves: Vec<Hash> = tracks.iter().map(|track| track.get_hash()).collect();

        let proof = proof_at(&leaves, 1, tracks[1]);
        let certified = CompressedTrack { state: 1, ..tracks[1] };
        archive.verify(&proof).expect("verify");
        archive.update(&proof, &certified).expect("update");
        leaves[1] = certified.get_hash();

        archive.remove(&proof_at(&leaves, 2, tracks[2])).expect("remove");
        leaves[2] = hash_leaf(&[]);

        archive.verify(&proof_at(&leaves, 1, certified)).expect("verify certified");
        assert_eq!(archive.num_tracks(), 2);
        assert_eq!(archive.tree_start(), TrackNumber(START));
    }

    // a track from before the clear has no position, so its proof is refused
    #[test]
    fn track_before_clear() {
        let tracks = [track(START), track(START + 1)];
        let mut archive = cleared(&tracks);
        let leaves: Vec<Hash> = tracks.iter().map(|track| track.get_hash()).collect();

        // Before the clear, track 1 sat at position 1, where a new track sits now.
        let old = proof_at(&leaves, 1, track(1));
        let certified = CompressedTrack { state: 1, ..old.state };

        assert_eq!(archive.verify(&old), Err(MerkleError::InvalidIndex));
        assert_eq!(archive.update(&old, &certified), Err(MerkleError::InvalidIndex));
        assert_eq!(archive.remove(&old), Err(MerkleError::InvalidIndex));
    }

    // a track numbered at the counter or past it is in no tree yet
    #[test]
    fn track_past_end() {
        let tracks = [track(START), track(START + 1)];
        let mut archive = cleared(&tracks);
        let leaves: Vec<Hash> = tracks.iter().map(|track| track.get_hash()).collect();

        let unwritten = proof_at(&leaves, 1, track(START + 2));

        assert_eq!(archive.verify(&unwritten), Err(MerkleError::InvalidProof));
        assert_eq!(archive.remove(&unwritten), Err(MerkleError::InvalidProof));
    }

    // a proof built for a track's number in place of its position fails
    #[test]
    fn proof_at_number() {
        let tracks = [track(START), track(START + 1)];
        let archive = cleared(&tracks);
        let mut leaves = vec![hash_leaf(&[]); START as usize];
        leaves.extend(tracks.iter().map(|track| track.get_hash()));

        let proof = proof_at(&leaves, START as usize, tracks[0]);

        assert_eq!(archive.verify(&proof), Err(MerkleError::InvalidProof));
    }
}
