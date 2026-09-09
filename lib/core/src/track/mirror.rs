//! Client-side mirror of a tape's on-chain track tree
//!
//! Certifying a track requires a merkle proof against the tape's current
//! track tree root, and every certify mutates that root, so the proofs for
//! a stream of tracks form a serial chain. The mirror seeds from a fetched
//! archive snapshot and replays the client's own appends and certified leaf
//! updates locally, so each proof comes from memory instead of a round trip
//! per track. The mirrored root must match the on-chain root at stream end;
//! a mismatch means an external writer touched the tape.

use tape_crypto::Hash;
use tape_crypto::merkle::{empty_subtree_root, hash_pair, MerkleError, MerkleTree};

use crate::track::archive::{TrackArchive, TRACK_TREE_HEIGHT};
use crate::track::types::{CompressedTrack, CompressedTrackProof};
use crate::types::TrackNumber;

/// Local replica of a track tree covering leaves appended by this client
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveMirror {
    // First track number appended through the mirror
    base_number: TrackNumber,
    // Frontier snapshot taken at seed time, before any mirrored appends
    base_subtrees: [Hash; TRACK_TREE_HEIGHT],

    // Live tree kept in lockstep with the chain
    tree: MerkleTree<TRACK_TREE_HEIGHT>,
    // Track number the next mirrored append must carry
    next_number: TrackNumber,

    // Current leaf state for every mirrored track
    tracks: Vec<CompressedTrack>,

    // Per-level hashes for every node touching the appended window, updated
    // on each append and certify so proofs read siblings instead of
    // rehashing the window
    levels: Vec<Vec<Hash>>,
}

impl ArchiveMirror {
    /// Seed the mirror from the fetched pre-stream archive state
    pub fn new(archive: &TrackArchive) -> Self {
        let mut tree = archive.tree;
        tree.ensure_initialized();

        Self {
            base_number: archive.next_number(),
            base_subtrees: tree.filled_subtrees,
            tree,
            next_number: archive.next_number(),
            tracks: Vec::new(),
            levels: vec![Vec::new(); TRACK_TREE_HEIGHT],
        }
    }

    /// Mirror the on-chain append for a track this client registers
    pub fn append(&mut self, track: &CompressedTrack) -> Result<(), MerkleError> {
        if self.next_number.as_u64() != self.tree.next_index {
            return Err(MerkleError::InvalidIndex);
        }
        if track.track_number != self.next_number {
            return Err(MerkleError::InvalidIndex);
        }

        let leaf = track.get_hash();
        self.tree.add_leaf_hash(leaf)?;
        self.tracks.push(*track);
        self.next_number.increment();
        self.levels[0].push(leaf);
        self.refresh_path(track.track_number.as_u64());

        Ok(())
    }

    /// Build a proof for a mirrored track against the current root
    pub fn proof_for(
        &self,
        track_number: TrackNumber,
    ) -> Result<CompressedTrackProof, MerkleError> {
        let state = self.tracks[self.offset_of(track_number)?];

        let mut proof = [Hash::default(); TRACK_TREE_HEIGHT];
        for (level, sibling) in proof.iter_mut().enumerate() {
            let sibling_index = (track_number.as_u64() >> level) ^ 1;
            *sibling = self.node_hash(level, sibling_index);
        }

        Ok(CompressedTrackProof { state, proof })
    }

    /// Mirror the on-chain leaf replacement performed by certify
    pub fn apply_certified(
        &mut self,
        track_number: TrackNumber,
        updated: &CompressedTrack,
    ) -> Result<(), MerkleError> {
        let proof = self.proof_for(track_number)?;
        self.apply_certified_with_proof(track_number, updated, &proof)
    }

    /// Mirror a certify reusing a proof already built against the current
    /// root, saving the rebuild when the caller just submitted with it
    pub fn apply_certified_with_proof(
        &mut self,
        track_number: TrackNumber,
        updated: &CompressedTrack,
        proof: &CompressedTrackProof,
    ) -> Result<(), MerkleError> {
        let offset = self.offset_of(track_number)?;

        let current = self.tracks[offset];
        if updated.track_number != current.track_number
            || updated.tape != current.tape
            || updated.key != current.key
        {
            return Err(MerkleError::InvalidIndex);
        }

        let leaf = updated.get_hash();
        self.tree.update_leaf_hash(
            track_number.as_u64(),
            &proof.proof,
            current.get_hash(),
            leaf,
        )?;
        self.tracks[offset] = *updated;
        self.levels[0][offset] = leaf;
        self.refresh_path(track_number.as_u64());

        Ok(())
    }

    /// Current locally-computed track tree root
    pub fn root(&self) -> Hash {
        self.tree.root()
    }

    /// Track number the next mirrored append must carry
    pub fn next_number(&self) -> TrackNumber {
        self.next_number
    }

    fn offset_of(&self, track_number: TrackNumber) -> Result<usize, MerkleError> {
        if track_number < self.base_number || track_number >= self.next_number {
            return Err(MerkleError::InvalidIndex);
        }

        Ok((track_number.as_u64() - self.base_number.as_u64()) as usize)
    }

    // Hash of the tree node at the given level and node index. Nodes right of
    // the appended range are empty-subtree constants. The only pre-seed node
    // a lookup can reference at each level is the completed left sibling on
    // the seed boundary path, whose hash the seeded frontier holds:
    // sequential insertion writes that slot when the sibling's last leaf
    // lands and never overwrites it before the boundary is crossed. Every
    // other node touches the appended window and sits in the level cache.
    fn node_hash(&self, level: usize, node_index: u64) -> Hash {
        let start = node_index << level;
        let end = start + (1u64 << level);
        let base = self.base_number.as_u64();

        if start >= self.tree.next_index {
            return empty_subtree_root(level);
        }
        if end <= base {
            return self.base_subtrees[level];
        }

        self.levels[level][(node_index - (base >> level)) as usize]
    }

    // Recompute the cached nodes on the path above a changed or appended
    // leaf. Each level rehashes one pair, so appends and certifies stay
    // linear in tree height.
    fn refresh_path(&mut self, leaf_index: u64) {
        let base = self.base_number.as_u64();

        for level in 1..TRACK_TREE_HEIGHT {
            let node_index = leaf_index >> level;
            let value = hash_pair(
                self.node_hash(level - 1, node_index << 1),
                self.node_hash(level - 1, (node_index << 1) | 1),
            );

            // The path node is either the last cached node at this level or
            // the next one over; window nodes fill left to right.
            let position = (node_index - (base >> level)) as usize;
            let nodes = &mut self.levels[level];
            if position == nodes.len() {
                nodes.push(value);
            } else {
                nodes[position] = value;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;

    use tape_crypto::address::Address;
    use tape_crypto::hash::hashv;

    use crate::spooler::GroupIndex;
    use crate::track::types::{TrackKind, TrackState};
    use crate::types::StorageUnits;

    use super::*;

    fn tape_address() -> Address {
        Address::from([7u8; 32])
    }

    fn registered(track_number: TrackNumber) -> CompressedTrack {
        CompressedTrack {
            tape: tape_address(),
            track_number,
            key: hashv(&[b"key", &track_number.pack()]),
            kind: TrackKind::Coded.into(),
            state: TrackState::Registered.into(),
            size: StorageUnits(1024),
            group: GroupIndex(3),
            value_hash: hashv(&[b"registered", &track_number.pack()]),
        }
    }

    fn certified(track: &CompressedTrack) -> CompressedTrack {
        let mut updated = *track;
        updated.state = TrackState::Certified.into();
        updated.value_hash = hashv(&[b"certified", &track.track_number.pack()]);
        updated
    }

    // every mirrored proof must verify against both the archive and the mirror
    fn assert_lockstep(archive: &TrackArchive, mirror: &ArchiveMirror) {
        assert_eq!(mirror.root(), archive.tree.root());
        assert_eq!(mirror.next_number(), archive.next_number());

        for track in &mirror.tracks {
            let proof = mirror
                .proof_for(track.track_number)
                .expect("proof for mirrored track");
            assert_eq!(proof.state, *track);
            archive.verify(&proof).expect("archive accepts proof");
            proof.verify(&mirror.tree).expect("mirror accepts proof");
        }
    }

    fn append_both(
        archive: &mut TrackArchive,
        mirror: &mut ArchiveMirror,
        track: &CompressedTrack,
    ) {
        archive.append(track).expect("archive append");
        mirror.append(track).expect("mirror append");
    }

    fn certify_both(
        archive: &mut TrackArchive,
        mirror: &mut ArchiveMirror,
        track: &CompressedTrack,
    ) {
        let updated = certified(track);
        let proof = mirror.proof_for(track.track_number).expect("certify proof");
        archive.update(&proof, &updated).expect("archive update");
        mirror
            .apply_certified(track.track_number, &updated)
            .expect("mirror update");
    }

    // proofs stay valid for every seed offset over pre-existing tracks
    #[test]
    fn seeded_offsets() {
        for pre_count in 0..=17u64 {
            let mut archive = TrackArchive::zeroed();
            for number in 0..pre_count {
                archive
                    .append(&registered(TrackNumber(number)))
                    .expect("pre-seed append");
            }

            let mut mirror = ArchiveMirror::new(&archive);
            for number in pre_count..pre_count + 5 {
                let track = registered(TrackNumber(number));
                append_both(&mut archive, &mut mirror, &track);
                assert_lockstep(&archive, &mirror);
            }
        }
    }

    // certifies interleaved with appends in track order keep proofs valid
    #[test]
    fn interleaved_certifies() {
        let mut archive = TrackArchive::zeroed();
        for number in 0..3 {
            archive
                .append(&registered(TrackNumber(number)))
                .expect("pre-seed append");
        }

        let mut mirror = ArchiveMirror::new(&archive);
        let mut appended = Vec::new();

        // Certify runs one track behind register, as the stream pipeline does.
        for number in 3..9u64 {
            let track = registered(TrackNumber(number));
            append_both(&mut archive, &mut mirror, &track);
            appended.push(track);
            assert_lockstep(&archive, &mirror);

            if appended.len() >= 2 {
                let target = appended[appended.len() - 2];
                certify_both(&mut archive, &mut mirror, &target);
                assert_lockstep(&archive, &mirror);
            }
        }

        let last = appended[appended.len() - 1];
        certify_both(&mut archive, &mut mirror, &last);
        assert_lockstep(&archive, &mirror);
    }

    // a proof built for an early leaf remains valid after later appends
    #[test]
    fn late_proof() {
        let mut archive = TrackArchive::zeroed();
        let mut mirror = ArchiveMirror::new(&archive);
        for number in 0..8u64 {
            append_both(&mut archive, &mut mirror, &registered(TrackNumber(number)));
        }

        let proof = mirror.proof_for(TrackNumber(0)).expect("late proof");

        archive.verify(&proof).expect("archive accepts late proof");
    }

    // proof requests outside the mirrored range are rejected
    #[test]
    fn unmirrored_track() {
        let mut archive = TrackArchive::zeroed();
        for number in 0..2 {
            archive
                .append(&registered(TrackNumber(number)))
                .expect("pre-seed append");
        }
        let mut mirror = ArchiveMirror::new(&archive);
        append_both(&mut archive, &mut mirror, &registered(TrackNumber(2)));

        assert_eq!(
            mirror.proof_for(TrackNumber(1)),
            Err(MerkleError::InvalidIndex)
        );
        assert_eq!(
            mirror.proof_for(TrackNumber(3)),
            Err(MerkleError::InvalidIndex)
        );
    }

    // appends with an out-of-sequence track number are rejected
    #[test]
    fn wrong_number() {
        let archive = TrackArchive::zeroed();
        let mut mirror = ArchiveMirror::new(&archive);

        let result = mirror.append(&registered(TrackNumber(1)));

        assert_eq!(result, Err(MerkleError::InvalidIndex));
    }

    // certify with a different track identity is rejected
    #[test]
    fn identity_mismatch() {
        let mut archive = TrackArchive::zeroed();
        let mut mirror = ArchiveMirror::new(&archive);
        let track = registered(TrackNumber(0));
        append_both(&mut archive, &mut mirror, &track);

        let mut updated = certified(&track);
        updated.key = hashv(&[b"other"]);

        assert_eq!(
            mirror.apply_certified(track.track_number, &updated),
            Err(MerkleError::InvalidIndex)
        );
    }
}
