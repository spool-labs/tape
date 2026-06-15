//! Anchor a fetched snapshot chunk-track list to the consensus-committed track
//! merkle root, so a reader can trust metadata served by an arbitrary peer.

use bytemuck::Zeroable;

use tape_core::track::archive::TrackArchive;
use tape_core::track::types::CompressedTrack;

use crate::SnapshotError;

/// Verify that `tracks` is exactly the chunk-track set committed by `committed`
/// (the snapshot tape's on-chain [`TrackArchive`]). Rebuilds the archive from the
/// tracks in `track_number` order and compares the resulting merkle root and
/// count, so a peer cannot add, drop, reorder, or alter a chunk track.
pub fn verify_snapshot_track_set(
    tracks: &[CompressedTrack],
    committed: &TrackArchive,
) -> Result<(), SnapshotError> {
    if tracks.len() as u64 != committed.num_tracks() {
        return Err(SnapshotError::TrackCountMismatch {
            expected: committed.num_tracks(),
            got: tracks.len(),
        });
    }

    let mut sorted: Vec<&CompressedTrack> = tracks.iter().collect();
    sorted.sort_by_key(|track| track.track_number.0);

    let mut rebuilt = TrackArchive::zeroed();
    for track in sorted {
        // `append` enforces contiguous track numbers starting at zero.
        rebuilt
            .append(track)
            .map_err(|_| SnapshotError::Contiguity)?;
    }

    if rebuilt.tree.root() != committed.tree.root() {
        return Err(SnapshotError::RootMismatch);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{TrackKind, TrackState};
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;

    fn track(n: u64) -> CompressedTrack {
        CompressedTrack {
            tape: Address::from([7u8; 32]),
            key: Hash::default(),
            track_number: TrackNumber(n),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(64),
            group: GroupIndex(n),
            value_hash: Hash::default(),
        }
    }

    fn committed(tracks: &[CompressedTrack]) -> TrackArchive {
        let mut archive = TrackArchive::zeroed();
        for track in tracks {
            archive.append(track).unwrap();
        }
        archive
    }

    #[test]
    fn accepts_matching_set_in_any_order() {
        let tracks: Vec<_> = (0..5).map(track).collect();
        let archive = committed(&tracks);

        let mut shuffled = tracks.clone();
        shuffled.reverse();

        assert!(verify_snapshot_track_set(&shuffled, &archive).is_ok());
    }

    #[test]
    fn rejects_dropped_track() {
        let tracks: Vec<_> = (0..5).map(track).collect();
        let archive = committed(&tracks);

        assert!(matches!(
            verify_snapshot_track_set(&tracks[..4], &archive),
            Err(SnapshotError::TrackCountMismatch { .. })
        ));
    }

    #[test]
    fn rejects_altered_track() {
        let tracks: Vec<_> = (0..5).map(track).collect();
        let archive = committed(&tracks);

        let mut tampered = tracks.clone();
        tampered[2].value_hash = tape_crypto::hash::hash(b"tampered");

        assert!(matches!(
            verify_snapshot_track_set(&tampered, &archive),
            Err(SnapshotError::RootMismatch)
        ));
    }
}
