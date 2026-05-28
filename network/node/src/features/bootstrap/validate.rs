//! Bootstrap store validation that must pass before live services start.

use store::Store;
use tape_core::tape::{tape_index, tape_namespace, TapeFlags, TapeNamespace};
use tape_core::types::{EpochNumber, TapeNumber};
use tape_crypto::Address;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};
use tape_store::TapeStore;
use tracing::debug;

use crate::core::error::NodeError;

const TRACK_SCAN_BATCH: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapInvariantStats {
    pub tracks_scanned: usize,
    pub system_tracks: usize,
}

pub fn validate_bootstrap_store<Db: Store>(
    store: &TapeStore<Db>,
) -> Result<BootstrapInvariantStats, NodeError> {
    let mut stats = BootstrapInvariantStats::default();
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, TRACK_SCAN_BATCH)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, metadata) in &tracks {
            stats.tracks_scanned += 1;

            let tape = store
                .get_tape(metadata.tape)
                .map_err(store_error)?
                .ok_or_else(|| {
                    violation(
                        *track,
                        metadata.tape,
                        "missing parent tape metadata",
                    )
                })?;

            let object = store
                .get_object_info(*track)
                .map_err(store_error)?
                .ok_or_else(|| {
                    violation(
                        *track,
                        metadata.tape,
                        "missing object metadata",
                    )
                })?;

            validate_object_track_address(*track, metadata.tape, &object)?;
            validate_tape_classification(*track, metadata.tape, &tape, &object, &mut stats)?;
        }

        cursor = tracks.last().map(|(track, _)| *track);
    }

    debug!(
        tracks_scanned = stats.tracks_scanned,
        system_tracks = stats.system_tracks,
        "bootstrap: store invariants validated"
    );

    Ok(stats)
}

fn validate_object_track_address(
    track: Address,
    tape: Address,
    object: &ObjectInfo,
) -> Result<(), NodeError> {
    let object_track = match object {
        ObjectInfo::Valid { track_address, .. }
        | ObjectInfo::System { track_address, .. } => Some(*track_address),
        ObjectInfo::Blacklisted | ObjectInfo::Invalid { .. } => None,
    };

    if let Some(object_track) = object_track {
        if object_track != track {
            return Err(violation(
                track,
                tape,
                format!("object metadata points at track {object_track}"),
            ));
        }
    }

    Ok(())
}

fn validate_tape_classification(
    track: Address,
    tape: Address,
    tape_info: &TapeInfo,
    object: &ObjectInfo,
    stats: &mut BootstrapInvariantStats,
) -> Result<(), NodeError> {
    if TapeFlags::is_system(tape_info.flags) {
        stats.system_tracks += 1;
        let expected = system_object_kind(tape_info.id)
            .map_err(|reason| violation(track, tape, reason))?;

        let ObjectInfo::System { kind, .. } = object else {
            return Err(violation(
                track,
                tape,
                format!(
                    "track on system tape classified as {}",
                    object_classification(object)
                ),
            ));
        };

        if *kind != expected {
            return Err(violation(
                track,
                tape,
                format!("system object kind mismatch: expected {expected:?}, got {kind:?}"),
            ));
        }
    } else if matches!(object, ObjectInfo::System { .. }) {
        return Err(violation(
            track,
            tape,
            "track on user tape classified as system",
        ));
    }

    Ok(())
}

fn system_object_kind(tape_id: TapeNumber) -> Result<SystemObjectKind, String> {
    match tape_namespace(tape_id) {
        Some(TapeNamespace::Snapshot) => Ok(SystemObjectKind::Snapshot {
            epoch: EpochNumber(tape_index(tape_id)),
        }),
        Some(TapeNamespace::History) => Ok(SystemObjectKind::History),
        Some(TapeNamespace::Blacklist) => Ok(SystemObjectKind::Blacklist),
        Some(TapeNamespace::User) => Err(format!("system tape uses user tape id {}", tape_id.0)),
        None => Err(format!("system tape uses unknown tape id {}", tape_id.0)),
    }
}

fn object_classification(object: &ObjectInfo) -> &'static str {
    match object {
        ObjectInfo::Blacklisted => "Blacklisted",
        ObjectInfo::Invalid { .. } => "Invalid",
        ObjectInfo::Valid { .. } => "Valid",
        ObjectInfo::System { .. } => "System",
    }
}

fn violation(track: Address, tape: Address, reason: impl std::fmt::Display) -> NodeError {
    NodeError::Store(format!(
        "bootstrap invariant violation: track {track} tape {tape}: {reason}"
    ))
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::spooler::GroupIndex;
    use tape_core::tape::{snapshot_tape_number, user_tape_number};
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{SlotNumber, StorageUnits, TrackNumber};
    use tape_crypto::{Address, Hash};
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn track_for(tape: Address, track_number: TrackNumber) -> (Address, CompressedTrack) {
        let (track, _) = track_pda(tape, track_number);
        let track = Address::from(track);
        (
            track,
            CompressedTrack {
                tape,
                track_number,
                key: Hash::default(),
                kind: TrackKind::Raw as u64,
                state: TrackState::Certified as u64,
                size: StorageUnits(1),
                group: GroupIndex::from(0),
                value_hash: Hash::default(),
            },
        )
    }

    fn user_tape_info() -> TapeInfo {
        TapeInfo {
            id: user_tape_number(1).unwrap(),
            flags: 0,
            end_epoch: EpochNumber(10),
            next_track_number: TrackNumber(0),
        }
    }

    #[test]
    fn accepts_valid_user_track() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track, metadata) = track_for(tape, TrackNumber(0));

        store.put_tape(tape, user_tape_info()).unwrap();
        store.put_track(track, metadata).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(1),
                    certified_epoch: Some(EpochNumber(2)),
                    slot: SlotNumber(10),
                },
            )
            .unwrap();

        let stats = validate_bootstrap_store(&store).unwrap();
        assert_eq!(stats.tracks_scanned, 1);
        assert_eq!(stats.system_tracks, 0);
    }

    #[test]
    fn rejects_missing_parent_tape() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track, metadata) = track_for(tape, TrackNumber(0));

        store.put_track(track, metadata).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(1),
                    certified_epoch: Some(EpochNumber(2)),
                    slot: SlotNumber(10),
                },
            )
            .unwrap();

        let error = validate_bootstrap_store(&store).unwrap_err();
        assert!(error.to_string().contains("missing parent tape metadata"));
    }

    #[test]
    fn rejects_system_track_marked_valid() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track, metadata) = track_for(tape, TrackNumber(0));

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: snapshot_tape_number(EpochNumber(3)),
                    flags: TapeFlags::SYSTEM,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(0),
                },
            )
            .unwrap();
        store.put_track(track, metadata).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: Some(EpochNumber(3)),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();

        let error = validate_bootstrap_store(&store).unwrap_err();
        assert!(error
            .to_string()
            .contains("track on system tape classified as Valid"));
    }
}
