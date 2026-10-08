//! Bootstrap store validation that must pass before live services start.

use tape_store_api::Store;
use tape_core::tape::{tape_index, tape_namespace, TapeFlags, TapeNamespace};
use tape_core::types::{EpochNumber, TapeNumber};
use tape_crypto::Address;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps, track_has_left};
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
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), TRACK_SCAN_BATCH)
            .map_err(store_error)?;

        let mut addresses: Vec<Address> = Vec::with_capacity(tracks.len());
        let mut parents: Vec<Address> = Vec::with_capacity(tracks.len());
        for (track, metadata) in &tracks {
            addresses.push(*track);
            parents.push(metadata.tape);
        }
        // The page's two reads once each. A validate walks every track there is,
        // so a round trip a row is the whole cost of the pass.
        let tapes = store.get_tapes(&parents).map_err(store_error)?;
        let objects = store.get_object_infos(&addresses).map_err(store_error)?;

        for (((track, metadata), held_tape), held_object) in
            tracks.iter().zip(tapes).zip(objects)
        {
            stats.tracks_scanned += 1;

            let object = held_object
                .ok_or_else(|| {
                    violation(
                        *track,
                        metadata.tape,
                        "missing object metadata",
                    )
                })?;

            validate_object_track_address(*track, metadata.tape, &object)?;

            // A track that has left its tape waits for the sweep. The row at
            // its tape's address is gone once the tape is closed, and is
            // another tape's once the address is reserved again.
            let tape = match held_tape {
                Some(tape) if !track_has_left(Some(&tape), metadata) => tape,
                Some(_) | None => continue,
            };

            validate_tape_classification(*track, metadata.tape, &tape, &object, &mut stats)?;
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
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
        | ObjectInfo::System { track_address, .. }
        | ObjectInfo::Deleted { track_address, .. } => Some(*track_address),
        ObjectInfo::Invalid {
            counted: Some(counted),
            ..
        } => Some(counted.track_address),
        ObjectInfo::Blacklisted | ObjectInfo::Invalid { counted: None, .. } => None,
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

        // A deleted blacklist entry keeps its kind until the epoch cleanup removes it.
        let (ObjectInfo::System { kind, .. } | ObjectInfo::Deleted { kind: Some(kind), .. }) =
            object
        else {
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
    } else if matches!(
        object,
        ObjectInfo::System { .. } | ObjectInfo::Deleted { kind: Some(_), .. }
    ) {
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
        ObjectInfo::Deleted { .. } => "Deleted",
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
    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::{blacklist_tape_number, snapshot_tape_number, user_tape_number};
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{NodeId, SlotNumber, StorageUnits, TrackNumber};
    use tape_crypto::{Address, Hash};
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn track_for(tape: Address, track_number: TrackNumber) -> (Address, CompressedTrack) {
        track_on(tape, user_tape_info().id, track_number)
    }

    fn track_on(
        tape: Address,
        tape_number: TapeNumber,
        track_number: TrackNumber,
    ) -> (Address, CompressedTrack) {
        let track = CompressedTrack {
            tape,
            tape_number,
            track_number,
            key: Hash::default(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits(1),
            group: VirtualGroup::from(0),
            value_hash: Hash::default(),
        };
        (track.address(), track)
    }

    fn user_tape_info() -> TapeInfo {
        TapeInfo {
            id: user_tape_number(1).unwrap(),
            flags: 0,
            end_epoch: EpochNumber(10),
            next_track_number: TrackNumber(0),
            tree_start: TrackNumber(0),
            authority: Address::default(),
            delegate: Address::default(),
            nonce: Hash::default(),
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

    // a track of a closed tape has no tape row, and a track from before a
    // rewind sits below the tree's start. Both wait for the sweep and pass.
    #[test]
    fn accepts_tracks_that_left() {
        let store = test_store();
        let closed = Address::new_unique();
        let rewound = Address::new_unique();
        store
            .put_tape(rewound, TapeInfo { tree_start: TrackNumber(1), ..user_tape_info() })
            .unwrap();

        for tape in [closed, rewound] {
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
        }

        let stats = validate_bootstrap_store(&store).unwrap();
        assert_eq!(stats.tracks_scanned, 2);
    }

    #[test]
    fn rejects_system_track_marked_valid() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track, metadata) =
            track_on(tape, snapshot_tape_number(EpochNumber(3)), TrackNumber(0));

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: snapshot_tape_number(EpochNumber(3)),
                    flags: TapeFlags::SYSTEM,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(0),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
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

    fn deleted_object(track: Address, kind: Option<SystemObjectKind>) -> ObjectInfo {
        ObjectInfo::Deleted {
            kind,
            track_address: track,
            registered_epoch: EpochNumber(1),
            certified_epoch: EpochNumber(2),
            deleted_epoch: EpochNumber(5),
        }
    }

    fn blacklist_tape_info() -> TapeInfo {
        TapeInfo {
            id: blacklist_tape_number(NodeId(3)),
            flags: TapeFlags::SYSTEM,
            end_epoch: EpochNumber(u64::MAX),
            next_track_number: TrackNumber(0),
            tree_start: TrackNumber(0),
            authority: Address::default(),
            delegate: Address::default(),
            nonce: Hash::default(),
        }
    }

    // a store holding a deleted user track and a removed blacklist entry starts
    #[test]
    fn accepts_deleted_tracks() {
        let store = test_store();
        let user_tape = Address::new_unique();
        let blacklist = Address::new_unique();
        let (track, metadata) = track_for(user_tape, TrackNumber(0));
        let (entry, entry_metadata) =
            track_on(blacklist, blacklist_tape_info().id, TrackNumber(0));

        store.put_tape(user_tape, user_tape_info()).unwrap();
        store.put_tape(blacklist, blacklist_tape_info()).unwrap();
        store.put_track(track, metadata).unwrap();
        store.put_track(entry, entry_metadata).unwrap();
        store.put_object_info(track, deleted_object(track, None)).unwrap();
        store
            .put_object_info(entry, deleted_object(entry, Some(SystemObjectKind::Blacklist)))
            .unwrap();

        let stats = validate_bootstrap_store(&store).unwrap();
        assert_eq!(stats.tracks_scanned, 2);
        assert_eq!(stats.system_tracks, 1);
    }

    // a deleted track with no system kind has no place on a system tape
    #[test]
    fn rejects_deleted_user_track_on_system_tape() {
        let store = test_store();
        let blacklist = Address::new_unique();
        let (entry, entry_metadata) =
            track_on(blacklist, blacklist_tape_info().id, TrackNumber(0));

        store.put_tape(blacklist, blacklist_tape_info()).unwrap();
        store.put_track(entry, entry_metadata).unwrap();
        store.put_object_info(entry, deleted_object(entry, None)).unwrap();

        let error = validate_bootstrap_store(&store).unwrap_err();
        assert!(error
            .to_string()
            .contains("track on system tape classified as Deleted"));
    }
}
