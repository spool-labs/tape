use core::mem::size_of;
use std::collections::HashSet;

use bytemuck::try_from_bytes;
use store::Store;
use tape_api::program::tapedrive::{blacklist_pda, track_pda};
use tape_core::system::BlacklistEntry;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::types::{ObjectInfo, SystemObjectKind};
use tape_store::TapeStore;

use crate::core::error::NodeError;

const BLACKLIST_SCAN_BATCH: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlacklistEntries {
    Ready(HashSet<BlacklistEntry>),
    Pending { track: Address },
}

pub fn refuses_object<Db: Store>(
    store: &TapeStore<Db>,
    node: Address,
    epoch: EpochNumber,
    track: Address,
    tape: Address,
) -> Result<bool, NodeError> {
    let entries = blacklist_entries_for_node(store, node, epoch)?;
    Ok(entries.contains(&BlacklistEntry::track(track)) || entries.contains(&BlacklistEntry::tape(tape)))
}

pub fn blacklist_entries_for_node<Db: Store>(
    store: &TapeStore<Db>,
    node: Address,
    epoch: EpochNumber,
) -> Result<HashSet<BlacklistEntry>, NodeError> {
    match read_blacklist_entries(store, node, epoch, None)? {
        BlacklistEntries::Ready(entries) => Ok(entries),
        BlacklistEntries::Pending { track } => Err(NodeError::Store(format!(
            "missing blacklist track data for {track}"
        ))),
    }
}

pub fn blacklist_entries_for_assignment<Db: Store>(
    store: &TapeStore<Db>,
    node: Address,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> Result<BlacklistEntries, NodeError> {
    read_blacklist_entries(store, node, target_epoch, Some(voting_epoch))
}

fn read_blacklist_entries<Db: Store>(
    store: &TapeStore<Db>,
    node: Address,
    active_epoch: EpochNumber,
    cutoff_epoch: Option<EpochNumber>,
) -> Result<BlacklistEntries, NodeError> {
    let blacklist = blacklist_pda(node).0;
    let Some(tape) = store
        .get_tape(blacklist)
        .map_err(|error| NodeError::Store(format!("blacklist tape lookup: {error}")))?
    else {
        return Ok(BlacklistEntries::Ready(HashSet::new()));
    };

    if tape.end_epoch <= active_epoch {
        return Ok(BlacklistEntries::Ready(HashSet::new()));
    }

    let mut cursor = None;
    let mut entries = HashSet::new();
    loop {
        let tracks = store
            .iter_tracks_by_tape_from(blacklist, cursor, BLACKLIST_SCAN_BATCH)
            .map_err(|error| NodeError::Store(format!("blacklist track scan: {error}")))?;

        if tracks.is_empty() {
            break;
        }

        for track in &tracks {
            let track_address = track_pda(blacklist, track.track_number).0;
            if let Some(cutoff_epoch) = cutoff_epoch {
                if !blacklist_track_is_before_cutoff(store, track_address, cutoff_epoch)? {
                    continue;
                }
            }

            let data = store
                .get_track_data(track_address)
                .map_err(|error| NodeError::Store(format!("blacklist track data lookup: {error}")))?;
            let Some(data) = data else {
                return Ok(BlacklistEntries::Pending {
                    track: track_address,
                });
            };

            let entry = decode_blacklist_entry(track_address, blacklist, track, &data)?;
            entries.insert(entry);
        }

        cursor = tracks.last().map(|track| track.track_number);
    }

    Ok(BlacklistEntries::Ready(entries))
}

pub fn decode_blacklist_entry(
    track_address: Address,
    blacklist: Address,
    track: &CompressedTrack,
    data: &BlobData,
) -> Result<BlacklistEntry, NodeError> {
    if track.tape != blacklist || !track.is_inline() || !track.is_certified() {
        return Err(NodeError::Store(format!(
            "invalid blacklist track metadata for {track_address}"
        )));
    }

    let expected_size = StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64);
    if track.size != expected_size {
        return Err(NodeError::Store(format!(
            "invalid blacklist track size {} for {track_address}",
            track.size.0
        )));
    }

    let BlobData::Inline(bytes) = data else {
        return Err(NodeError::Store(format!("invalid blacklist track data for {track_address}")));
    };

    let entry = try_from_bytes::<BlacklistEntry>(&bytes).map_err(|_| {
        NodeError::Store(format!("invalid blacklist entry data for {track_address}"))
    })?;
    if !entry.is_valid() || track.key != entry.key() || track.value_hash != entry.key() {
        return Err(NodeError::Store(format!(
            "invalid blacklist entry hash for {track_address}"
        )));
    }

    Ok(*entry)
}

fn blacklist_track_is_before_cutoff<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    cutoff_epoch: EpochNumber,
) -> Result<bool, NodeError> {
    let Some(info) = store
        .get_object_info(track)
        .map_err(|error| NodeError::Store(format!("blacklist object info lookup: {error}")))?
    else {
        return Err(NodeError::Store(format!(
            "missing blacklist object info for {track}"
        )));
    };

    let (track_address, registered_epoch, certified_epoch) = match info {
        ObjectInfo::Valid {
            track_address,
            registered_epoch,
            certified_epoch,
            ..
        }
        | ObjectInfo::System {
            kind: SystemObjectKind::Blacklist,
            track_address,
            registered_epoch,
            certified_epoch,
            ..
        } => (track_address, registered_epoch, certified_epoch),
        _ => {
            return Err(NodeError::Store(format!(
                "invalid blacklist object info for {track}"
            )))
        }
    };

    if track_address != track {
        return Err(NodeError::Store(format!(
            "blacklist object info points at a different track for {track}"
        )));
    }

    let Some(certified_epoch) = certified_epoch else {
        return Err(NodeError::Store(format!(
            "blacklist track is not certified for {track}"
        )));
    };

    Ok(registered_epoch < cutoff_epoch && certified_epoch < cutoff_epoch)
}
