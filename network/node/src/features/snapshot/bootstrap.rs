//! Snapshot bootstrap — download, outer-decode, replay for fast node catch-up.
//!
//! New or lagging nodes can download epoch snapshots instead of replaying
//! all Solana blocks from genesis. The bootstrap process:
//! 1. Read `SnapshotState` from chain (head pointer, latest_epoch)
//! 2. Walk the linked list of snapshot tracks backward
//! 3. Download k_outer certified chunks (each from k_inner=7 slices)
//! 4. Outer-RS-decode to get the serialized SnapshotLog
//! 5. Replay events through block processor handlers

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_core::erasure::{group_start, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::{ReplayableEvent, SnapshotLog};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_node_client::NodeClientBuilder;
use tape_slicer::{ClayCoder, ClayParams, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER};
use tape_store::ops::CommitteeOps;
use tape_store::types::Pubkey as StorePubkey;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::context::NodeContext;
use crate::features::block_processing::{
    handle_advance_epoch, handle_certify_track, handle_delete_track, handle_destroy_tape,
    handle_invalidate_track, handle_register_track, handle_reserve_tape,
};

use super::builder::SnapshotError;

/// Max concurrent downloads per spool group.
const DOWNLOAD_CONCURRENCY: usize = 10;

/// Download a snapshot for the given epoch from network peers.
///
/// For each of the 50 spool groups, downloads k_inner slices from peers,
/// inner-Clay-decodes to recover the chunk, then outer-RS-decodes all
/// k_outer chunks to get the SnapshotLog.
pub async fn download_snapshot<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    epoch: EpochNumber,
    tracks: &[(SpoolGroup, Pubkey)],
) -> Result<SnapshotLog, SnapshotError> {
    info!(epoch = epoch.as_u64(), "Downloading snapshot");

    let committee = ctx
        .storage
        .store
        .get_committee(ctx.control_plane.current_epoch())
        .map_err(SnapshotError::Store)?
        .ok_or_else(|| SnapshotError::Decode("no committee found".into()))?;

    let insecure = ctx.config.insecure;
    let clay_params = ClayParams::default();
    let k_inner = clay_params.k() as usize;

    // Build group_idx → track address map from pre-resolved addresses
    let track_map: HashMap<SpoolGroup, Pubkey> = tracks
        .iter()
        .copied()
        .collect();

    // For each spool group, try to download k_inner slices and decode the chunk
    let mut decoded_chunks: Vec<(usize, Vec<u8>)> = Vec::new();

    for group_idx in 0..SPOOL_GROUP_COUNT {
        let track_addr = match track_map.get(&(group_idx as SpoolGroup)) {
            Some(addr) => *addr,
            None => {
                debug!(group = group_idx, "No track address for group, skipping");
                continue;
            }
        };
        let track_id = StorePubkey::from(track_addr).to_string();
        let start = group_start(group_idx as u64);

        // Build spool → client mapping for this group
        let mut available: Vec<(usize, u16, tape_node_client::NodeClient)> = Vec::new();
        for position in 0..SPOOL_GROUP_SIZE {
            let spool_idx = start + position as u16;
            // Find which committee member owns this spool
            let member = committee.iter().find(|m| m.spools.contains(&spool_idx));
            if let Some(member) = member {
                let addr = match member.network_address.to_socket_addr() {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                match NodeClientBuilder::new()
                    .accept_invalid_certs(insecure)
                    .build(&addr.to_string())
                {
                    Ok(client) => available.push((position, spool_idx, client)),
                    Err(_) => continue,
                }
            }
        }

        if available.len() < k_inner {
            debug!(
                group = group_idx,
                available = available.len(),
                needed = k_inner,
                "Not enough peers for this group, skipping"
            );
            continue;
        }

        // Download slices concurrently with early exit
        let collected_count = Arc::new(AtomicUsize::new(0));
        let k = k_inner;

        let results: Vec<(usize, Result<Vec<u8>, String>)> =
            stream::iter(available.into_iter())
                .map(|(position, spool_idx, client)| {
                    let tid = track_id.clone();
                    let collected = Arc::clone(&collected_count);
                    async move {
                        if collected.load(Ordering::Relaxed) >= k {
                            return (position, Err("skipped".into()));
                        }
                        let result = client
                            .get_slice(&tid, spool_idx)
                            .await
                            .map_err(|e| e.to_string());
                        if result.is_ok() {
                            collected.fetch_add(1, Ordering::Relaxed);
                        }
                        (position, result)
                    }
                })
                .buffer_unordered(DOWNLOAD_CONCURRENCY)
                .collect()
                .await;

        let mut slices: Vec<(usize, Vec<u8>)> = Vec::new();
        for (position, result) in results {
            if let Ok(data) = result {
                slices.push((position, data));
                if slices.len() >= k_inner {
                    break;
                }
            }
        }

        if slices.len() < k_inner {
            debug!(
                group = group_idx,
                collected = slices.len(),
                needed = k_inner,
                "Not enough slices for inner decode, skipping group"
            );
            continue;
        }

        // Inner Clay decode to recover the chunk
        let decoded = tokio::task::spawn_blocking(move || {
            let coder = ClayCoder::from_params(clay_params);
            let mut slicer = Slicer::new(coder);
            let chunks: Vec<(usize, &[u8])> =
                slices.iter().map(|(pos, data)| (*pos, data.as_slice())).collect();
            slicer
                .decode(&chunks)
                .map_err(|e| SnapshotError::Decode(format!("inner decode group {}: {:?}", group_idx, e)))
        })
        .await
        .map_err(|e| SnapshotError::Decode(format!("spawn_blocking: {}", e)))??;

        decoded_chunks.push((group_idx, decoded));
        debug!(
            group = group_idx,
            total = decoded_chunks.len(),
            needed = DEFAULT_K_OUTER,
            "Decoded snapshot chunk"
        );

        if decoded_chunks.len() >= DEFAULT_K_OUTER {
            info!(
                chunks = decoded_chunks.len(),
                "Collected enough chunks for outer decode"
            );
            break;
        }
    }

    if decoded_chunks.len() < DEFAULT_K_OUTER {
        return Err(SnapshotError::Decode(format!(
            "not enough chunks: got {}, need {}",
            decoded_chunks.len(),
            DEFAULT_K_OUTER,
        )));
    }

    // Outer RS decode
    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let refs: Vec<(usize, &[u8])> = decoded_chunks
        .iter()
        .map(|(idx, data)| (*idx, data.as_slice()))
        .collect();

    let serialized = outer
        .decode(&refs)
        .map_err(|e| SnapshotError::Decode(format!("outer decode: {:?}", e)))?;

    // Deserialize SnapshotLog
    let log: SnapshotLog = wincode::deserialize(&serialized)
        .map_err(|e| SnapshotError::Serialization(e.to_string()))?;

    info!(
        epoch = log.epoch.as_u64(),
        entries = log.entries.len(),
        "Snapshot downloaded and decoded"
    );

    Ok(log)
}

/// Replay a snapshot log through block processor handlers.
///
/// For each SnapshotEntry in order, dispatches each ReplayableEvent
/// to the corresponding handler — same code path as live block processing.
pub async fn replay_snapshot<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    log: &SnapshotLog,
) -> Result<(), SnapshotError> {
    info!(
        epoch = log.epoch.as_u64(),
        entries = log.entries.len(),
        "Replaying snapshot"
    );

    for entry in &log.entries {
        for event in &entry.events {
            if let Err(e) = replay_event(ctx, event) {
                warn!(
                    slot = entry.slot.as_u64(),
                    error = %e,
                    "Failed to replay event"
                );
            }
        }

        // Update sync cursor after each entry
        ctx.control_plane.set_last_processed_slot(entry.slot);
    }

    info!(
        epoch = log.epoch.as_u64(),
        "Snapshot replay complete"
    );

    Ok(())
}

/// Replay a single event through block processor handlers.
fn replay_event<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    event: &ReplayableEvent,
) -> Result<(), SnapshotError> {
    match event {
        ReplayableEvent::RegisterTrack { track, event_data } => {
            let track_registered =
                bytemuck::try_from_bytes::<tape_api::event::TrackRegistered>(event_data)
                    .map_err(|e| SnapshotError::Decode(format!("TrackRegistered parse: {}", e)))?;
            handle_register_track(&ctx.storage.store, *track, track_registered)
                .map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::CertifyTrack { track, epoch } => {
            handle_certify_track(
                &ctx.storage.store,
                StorePubkey(*track),
                *epoch,
            )
            .map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::DeleteTrack { track, epoch } => {
            handle_delete_track(&ctx.storage.store, *track, *epoch).map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::InvalidateTrack { track, epoch } => {
            let owned_spools = ctx.control_plane.get_our_spools();
            handle_invalidate_track(&ctx.storage.store, *track, *epoch, &owned_spools)
                .map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::AdvanceEpoch {
            old_epoch,
            new_epoch,
        } => {
            ctx.control_plane.set_current_epoch(*new_epoch);
            ctx.control_plane.start_epoch_sync(*new_epoch);
            handle_advance_epoch(&ctx.storage.store, *old_epoch, *new_epoch)
                .map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::SyncEpoch {
            node_id, epoch, ..
        } => {
            let system = ctx.control_plane.get_system();
            let spool_count = match system.committee.index_of(node_id) {
                Some(idx) => system.spools.weight(idx) as u64,
                None => 0,
            };
            ctx.control_plane
                .record_node_sync(*epoch, *node_id, spool_count);
        }

        ReplayableEvent::ReserveTape {
            tape,
            authority,
            active_epoch,
            expiry_epoch,
        } => {
            handle_reserve_tape(
                &ctx.storage.store,
                *tape,
                *authority,
                *active_epoch,
                *expiry_epoch,
            )
            .map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::DestroyTape { tape, epoch } => {
            handle_destroy_tape(&ctx.storage.store, *tape, *epoch).map_err(SnapshotError::Store)?;
        }

        ReplayableEvent::RegisterNode { .. } | ReplayableEvent::JoinNetwork { .. } => {
            // No-ops in the block processor, no local state to update
        }
    }

    Ok(())
}

/// Full bootstrap orchestration: download and replay snapshots for all
/// missing epochs.
///
/// 1. Read SnapshotState from chain (one RPC call)
/// 2. Pause the block processor
/// 3. For each epoch (oldest to newest): download + replay
/// 4. Resume the block processor
pub async fn bootstrap_from_snapshots<S: Store>(
    ctx: Arc<NodeContext<S>>,
    cancel: CancellationToken,
) -> Result<(), SnapshotError> {
    let snapshot_state = ctx.rpc.get_snapshot_state().await
        .map_err(|e| SnapshotError::Decode(format!("failed to fetch SnapshotState: {e}")))?;

    let latest_snapshot = snapshot_state.latest_epoch;

    if latest_snapshot.as_u64() == 0 {
        info!("No snapshots available on-chain, skipping bootstrap");
        return Ok(());
    }

    let current = ctx.control_plane.current_epoch();
    if current >= latest_snapshot {
        info!(
            current = current.as_u64(),
            latest_snapshot = latest_snapshot.as_u64(),
            "Already caught up, no bootstrap needed"
        );
        return Ok(());
    }

    info!(
        from = current.as_u64(),
        to = latest_snapshot.as_u64(),
        "Starting snapshot bootstrap"
    );

    // Walk the on-chain linked list from head backward to collect track addresses
    let mut track_addr = snapshot_state.head;
    let mut tracks_by_epoch: BTreeMap<EpochNumber, Vec<(SpoolGroup, Pubkey)>> = BTreeMap::new();

    while track_addr != Pubkey::default() {
        let track = ctx.rpc.get_track_by_address(&track_addr).await
            .map_err(|e| SnapshotError::Decode(format!("fetch track {}: {e}", track_addr)))?;

        let epoch = track.data.registered_epoch;
        let group = track.data.spool_group;

        if epoch > current && epoch <= latest_snapshot {
            tracks_by_epoch.entry(epoch).or_default().push((group, track_addr));
        }

        // Follow back-pointer (track.key stores previous head address as Hash)
        track_addr = Pubkey::new_from_array(track.key.0);

        if epoch <= current { break; }
    }

    info!(
        epochs = tracks_by_epoch.len(),
        "Collected snapshot tracks from linked list"
    );

    // Pause block processor to prevent concurrent state modification
    ctx.control_plane.request_block_processor_pause().await;
    info!("Block processor paused for snapshot bootstrap");

    // Download and replay each epoch using resolved addresses
    for (epoch, tracks) in &tracks_by_epoch {
        if cancel.is_cancelled() {
            info!("Snapshot bootstrap cancelled");
            break;
        }
        info!(epoch = epoch.as_u64(), "Bootstrapping epoch from snapshot");

        match download_snapshot(&ctx, *epoch, tracks).await {
            Ok(log) => {
                if let Err(e) = replay_snapshot(&ctx, &log).await {
                    warn!(epoch = epoch.as_u64(), error = %e, "Snapshot replay failed");
                    break;
                }
                ctx.control_plane.set_current_epoch(*epoch);
                info!(epoch = epoch.as_u64(), "Epoch bootstrap complete");
            }
            Err(e) => {
                warn!(epoch = epoch.as_u64(), error = %e, "Snapshot download failed");
                break;
            }
        }
    }

    // Resume block processor
    ctx.control_plane.resume_block_processor();
    info!("Block processor resumed after snapshot bootstrap");

    Ok(())
}
