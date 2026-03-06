//! SpoolRecovery — restore missing slices via Clay repair or full recovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::network::NetworkAddress;
use tape_protocol::api::{RepairRequest, StripeSubChunkRequest};
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_slicer::{ClayCoder, ErasureCoder, RepairPlan, Slicer, SliceIndex, SliceMetadata};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{NodeInfo, Pubkey as StorePubkey, SpoolState, SpoolStatus, TrackInfo};
use tokio_util::sync::CancellationToken;

use crate::core::validate_slice_entry;
use crate::core::NodeContext;
use crate::core::PeerHandle;
use crate::TaskOutcome;

const RECOVERY_BATCH_SIZE: usize = 10;

enum TrackResult {
    Recovered,
    AlreadyPresent,
    NeedsFull(TrackInfo),
    Failed,
}

/// Peer map + cooldown handle for a spool group, built once per recovery task.
struct GroupPeers {
    handle: PeerHandle,
    map: HashMap<SpoolIndex, NetworkAddress>,
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    peer_handle: PeerHandle,
    spool: SpoolIndex,
    cancel: CancellationToken,
) -> TaskOutcome {
    let chain = ctx.chain_state.load();

    if chain.epoch.is_zero() {
        return TaskOutcome::Success;
    }

    if chain.epoch.is_one() {
        return TaskOutcome::Success;
    }

    let committee = chain.committee_prev.clone();
    let spool_group = SpoolGroup::of(spool);
    let peers = GroupPeers {
        handle: peer_handle,
        map: build_peer_map(&committee, spool, spool_group),
    };

    let mut any_failed = false;

    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let pending = match ctx.store
            .iter_pending_recoveries(spool, RECOVERY_BATCH_SIZE) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
        };

        if pending.is_empty() {
            break;
        }

        let mut recovered: Vec<StorePubkey> = Vec::new();
        let mut needs_full: Vec<(StorePubkey, TrackInfo)> = Vec::new();

        // Pass 1: attempt Clay repair for each track.
        for track_addr in &pending {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            match try_clay_repair(&ctx, spool, &peers, *track_addr).await {
                TrackResult::Recovered => recovered.push(*track_addr),
                TrackResult::AlreadyPresent => recovered.push(*track_addr),
                TrackResult::NeedsFull(info) => needs_full.push((*track_addr, info)),
                TrackResult::Failed => { any_failed = true; }
            }
        }

        // Pass 2: full recovery for tracks where Clay repair was insufficient.
        for (track_addr, track_info) in &needs_full {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            let profile = track_info.profile();
            let coder = ClayCoder::from_params(profile.clay_params());
            let mut slicer = Slicer::with_profile(
                coder,
                track_info.stripe_size as usize,
                true,
                profile,
            );

            match recover_from_peers(
                &ctx, spool, &peers, *track_addr, &mut slicer,
            ).await {
                Ok(data) => {
                    match persist_recovered_slice(&ctx, spool, *track_addr, track_info, data) {
                        Ok(()) => {
                            tracing::debug!(?track_addr, spool, "recovered slice via full fallback");
                            recovered.push(*track_addr);
                        }
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "persist after full recovery: {e}");
                            any_failed = true;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "full recovery failed: {e}");
                    any_failed = true;
                }
            }
        }

        // Batch remove all recovered/already-present entries.
        for addr in &recovered {
            if let Err(e) = ctx.store.remove_pending_recovery(spool, *addr) {
                tracing::warn!(?addr, spool, "remove pending recovery: {e}");
            }
        }

        if recovered.is_empty() {
            break;
        }
    }

    if any_failed {
        TaskOutcome::Retryable("some tracks could not be recovered".into())
    } else {
        let scan_done = match ctx.store.is_scan_done(spool) {
            Ok(done) => done,
            Err(e) => return TaskOutcome::Retryable(format!("read scan_done: {e}")),
        };
        if !scan_done {
            return TaskOutcome::Pending(Duration::from_secs(5));
        }
        match ctx.store.get_spool_state(spool) {
            Ok(Some(state)) if state.is_recovering() => {
                let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch };
                if let Err(e) = ctx.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool active: {e}"));
                }
                let _ = ctx.store.clear_scan_done(spool);
                tracing::info!(spool, "spool recovery complete, marked active");
            }
            Ok(_) => {}
            Err(e) => return TaskOutcome::Retryable(format!("get_spool_state: {e}")),
        }
        TaskOutcome::Success
    }
}

/// Build a map from SpoolIndex → peer NetworkAddress for a spool group.
fn build_peer_map(
    committee: &[NodeInfo],
    our_spool: SpoolIndex,
    spool_group: SpoolGroup,
) -> HashMap<SpoolIndex, NetworkAddress> {
    let mut peer_map: HashMap<SpoolIndex, NetworkAddress> = HashMap::new();
    for node in committee.iter() {
        for &s in &node.spools {
            if s == our_spool || !spool_group.contains(s) {
                continue;
            }
            peer_map.entry(s).or_insert(node.network_address);
        }
    }
    peer_map
}

/// Attempt Clay repair for a single track. Returns the outcome.
async fn try_clay_repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    peers: &GroupPeers,
    track_addr: StorePubkey,
) -> TrackResult {
    // Get track metadata.
    let track_info = match ctx.store.get_track(track_addr) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!(?track_addr, "track missing from local store");
            return TrackResult::Failed;
        }
        Err(e) => {
            tracing::warn!(?track_addr, "get_track error: {e}");
            return TrackResult::Failed;
        }
    };

    // Already have the slice?
    match ctx.store.get_slice(spool, track_addr) {
        Ok(Some(_)) => return TrackResult::AlreadyPresent,
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(?track_addr, spool, "get_slice error: {e}");
            return TrackResult::Failed;
        }
    }

    // Validate encoding.
    let profile = track_info.profile();
    if !profile.is_clay() {
        tracing::warn!(?track_addr, spool, "repair only supported for clay encoding");
        return TrackResult::Failed;
    }

    if track_info.stripe_count == 0 || track_info.stripe_size == 0 {
        tracing::warn!(?track_addr, spool, "invalid stripe parameters");
        return TrackResult::Failed;
    }

    // Derive lost index from spool position in group.
    let spool_group = SpoolGroup::of(spool);
    debug_assert_eq!(track_info.spool_group, spool_group);
    let lost = SliceIndex::new(spool_group.slice_of(spool).unwrap());

    // Build available slice indices from peer map.
    let available: Vec<SliceIndex> = peers.map.keys()
        .map(|&s| SliceIndex::new(spool_group.slice_of(s).unwrap()))
        .collect();
    if available.is_empty() {
        tracing::warn!(?track_addr, spool, "no peers found for repair");
        return TrackResult::Failed;
    }

    // Build slicer + repair plan.
    let coder = ClayCoder::from_params(profile.clay_params());
    let slicer = Slicer::with_profile(
        coder,
        track_info.stripe_size as usize,
        true,
        profile,
    );

    let plan = match slicer.repair_plan_from_params(
        lost,
        &available,
        track_info.original_size as usize,
        track_info.stripe_size as usize,
    ) {
        Ok(p) => p,
        Err(_) => return TrackResult::NeedsFull(track_info),
    };

    // Build per-helper requests and send them.
    let per_helper = build_per_helper_requests(&plan, spool_group);

    let mut required: Vec<SliceIndex> = Vec::new();
    for stripe_repair in &plan.stripes {
        for hp in &stripe_repair.helpers {
            if !required.contains(&hp.slice) {
                required.push(hp.slice);
            }
        }
    }

    let mut helper_data: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
    for (slice_idx, request) in &per_helper {
        let peer_spool = spool_group.spool_at(**slice_idx);
        let peer_addr = match peers.map.get(&peer_spool) {
            Some(addr) => *addr,
            None => continue,
        };

        let addr = match peer_addr.to_socket_addr() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(?track_addr, "parse peer address: {e}");
                continue;
            }
        };

        match peers.handle.is_cooling_down(addr).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(?track_addr, spool, "peer tracker unavailable: {e}");
                continue;
            }
        }

        let client = match NodeClientBuilder::new().build(&addr.to_string()) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(?track_addr, "build peer client: {e}");
                continue;
            }
        };

        match with_retry(&RetryConfig::three(), || {
            client.request_repair(track_addr, request)
        })
        .await
        {
            Ok(data) if !data.is_empty() => {
                ctx.stats.add_repair_received(data.len() as u64);
                if let Err(e) = peers.handle.record_success(addr).await {
                    tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                }
                helper_data.insert(*slice_idx, data);
            }
            Ok(_) => {
                if let Err(e) = peers.handle.record_success(addr).await {
                    tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                }
                tracing::debug!(?track_addr, spool, peer = ?peer_addr, "empty repair response");
            }
            Err(e) => {
                if let Err(err) = peers.handle.record_failure(addr).await {
                    tracing::warn!(?track_addr, spool, "failed to record peer failure for {addr}: {err}");
                }
                tracing::debug!(?track_addr, spool, peer = ?peer_addr, "repair error: {e}");
            }
        }
    }

    // Insufficient helpers → fall back to full recovery.
    if !required.iter().all(|si| helper_data.contains_key(si)) {
        return TrackResult::NeedsFull(track_info);
    }

    // Run Clay repair.
    let metadata = SliceMetadata::with_profile(
        track_info.original_size as usize,
        track_info.stripe_size as usize,
        profile,
    );
    let metadata_bytes = metadata.to_bytes();

    let repaired = match slicer.repair(&plan, &helper_data, &metadata_bytes) {
        Ok(data) => data,
        Err(_) => return TrackResult::NeedsFull(track_info),
    };

    // Persist the repaired slice.
    match persist_recovered_slice(ctx, spool, track_addr, &track_info, repaired) {
        Ok(()) => {
            tracing::debug!(?track_addr, spool, "repaired slice via clay");
            TrackResult::Recovered
        }
        Err(_) => TrackResult::NeedsFull(track_info),
    }
}

async fn recover_from_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    peers: &GroupPeers,
    track_addr: StorePubkey,
    slicer: &mut Slicer<ClayCoder>,
) -> Result<Vec<u8>, String> {
    let spool_group = SpoolGroup::of(spool);
    let lost = SliceIndex::new(spool_group.slice_of(spool).unwrap());
    let needed = slicer.k();

    let mut peer_spools: Vec<SpoolIndex> = peers.map.keys().copied().collect();
    peer_spools.sort_unstable();

    let mut full_slices: Vec<(SliceIndex, Vec<u8>)> = Vec::with_capacity(needed);
    for peer_spool in peer_spools {
        if full_slices.len() >= needed {
            break;
        }

        let peer_addr = match peers.map.get(&peer_spool) {
            Some(addr) => *addr,
            None => continue,
        };
        let addr = match peer_addr.to_socket_addr() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(?track_addr, "parse peer address: {e}");
                continue;
            }
        };

        match peers.handle.is_cooling_down(addr).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(?track_addr, "peer tracker unavailable: {e}");
                continue;
            }
        }

        let client = match NodeClientBuilder::new().build(&addr.to_string()) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(?track_addr, "build peer client: {e}");
                continue;
            }
        };

        match with_retry(&RetryConfig::three(), || {
            client.get_slice(track_addr, peer_spool)
        })
        .await
        {
            Ok(data) if !data.is_empty() => {
                ctx.stats.add_recovery_received(data.len() as u64);
                if let Err(e) = peers.handle.record_success(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer success for {addr}: {e}");
                }
                let slice_idx = SliceIndex::new(spool_group.slice_of(peer_spool).unwrap());
                full_slices.push((slice_idx, data));
            }
            Ok(_) => {
                if let Err(e) = peers.handle.record_success(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer success for {addr}: {e}");
                }
                tracing::debug!(?track_addr, peer = ?peer_addr, "empty full-slice response");
            }
            Err(e) => {
                if let Err(err) = peers.handle.record_failure(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer failure for {addr}: {err}");
                }
                tracing::debug!(?track_addr, peer = ?peer_addr, "full-slice fetch error: {e}");
            }
        }
    }

    if full_slices.len() < needed {
        return Err(format!(
            "insufficient full slices for fallback: got {} need {}",
            full_slices.len(),
            needed,
        ));
    }

    reconstruct_slice(slicer, lost, &full_slices)
}

fn reconstruct_slice(
    slicer: &mut Slicer<ClayCoder>,
    lost: SliceIndex,
    peer_slices: &[(SliceIndex, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    let Some((_, sample)) = peer_slices.first() else {
        return Err("no peer slices provided".into());
    };

    let metadata = SliceMetadata::from_slice(sample)
        .map_err(|e| format!("parse peer metadata failed: {e}"))?;
    slicer.set_chunk_index(metadata.chunk_index);

    let slice_refs: Vec<(usize, &[u8])> = peer_slices
        .iter()
        .map(|(idx, data)| (**idx, data.as_slice()))
        .collect();

    let decoded = slicer
        .decode(&slice_refs)
        .map_err(|e| format!("decode fallback failed: {e}"))?;
    let reencoded = slicer
        .encode(&decoded)
        .map_err(|e| format!("re-encode fallback failed: {e}"))?;
    reencoded
        .get(*lost)
        .cloned()
        .ok_or_else(|| format!("lost slice index {} out of bounds", *lost))
}

fn persist_recovered_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    track_addr: StorePubkey,
    track_info: &TrackInfo,
    recovered: Vec<u8>,
) -> Result<(), String> {
    validate_slice_entry(spool, track_info, &recovered)
        .map_err(|reason| format!("slice validation failed: {reason}"))?;
    ctx.store
        .put_slice(spool, track_addr, recovered)
        .map_err(|e| format!("put_slice error: {e}"))?;
    Ok(())
}


/// Invert a `RepairPlan` (per-stripe, per-helper) into per-helper `RepairRequest`s.
/// Each helper gets only its specific sub-chunks across all stripes.
fn build_per_helper_requests(
    plan: &RepairPlan,
    spool_group: SpoolGroup,
) -> HashMap<SliceIndex, RepairRequest> {
    let mut map: HashMap<SliceIndex, Vec<StripeSubChunkRequest>> = HashMap::new();
    for stripe_repair in &plan.stripes {
        for hp in &stripe_repair.helpers {
            map.entry(hp.slice)
                .or_default()
                .push(StripeSubChunkRequest {
                    stripe: stripe_repair.stripe,
                    sub_chunks: hp.sub_chunks.clone(),
                });
        }
    }

    map.into_iter()
        .map(|(slice_idx, stripes)| {
            let helper_spool = spool_group.spool_at(*slice_idx);
            (slice_idx, RepairRequest { helper_spool, stripes })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::EpochNumber;
    use tape_core::system::EpochPhase;
    use tape_store::types::TrackInfo;
    use tokio_util::sync::CancellationToken;

    use crate::state::ChainState;
    use crate::core::test_utils::test_context;

    #[tokio::test]
    async fn recovery_empty_queue() {
        let ctx = test_context();
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(2),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        ctx.store.set_scan_done(5).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx, peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn recovery_partial_failure() {
        let ctx = test_context();
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(2),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        let track = tape_store::types::Pubkey([1u8; 32]);
        ctx.store
            .put_track(
                track,
                TrackInfo {
                    tape_address: tape_store::types::Pubkey([0u8; 32]),
                    spool_group: SpoolGroup(0),
                    original_size: 1024,
                    stripe_size: 512,
                    stripe_count: 2,
                    encoding_type: 0,
                    encoding_params: 0,
                    commitment: vec![],
                },
            )
            .unwrap();
        ctx.store.add_pending_recovery(5, track).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx, peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn recovery_gate_on_scan() {
        let ctx = test_context();
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(2),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx, peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Pending(_)));
    }

    #[tokio::test]
    async fn recovery_promotes_active() {
        let ctx = test_context();
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(2),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        use tape_store::types::SpoolState;
        ctx.store.set_spool_state(5, SpoolState { status: SpoolStatus::ActiveRecover, epoch: EpochNumber(0) }).unwrap();
        ctx.store.set_scan_done(5).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx.clone(), peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert_eq!(
            ctx.store.get_spool_state(5).unwrap().unwrap().status,
            SpoolStatus::Active,
        );
        assert!(!ctx.store.is_scan_done(5).unwrap());
    }

    #[test]
    fn reconstruct_from_peer_slices() {
        let profile = tape_core::encoding::EncodingProfile::clay_default();
        let mut encoder = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            2_000,
            true,
            profile,
        );
        encoder.set_chunk_index(42);

        let payload: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
        let slices = encoder.encode(&payload).unwrap();

        let lost = SliceIndex::new(0);
        let peer_count = encoder.k();
        let peer_slices: Vec<(SliceIndex, Vec<u8>)> = slices
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != *lost)
            .take(peer_count)
            .map(|(idx, data)| (SliceIndex::new(idx), data.clone()))
            .collect();

        let mut recovery_slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            2_000,
            true,
            profile,
        );
        let recovered =
            reconstruct_slice(&mut recovery_slicer, lost, &peer_slices)
                .unwrap();
        assert_eq!(recovered, slices[*lost]);
    }
}
