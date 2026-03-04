//! SpoolRecovery — recover missing slices via Clay repair protocol.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{slice_for_spool, spool_for_slice, spool_in_group};
use tape_core::types::network::NetworkAddress;
use tape_node_api::{RepairRequest, StripeSubChunkRequest};
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_slicer::{ClayCoder, ErasureCoder, RepairPlan, Slicer, SliceIndex, SliceMetadata};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey as StorePubkey, SpoolState, SpoolStatus, TrackInfo};
use tokio_util::sync::CancellationToken;

use crate::core::validate_slice_entry;
use crate::core::NodeContext;
use crate::core::PeerHandle;
use crate::TaskOutcome;

const RECOVERY_BATCH_SIZE: usize = 10;

pub async fn run<S: Store, R: Rpc>(
    ctx: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    // Get the latest known onchain state
    let chain = ctx.chain_state.load();

    // No previous committee_prev exists in epoch 0
    if chain.epoch.is_zero() {
        return TaskOutcome::Success;
    }

    // The previous committee_prev exists, but no data could have been asigned yet.
    if chain.epoch.is_one() {
        return TaskOutcome::Success;
    }

    let committee = chain.committee_prev.clone();

    // TODO: this should be a vector of failed items.
    let mut any_failed = false;

    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        // Iterate in bounded batches so this task stays cancellable and avoids
        // monopolizing the runtime when there are many missing slices.
        let pending = match ctx.store
            .iter_pending_recoveries(spool, RECOVERY_BATCH_SIZE) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
        };

        if pending.is_empty() {
            break;
        }

        // TODO: loop should be a function

        let mut removed_any = false;
        for track_addr in pending {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            // Get the track metadata
            let track_info = match ctx.store.get_track(track_addr) {
                Ok(Some(t)) => t,
                Ok(None) => {
                    tracing::warn!(?track_addr, "track is missing locally");
                    any_failed = true;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(?track_addr, "get_track error: {e}");
                    any_failed = true;
                    continue;
                }
            };

            // Check if we already have the slice data for this spool
            match ctx.store.get_slice(spool, track_addr) {
                Ok(Some(_)) => {
                    // TODO: we should instead add removed entries to a list, then remove them at
                    // the end (instead of the remove_any bool). We're swallowing store errors here.
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    removed_any = true;
                    continue;
                }
                Ok(None) => {
                    // No-op, we need to continue with the recovery
                }
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "get_slice error: {e}");
                    any_failed = true;
                    continue;
                }
            }

            // Validate encoding type before attempting repair.
            let profile = track_info.profile();

            if !profile.is_clay() {
                tracing::warn!(?track_addr, spool, "repair only supported for clay encoding");
                any_failed = true;
                continue;
            }

            if track_info.stripe_count == 0 || track_info.stripe_size == 0 {
                tracing::warn!(?track_addr, spool, "invalid stripe parameters");
                any_failed = true;
                continue;
            }

            // Compute the local spoolgroup index for the lost slice
            let lost_idx = match slice_for_spool(track_info.spool_group, spool) {
                Some(idx) => idx,
                None => {
                    tracing::warn!(?track_addr, spool, "spool not in track's group");
                    any_failed = true;
                    continue;
                }
            };

            // TODO: this really should not be an Option<SliceIndex>, fix the SliceIndex
            // constructor
            let lost = match SliceIndex::new(lost_idx) {
                Some(si) => si,
                None => {
                    tracing::warn!(?track_addr, spool, "invalid slice index {lost_idx}");
                    any_failed = true;
                    continue;
                }
            };

            // Build helper map: SliceIndex -> helper network address.
            // Each committee member may own multiple spools in the group.
            let mut helper_map: HashMap<SliceIndex, NetworkAddress> = HashMap::new();
            for node in committee.iter() {
                for &s in &node.spools {
                    if s == spool || !spool_in_group(s, track_info.spool_group) {
                        continue;
                    }
                    if let Some(idx) = slice_for_spool(track_info.spool_group, s) {
                        if let Some(si) = SliceIndex::new(idx) {
                            helper_map.entry(si).or_insert(node.network_address);
                        }
                    }
                }
            }

            let available: Vec<SliceIndex> = helper_map.keys().copied().collect();
            if available.is_empty() {
                tracing::warn!(?track_addr, spool, "no helpers found for repair");
                any_failed = true;
                continue;
            }

            // Build the repair plan via the slicer.
            let coder = ClayCoder::from_params(profile.clay_params());
            let mut slicer = Slicer::with_profile(
                coder,
                track_info.stripe_size as usize,
                true, // rotated — SDK encoder always uses rotation for Clay tracks
                profile,
            );

            let plan = match slicer.repair_plan_from_params(
                lost,
                &available,
                track_info.original_size as usize,
                track_info.stripe_size as usize,
            ) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "repair plan: {e}");

                    // TODO: this should not be right here, we should build a list of those that
                    // need a full repair and do that later
                    match recover_with_full_fallback(
                        &ctx,
                        &peer_handle,
                        track_addr,
                        spool,
                        lost,
                        &helper_map,
                        &mut slicer,
                        &track_info,
                        "repair-plan failure",
                    )
                    .await
                    {
                        Ok(()) => {
                            removed_any = true;
                            continue;
                        }
                        Err(reason) => {
                            tracing::warn!(
                                ?track_addr,
                                spool,
                                "full recovery fallback after repair-plan failure failed: {reason}"
                            );
                            any_failed = true;
                            continue;
                        }
                    }
                }
            };

            // Build per-helper RepairRequests from the plan.
            let per_helper = build_per_helper_requests(&plan, track_info.spool_group);

            // Collect required helper slice indices from the plan.
            let mut required: Vec<SliceIndex> = Vec::new();
            for stripe_repair in &plan.stripes {
                for hp in &stripe_repair.helpers {
                    if !required.contains(&hp.slice) {
                        required.push(hp.slice);
                    }
                }
            }

            // Send requests to each helper and collect partial data.
            // Could be parallelized with FuturesUnordered if throughput matters.
            let mut helper_data: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
            for (slice_idx, request) in &per_helper {
                let helper_addr = match helper_map.get(slice_idx) {
                    Some(addr) => *addr,
                    None => continue,
                };

                let addr = match helper_addr.to_socket_addr() {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(?track_addr, "parse helper address: {e}");
                        continue;
                    }
                };

                match peer_handle.is_cooling_down(addr).await {
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
                        tracing::warn!(?track_addr, "build helper client: {e}");
                        continue;
                    }
                };

                match with_retry(&RetryConfig::fast(), || {
                    client.request_repair(track_addr, request)
                })
                .await
                {
                    Ok(data) if !data.is_empty() => {
                        ctx.stats.add_repair_received(data.len() as u64);
                        if let Err(e) = peer_handle.record_success(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                        }
                        helper_data.insert(*slice_idx, data);
                    }
                    Ok(_) => {
                        if let Err(e) = peer_handle.record_success(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                        }
                        tracing::debug!(?track_addr, spool, helper = ?helper_addr, "empty repair response");
                    }
                    Err(e) => {
                        if let Err(err) = peer_handle.record_failure(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer failure for {addr}: {err}");
                        }
                        tracing::debug!(?track_addr, spool, helper = ?helper_addr, "repair error: {e}");
                    }
                }
            }

            // Check that all required helpers responded. If any are missing,
            // fall back to full recovery using k full slices.
            if !required.iter().all(|si| helper_data.contains_key(si)) {
                tracing::debug!(?track_addr, spool, "insufficient helper responses for repair");
                // TODO: once again we have this inlined... this should not be called here.
                match recover_with_full_fallback(
                    &ctx,
                    &peer_handle,
                    track_addr,
                    spool,
                    lost,
                    &helper_map,
                    &mut slicer,
                    &track_info,
                    "insufficient helper responses",
                )
                .await
                {
                    Ok(()) => {
                        removed_any = true;
                        continue;
                    }
                    Err(reason) => {
                        tracing::warn!(
                            ?track_addr,
                            spool,
                            "full recovery fallback after insufficient helpers failed: {reason}"
                        );
                        any_failed = true;
                        continue;
                    }
                }
            }

            // Construct slice metadata for the repair.
            let metadata = SliceMetadata::with_profile(
                track_info.original_size as usize,
                track_info.stripe_size as usize,
                profile,
            );
            let metadata_bytes = metadata.to_bytes();

            // Run Clay repair to reconstruct the lost slice.
            let repaired = match slicer.repair(&plan, &helper_data, &metadata_bytes) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "clay repair failed: {e}");
                    // TODO: once again we have this inlined... this should not be called here.
                    match recover_with_full_fallback(
                        &ctx,
                        &peer_handle,
                        track_addr,
                        spool,
                        lost,
                        &helper_map,
                        &mut slicer,
                        &track_info,
                        "clay repair failure",
                    )
                    .await
                    {
                        Ok(()) => {
                            removed_any = true;
                            continue;
                        }
                        Err(reason) => {
                            tracing::warn!(
                                ?track_addr,
                                spool,
                                "full recovery fallback after clay repair failure failed: {reason}"
                            );
                            any_failed = true;
                            continue;
                        }
                    }
                }
            };

            if let Err(e) = persist_recovered_slice(
                &ctx,
                spool,
                track_addr,
                &track_info,
                repaired,
            ) {
                tracing::warn!(?track_addr, spool, "persist repaired slice: {e}");

                // TODO: once again we have this inlined... this should not be called here.
                match recover_with_full_fallback(
                    &ctx, &peer_handle, track_addr, spool, lost,
                    &helper_map, &mut slicer, &track_info, "clay validation failure",
                ).await {
                    Ok(()) => { removed_any = true; continue; }
                    Err(reason) => {
                        tracing::warn!(?track_addr, spool, "full fallback after validation failure: {reason}");
                        any_failed = true;
                        continue;
                    }
                }
            }
            tracing::debug!(?track_addr, spool, "repaired slice via clay");
            removed_any = true;
        }

        if !removed_any {
            break;
        }
    }

    if any_failed {
        TaskOutcome::Retryable("some tracks could not be recovered".into())
    } else {
        // Only transition to Active if RecoveryScan has completed.
        // Use Pending (not Retryable) — this is a wait condition, not a failure.
        let scan_done = match ctx.store.is_scan_done(spool) {
            Ok(done) => done,
            Err(e) => return TaskOutcome::Retryable(format!("read scan_done: {e}")),
        };
        if !scan_done {
            return TaskOutcome::Pending(Duration::from_secs(5));
        }
        if let Ok(Some(state)) = ctx.store.get_spool_state(spool) {
            if state.is_recovering() {
                let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch };
                if let Err(e) = ctx.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool active: {e}"));
                }
                let _ = ctx.store.clear_scan_done(spool);
                tracing::info!(spool, "spool recovery complete, marked active");
            }
        }
        TaskOutcome::Success
    }
}

fn persist_recovered_slice<S: Store, R: Rpc>(
    ctx: &Arc<NodeContext<S, R>>,
    spool: u16,
    track_addr: StorePubkey,
    track_info: &TrackInfo,
    recovered: Vec<u8>,
) -> Result<(), String> {
    validate_slice_entry(spool, track_info, &recovered)
        .map_err(|reason| format!("slice validation failed: {reason}"))?;
    ctx
        .store
        .put_slice(spool, track_addr, recovered)
        .map_err(|e| format!("put_slice error: {e}"))?;
    ctx
        .store
        .remove_pending_recovery(spool, track_addr)
        .map_err(|e| format!("remove pending recovery: {e}"))?;
    Ok(())
}

async fn recover_with_full_fallback<S: Store, R: Rpc>(
    ctx: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    track_addr: StorePubkey,
    spool: u16,
    lost: SliceIndex,
    helper_map: &HashMap<SliceIndex, NetworkAddress>,
    slicer: &mut Slicer<ClayCoder>,
    track_info: &TrackInfo,
    reason: &str,
) -> Result<(), String> {
    let recovered = attempt_full_recovery_from_helpers(
        ctx,
        peer_handle,
        track_addr,
        lost,
        helper_map,
        slicer,
        track_info.spool_group,
    )
    .await?;
    persist_recovered_slice(ctx, spool, track_addr, track_info, recovered)?;
    tracing::debug!(?track_addr, spool, reason, "recovered slice via full fallback");
    Ok(())
}

async fn attempt_full_recovery_from_helpers<S: Store, R: Rpc>(
    ctx: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    track_addr: StorePubkey,
    lost: SliceIndex,
    helper_map: &HashMap<SliceIndex, NetworkAddress>,
    slicer: &mut Slicer<ClayCoder>,
    spool_group: u64,
) -> Result<Vec<u8>, String> {
    let needed = slicer.k();
    let mut helper_indices: Vec<SliceIndex> = helper_map.keys().copied().collect();
    helper_indices.sort_unstable_by_key(|si| **si);

    let mut full_slices: Vec<(usize, Vec<u8>)> = Vec::with_capacity(needed);
    for slice_idx in helper_indices {
        if full_slices.len() >= needed {
            break;
        }

        let helper_addr = match helper_map.get(&slice_idx) {
            Some(addr) => *addr,
            None => continue,
        };
        let addr = match helper_addr.to_socket_addr() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(?track_addr, "parse helper address: {e}");
                continue;
            }
        };

        match peer_handle.is_cooling_down(addr).await {
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
                tracing::warn!(?track_addr, "build helper client: {e}");
                continue;
            }
        };

        // Convert group-relative slice index to global spool ID for the HTTP API.
        let global_spool = spool_for_slice(spool_group, *slice_idx) as u16;
        match with_retry(&RetryConfig::fast(), || {
            client.get_slice(track_addr, global_spool)
        })
        .await
        {
            Ok(data) if !data.is_empty() => {
                ctx.stats.add_repair_received(data.len() as u64);
                if let Err(e) = peer_handle.record_success(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer success for {addr}: {e}");
                }
                full_slices.push((*slice_idx, data));
            }
            Ok(_) => {
                if let Err(e) = peer_handle.record_success(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer success for {addr}: {e}");
                }
                tracing::debug!(?track_addr, helper = ?helper_addr, "empty full-slice response");
            }
            Err(e) => {
                if let Err(err) = peer_handle.record_failure(addr).await {
                    tracing::warn!(?track_addr, "failed to record peer failure for {addr}: {err}");
                }
                tracing::debug!(?track_addr, helper = ?helper_addr, "full-slice fetch error: {e}");
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

    reconstruct_slice_via_full_recovery(slicer, lost, &full_slices)
}

fn reconstruct_slice_via_full_recovery(
    slicer: &mut Slicer<ClayCoder>,
    lost: SliceIndex,
    helpers: &[(usize, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    let Some((_, sample)) = helpers.first() else {
        return Err("no helper slices provided".into());
    };

    let metadata = SliceMetadata::from_slice(sample)
        .map_err(|e| format!("parse helper metadata failed: {e}"))?;
    slicer.set_chunk_index(metadata.chunk_index);

    let helper_refs: Vec<(usize, &[u8])> = helpers
        .iter()
        .map(|(idx, data)| (*idx, data.as_slice()))
        .collect();

    let decoded = slicer
        .decode(&helper_refs)
        .map_err(|e| format!("decode fallback failed: {e}"))?;
    let reencoded = slicer
        .encode(&decoded)
        .map_err(|e| format!("re-encode fallback failed: {e}"))?;
    reencoded
        .get(*lost)
        .cloned()
        .ok_or_else(|| format!("lost slice index {} out of bounds", *lost))
}

/// Invert a `RepairPlan` (per-stripe, per-helper) into per-helper `RepairRequest`s.
/// Each helper gets only its specific sub-chunks across all stripes.
fn build_per_helper_requests(
    plan: &RepairPlan,
    spool_group: u64,
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
            let helper_spool = spool_for_slice(spool_group, *slice_idx);
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
            epoch: EpochNumber(1),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        // With scan_done set, empty queue means recovery is complete
        ctx.store.set_scan_done(5).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx, peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn recovery_partial_failure() {
        let ctx = test_context();
        // Empty committee → no helpers available
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(1),
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
                    spool_group: 0,
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
            epoch: EpochNumber(1),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        // Empty queue + no scan flag → Pending (waiting for scan)
        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = crate::core::PeerService::new();
        let result = run(ctx, peer_handle, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Pending(_)));
    }

    #[tokio::test]
    async fn recovery_promotes_active() {
        let ctx = test_context();
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(1),
            phase: EpochPhase::Active,
            committee: vec![],
            ..Default::default()
        });

        // Set up ActiveRecover spool with scan_done flag
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
    fn full_fallback_reconstructs_lost_slice_from_k_helpers() {
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

        let lost = SliceIndex::new(0).unwrap();
        let helper_count = encoder.k();
        let helpers: Vec<(usize, Vec<u8>)> = slices
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != *lost)
            .take(helper_count)
            .map(|(idx, data)| (idx, data.clone()))
            .collect();

        let mut recovery_slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            2_000,
            true,
            profile,
        );
        let recovered =
            reconstruct_slice_via_full_recovery(&mut recovery_slicer, lost, &helpers)
                .unwrap();
        assert_eq!(recovered, slices[*lost]);
    }
}
