//! SpoolRecovery — recover missing slices via Clay repair protocol.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::encoding::EncodingType;
use tape_core::erasure::{slice_for_spool, spool_for_slice, spool_in_group};
use tape_node_api::{RepairRequest, StripeSubChunkRequest};
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_slicer::{ClayCoder, RepairPlan, Slicer, SliceIndex, SliceMetadata};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{SpoolState, SpoolStatus};
use tokio_util::sync::CancellationToken;

use crate::core::validate_slice_entry;
use crate::core::NodeContext;
use crate::core::PeerHandle;
use crate::core::require_epoch;
use crate::TaskOutcome;

const RECOVERY_BATCH_SIZE: usize = 10;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    let epoch = match require_epoch(&context.chain_state) {
        Ok(e) => e,
        Err(outcome) => return outcome,
    };

    let cs = context.chain_state.load();
    let committee = match cs.committee_for(epoch) {
        Some(c) => c.clone(),
        None => return TaskOutcome::Retryable("no committee for current epoch".into()),
    };

    let mut any_failed = false;

    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        // Iterate in bounded batches so this task stays cancellable and avoids
        // monopolizing the runtime when there are many missing slices.
        let pending = match context.store.iter_pending_recoveries(spool, RECOVERY_BATCH_SIZE) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
        };

        if pending.is_empty() {
            break;
        }

        let mut removed_any = false;

        for track_addr in pending {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            // Ignore stale pending entries where local metadata is already gone.
            let track_info = match context.store.get_track(track_addr) {
                Ok(Some(t)) => t,
                Ok(None) => {
                    let _ = context.store.remove_pending_recovery(spool, track_addr);
                    removed_any = true;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(?track_addr, "get_track error: {e}");
                    any_failed = true;
                    continue;
                }
            };

            match context.store.get_slice(spool, track_addr) {
                Ok(Some(_)) => {
                    let _ = context.store.remove_pending_recovery(spool, track_addr);
                    removed_any = true;
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "get_slice error: {e}");
                    any_failed = true;
                    continue;
                }
            }

            // Validate encoding type before attempting repair.
            let profile = track_info.profile();
            let encoding = match profile.encoding_type() {
                Some(e) => e,
                None => {
                    tracing::warn!(?track_addr, spool, "unknown encoding type");
                    any_failed = true;
                    continue;
                }
            };
            if !matches!(encoding, EncodingType::Clay) {
                tracing::warn!(?track_addr, spool, "repair only supported for clay encoding");
                any_failed = true;
                continue;
            }
            if track_info.stripe_count == 0 || track_info.stripe_size == 0 {
                tracing::warn!(?track_addr, spool, "invalid stripe parameters");
                any_failed = true;
                continue;
            }

            // Compute the lost slice index within the spool group.
            let lost_idx = match slice_for_spool(track_info.spool_group, spool) {
                Some(idx) => idx,
                None => {
                    tracing::warn!(?track_addr, spool, "spool not in track's group");
                    any_failed = true;
                    continue;
                }
            };
            let lost = match SliceIndex::new(lost_idx) {
                Some(si) => si,
                None => {
                    tracing::warn!(?track_addr, spool, "invalid slice index {lost_idx}");
                    any_failed = true;
                    continue;
                }
            };

            // Build helper map: SliceIndex -> (NodeInfo, helper_spool).
            // Each committee member may own multiple spools in the group.
            let mut helper_map: HashMap<SliceIndex, (&_, u16)> = HashMap::new();
            for node in committee.iter() {
                for &s in &node.spools {
                    if s == spool || !spool_in_group(s, track_info.spool_group) {
                        continue;
                    }
                    if let Some(idx) = slice_for_spool(track_info.spool_group, s) {
                        if let Some(si) = SliceIndex::new(idx) {
                            helper_map.entry(si).or_insert((node, s));
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
            let slicer = Slicer::with_profile(
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
                    any_failed = true;
                    continue;
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
            // Requests are sequential (~10ms each, d=16 helpers = ~160ms total).
            // Could be parallelized with FuturesUnordered if throughput matters.
            let mut helper_data: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
            for (slice_idx, request) in &per_helper {
                let (helper, _helper_spool) = match helper_map.get(slice_idx) {
                    Some(h) => h,
                    None => continue,
                };

                let addr = match helper.network_address.to_socket_addr() {
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
                        context.stats.add_repair_received(data.len() as u64);
                        if let Err(e) = peer_handle.record_success(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                        }
                        helper_data.insert(*slice_idx, data);
                    }
                    Ok(_) => {
                        if let Err(e) = peer_handle.record_success(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                        }
                        tracing::debug!(?track_addr, spool, helper = ?helper.network_address, "empty repair response");
                    }
                    Err(e) => {
                        if let Err(err) = peer_handle.record_failure(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer failure for {addr}: {err}");
                        }
                        tracing::debug!(?track_addr, spool, helper = ?helper.network_address, "repair error: {e}");
                    }
                }
            }

            // Check that all required helpers responded. If any are missing
            // we skip this track rather than re-planning with fewer helpers.
            // The task returns Retryable and peer cooldowns may produce a
            // different helper set on the next attempt.
            if !required.iter().all(|si| helper_data.contains_key(si)) {
                tracing::debug!(?track_addr, spool, "insufficient helper responses for repair");
                any_failed = true;
                continue;
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
                    any_failed = true;
                    continue;
                }
            };

            // Validate the reconstructed slice against the commitment.
            if let Err(reason) = validate_slice_entry(spool, &track_info, &repaired) {
                tracing::warn!(?track_addr, spool, "repaired slice validation failed: {reason}");
                any_failed = true;
                continue;
            }

            // Store the repaired slice and clear pending state.
            if let Err(e) = context.store.put_slice(spool, track_addr, repaired) {
                tracing::warn!(?track_addr, spool, "put_slice error: {e}");
                any_failed = true;
                continue;
            }
            if let Err(e) = context.store.remove_pending_recovery(spool, track_addr) {
                tracing::warn!(?track_addr, spool, "remove pending recovery: {e}");
                any_failed = true;
                continue;
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
        let scan_done = match context.store.is_scan_done(spool) {
            Ok(done) => done,
            Err(e) => return TaskOutcome::Retryable(format!("read scan_done: {e}")),
        };
        if !scan_done {
            return TaskOutcome::Pending(Duration::from_secs(5));
        }
        if let Ok(Some(state)) = context.store.get_spool_state(spool) {
            if state.is_recovering() {
                let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch };
                if let Err(e) = context.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool active: {e}"));
                }
                let _ = context.store.clear_scan_done(spool);
                tracing::info!(spool, "spool recovery complete, marked active");
            }
        }
        TaskOutcome::Success
    }
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
}
