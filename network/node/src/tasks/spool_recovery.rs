//! SpoolRecovery — recover missing slices via Clay repair protocol.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::encoding::EncodingType;
use tape_core::erasure::spool_in_group;
use tape_node_api::{RepairRequest, StripeSubChunkRequest};
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_slicer::ClayCoder;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{NodeInfo, Pubkey as StorePubkey, SpoolState, SpoolStatus, TrackInfo};
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

            // Build the request shape once so every helper query is identical;
            // this avoids candidate skew from caller-specific request construction.
            let request_template = match build_repair_request(&track_info) {
                Ok(req) => req,
                Err(reason) => {
                    tracing::warn!(?track_addr, spool, "build repair request: {reason}");
                    any_failed = true;
                    continue;
                }
            };

            let helpers: Vec<(NodeInfo, u16)> = committee
                .iter()
                .filter_map(|node| {
                    node.spools
                        .iter()
                        .find(|&&s| s != spool && spool_in_group(s, track_info.spool_group))
                        .map(|&helper_spool| (node.clone(), helper_spool))
                })
                .collect();

            if helpers.is_empty() {
                tracing::warn!(?track_addr, spool, "no helper found for recovery");
                any_failed = true;
                continue;
            }

            let mut recovered = false;

            // Try multiple candidate helpers. A single response is treated as
            // untrusted; we only persist when validation and optional cross-check
            // confirm improvement over current local state.
            for (helper_idx, (helper, helper_spool)) in helpers.iter().enumerate() {
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
                        any_failed = true;
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

                let request = RepairRequest {
                    helper_spool: *helper_spool,
                    stripes: request_template.stripes.clone(),
                };

                // Ask one helper for this full track request, then validate and
                // merge only if the bytes are structurally safe.
                match with_retry(&RetryConfig::fast(), || {
                    client.request_repair(track_addr, &request)
                })
                .await
                {
                    Ok(data) if !data.is_empty() => {
                        context.stats.add_repair_received(data.len() as u64);
                        // Validate before using helper data because peers can be
                        // stale, buggy, or malicious.
                        if let Err(reason) = validate_slice_entry(spool, &track_info, &data) {
                            if let Err(e) = peer_handle.record_failure(addr).await {
                                tracing::warn!(?track_addr, spool, "failed to record peer failure for {addr}: {e}");
                            }
                            tracing::warn!(?track_addr, spool, "validation error: {reason}");
                            continue;
                        }

                        match context.store.get_slice(spool, track_addr) {
                            Ok(Some(existing)) if existing == data => {
                                // Fast path: the missing slice is already present and
                                // identical after refresh, so clear pending state only.
                                if let Err(e) =
                                    context.store.remove_pending_recovery(spool, track_addr)
                                {
                                    tracing::warn!(?track_addr, spool, "remove pending recovery: {e}");
                                    any_failed = true;
                                    continue;
                                }
                                recovered = true;
                                removed_any = true;
                                break;
                            }
                            Ok(Some(_)) => {
                                // Existing local slice differs; require a second helper
                                // to return the same bytes before we overwrite.
                                if !helpers_match(
                                    &helpers,
                                    helper_idx,
                                    &track_info,
                                    spool,
                                    track_addr,
                                    &data,
                                    &request_template,
                                    &peer_handle,
                                    &context,
                                )
                                .await
                                {
                                    tracing::debug!(?track_addr, spool, "second helper disagreement");
                                    continue;
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(?track_addr, spool, "get_slice error: {e}");
                                any_failed = true;
                                continue;
                            }
                        }

                        if let Err(e) = peer_handle.record_success(addr).await {
                            tracing::warn!(?track_addr, spool, "failed to record peer success for {addr}: {e}");
                        }
                        if let Err(e) = context.store.put_slice(spool, track_addr, data) {
                            tracing::warn!(?track_addr, "put_slice error: {e}");
                            any_failed = true;
                            continue;
                        }

                        // Persisting after validation and optional quorum check
                        // guarantees local state only improves, never regresses.
                        if let Err(e) = context.store.remove_pending_recovery(spool, track_addr) {
                            tracing::warn!(?track_addr, spool, "remove pending recovery: {e}");
                            any_failed = true;
                            continue;
                        }
                        tracing::debug!(?track_addr, spool, "recovered slice");
                        recovered = true;
                        removed_any = true;
                        break;
                    }
                    Ok(_) => {
                        // Empty payloads are non-fatal; they are usually transient
                        // helper-side load responses but do not prove recovery.
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

            if !recovered {
                tracing::debug!(?track_addr, spool, "all helpers exhausted");
                any_failed = true;
            }
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

/// Build the repair request from track metadata so recovery uses deterministic
/// and complete extraction for all stripes/sub-chunks.
///
/// Why:
/// - avoids repeating request-shape construction in every helper loop
/// - guarantees helper peers see identical, reproducible repair instructions
fn build_repair_request(track_info: &TrackInfo) -> Result<RepairRequest, String> {
    let profile = track_info.profile();
    let encoding = profile.encoding_type().ok_or_else(|| "unknown encoding".to_string())?;
    if !matches!(encoding, EncodingType::Clay) {
        return Err("repair only supported for clay encoding".to_string());
    }

    let coder = ClayCoder::from_params(profile.clay_params());
    let alpha = coder.alpha();
    if alpha == 0 {
        return Err("invalid clay alpha".to_string());
    }
    if track_info.stripe_count == 0 {
        return Err("no stripes to repair".to_string());
    }
    if track_info.stripe_size == 0 {
        return Err("invalid stripe size".to_string());
    }

    let stripe_count = usize::try_from(track_info.stripe_count)
        .map_err(|_| "stripe count overflow".to_string())?;
    let sub_chunks: Vec<u32> = (0..alpha).map(|value| value as u32).collect();
    let stripes = (0..stripe_count)
        .map(|stripe| StripeSubChunkRequest {
            stripe: stripe as u32,
            sub_chunks: sub_chunks.clone(),
        })
        .collect();

    Ok(RepairRequest {
        helper_spool: 0,
        stripes,
    })
}

/// Ask a second helper for the same track to confirm candidate bytes.
///
/// Returns true only when another helper returns the same non-empty, validated
/// bytes, preventing silent acceptance of one faulty or malicious helper.
async fn helpers_match<S: Store, R: Rpc>(
    helper_pairs: &[(NodeInfo, u16)],
    skip_index: usize,
    track_info: &TrackInfo,
    spool: u16,
    track_addr: StorePubkey,
    candidate: &[u8],
    request_template: &RepairRequest,
    peer_handle: &PeerHandle,
    context: &NodeContext<S, R>,
) -> bool {
    for (helper, helper_spool) in helper_pairs.iter().skip(skip_index + 1) {
        // Only query peers that can currently be trusted for retries.

        let addr = match helper.network_address.to_socket_addr() {
            Ok(a) => a,
            Err(_) => continue,
        };

        if peer_handle
            .is_cooling_down(addr)
            .await
            .unwrap_or(true)
        {
            continue;
        }

        let client = match NodeClientBuilder::new().build(&addr.to_string()) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let request = RepairRequest {
            helper_spool: *helper_spool,
            stripes: request_template.stripes.clone(),
        };

        let response = with_retry(&RetryConfig::fast(), || {
            client.request_repair(track_addr, &request)
        })
        .await;

        // Any failure to parse or validate is treated as disagreement and ignored.
        let Ok(data) = response else {
            continue;
        };
        context.stats.add_repair_received(data.len() as u64);
        if data.is_empty() || validate_slice_entry(spool, track_info, &data).is_err() {
            continue;
        }
        if data == candidate {
            return true;
        }
    }
    false
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
