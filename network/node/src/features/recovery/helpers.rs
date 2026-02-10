//! Recovery helper resolution and fan-out.
//!
//! Resolves spool group members from the local committee cache and
//! fans out repair sub-chunk requests to helper nodes.

use std::collections::HashMap;

use futures::stream::{self, StreamExt};
use store::Store;
use tape_core::erasure::{group_for_spool, group_start, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolIndex;
use tape_node_api::{RepairRequest, StripeSubChunkRequest};
use tape_node_client::{NodeClient, NodeClientBuilder};
use tape_slicer::repair::RepairPlan;
use tape_slicer::SliceIndex;
use tape_store::ops::CommitteeOps;
use tracing::{debug, warn};

use crate::core::context::NodeContext;

use super::worker::RecoveryError;

/// A helper node within the same spool group.
pub struct GroupHelper {
    /// Position within the group (0..SPOOL_GROUP_SIZE), also the slice index.
    pub position: usize,
    /// Absolute spool index (group_start + position).
    pub spool_idx: SpoolIndex,
    /// Pre-built node client (reused across repair requests).
    pub client: NodeClient,
}

/// Maximum concurrent repair requests per fan-out.
const FAN_OUT_CONCURRENCY: usize = 8;

/// Resolve helpers in the same spool group from the local committee cache.
///
/// Returns all other members in the group (skipping our own position).
/// Zero RPC calls — all data comes from the locally-cached committee.
pub fn resolve_group_helpers<S: Store>(
    ctx: &NodeContext<S>,
    our_spool: SpoolIndex,
    insecure: bool,
) -> Result<Vec<GroupHelper>, RecoveryError> {
    let group = group_for_spool(our_spool);
    let start = group_start(group);
    let our_position = (our_spool - start) as usize;

    let epoch = ctx.control_plane.current_epoch();
    let committee = ctx
        .storage
        .store
        .get_committee(epoch)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?
        .ok_or(RecoveryError::NoCommittee)?;

    // Build spool → NodeInfo index from committee members
    let mut spool_to_node: HashMap<SpoolIndex, usize> = HashMap::new();
    for (idx, member) in committee.iter().enumerate() {
        for &spool in &member.spools {
            spool_to_node.insert(spool, idx);
        }
    }

    let mut helpers = Vec::with_capacity(SPOOL_GROUP_SIZE - 1);
    for position in 0..SPOOL_GROUP_SIZE {
        if position == our_position {
            continue;
        }
        let spool_idx = start + position as SpoolIndex;
        match spool_to_node.get(&spool_idx) {
            Some(&member_idx) => {
                let member = &committee[member_idx];
                match member.network_address.to_socket_addr() {
                    Ok(address) => {
                        let client = NodeClientBuilder::new()
                            .accept_invalid_certs(insecure)
                            .build(&address.to_string())
                            .map_err(|e| RecoveryError::NodeClient(e.to_string()))?;
                        helpers.push(GroupHelper {
                            position,
                            spool_idx,
                            client,
                        });
                    }
                    Err(e) => {
                        warn!(
                            spool = spool_idx,
                            position,
                            "failed to resolve network address: {e}"
                        );
                    }
                }
            }
            None => {
                warn!(spool = spool_idx, position, "no committee member for spool");
            }
        }
    }

    Ok(helpers)
}

/// Fan out repair requests to helper nodes and collect sub-chunk responses.
///
/// Groups the per-stripe helper plans by network-level slice index,
/// builds a RepairRequest per unique helper, and sends them concurrently.
/// Returns a map from SliceIndex to concatenated sub-chunk bytes.
pub async fn fan_out_repair_requests(
    helpers: &[GroupHelper],
    plan: &RepairPlan,
    track_id: &str,
) -> Result<HashMap<SliceIndex, Vec<u8>>, RecoveryError> {
    // Collect which slice indices the plan actually needs
    let mut needed_slices: HashMap<usize, Vec<(u32, Vec<u32>)>> = HashMap::new();
    for stripe in &plan.stripes {
        for hp in &stripe.helpers {
            needed_slices
                .entry(*hp.slice)
                .or_default()
                .push((stripe.stripe, hp.sub_chunks.clone()));
        }
    }

    // Build position → helper lookup
    let helper_by_position: HashMap<usize, &GroupHelper> =
        helpers.iter().map(|h| (h.position, h)).collect();

    let needed = needed_slices.len();

    // Build (slice_index, client, request) tuples — clone pre-built client per helper
    let mut requests: Vec<(SliceIndex, NodeClient, RepairRequest)> = Vec::new();
    for (slice_idx, stripe_plans) in &needed_slices {
        let helper = match helper_by_position.get(slice_idx) {
            Some(h) => h,
            None => {
                warn!(slice = slice_idx, "no helper available for needed slice");
                continue;
            }
        };

        let client = helper.client.clone();

        let stripes: Vec<StripeSubChunkRequest> = stripe_plans
            .iter()
            .map(|(stripe, sub_chunks)| StripeSubChunkRequest {
                stripe: *stripe,
                sub_chunks: sub_chunks.clone(),
            })
            .collect();

        let request = RepairRequest {
            lost_slice: *plan.lost as u16,
            helper_spool: helper.spool_idx,
            stripes,
        };

        let si = SliceIndex::new(*slice_idx).ok_or(RecoveryError::RepairFailed(
            format!("invalid slice index {slice_idx}"),
        ))?;
        requests.push((si, client, request));
    }

    if requests.len() < needed {
        return Err(RecoveryError::NotEnoughHelpers {
            needed,
            available: requests.len(),
        });
    }

    // Fan out concurrently
    let results: Vec<(SliceIndex, Result<Vec<u8>, RecoveryError>)> = stream::iter(requests)
        .map(|(si, client, req)| {
            let track_id = track_id.to_string();
            async move {
                let result = client
                    .request_repair(&track_id, &req)
                    .await
                    .map_err(|e| RecoveryError::NodeClient(e.to_string()));
                (si, result)
            }
        })
        .buffer_unordered(FAN_OUT_CONCURRENCY)
        .collect()
        .await;

    let mut collected: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
    let mut failures = 0;
    for (si, result) in results {
        match result {
            Ok(data) => {
                collected.insert(si, data);
            }
            Err(e) => {
                warn!(slice = *si, error = %e, "repair request failed");
                failures += 1;
            }
        }
    }

    if collected.len() < needed {
        return Err(RecoveryError::NotEnoughHelpers {
            needed,
            available: collected.len(),
        });
    }

    debug!(
        track = track_id,
        helpers = collected.len(),
        failures,
        "collected repair sub-chunks"
    );

    Ok(collected)
}
