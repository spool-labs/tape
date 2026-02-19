use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE, group_for_spool, slice_for_spool};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_crypto::hash::Hash;
use tape_node_client::{NodeClient, NodeClientBuilder, RetryConfig, with_retry};
use tape_store::types::{NodeInfo, Pubkey};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::peers::PeerHandle;
use crate::supervisor::TaskOutcome;

#[derive(Default)]
pub struct GroupPeerMetrics {
    pub members_considered: usize,
    pub members_no_weight: usize,
    pub peer_addr_invalid: usize,
    pub peer_client_build_fail: usize,
    pub peer_rpc_success: usize,
    pub peer_rpc_fail: usize,
    pub epoch_mismatch: usize,
    pub sig_invalid: usize,
    pub member_index_overflow: usize,
}

pub struct GroupPartials {
    pub signatures: Vec<tape_core::bls::BlsSignature>,
    pub member_indices: Vec<u8>,
    pub weight: u64,
    pub metrics: GroupPeerMetrics,
}

pub async fn fetch_commitments(
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    target: EpochNumber,
) -> Result<Vec<Hash>, TaskOutcome> {
    let config = RetryConfig::fast();

    for member in committee {
        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        let result = with_retry(&config, || client.get_snapshot_commitments(target.0)).await;
        match result {
            Ok(commitments) if commitments.len() == SPOOL_GROUP_COUNT => {
                if let Err(e) = peer_handle.record_success(addr).await {
                    tracing::warn!("failed to record peer success for {addr}: {e}");
                }
                return Ok(commitments);
            }
            Ok(_) => {
                if let Err(e) = peer_handle.record_failure(addr).await {
                    tracing::warn!("failed to record peer failure for {addr}: {e}");
                }
            }
            Err(e) => {
                if let Err(err) = peer_handle.record_failure(addr).await {
                    tracing::warn!("failed to record peer failure for {addr}: {err}");
                }
                tracing::debug!("fetch commitments failed from {addr}: {e}");
            }
        }
    }

    Err(TaskOutcome::Retryable(
        "snapshot commitments unavailable".into(),
    ))
}

pub async fn collect_group_slices(
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    group: SpoolGroup,
    track: Pubkey,
    needed: usize,
) -> Result<Vec<(usize, Vec<u8>)>, TaskOutcome> {
    let mut seen = HashSet::new();
    let mut slices = Vec::new();

    for member in committee {
        if slices.len() >= needed {
            break;
        }

        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        for &spool in &member.spools {
            if slices.len() >= needed {
                break;
            }
            if group_for_spool(spool) != group {
                continue;
            }

            let Some(slice_index) = slice_for_spool(group, spool) else {
                continue;
            };
            if seen.contains(&slice_index) {
                continue;
            }

            match client.get_slice(track, slice_index as u16).await {
                Ok(data) => {
                    seen.insert(slice_index);
                    slices.push((slice_index, data));
                    if let Err(e) = peer_handle.record_success(addr).await {
                        tracing::warn!("failed to record peer success for {addr}: {e}");
                    }
                }
                Err(e) => {
                    if let Err(err) = peer_handle.record_failure(addr).await {
                        tracing::warn!("failed to record peer failure for {addr}: {err}");
                    }
                    tracing::trace!("fetch slice failed from {addr}: {e}");
                }
            }
        }
    }

    Ok(slices)
}

pub async fn collect_group_partials(
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    target: EpochNumber,
    group: SpoolGroup,
    commitment: Hash,
    sign_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<GroupPartials, TaskOutcome> {
    let mut partials = GroupPartials {
        signatures: Vec::new(),
        member_indices: Vec::new(),
        weight: 0,
        metrics: GroupPeerMetrics::default(),
    };

    for (index, member) in committee.iter().enumerate() {
        if cancel.is_cancelled() {
            return Err(TaskOutcome::Success);
        }

        partials.metrics.members_considered += 1;
        let member_weight: u64 = member
            .spools
            .iter()
            .filter(|&&spool| group_for_spool(spool) == group)
            .count() as u64;
        if member_weight == 0 {
            partials.metrics.members_no_weight += 1;
            continue;
        }

        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            partials.metrics.peer_addr_invalid += 1;
            continue;
        };

        tracing::debug!(
            epoch = target.0,
            group,
            member = index,
            timeout_secs = sign_timeout.as_secs(),
            "snapshot sign request start"
        );
        let start = Instant::now();
        let response = match timeout(sign_timeout, client.get_snapshot_signature(target.0, group)).await {
            Ok(Ok(response)) => {
                partials.metrics.peer_rpc_success += 1;
                if let Err(e) = peer_handle.record_success(addr).await {
                    tracing::warn!("failed to record peer success for {addr}: {e}");
                }
                tracing::debug!(
                    epoch = target.0,
                    group,
                    member = index,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "snapshot sign request success"
                );
                response
            }
            Ok(Err(e)) => {
                partials.metrics.peer_rpc_fail += 1;
                if let Err(err) = peer_handle.record_failure(addr).await {
                    tracing::warn!("failed to record peer failure for {addr}: {err}");
                }
                tracing::debug!(
                    epoch = target.0,
                    group,
                    member = index,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "snapshot sign request failed: {e}"
                );
                continue;
            }
            Err(_) => {
                partials.metrics.peer_rpc_fail += 1;
                if let Err(e) = peer_handle.record_failure(addr).await {
                    tracing::warn!("failed to record peer failure for {addr}: {e}");
                }
                tracing::warn!(
                    epoch = target.0,
                    group,
                    member = index,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    timeout_secs = sign_timeout.as_secs(),
                    "snapshot sign request timed out"
                );
                continue;
            }
        };

        if response.epoch != target {
            partials.metrics.epoch_mismatch += 1;
            tracing::warn!(member = index, "epoch mismatch in sign response");
            continue;
        }

        let signature = response.signature;
        let message = SnapshotMessage::new(target, commitment.0).to_bytes();
        if signature
            .verify_aggregate(message, &[member.bls_pubkey])
            .is_err()
        {
            partials.metrics.sig_invalid += 1;
            tracing::warn!(member = index, "invalid snapshot partial signature");
            continue;
        }

        let member_index = match u8::try_from(index) {
            Ok(member_index) => member_index,
            Err(_) => {
                partials.metrics.member_index_overflow += 1;
                tracing::warn!(member = index, "committee index overflow");
                continue;
            }
        };

        partials.signatures.push(signature);
        partials.member_indices.push(member_index);
        partials.weight += member_weight;

        if tape_core::bft::is_supermajority(partials.weight, SPOOL_GROUP_SIZE as u64) {
            break;
        }
    }

    Ok(partials)
}

async fn peer_client(
    peer_handle: &PeerHandle,
    member: &NodeInfo,
) -> Result<Option<(SocketAddr, NodeClient)>, TaskOutcome> {
    let addr = match member.network_address.to_socket_addr() {
        Ok(addr) => addr,
        Err(_) => return Ok(None),
    };

    match peer_handle.is_cooling_down(addr).await {
        Ok(true) => return Ok(None),
        Ok(false) => {}
        Err(e) => return Err(TaskOutcome::Retryable(format!("peer tracker unavailable: {e}"))),
    }

    let client = match NodeClientBuilder::new().build(&addr.to_string()) {
        Ok(client) => client,
        Err(_) => return Ok(None),
    };

    Ok(Some((addr, client)))
}
