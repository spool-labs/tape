use std::collections::HashSet;
use std::net::SocketAddr;

use tape_core::erasure::{SPOOL_GROUP_COUNT, group_for_spool, slice_for_spool};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_crypto::hash::Hash;
use tape_node_client::{NodeClient, NodeClientBuilder, RetryConfig, with_retry};
use tape_store::types::{NodeInfo, Pubkey};

use crate::runtime::PeerHandle;
use crate::supervisor::TaskOutcome;

pub async fn fetch_commitments(
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    local_epoch: EpochNumber,
) -> Result<Vec<Hash>, TaskOutcome> {
    let config = RetryConfig::fast();

    for member in committee {
        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        let result = with_retry(&config, || client.get_snapshot_commitments(local_epoch.0)).await;
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

pub async fn peer_client(
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
