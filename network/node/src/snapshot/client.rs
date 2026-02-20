use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::snapshot_pda;
use tape_api::state::Track;
use tape_core::bft::min_correct;
use tape_core::erasure::{SPOOL_GROUP_COUNT, group_for_spool, slice_for_spool};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_crypto::hash::Hash;
use tape_node_client::{NodeClient, NodeClientBuilder, RetryConfig, with_retry};
use tape_store::types::{NodeInfo, Pubkey};

use crate::runtime::NodeContext;
use crate::runtime::PeerHandle;
use crate::supervisor::TaskOutcome;

pub async fn fetch_commitments<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    local_epoch: EpochNumber,
) -> Result<Vec<Hash>, TaskOutcome> {
    let config = RetryConfig::fast();
    if committee.is_empty() {
        return Err(TaskOutcome::Retryable(
            "snapshot committee is empty".into(),
        ));
    }

    let total = committee.len() as u64;
    let quorum = min_correct(total) as usize;
    let mut ballots: HashMap<Vec<Hash>, (usize, Vec<SocketAddr>)> = HashMap::new();
    let mut valid_peer_answers = 0usize;

    for member in committee {
        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        let result = with_retry(&config, || client.get_snapshot_commitments(local_epoch.0)).await;
        match result {
            Ok(commitments) if commitments.len() == SPOOL_GROUP_COUNT => {
                let entry = ballots.entry(commitments.clone()).or_insert((0, Vec::new()));
                entry.0 += 1;
                entry.1.push(addr);
                valid_peer_answers += 1;
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

    let (consensus_commitments, peer_addrs, votes) = match ballots
        .into_iter()
        .max_by_key(|(_, (count, _))| *count)
    {
        Some((commitments, (count, peers))) => (commitments, peers, count),
        None => {
            return Err(TaskOutcome::Retryable(
                "snapshot commitments unavailable".into(),
            ))
        }
    };

    if votes < quorum {
        return Err(TaskOutcome::Retryable(format!(
            "snapshot commitments insufficient quorum: {votes}/{quorum} for epoch {}",
            local_epoch.0
        )));
    }

    verify_commitments(context, local_epoch, &consensus_commitments).await?;

    for addr in peer_addrs {
        if let Err(e) = peer_handle.record_success(addr).await {
            tracing::warn!("failed to record peer success for {addr}: {e}");
        }
    }

    tracing::debug!(
        local_epoch = local_epoch.0,
        valid_peer_answers,
        votes,
        quorum,
        "snapshot commitments reached quorum and matched onchain"
    );

    Ok(consensus_commitments)
}

async fn verify_commitments<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    local_epoch: EpochNumber,
    commitments: &[Hash],
) -> Result<(), TaskOutcome> {
    let track_addresses: Vec<solana_sdk::pubkey::Pubkey> = commitments
        .iter()
        .map(|commitment| snapshot_pda(local_epoch, *commitment).0)
        .collect();

    let accounts = context
        .rpc
        .rpc()
        .get_multiple_accounts(&track_addresses)
        .await
        .map_err(|error| {
            TaskOutcome::Retryable(format!(
                "snapshot commitments track read failed for epoch {}: {error}",
                local_epoch.0
            ))
        })?;

    if accounts.len() != commitments.len() {
        return Err(TaskOutcome::Retryable(format!(
            "snapshot commitments track read returned {} addresses for {} commitments",
            accounts.len(),
            commitments.len()
        )));
    }

    for (group, maybe_account) in accounts.into_iter().enumerate() {
        let commitment = commitments[group];
        let track_address = track_addresses[group];

        let account = match maybe_account {
            Some(account) => account,
            None => {
                return Err(TaskOutcome::Retryable(format!(
                    "snapshot track {track_address} for epoch {} group {} is not onchain",
                    local_epoch.0, group
                )));
            }
        };

        let track = Track::unpack_with_discriminator(&account.data).map_err(|error| {
            TaskOutcome::Retryable(format!(
                "snapshot track decode failed for epoch {} group {}: {error}",
                local_epoch.0, group
            ))
        })?;

        if track.data.registered_epoch != local_epoch {
            return Err(TaskOutcome::Retryable(format!(
                "snapshot track epoch mismatch for epoch {} group {}: onchain epoch {}",
                local_epoch.0,
                group,
                track.data.registered_epoch.0
            )));
        }

        if track.data.commitment_hash != commitment {
            return Err(TaskOutcome::Retryable(format!(
                "snapshot track hash mismatch for epoch {} group {}",
                local_epoch.0, group
            )));
        }

        if track.data.spool_group != group as u64 {
            return Err(TaskOutcome::Retryable(format!(
                "snapshot track spool mismatch for epoch {} group {}: got {}",
                local_epoch.0,
                group,
                track.data.spool_group
            )));
        }

        if track.data.state.is_invalidated() {
            return Err(TaskOutcome::Retryable(format!(
                "snapshot track invalidated for epoch {} group {}",
                local_epoch.0, group
            )));
        }
    }

    Ok(())
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
