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

use crate::core::NodeContext;
use crate::core::PeerHandle;
use crate::TaskOutcome;

pub async fn fetch_commitments<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    local_epoch: EpochNumber,
) -> Result<Vec<Hash>, TaskOutcome> {
    let config = RetryConfig::three();
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

        if track.data.spool_group != SpoolGroup(group as u64) {
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
    let group_total_weight = group_total_weight(committee, group);
    if group_total_weight == 0 {
        return Err(TaskOutcome::Retryable(format!(
            "snapshot group {group} has no weighted committee members",
        )));
    }

    let quorum = min_correct(group_total_weight);
    let mut seen_peer_slice_indices = HashSet::new();
    let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<SocketAddr>)>> =
        HashMap::new();

    for member in committee {
        let member_weight = group_weight(member, group);
        if member_weight == 0 {
            continue;
        }

        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        for &spool in &member.spools {
            if group_for_spool(spool) != group {
                continue;
            }

            let Some(slice_index) = slice_for_spool(group, spool) else {
                continue;
            };
            if !seen_peer_slice_indices.insert((addr, slice_index)) {
                continue;
            }

            // get_slice expects a global spool ID, not a group-relative index.
            match client.get_slice(track, spool).await {
                Ok(data) if !data.is_empty() => {
                    let (weight, peers) = index_votes
                        .entry(slice_index)
                        .or_default()
                        .entry(data)
                        .or_insert_with(|| (0, HashSet::new()));
                    *weight += member_weight;
                    peers.insert(addr);
                }
                Ok(_data) => {
                    tracing::debug!("snapshot slice empty for {addr}: group {group}, slice {slice_index}");
                    if let Err(e) = peer_handle.record_failure(addr).await {
                        tracing::warn!("failed to record peer failure for {addr}: {e}");
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

    let (mut slices, successful_peers) = select_quorum_slices(index_votes, quorum);
    if successful_peers.is_empty() {
        return Err(TaskOutcome::Retryable("snapshot collect no slices reached consensus".into()));
    }

    for addr in successful_peers {
        if let Err(e) = peer_handle.record_success(addr).await {
            tracing::warn!("failed to record peer success for {addr}: {e}");
        }
    }

    if slices.len() < needed {
        return Err(TaskOutcome::Retryable(format!(
            "snapshot collect insufficient quorum-backed slices: {}/{}",
            slices.len(),
            needed
        )));
    }

    slices.sort_unstable_by_key(|(index, _)| *index);
    slices.truncate(needed);
    Ok(slices)
}

fn group_weight(member: &NodeInfo, group: SpoolGroup) -> u64 {
    member
        .spools
        .iter()
        .filter(|&&spool| group_for_spool(spool) == group)
        .count() as u64
}

fn group_total_weight(committee: &[NodeInfo], group: SpoolGroup) -> u64 {
    committee.iter().map(|member| group_weight(member, group)).sum()
}

fn select_quorum_slices(
    index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<SocketAddr>)>>,
    quorum: u64,
) -> (Vec<(usize, Vec<u8>)>, HashSet<SocketAddr>) {
    let mut slices = Vec::new();
    let mut successful_peers = HashSet::new();

    for (slice_index, mut values) in index_votes {
        let mut winner: Option<(u64, Vec<u8>, HashSet<SocketAddr>)> = None;

        for (data, (weight, peers)) in values.drain() {
            match winner {
                None => winner = Some((weight, data, peers)),
                Some((best_weight, _, _)) if weight > best_weight => {
                    winner = Some((weight, data, peers))
                }
                Some(_) => {}
            }
        }

        let Some((weight, data, peers)) = winner else {
            continue;
        };
        if weight < quorum {
            continue;
        }
        slices.push((slice_index, data));
        successful_peers.extend(peers);
    }

    (slices, successful_peers)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn addr_for_octet(value: u8) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), value.into()))
    }

    #[test]
    fn select_quorum_slices_prefers_max_weight_candidate() {
        let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<SocketAddr>)>> =
            HashMap::new();

        index_votes.insert(
            0,
            HashMap::from([
                (vec![0x01], (1, HashSet::from([addr_for_octet(1)]))),
                (vec![0x02], (2, HashSet::from([addr_for_octet(2), addr_for_octet(3)]))),
            ]),
        );

        let (slices, peers) = select_quorum_slices(index_votes, 2);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0, 0);
        assert_eq!(slices[0].1, vec![0x02]);
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn select_quorum_slices_rejects_conflict_without_quorum() {
        let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<SocketAddr>)>> =
            HashMap::new();
        index_votes.insert(
            0,
            HashMap::from([
                (vec![0x01], (1, HashSet::from([addr_for_octet(1)]))),
                (vec![0x02], (1, HashSet::from([addr_for_octet(2)]))),
            ]),
        );

        let (slices, peers) = select_quorum_slices(index_votes, 2);
        assert!(slices.is_empty());
        assert!(peers.is_empty());
    }

    #[test]
    fn group_weight_counts_group_spools() {
        let committee = vec![
            NodeInfo {
                node_id: tape_core::types::NodeId(1),
                node_address: tape_store::types::Pubkey::new([0u8; 32]),
                bls_pubkey: tape_core::bls::BlsPubkey::new_unique(),
                tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
                network_address: tape_core::types::network::NetworkAddress::from("127.0.0.1:10001")
                    .unwrap(),
                spools: vec![0, 1, 2],
            },
            NodeInfo {
                node_id: tape_core::types::NodeId(2),
                node_address: tape_store::types::Pubkey::new([1u8; 32]),
                bls_pubkey: tape_core::bls::BlsPubkey::new_unique(),
                tls_pubkey: tape_store::types::Pubkey::new([1u8; 32]),
                network_address: tape_core::types::network::NetworkAddress::from("127.0.0.1:10002")
                    .unwrap(),
                spools: vec![100, 101],
            },
        ];

        assert_eq!(group_weight(&committee[0], SpoolGroup(0)), 3);
        assert_eq!(group_weight(&committee[1], SpoolGroup(0)), 0);
        assert_eq!(group_total_weight(&committee, SpoolGroup(0)), 3);
    }
}
