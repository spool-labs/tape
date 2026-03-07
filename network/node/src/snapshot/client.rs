use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use tape_protocol::api::{GetSliceReq, GetSnapshotReq};
use store::Store;
use tape_api::program::tapedrive::snapshot_pda;
use tape_api::state::Track;
use tape_core::bft::min_correct;
use tape_core::erasure::{SPOOL_GROUP_COUNT, group_for_spool, slice_for_spool};
use tape_core::spooler::SpoolGroup;
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::hash::Hash;
use tape_protocol::state::ProtocolState;
use tape_store::types::Pubkey;

use crate::core::NodeContext;
use crate::TaskOutcome;

pub async fn fetch_commitments<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    local_epoch: EpochNumber,
) -> Result<Vec<Hash>, TaskOutcome> {
    if state.committee.is_empty() {
        return Err(TaskOutcome::Retryable(
            "snapshot committee is empty".into(),
        ));
    }

    let total = state.committee.len() as u64;
    let quorum = min_correct(total) as usize;
    let mut ballots: HashMap<Vec<Hash>, (usize, Vec<NodeId>)> = HashMap::new();
    let mut valid_peer_answers = 0usize;

    let api = context.peer_manager.api();

    for member in &state.committee {
        if !context.peer_manager.is_healthy(member.id) {
            continue;
        }

        let req = GetSnapshotReq { epoch: local_epoch };
        let result = api.get_snapshot(member.id, &req).await;
        match result {
            Ok(res) if res.commitments.len() == SPOOL_GROUP_COUNT => {
                let entry = ballots.entry(res.commitments.clone()).or_insert((0, Vec::new()));
                entry.0 += 1;
                entry.1.push(member.id);
                valid_peer_answers += 1;
            }
            Ok(_) => {
                context.peer_manager.report_failure(member.id);
            }
            Err(e) => {
                context.peer_manager.report_failure(member.id);
                tracing::debug!(node = member.id.0, "fetch commitments failed: {e}");
            }
        }
    }

    let (consensus_commitments, peer_ids, votes) = match ballots
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

    for node_id in peer_ids {
        context.peer_manager.report_success(node_id);
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

async fn verify_commitments<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
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

pub async fn collect_group_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    group: SpoolGroup,
    track: Pubkey,
    needed: usize,
) -> Result<Vec<(usize, Vec<u8>)>, TaskOutcome> {
    let group_total_weight = compute_group_total_weight(state, group);
    if group_total_weight == 0 {
        return Err(TaskOutcome::Retryable(format!(
            "snapshot group {group} has no weighted committee members",
        )));
    }

    let quorum = min_correct(group_total_weight);
    let mut seen_peer_slice_indices = HashSet::new();
    let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<NodeId>)>> =
        HashMap::new();

    let api = context.peer_manager.api();
    let track_pubkey: tape_crypto::Pubkey = track.into();

    for (member_index, member) in state.committee.iter().enumerate() {
        let member_spools = state.member_spools(member_index);
        let member_weight = member_spools
            .iter()
            .filter(|&&spool| group_for_spool(spool) == group)
            .count() as u64;
        if member_weight == 0 {
            continue;
        }

        if !context.peer_manager.is_healthy(member.id) {
            continue;
        }

        for &spool in &member_spools {
            if group_for_spool(spool) != group {
                continue;
            }

            let Some(slice_index) = slice_for_spool(group, spool) else {
                continue;
            };
            if !seen_peer_slice_indices.insert((member.id, slice_index)) {
                continue;
            }

            let req = GetSliceReq { track: track_pubkey, spool };
            match api.get_slice(member.id, &req).await {
                Ok(res) if !res.data.is_empty() => {
                    let (weight, peers) = index_votes
                        .entry(slice_index)
                        .or_default()
                        .entry(res.data)
                        .or_insert_with(|| (0, HashSet::new()));
                    *weight += member_weight;
                    peers.insert(member.id);
                }
                Ok(_) => {
                    tracing::debug!(node = member.id.0, "snapshot slice empty: group {group}, slice {slice_index}");
                    context.peer_manager.report_failure(member.id);
                }
                Err(e) => {
                    context.peer_manager.report_failure(member.id);
                    tracing::trace!(node = member.id.0, "fetch slice failed: {e}");
                }
            }
        }
    }

    let (mut slices, successful_peers) = select_quorum_slices(index_votes, quorum);
    if successful_peers.is_empty() {
        return Err(TaskOutcome::Retryable("snapshot collect no slices reached consensus".into()));
    }

    for node_id in successful_peers {
        context.peer_manager.report_success(node_id);
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

fn compute_group_total_weight(state: &ProtocolState, group: SpoolGroup) -> u64 {
    (0..state.committee.len())
        .map(|i| {
            state.member_spools(i)
                .iter()
                .filter(|&&spool| group_for_spool(spool) == group)
                .count() as u64
        })
        .sum()
}

fn select_quorum_slices(
    index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<NodeId>)>>,
    quorum: u64,
) -> (Vec<(usize, Vec<u8>)>, HashSet<NodeId>) {
    let mut slices = Vec::new();
    let mut successful_peers = HashSet::new();

    for (slice_index, mut values) in index_votes {
        let mut winner: Option<(u64, Vec<u8>, HashSet<NodeId>)> = None;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::NodeId;

    #[test]
    fn select_quorum_slices_prefers_max_weight_candidate() {
        let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<NodeId>)>> =
            HashMap::new();

        index_votes.insert(
            0,
            HashMap::from([
                (vec![0x01], (1, HashSet::from([NodeId(1)]))),
                (vec![0x02], (2, HashSet::from([NodeId(2), NodeId(3)]))),
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
        let mut index_votes: HashMap<usize, HashMap<Vec<u8>, (u64, HashSet<NodeId>)>> =
            HashMap::new();
        index_votes.insert(
            0,
            HashMap::from([
                (vec![0x01], (1, HashSet::from([NodeId(1)]))),
                (vec![0x02], (1, HashSet::from([NodeId(2)]))),
            ]),
        );

        let (slices, peers) = select_quorum_slices(index_votes, 2);
        assert!(slices.is_empty());
        assert!(peers.is_empty());
    }
}
