use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::system::SpoolState;
use tape_core::types::NodeId;
use tape_crypto::Pubkey;
use tape_protocol::{Api, ProtocolState};
use tape_protocol::api::{GetSliceReq, RepairReq, RepairRes};
use tape_slicer::{ClayCoder, ErasureCoder, RepairPlan, Slicer, SliceIndex, SliceMetadata};
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::peer_call::call_peer;
use crate::features::spool::repair::{build_per_helper_requests, persist_validated_slice};
use crate::features::spool::types::{SpoolTaskSummary, SpoolWorkItem};

struct GroupPeers {
    current: HashMap<SpoolIndex, NodeId>,
    previous: HashMap<SpoolIndex, NodeId>,
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    work: SpoolWorkItem,
    cancel: &CancellationToken,
) -> Result<SpoolTaskSummary, NodeError> {

    let spool_state = match context.store.get_spool_state(work.spool_id).map_err(store_error)? {
        Some(state) => state,
        None => return Ok(SpoolTaskSummary::RecoverDone { remaining: 0 }),
    };

    let peers = build_peer_maps(&context.state(), &spool_state, work.spool_id);
    let batch_size = config.recover_batch_size.max(1);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let pending = context
            .store
            .iter_pending_recoveries(work.spool_id, batch_size)
            .map_err(store_error)?;

        if pending.is_empty() {
            break;
        }

        let mut recovered = 0usize;

        for track in pending {
            if cancel.is_cancelled() {
                break;
            }

            match recover_track(&context, config, &peers, work.spool_id, track, cancel).await? {
                TrackRecovery::Recovered | TrackRecovery::AlreadyPresent | TrackRecovery::Stale => {
                    context
                        .store
                        .remove_pending_recovery(work.spool_id, track)
                        .map_err(store_error)?;
                    recovered += 1;
                }
                TrackRecovery::Pending => {}
            }
        }

        if recovered == 0 {
            break;
        }
    }

    let remaining = context
        .store
        .iter_pending_recoveries(work.spool_id, usize::MAX)
        .map_err(store_error)?
        .len();

    info!(spool_id = work.spool_id, epoch = work.epoch.0, remaining, "spool recovery pass complete");

    Ok(SpoolTaskSummary::RecoverDone { remaining })
}

enum TrackRecovery {
    Recovered,
    AlreadyPresent,
    Stale,
    Pending,
}

async fn recover_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    peers: &GroupPeers,
    spool_id: SpoolIndex,
    track: StorePubkey,
    cancel: &CancellationToken,
) -> Result<TrackRecovery, NodeError> {

    if context.store.has_slice(spool_id, track).map_err(store_error)? {
        return Ok(TrackRecovery::AlreadyPresent);
    }

    let Some(track_info) = context.store.get_track(track).map_err(store_error)? else {
        return Ok(TrackRecovery::Stale);
    };

    let object = context.store
        .get_object_info(track)
        .map_err(store_error)?;

    if !matches!(
        object,
        Some(ObjectInfo::Valid {
            certified_epoch: Some(_),
            ..
        })
    ) {
        return Ok(TrackRecovery::Stale);
    }

    if let Some(recovered) =
        try_repair(context, config, peers, spool_id, track, &track_info, cancel).await?
    {
        persist_validated_slice(
            context.store.as_ref(),
            spool_id,
            track,
            &track_info,
            recovered,
        )
        .map_err(|error| NodeError::Store(error.to_string()))?;
        return Ok(TrackRecovery::Recovered);
    }

    if let Some(recovered) =
        try_full_recovery(context, config, peers, spool_id, track, &track_info, cancel).await?
    {
        persist_validated_slice(
            context.store.as_ref(),
            spool_id,
            track,
            &track_info,
            recovered,
        )
        .map_err(|error| NodeError::Store(error.to_string()))?;
        return Ok(TrackRecovery::Recovered);
    }

    Ok(TrackRecovery::Pending)
}

async fn try_repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    peers: &GroupPeers,
    spool_id: SpoolIndex,
    track: StorePubkey,
    track_info: &TrackInfo,
    cancel: &CancellationToken,
) -> Result<Option<Vec<u8>>, NodeError> {
    let profile = track_info.profile();
    if !profile.is_clay() {
        return Ok(None);
    }

    let spool_group = SpoolGroup::of(spool_id);
    let Some(lost_index) = spool_group.slice_of(spool_id) else {
        return Ok(None);
    };

    let available: Vec<SliceIndex> = peers
        .current
        .keys()
        .chain(peers.previous.keys())
        .filter_map(|peer_spool| spool_group.slice_of(*peer_spool))
        .map(SliceIndex::new)
        .collect();

    if available.is_empty() {
        return Ok(None);
    }

    let coder = ClayCoder::from_params(profile.clay_params());
    let slicer = Slicer::with_profile(coder, track_info.stripe_size as usize, true, profile);
    let plan = match slicer.repair_plan_from_params(
        SliceIndex::new(lost_index),
        &available,
        track_info.original_size as usize,
        track_info.stripe_size as usize,
    ) {
        Ok(plan) => plan,
        Err(_) => return Ok(None),
    };

    let helper_requests = build_per_helper_requests(&plan, spool_group);
    if helper_requests.is_empty() {
        return Ok(None);
    }

    let mut helper_data = HashMap::new();
    for (helper_index, stripes) in helper_requests {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        let helper_spool = spool_group.spool_at(*helper_index);
        let request = RepairReq {
            track: Pubkey::from(track),
            helper_spool,
            stripes,
        };

        if let Some(response) = request_repair_data(context, config, peers, track, helper_spool, request, cancel).await? {
            helper_data.insert(helper_index, response.data);
        }
    }

    let required: Vec<SliceIndex> = required_helper_indices(&plan);
    if !required.iter().all(|helper| helper_data.contains_key(helper)) {
        return Ok(None);
    }

    let metadata = SliceMetadata::with_profile(
        track_info.original_size as usize,
        track_info.stripe_size as usize,
        profile,
    );

    match slicer.repair(&plan, &helper_data, &metadata.to_bytes()) {
        Ok(recovered) => Ok(Some(recovered)),
        Err(error) => {
            debug!(
                spool_id,
                track = %Pubkey::from(track),
                error = %error,
                "clay repair failed"
            );
            Ok(None)
        }
    }
}

async fn try_full_recovery<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    peers: &GroupPeers,
    spool_id: SpoolIndex,
    track: StorePubkey,
    track_info: &TrackInfo,
    cancel: &CancellationToken,
) -> Result<Option<Vec<u8>>, NodeError> {
    let profile = track_info.profile();
    if !profile.is_clay() {
        return Ok(None);
    }

    let spool_group = SpoolGroup::of(spool_id);
    let Some(lost_index) = spool_group.slice_of(spool_id) else {
        return Ok(None);
    };

    let coder = ClayCoder::from_params(profile.clay_params());
    let mut slicer = Slicer::with_profile(coder, track_info.stripe_size as usize, true, profile);
    let needed = slicer.k();

    let mut peer_spools: Vec<SpoolIndex> = peers
        .current
        .keys()
        .chain(peers.previous.keys())
        .copied()
        .collect();
    peer_spools.sort_unstable();
    peer_spools.dedup();

    let mut full_slices = Vec::with_capacity(needed);
    for peer_spool in peer_spools {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        if full_slices.len() >= needed {
            break;
        }

        let request = GetSliceReq {
            track: Pubkey::from(track),
            spool: peer_spool,
        };

        if let Some(data) =
            request_full_slice(context, config, peers, track, peer_spool, request, cancel).await?
        {
            if let Some(slice_index) = spool_group.slice_of(peer_spool) {
                full_slices.push((SliceIndex::new(slice_index), data));
            }
        }
    }

    if full_slices.len() < needed {
        warn!(
            spool_id,
            track = %Pubkey::from(track),
            got = full_slices.len(),
            need = needed,
            "insufficient full slices for fallback"
        );
        return Ok(None);
    }

    let metadata = SliceMetadata::from_slice(&full_slices[0].1)
        .map_err(|error| NodeError::Store(format!("parse slice metadata: {error}")))?;

    slicer.set_chunk_index(metadata.chunk_index);

    let slice_refs: Vec<(usize, &[u8])> = full_slices
        .iter()
        .map(|(index, data)| (**index, data.as_slice()))
        .collect();

    let decoded = match slicer.decode(&slice_refs) {
        Ok(decoded) => decoded,
        Err(error) => {
            debug!(
                spool_id,
                track = %Pubkey::from(track),
                error = %error,
                "decode fallback failed"
            );
            return Ok(None);
        }
    };

    let reencoded = match slicer.encode(&decoded) {
        Ok(reencoded) => reencoded,
        Err(error) => {
            debug!(
                spool_id,
                track = %Pubkey::from(track),
                error = %error,
                "re-encode fallback failed"
            );
            return Ok(None);
        }
    };

    Ok(reencoded.get(lost_index).cloned())
}

async fn request_repair_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    peers: &GroupPeers,
    track: StorePubkey,
    helper_spool: SpoolIndex,
    request: RepairReq,
    cancel: &CancellationToken,
) -> Result<Option<RepairRes>, NodeError> {
    for peer_node in peer_candidates(peers, helper_spool) {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        if !context.peer_manager.is_healthy(peer_node) {
            continue;
        }

        let response = call_peer(
            context.peer_manager.as_ref(),
            config.peer_retry.clone(),
            peer_node,
            Some(cancel),
            || {
                let api = context.api.clone();
                let request = request.clone();
                async move { api.repair(peer_node, &request).await }
            },
        )
        .await;

        match response {
            Ok(response) if !response.data.is_empty() => return Ok(Some(response)),
            Ok(_) => {
                debug!(
                    track = %Pubkey::from(track),
                    helper_spool,
                    peer = peer_node.0,
                    "empty repair response"
                );
            }
            Err(error) => {
                debug!(
                    track = %Pubkey::from(track),
                    helper_spool,
                    peer = peer_node.0,
                    error = %error,
                    "repair request failed"
                );
            }
        }
    }

    Ok(None)
}

async fn request_full_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    peers: &GroupPeers,
    track: StorePubkey,
    peer_spool: SpoolIndex,
    request: GetSliceReq,
    cancel: &CancellationToken,
) -> Result<Option<Vec<u8>>, NodeError> {
    for peer_node in peer_candidates(peers, peer_spool) {
        if cancel.is_cancelled() {
            return Ok(None);
        }

        if !context.peer_manager.is_healthy(peer_node) {
            continue;
        }

        let response = call_peer(
            context.peer_manager.as_ref(),
            config.peer_retry.clone(),
            peer_node,
            Some(cancel),
            || {
                let api = context.api.clone();
                let request = request.clone();
                async move { api.get_slice(peer_node, &request).await }
            },
        )
        .await;

        match response {
            Ok(response) if !response.data.is_empty() => return Ok(Some(response.data)),
            Ok(_) => {
                debug!(
                    track = %Pubkey::from(track),
                    peer_spool,
                    peer = peer_node.0,
                    "empty get_slice response"
                );
            }
            Err(error) => {
                debug!(
                    track = %Pubkey::from(track),
                    peer_spool,
                    peer = peer_node.0,
                    error = %error,
                    "get_slice request failed"
                );
            }
        }
    }

    Ok(None)
}

fn build_peer_maps(
    protocol_state: &ProtocolState,
    spool_state: &SpoolState,
    spool_id: SpoolIndex
) -> GroupPeers {

    let spool_group = SpoolGroup::of(spool_id);
    let current = protocol_state
        .group_peers(spool_group)
        .into_iter()
        .filter(|(peer_spool, _)| *peer_spool != spool_id)
        .collect();

    let mut previous = HashMap::new();

    for (index, helper) in spool_state.prev_helpers.iter().enumerate().take(SPOOL_GROUP_SIZE) {
        let peer_spool = spool_group.spool_at(index);
        if peer_spool == spool_id {
            continue;
        }
        if let Some(node_id) = helper {
            previous.insert(peer_spool, *node_id);
        }
    }

    GroupPeers { current, previous }
}

fn peer_candidates(peers: &GroupPeers, peer_spool: SpoolIndex) -> Vec<NodeId> {
    let mut output = Vec::new();

    if let Some(node_id) = peers.previous.get(&peer_spool) {
        output.push(*node_id);
    }

    if let Some(node_id) = peers.current.get(&peer_spool) {
        if output.last().copied() != Some(*node_id) {
            output.push(*node_id);
        }
    }

    output
}

fn required_helper_indices(plan: &RepairPlan) -> Vec<SliceIndex> {
    let mut required = Vec::new();
    for stripe_repair in &plan.stripes {
        for helper in &stripe_repair.helpers {
            if !required.contains(&helper.slice) {
                required.push(helper.slice);
            }
        }
    }
    required
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
