use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::NodeId;
use tape_crypto::Pubkey;
use tape_protocol::{Api, ApiError};
use tape_protocol::api::SyncReq;
use tape_retry::Retryable;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::peer_call::call_peer;
use crate::features::spool::repair::validate_slice_entry;
use crate::features::spool::types::{SpoolTaskSummary, SpoolWorkItem};

enum SyncSource {
    VerifyLocal,
    SyncFrom { node_id: NodeId },
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    work: SpoolWorkItem,
    cancel: &CancellationToken,
) -> Result<SpoolTaskSummary, NodeError> {

    let spool_state = match context.store
        .get_spool_state(work.spool_id)
        .map_err(store_error)? {
        Some(state) => state,
        None => return Ok(SpoolTaskSummary::SyncUnavailable),
    };

    let source = match spool_state.prev_owner {
        None => SyncSource::VerifyLocal,
        Some(node_id) if node_id == context.node_id() => SyncSource::VerifyLocal,
        Some(node_id) => SyncSource::SyncFrom { node_id },
    };

    match source {
        SyncSource::VerifyLocal => {
            info!(spool_id = work.spool_id, epoch = work.epoch.0, "spool sync verified locally");
            Ok(SpoolTaskSummary::SyncDone)
        }
        SyncSource::SyncFrom { node_id } => {
            let mut cursor = context
                .store
                .get_spool_sync_cursor(work.spool_id)
                .map_err(store_error)?
                .map(|track| track.0);
            let limit = config.sync_batch_size.clamp(1, 1000) as u32;

            loop {
                if cancel.is_cancelled() {
                    return Ok(SpoolTaskSummary::SyncUnavailable);
                }

                let request = SyncReq {
                    spool_index: work.spool_id,
                    cursor,
                    limit,
                };

                let response = call_peer(
                    context.peer_manager.as_ref(),
                    config.peer_retry.clone(),
                    node_id,
                    Some(cancel),
                    || {
                        let api = context.api.clone();
                        let request = request.clone();
                        async move { api.sync(node_id, &request).await }
                    },
                )
                .await;

                let response = match response {
                    Ok(response) => response,
                    Err(error) if error.is_retryable() || is_sync_unavailable(&error) => {
                        warn!(
                            spool_id = work.spool_id,
                            epoch = work.epoch.0,
                            peer = node_id.0,
                            error = %error,
                            "spool sync peer unavailable"
                        );
                        return Ok(SpoolTaskSummary::SyncUnavailable);
                    }
                    Err(error) => {
                        warn!(
                            spool_id = work.spool_id,
                            epoch = work.epoch.0,
                            peer = node_id.0,
                            error = %error,
                            "spool sync peer failed permanently"
                        );
                        return Ok(SpoolTaskSummary::SyncUnavailable);
                    }
                };

                if response.entries.is_empty() && response.next_cursor.is_none() {
                    break;
                }

                let mut last_track = None;
                for entry in &response.entries {
                    if cancel.is_cancelled() {
                        return Ok(SpoolTaskSummary::SyncUnavailable);
                    }

                    let track = StorePubkey(entry.track_address);
                    last_track = Some(track);

                    if context
                        .store
                        .has_slice(work.spool_id, track)
                        .map_err(store_error)?
                    {
                        continue;
                    }

                    if let Some(track_info) = context.store.get_track(track).map_err(store_error)? {
                        if let Err(reason) =
                            validate_slice_entry(work.spool_id, &track_info, &entry.slice_data)
                        {
                            debug!(
                                spool_id = work.spool_id,
                                track = %Pubkey::from(track),
                                reason,
                                "skipping invalid synced slice"
                            );
                            continue;
                        }
                    }

                    context
                        .store
                        .put_slice(work.spool_id, track, entry.slice_data.clone())
                        .map_err(store_error)?;
                }

                if let Some(track) = last_track {
                    context
                        .store
                        .set_spool_sync_cursor(work.spool_id, track)
                        .map_err(store_error)?;
                }

                match response.next_cursor {
                    Some(next_cursor) => cursor = Some(next_cursor),
                    None => break,
                }
            }

            info!(spool_id = work.spool_id, epoch = work.epoch.0, peer = node_id.0, "spool sync complete");

            Ok(SpoolTaskSummary::SyncDone)
        }
    }
}

fn is_sync_unavailable(error: &ApiError) -> bool {
    matches!(error, ApiError::NotResponsible | ApiError::NotFound)
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
