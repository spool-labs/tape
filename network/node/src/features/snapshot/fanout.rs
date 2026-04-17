use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_protocol::api::{PushSnapshotFinalizeSigReq, PushSnapshotWriteSigReq};
use tape_protocol::Api;
use tape_retry::RetryConfig;
use tape_store::ops::SnapshotOps;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::snapshot::quorum::{
    bitmap_index_in_group, group_peers, local_write_value_hash, quorum_threshold,
    snapshot_chunk_hash, snapshot_written_hashes,
};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;

const FANOUT_INTERVAL: Duration = Duration::from_secs(30);

pub async fn run<Db, Cluster, Blockchain>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let state = ctx.state();
        let my_node_id = ctx.node_id();
        let threshold = quorum_threshold();

        let written_hashes = match snapshot_written_hashes(&ctx, epoch) {
            Ok(hashes) => hashes,
            Err(error) => {
                debug!(?error, epoch = epoch.0, "snapshot fanout: scan written snapshot tracks failed");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(FANOUT_INTERVAL) => {}
                }
                continue;
            }
        };

        for group in local_groups(&ctx) {
            let Some(local_index) = bitmap_index_in_group(&state, group, my_node_id) else {
                continue;
            };

            let peers: Vec<_> = group_peers(&state, group)
                .into_iter()
                .filter(|peer| peer.node_id != my_node_id)
                .collect();

            let chunks = match ctx.store.iter_snapshot_artifact_chunks(epoch, group) {
                Ok(chunks) => chunks,
                Err(error) => {
                    debug!(?error, epoch = epoch.0, group = group.0, "snapshot fanout: iter artifact chunks failed");
                    continue;
                }
            };

            let mut local_chunks = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                let local_value_hash = match local_write_value_hash(&ctx, epoch, group, chunk, local_index) {
                    Ok(Some(hash)) => hash,
                    Ok(None) => continue,
                    Err(error) => {
                        debug!(?error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot fanout: decode local write vote failed");
                        continue;
                    }
                };
                local_chunks.push((chunk, local_value_hash));
            }

            for (chunk, local_value_hash) in &local_chunks {
                if written_hashes
                    .get(&snapshot_chunk_hash(epoch, group, *chunk))
                    .is_some_and(|written_hash| *written_hash == *local_value_hash)
                {
                    continue;
                }

                let local_vote = match ctx
                    .store
                    .get_snapshot_write_sig(epoch, group, *chunk, local_index)
                {
                    Ok(Some(vote)) => vote,
                    Ok(None) => continue,
                    Err(error) => {
                        debug!(?error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot fanout: get local write sig failed");
                        continue;
                    }
                };

                let sig_count = match ctx.store.iter_snapshot_write_sigs(epoch, group, *chunk) {
                    Ok(sigs) => sigs
                        .into_iter()
                        .filter(|(_, vote)| vote.message == local_vote.message)
                        .count(),
                    Err(error) => {
                        debug!(?error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot fanout: iter write sigs failed");
                        continue;
                    }
                };
                if sig_count >= threshold {
                    continue;
                }

                let req = PushSnapshotWriteSigReq {
                    node_id: my_node_id,
                    message: local_vote.message,
                    signature: local_vote.signature,
                };

                for peer in &peers {
                    if cancel.is_cancelled() {
                        return;
                    }

                    let _ = call_peer(
                        &ctx.peer_manager,
                        RetryConfig::none(),
                        peer.node_id,
                        Some(&cancel),
                        || ctx.api.push_snapshot_write_sig(peer.node_id, &req),
                    )
                    .await;
                }
            }

            let finalize_sig_count = match ctx.store.count_snapshot_finalize_sigs(epoch, group) {
                Ok(count) => count,
                Err(error) => {
                    debug!(?error, epoch = epoch.0, group = group.0, "snapshot fanout: count finalize sigs failed");
                    continue;
                }
            };
            if finalize_sig_count < threshold
                && !local_chunks.is_empty()
                && local_chunks.iter().all(|(chunk, local_value_hash)| {
                    written_hashes
                        .get(&snapshot_chunk_hash(epoch, group, *chunk))
                        .is_some_and(|written_hash| *written_hash == *local_value_hash)
                })
            {
                let local_vote = match ctx
                    .store
                    .get_snapshot_finalize_sig(epoch, group, local_index)
                {
                    Ok(Some(vote)) => vote,
                    Ok(None) => continue,
                    Err(error) => {
                        debug!(?error, epoch = epoch.0, group = group.0, "snapshot fanout: get local finalize sig failed");
                        continue;
                    }
                };
                let req = PushSnapshotFinalizeSigReq {
                    node_id: my_node_id,
                    message: local_vote.message,
                    signature: local_vote.signature,
                };
                for peer in &peers {
                    if cancel.is_cancelled() {
                        return;
                    }
                    let _ = call_peer(
                        &ctx.peer_manager,
                        RetryConfig::none(),
                        peer.node_id,
                        Some(&cancel),
                        || ctx.api.push_snapshot_finalize_sig(peer.node_id, &req),
                    )
                    .await;
                }
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(FANOUT_INTERVAL) => {}
        }
    }
}

fn local_groups<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Vec<SpoolGroup> {
    let mut groups: Vec<_> = ctx.my_spools().into_iter().map(SpoolGroup::of).collect();
    groups.sort_unstable_by_key(|group| group.0);
    groups.dedup_by_key(|group| group.0);
    groups
}
