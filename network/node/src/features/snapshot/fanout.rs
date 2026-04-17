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
use crate::features::snapshot::quorum::{bitmap_index_in_group, group_peers, quorum_threshold};
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

        for group in local_groups(&ctx) {
            let Some(local_index) = bitmap_index_in_group(&state, group, my_node_id) else {
                continue;
            };

            let peers: Vec<_> = group_peers(&state, group)
                .into_iter()
                .filter(|peer| peer.node_id != my_node_id)
                .collect();

            if let Ok(artifacts) = ctx.store.iter_snapshot_artifacts(epoch, group) {
                for (chunk, artifact) in artifacts {
                    if artifact.is_written() {
                        continue;
                    }

                    let sig_count = match ctx.store.count_snapshot_write_sigs(epoch, group, chunk) {
                        Ok(count) => count,
                        Err(error) => {
                            debug!(?error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot fanout: count write sigs failed");
                            continue;
                        }
                    };
                    if sig_count >= threshold {
                        continue;
                    }

                    let local_sig = match ctx.store.iter_snapshot_write_sigs(epoch, group, chunk) {
                        Ok(sigs) => sigs.into_iter().find(|(bitmap_index, _)| *bitmap_index == local_index),
                        Err(error) => {
                            debug!(?error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot fanout: iter write sigs failed");
                            continue;
                        }
                    };
                    let Some((_, signature)) = local_sig else {
                        continue;
                    };

                    let req = PushSnapshotWriteSigReq {
                        epoch,
                        group,
                        chunk,
                        node_id: my_node_id,
                        signature,
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
            }

            let finalize_sig_count = match ctx.store.count_snapshot_finalize_sigs(epoch, group) {
                Ok(count) => count,
                Err(error) => {
                    debug!(?error, epoch = epoch.0, group = group.0, "snapshot fanout: count finalize sigs failed");
                    continue;
                }
            };
            if finalize_sig_count < threshold {
                let local_sig = match ctx.store.iter_snapshot_finalize_sigs(epoch, group) {
                    Ok(sigs) => sigs.into_iter().find(|(bitmap_index, _)| *bitmap_index == local_index),
                    Err(error) => {
                        debug!(?error, epoch = epoch.0, group = group.0, "snapshot fanout: iter finalize sigs failed");
                        continue;
                    }
                };
                if let Some((_, signature)) = local_sig {
                    let req = PushSnapshotFinalizeSigReq {
                        epoch,
                        group,
                        node_id: my_node_id,
                        signature,
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
