use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::spool_in_group;
use tape_protocol::Api;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::ObjectInfo;
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::types::{SpoolTaskSummary, SpoolWorkItem};

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    work: SpoolWorkItem,
    cancel: &CancellationToken,
) -> Result<SpoolTaskSummary, NodeError> {

    context
        .store
        .clear_all_pending_recoveries(work.spool_id)
        .map_err(store_error)?;

    let mut cursor = None;
    let mut gaps = 0usize;
    let batch_size = config.scan_batch_size.max(1);

    loop {
        if cancel.is_cancelled() {
            return Ok(SpoolTaskSummary::ScanDone { gaps });
        }

        let tracks = context
            .store
            .iter_tracks_from(cursor, batch_size)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, info) in &tracks {
            if !spool_in_group(work.spool_id, info.spool_group) {
                continue;
            }

            let certified = matches!(
                context.store.get_object_info(*track).map_err(store_error)?,
                Some(ObjectInfo::Valid {
                    certified_epoch: Some(_),
                    ..
                })
            );

            if !certified {
                continue;
            }

            if !context
                .store
                .has_slice(work.spool_id, *track)
                .map_err(store_error)?
            {
                context
                    .store
                    .add_pending_recovery(work.spool_id, *track)
                    .map_err(store_error)?;
                gaps += 1;
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
        yield_now().await;
    }

    info!(spool_id = work.spool_id, epoch = work.epoch.0, gaps, "spool scan complete");
    Ok(SpoolTaskSummary::ScanDone { gaps })
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
    use tape_store::types::{ObjectInfo, Pubkey, TrackInfo};
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn valid_certified(track: Pubkey) -> ObjectInfo {
        ObjectInfo::Valid {
            is_stored: false,
            track_address: track,
            registered_epoch: EpochNumber(1),
            certified_epoch: Some(EpochNumber(2)),
            slot: SlotNumber(3),
        }
    }

    fn track_info(spool: u16) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: SpoolGroup::of(spool),
            original_size: 1024,
            stripe_size: 64,
            stripe_count: 2,
            encoding_type: 0,
            encoding_params: 0,
            commitment: Vec::new(),
        }
    }

    #[test]
    fn scan_targets_only_certified_tracks() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let uncertified = Pubkey::new_unique();

        store.put_track(track, track_info(7)).unwrap();
        store.put_track(uncertified, track_info(7)).unwrap();
        store.put_object_info(track, valid_certified(track)).unwrap();
        store
            .put_object_info(
                uncertified,
                ObjectInfo::Valid {
                    is_stored: false,
                    track_address: uncertified,
                    registered_epoch: EpochNumber(1),
                    certified_epoch: None,
                    slot: SlotNumber(4),
                },
            )
            .unwrap();

        store.clear_all_pending_recoveries(7).unwrap();
        for (track, info) in store.iter_tracks_from(None, 100).unwrap() {
            if tape_core::erasure::spool_in_group(7, info.spool_group) {
                let certified = matches!(
                    store.get_object_info(track).unwrap(),
                    Some(ObjectInfo::Valid {
                        certified_epoch: Some(_),
                        ..
                    })
                );
                if certified && !store.has_slice(7, track).unwrap() {
                    store.add_pending_recovery(7, track).unwrap();
                }
            }
        }

        assert!(store.has_pending_recovery(7, track).unwrap());
        assert!(!store.has_pending_recovery(7, uncertified).unwrap());
    }
}
