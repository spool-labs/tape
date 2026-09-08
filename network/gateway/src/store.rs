use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent};
use tape_core::track::data::BlobData;
use tape_core::types::SlotNumber;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::core::types::ChannelName;
use tape_node::features::replay::types::{RawTrack, ReplayBatch};
use tape_node::features::store::apply::apply_slot;
use tape_protocol::Api;
use tape_store::ops::{MetaOps, TrackDataOps};
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct GatewayStoreManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> GatewayStoreManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        rx: mpsc::Receiver<ReplayBatch>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),

                received = self.rx.recv() => {
                    let Some(batch) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::StoreManager })
                        };
                    };

                    persist_batch(self.context.store.as_ref(), &batch)?;

                    self.context.pending.drop_slot(batch.slot);
                }
            }
        }
    }
}

pub fn persist_batch<Db: Store>(
    store: &TapeStore<Db>,
    batch: &ReplayBatch,
) -> Result<(), NodeError> {
    apply_records(store, batch.slot, batch.block_time, &batch.records)?;
    persist_raw_tracks(store, &batch.raw_tracks)?;

    store
        .set_sync_cursor(batch.slot)
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))
}

fn apply_records<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    block_time: Option<i64>,
    records: &[ReplayRecord],
) -> Result<(), NodeError> {
    let events: Vec<ReplayableEvent> = records.iter().map(|record| record.event.clone()).collect();
    apply_slot(store, slot, block_time, &events)
}

fn persist_raw_tracks<Db: Store>(
    store: &TapeStore<Db>,
    raw_tracks: &[RawTrack],
) -> Result<(), NodeError> {
    for raw_track in raw_tracks {
        store
            .put_track_data(raw_track.track, BlobData::Inline(raw_track.data.clone()))
            .map_err(|error| NodeError::Store(format!("put_track_data: {error}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::spooler::GroupIndex;
    use tape_core::track::data::BlobData;
    use tape_core::types::SlotNumber;
    use tape_crypto::address::Address;
    use tape_node::features::replay::types::{RawTrack, ReplayBatch};
    use tape_store::ops::{MetaOps, TrackDataOps};
    use tape_store::TapeStore;

    use super::persist_batch;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn persists_all_raw_tracks_without_spool_ownership() {
        let store = test_store();
        let first = Address::new_unique();
        let second = Address::new_unique();

        let batch = ReplayBatch {
            slot: SlotNumber(42),
            block_time: None,
            records: Vec::new(),
            raw_tracks: vec![
                RawTrack {
                    track: first,
                    group: GroupIndex::from(0),
                    data: vec![1, 2, 3],
                },
                RawTrack {
                    track: second,
                    group: GroupIndex::from(19),
                    data: vec![4, 5, 6],
                },
            ],
        };

        persist_batch(&store, &batch).unwrap();

        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(42)));
        assert_eq!(
            store.get_track_data(first).unwrap(),
            Some(BlobData::Inline(vec![1, 2, 3]))
        );
        assert_eq!(
            store.get_track_data(second).unwrap(),
            Some(BlobData::Inline(vec![4, 5, 6]))
        );
    }
}
