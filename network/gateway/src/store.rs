use std::sync::Arc;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::core::types::ChannelName;
use tape_node::features::replay::types::ReplayBatch;
use tape_node::features::store::manager::{
    maintenance_ticker, persist_batch_with, settle_maintenance, tick_maintenance,
    MaintenancePass, RawTrackPolicy,
};
use tape_protocol::Api;
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::blacklist::denials::GatewayBlacklists;
use crate::blacklist::follow::BlacklistFollow;
use crate::cache::GatewaySliceCache;
use crate::sweep::ExpiredTapeSweep;

pub struct GatewayStoreManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<ReplayBatch>,
    follow: BlacklistFollow<Db, Cluster, Blockchain>,
    sweep: ExpiredTapeSweep,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api, Blockchain: Rpc> GatewayStoreManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        rx: mpsc::Receiver<ReplayBatch>,
        slice_cache: Arc<GatewaySliceCache<Db>>,
        blacklists: Arc<GatewayBlacklists>,
        cancel: CancellationToken,
    ) -> Self {
        let follow = BlacklistFollow::new(context.clone(), slice_cache, blacklists);
        Self {
            context,
            rx,
            follow,
            sweep: ExpiredTapeSweep::default(),
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut pass = None;
        let result = self.drive(&mut pass).await;
        settle_maintenance(pass).await;
        result
    }

    async fn drive(&mut self, pass: &mut Option<MaintenancePass>) -> Result<(), NodeError> {
        // Live blocks wait in the channel until the batches already in the store
        // are applied, so every list applies its batches in order.
        self.follow.catch_up(&self.cancel).await?;

        // The same read-write volume a node opens, through the same
        // `build_context` and ingesting the same blocks. The gateway runs no GC
        // manager, so without this nothing here drives maintenance at all.
        let mut ticker = maintenance_ticker();

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
                    self.follow.apply_block(&batch, &self.cancel).await?;

                    self.context.pending.drop_slot(batch.slot);
                }

                _ = ticker.tick() => {
                    tick_maintenance(&self.context.store, pass).await;
                    self.sweep.tick(&self.context).await;
                    self.follow.report();
                }
            }
        }
    }
}

/// Every raw track is kept: a gateway serves reads for all of them
pub fn persist_batch<Db: Store>(
    store: &TapeStore<Db>,
    batch: &ReplayBatch,
) -> Result<(), NodeError> {
    persist_batch_with(store, batch, RawTrackPolicy::All)
}

#[cfg(test)]
mod tests {
    use tape_store_memory::MemoryStore;
    use tape_core::spooler::GroupIndex;
    use tape_core::track::data::BlobData;
    use tape_core::types::SlotNumber;
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
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
            blockhash: Hash::new_unique(),
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
