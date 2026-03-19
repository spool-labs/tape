use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::config::StoreConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::replay::types::ReplayBatch;
use crate::features::store::apply::apply_slot;

pub struct StoreManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: StoreConfig,
    rx: mpsc::Receiver<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> StoreManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: StoreConfig,
        rx: mpsc::Receiver<ReplayBatch>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "store manager started"
        );

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
                }
            }
        }
    }
}

fn persist_batch<Db: Store>(store: &TapeStore<Db>, batch: &ReplayBatch) -> Result<(), NodeError> {
    apply_slot(store, batch.slot, &batch.events)?;

    store
        .set_sync_cursor(batch.slot)
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::SlotNumber;
    use tape_store::ops::MetaOps;
    use tape_store::TapeStore;

    use super::persist_batch;
    use crate::features::replay::types::ReplayBatch;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn empty_slots() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(99),
            events: Vec::new(),
        };

        persist_batch(&store, &batch).unwrap();

        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(99)));
    }

    #[test]
    fn no_partial_cursor() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(77),
            events: vec![ReplayableEvent::RegisterTrack {
                track: [0x11; 32],
                event_data: vec![0x22; 8],
            }],
        };

        assert!(persist_batch(&store, &batch).is_err());
        assert_eq!(store.get_sync_cursor().unwrap(), None);
    }
}
