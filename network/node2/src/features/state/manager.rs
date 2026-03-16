use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::snapshot::ReplayableEvent;
use tape_protocol::Api;
use tape_store::ops::{MetaOps, SliceOps, TrackOps};
use tape_store::types::Pubkey;
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::channels::send_spool_event;
use crate::core::config::StateConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::replay::types::ReplayBatch;
use crate::features::spool::types::SpoolEvent;
use crate::features::state::apply::apply_slot;

pub struct StateManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: StateConfig,
    rx: mpsc::Receiver<ReplayBatch>,
    spool_tx: mpsc::Sender<SpoolEvent>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> StateManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: StateConfig,
        rx: mpsc::Receiver<ReplayBatch>,
        spool_tx: mpsc::Sender<SpoolEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            spool_tx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "state manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(batch) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::StateManager })
                        };
                    };

                    persist_batch(self.context.store.as_ref(), &batch)?;
                    self.emit_missing_certified_slices(&batch).await?;
                }
            }
        }
    }

    async fn emit_missing_certified_slices(&self, batch: &ReplayBatch) -> Result<(), NodeError> {
        let owned_spools = self.context.my_spools();
        if owned_spools.is_empty() {
            return Ok(());
        }

        for event in &batch.events {
            let ReplayableEvent::CertifyTrack { track, .. } = event else {
                continue;
            };

            let track_key = Pubkey(*track);
            let Some(track_info) = self.context.store.get_track(track_key).map_err(store_error)? else {
                continue;
            };

            for spool_id in owned_spools.iter().copied() {
                if !track_info.spool_group.contains(spool_id) {
                    continue;
                }

                if self
                    .context
                    .store
                    .has_slice(spool_id, track_key)
                    .map_err(store_error)?
                {
                    continue;
                }

                send_spool_event(
                    &self.spool_tx,
                    SpoolEvent::MissingCertifiedSlice {
                        spool_id,
                        track: track_key,
                    },
                )
                .await?;
            }
        }

        Ok(())
    }
}

fn persist_batch<Db: Store>(store: &TapeStore<Db>, batch: &ReplayBatch) -> Result<(), NodeError> {
    apply_slot(store, batch.slot, &batch.events)?;

    store.set_sync_cursor(batch.slot).map_err(|error| {
        NodeError::Store(format!("set_sync_cursor: {error}"))
    })
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
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
    fn persist_batch_advances_cursor_for_empty_slots() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(99),
            events: Vec::new(),
        };

        persist_batch(&store, &batch).unwrap();

        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(99)));
    }

    #[test]
    fn persist_batch_only_advances_cursor_after_successful_apply() {
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
