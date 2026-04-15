use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_core::track::data::TrackData;
use tape_store::ops::{MetaOps, TrackDataOps};
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::replay::types::{RawTrack, ReplayBatch};
use crate::features::store::apply::apply_slot;
use crate::features::store::util::is_responsible_for_group;

pub struct StoreManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> StoreManager<Db, Cluster, Blockchain> {
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
                }
            }
        }
    }
}

fn persist_batch<Db: Store>(store: &TapeStore<Db>, batch: &ReplayBatch) -> Result<(), NodeError> {
    apply_slot(store, batch.slot, &batch.events)?;
    persist_raw_tracks(store, &batch.raw_tracks)?;

    store
        .set_sync_cursor(batch.slot)
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))
}

fn persist_raw_tracks<Db: Store>(
    store: &TapeStore<Db>,
    raw_tracks: &[RawTrack],
) -> Result<(), NodeError> {
    for raw_track in raw_tracks {
        if !is_responsible_for_group(store, raw_track.spool_group)? {
            continue;
        }

        store
            .put_track_data(raw_track.track, TrackData::Raw(raw_track.data.clone()))
            .map_err(|error| NodeError::Store(format!("put_track_data: {error}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::snapshot::replay::{ReplayTrack, ReplayableEvent};
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TrackNumber};
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_store::ops::{MetaOps, SpoolOps, TrackDataOps};
    use tape_store::TapeStore;

    use super::persist_batch;
    use crate::features::replay::types::{RawTrack, ReplayBatch};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn empty_slots() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(99),
            events: Vec::new(),
            raw_tracks: Vec::new(),
        };

        persist_batch(&store, &batch).unwrap();

        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(99)));
    }

    #[test]
    fn no_partial_cursor() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(77),
            events: vec![ReplayableEvent::Track(ReplayTrack {
                state: CompressedTrack {
                    tape: Address::from([0x11; 32]),
                    key: Hash::default(),
                    track_number: TrackNumber(0),
                    kind: TrackKind::Raw as u64,
                    state: TrackState::Certified as u64,
                    size: StorageUnits(1),
                    spool_group: SpoolGroup::from(0),
                    value_hash: Hash::default(),
                },
                epoch: EpochNumber(1),
                blob: None,
            })],
            raw_tracks: Vec::new(),
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(77)));
    }

    #[test]
    fn persists_raw_writes_for_owned_spools() {
        let store = test_store();
        let track = Address::new_unique();
        let spool_group = SpoolGroup::from(5);
        let raw = vec![1, 2, 3, 4];

        store
            .set_spool_state(
                spool_group.spool_at(0),
                SpoolState::new(SpoolStatus::Active, EpochNumber(1)),
            )
            .unwrap();

        let batch = ReplayBatch {
            slot: SlotNumber(78),
            events: Vec::new(),
            raw_tracks: vec![RawTrack {
                track,
                spool_group,
                data: raw.clone(),
            }],
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_track_data(track).unwrap(), Some(TrackData::Raw(raw)));
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(78)));
    }

    #[test]
    fn skips_raw_writes_for_non_owners() {
        let store = test_store();
        let track = Address::new_unique();
        let spool_group = SpoolGroup::from(6);

        let batch = ReplayBatch {
            slot: SlotNumber(79),
            events: Vec::new(),
            raw_tracks: vec![RawTrack {
                track,
                spool_group,
                data: vec![9, 8, 7],
            }],
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_track_data(track).unwrap(), None);
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(79)));
    }
}
