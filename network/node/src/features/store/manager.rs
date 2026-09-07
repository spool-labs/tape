use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent};
use tape_core::track::data::BlobData;
use tape_core::types::SlotNumber;
use tape_protocol::Api;
use tape_store::ops::{MetaOps, TrackDataOps};
use tape_store::TapeStore;
use tokio::sync::mpsc;
use tokio::task::{spawn_blocking, JoinError, JoinHandle};
use tokio::time::{interval, Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::context::NodeContext;
use crate::core::atlas::short_label;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::replay::types::{RawTrack, ReplayBatch};
use crate::features::store::apply::apply_slot;
use crate::features::store::util::is_responsible_for_group;

/// How often the store is asked for one maintenance pass
///
/// A pass rewrites at most one segment, so the cadence is what decides whether
/// owed work drains; the engine's operating guide calls for a one-second loop,
/// and each pass paces itself against the volume's own rates, so this is a
/// cadence and not a budget.
const MAINTAIN_INTERVAL: Duration = Duration::from_secs(1);

/// A maintenance pass running on the blocking pool
pub type MaintenancePass = JoinHandle<store::Result<()>>;

/// The timer a store manager drives its maintenance from
///
/// The engine starts no threads of its own, so a volume nobody ticks never
/// compacts, merges or prunes its graves, and grows without bound.
pub fn maintenance_ticker() -> Interval {
    let mut ticker = interval(MAINTAIN_INTERVAL);
    // A pass that outran its slot owes one pass, not every slot it sat through.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

/// Start a pass unless one is still running, and report whatever the last returned
///
/// A backend that keeps its own housekeeping threads, and a volume opened
/// read-only, both answer this without work, so the tick costs them one call.
pub async fn tick_maintenance<Db: Store + 'static>(
    store: &Arc<TapeStore<Db>>,
    pass: &mut Option<MaintenancePass>,
) {
    // Ingest must never queue behind maintenance, so a pass that outlasts its
    // slot keeps running and the tick is dropped rather than waited on.
    if pass.as_ref().is_some_and(|running| !running.is_finished()) {
        debug!("store maintenance still running, tick skipped");
        return;
    }

    // Finished if it is here at all, so this resolves without yielding.
    if let Some(done) = pass.take() {
        settle(done.await);
    }

    let store = store.clone();
    // Off the runtime: a pass is bounded in bytes rather than wall clock, so on
    // a paced volume it blocks for as long as the bytes it moves owe.
    *pass = Some(spawn_blocking(move || store.inner().inner().maintain()));
}

/// Wait out a pass still running, leaving the volume idle for whatever comes next
pub async fn settle_maintenance(pass: Option<MaintenancePass>) {
    if let Some(running) = pass {
        settle(running.await);
    }
}

/// Space is best effort: a failed pass is retried on the next tick and kills nothing
fn settle(joined: Result<store::Result<()>, JoinError>) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "store maintenance pass failed"),
        Err(source) => warn!(%source, "store maintenance pass panicked"),
    }
}

pub struct StoreManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api, Blockchain: Rpc> StoreManager<Db, Cluster, Blockchain> {
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
        let mut pass = None;
        let result = self.drive(&mut pass).await;

        // However the loop ended, the volume is left idle. The shutdown
        // checkpoint takes its own cue and claims the compaction plane, which a
        // pass still running would be holding.
        settle_maintenance(pass).await;

        result
    }

    async fn drive(&mut self, pass: &mut Option<MaintenancePass>) -> Result<(), NodeError> {
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
                    self.feed_atlas(&batch);

                    self.context.pending
                        .drop_slot(batch.slot);
                }

                _ = ticker.tick() => tick_maintenance(&self.context.store, pass).await,
            }
        }
    }

    /// Surface freshly stored user objects to the atlas display. This runs on
    /// the live tail only, so bootstrap replay never floods the feed.
    fn feed_atlas(&self, batch: &ReplayBatch) {
        if !self.context.atlas.enabled() {
            return;
        }
        for record in &batch.records {
            let ReplayableEvent::Track(replay) = &record.event else {
                continue;
            };
            let Some(object) = replay.object.as_ref() else {
                continue;
            };
            let (track, _) = track_pda(replay.state.tape, replay.state.track_number);
            self.context.atlas.push_object(
                short_label(&track.to_string()),
                object.logical_size.0,
                object.content_type.to_str(),
            );
        }
    }
}

/// Which raw track payloads to keep when persisting a replay batch
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawTrackPolicy {
    /// Keep payloads for groups this node owns spools in
    OwnedGroups,
    /// Keep every payload, for stores rebuilt outside a running node
    All,
}

pub fn persist_batch<Db: Store>(
    store: &TapeStore<Db>,
    batch: &ReplayBatch,
) -> Result<(), NodeError> {
    persist_batch_with(store, batch, RawTrackPolicy::OwnedGroups)
}

pub fn persist_batch_with<Db: Store>(
    store: &TapeStore<Db>,
    batch: &ReplayBatch,
    policy: RawTrackPolicy,
) -> Result<(), NodeError> {
    apply_records(store, batch.slot, batch.block_time, &batch.records)?;
    persist_raw_tracks(store, &batch.raw_tracks, policy)?;

    store
        .set_sync_cursor(batch.slot, Some(batch.blockhash))
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))
}

fn apply_records<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    block_time: Option<i64>,
    records: &[ReplayRecord],
) -> Result<(), NodeError> {
    let events: Vec<_> = records.iter().map(|record| record.event.clone()).collect();
    apply_slot(store, slot, block_time, &events)
}

fn persist_raw_tracks<Db: Store>(
    store: &TapeStore<Db>,
    raw_tracks: &[RawTrack],
    policy: RawTrackPolicy,
) -> Result<(), NodeError> {
    for raw_track in raw_tracks {
        if policy == RawTrackPolicy::OwnedGroups && !is_responsible_for_group(store, raw_track.group)? {
            continue;
        }

        store
            .put_track_data(raw_track.track, BlobData::Inline(raw_track.data.clone()))
            .map_err(|error| NodeError::Store(format!("put_track_data: {error}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayTrack, ReplayableEvent};
    use tape_core::spooler::GroupIndex;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TrackNumber};
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_crypto::address::Address;
    use tape_crypto::tx::Txid;
    use tape_crypto::Hash;
    use tape_store::ops::{MetaOps, SpoolOps, TrackDataOps};
    use tape_store::TapeStore;

    use super::persist_batch;
    use crate::features::replay::types::{RawTrack, ReplayBatch};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
        }
    }

    #[test]
    fn empty_slots() {
        let store = test_store();
        let batch = ReplayBatch {
            slot: SlotNumber(99),
            blockhash: Hash::new_unique(),
            block_time: None,
            records: Vec::new(),
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
            blockhash: Hash::new_unique(),
            block_time: None,
            records: vec![record(ReplayableEvent::Track(ReplayTrack {
                state: CompressedTrack {
                    tape: Address::from([0x11; 32]),
                    key: Hash::default(),
                    track_number: TrackNumber(0),
                    kind: TrackKind::Inline as u64,
                    state: TrackState::Certified as u64,
                    size: StorageUnits(1),
                    group: GroupIndex::from(0),
                    value_hash: Hash::default(),
                },
                epoch: EpochNumber(1),
                blob: None,
                object: None,
            }))],
            raw_tracks: Vec::new(),
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(77)));
    }

    #[test]
    fn persists_raw_writes_for_owned_spools() {
        let store = test_store();
        let track = Address::new_unique();
        let group = GroupIndex::from(5);
        let raw = vec![1, 2, 3, 4];

        store
            .set_spool_state(
                group.spool_at(0),
                SpoolState::new(SpoolStatus::Active, EpochNumber(1)),
            )
            .unwrap();

        let batch = ReplayBatch {
            slot: SlotNumber(78),
            blockhash: Hash::new_unique(),
            block_time: None,
            records: Vec::new(),
            raw_tracks: vec![RawTrack {
                track,
                group,
                data: raw.clone(),
            }],
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_track_data(track).unwrap(), Some(BlobData::Inline(raw)));
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(78)));
    }

    #[test]
    fn skips_raw_writes_for_non_owners() {
        let store = test_store();
        let track = Address::new_unique();
        let group = GroupIndex::from(6);

        let batch = ReplayBatch {
            slot: SlotNumber(79),
            blockhash: Hash::new_unique(),
            block_time: None,
            records: Vec::new(),
            raw_tracks: vec![RawTrack {
                track,
                group,
                data: vec![9, 8, 7],
            }],
        };

        persist_batch(&store, &batch).unwrap();
        assert_eq!(store.get_track_data(track).unwrap(), None);
        assert_eq!(store.get_sync_cursor().unwrap(), Some(SlotNumber(79)));
    }
}
