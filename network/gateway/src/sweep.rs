//! Expired-tape sweep for a gateway's store, run from its maintenance tick

use rpc::Rpc;
use store::Store;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_node::config::store::GcConfig;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::features::gc::manager::at_durable_tip;
use tape_node::features::gc::sweep::{sweep_expired_tapes, GcSweepStats};
use tape_protocol::Api;
use tape_store::TapeStore;
use tracing::{debug, warn};

/// Sweeps expired tapes once per epoch, with the node GC's sweep and at-tip guard
#[derive(Debug, Default)]
pub struct ExpiredTapeSweep {
    swept_epoch: Option<EpochNumber>,
}

impl ExpiredTapeSweep {
    /// One maintenance tick; a deferred or failed sweep is retried on the next
    pub async fn tick<Db, Cluster, Blockchain>(
        &mut self,
        context: &NodeContext<Db, Cluster, Blockchain>,
    ) where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        let config = &context.config.store.gc;
        if !config.enabled {
            return;
        }

        let at_tip = match at_durable_tip(context) {
            Ok(at_tip) => at_tip,
            Err(error) => {
                warn!(%error, "gateway sweep: tip check failed");
                return;
            }
        };
        let (epoch, epoch_start_slot) = {
            let state = context.state();
            (state.epoch(), state.current.epoch.start_slot)
        };

        let swept = self
            .sweep(context.store.as_ref(), config, epoch, epoch_start_slot, at_tip)
            .await;
        match swept {
            Ok(Some(stats)) => debug!(
                %epoch,
                tapes_deleted = stats.tapes_deleted,
                tracks_deleted = stats.tracks_deleted,
                "gateway swept expired tapes"
            ),
            Ok(None) => {}
            Err(error) => warn!(%error, "gateway expired-tape sweep failed"),
        }
    }

    /// Sweep unless this epoch is already swept or the store is behind the tip
    pub async fn sweep<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        config: &GcConfig,
        epoch: EpochNumber,
        epoch_start_slot: SlotNumber,
        at_tip: bool,
    ) -> Result<Option<GcSweepStats>, NodeError> {
        // Behind the tip the epoch stays unswept, so the next tick tries again.
        let is_swept = self.swept_epoch.is_some_and(|swept| swept >= epoch);
        if is_swept || !at_tip {
            return Ok(None);
        }

        let stats = sweep_expired_tapes(store, config, epoch, epoch_start_slot, at_tip).await?;
        self.swept_epoch = Some(epoch);
        Ok(Some(stats))
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_node::config::store::GcConfig;
    use tape_store::ops::{TapeOps, TrackOps};
    use tape_store::types::TapeInfo;
    use tape_store::TapeStore;

    use super::ExpiredTapeSweep;

    /// A tape ending at `end_epoch` holding one track, returned as (tape, track)
    fn put_tape(store: &TapeStore<MemoryStore>, end_epoch: EpochNumber) -> (Address, Address) {
        let tape = Address::new_unique();
        let track = track_pda(tape, TrackNumber(0)).0;

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch,
                    next_track_number: TrackNumber(1),
                },
            )
            .expect("put tape");
        store
            .put_track(
                track,
                CompressedTrack {
                    tape,
                    key: Hash::new_unique(),
                    track_number: TrackNumber(0),
                    kind: TrackKind::Coded as u64,
                    state: TrackState::Certified as u64,
                    size: StorageUnits::from_bytes(1024),
                    group: GroupIndex(1),
                    value_hash: Hash::new_unique(),
                },
            )
            .expect("put track");

        (tape, track)
    }

    // an expired tape and its track go only at the tip, and a live tape stays
    #[tokio::test]
    async fn sweeps_expired() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let (expired, expired_track) = put_tape(&store, EpochNumber(2));
        let (live, live_track) = put_tape(&store, EpochNumber(10));
        let mut sweep = ExpiredTapeSweep::default();

        let behind = sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), false)
            .await
            .expect("sweep behind tip");
        assert!(behind.is_none());
        assert!(store.get_tape(expired).expect("get tape").is_some());

        let stats = sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), true)
            .await
            .expect("sweep at tip")
            .expect("epoch swept");

        assert_eq!((stats.tapes_deleted, stats.tracks_deleted), (1, 1));
        assert!(store.get_tape(expired).expect("get tape").is_none());
        assert!(store.get_track(expired_track).expect("get track").is_none());
        assert!(store.get_tape(live).expect("get tape").is_some());
        assert!(store.get_track(live_track).expect("get track").is_some());
    }

    // a swept epoch is not swept again, and the next epoch is
    #[tokio::test]
    async fn once_per_epoch() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let mut sweep = ExpiredTapeSweep::default();
        sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), true)
            .await
            .expect("first sweep");
        let (tape, _) = put_tape(&store, EpochNumber(2));

        let repeat = sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), true)
            .await
            .expect("repeat sweep");
        assert!(repeat.is_none());
        assert!(store.get_tape(tape).expect("get tape").is_some());

        sweep
            .sweep(&store, &config, EpochNumber(4), SlotNumber(0), true)
            .await
            .expect("next epoch sweep");
        assert!(store.get_tape(tape).expect("get tape").is_none());
    }
}
