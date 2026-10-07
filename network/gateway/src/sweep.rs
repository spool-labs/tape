//! Expired-tape sweep for a gateway's store, run from its maintenance tick

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_node::config::store::GcConfig;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::features::gc::manager::reached_epoch;
use tape_node::features::gc::sweep::{sweep_expired_tapes, GcSweepStats};
use tape_protocol::Api;
use tape_store::TapeStore;
use tracing::{debug, warn};

/// Sweeps expired tapes once per epoch the store has reached, with the node GC's sweep
#[derive(Debug, Default)]
pub struct ExpiredTapeSweep {
    swept_epoch: Option<EpochNumber>,
}

impl ExpiredTapeSweep {
    /// One maintenance tick; a failed sweep is retried on the next
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

        let Some(epoch) = reached_epoch(context) else {
            return;
        };
        let epoch_start_slot = context.state().current.epoch.start_slot;

        let profile = context.params().profile;
        let swept = self
            .sweep(context.store.as_ref(), config, epoch, epoch_start_slot, profile)
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

    /// Sweep unless this epoch is already swept
    pub async fn sweep<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        config: &GcConfig,
        epoch: EpochNumber,
        epoch_start_slot: SlotNumber,
        profile: ErasureCode,
    ) -> Result<Option<GcSweepStats>, NodeError> {
        let is_swept = self.swept_epoch.is_some_and(|swept| swept >= epoch);
        if is_swept {
            return Ok(None);
        }

        let stats = sweep_expired_tapes(store, config, epoch, epoch_start_slot, profile).await?;
        self.swept_epoch = Some(epoch);
        Ok(Some(stats))
    }
}

#[cfg(test)]
mod tests {
    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
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
        let info = CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group: VirtualGroup(1),
            value_hash: Hash::new_unique(),
        };
        let track = info.address();

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch,
                    next_track_number: TrackNumber(1),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("put tape");
        store.put_track(track, info).expect("put track");

        (tape, track)
    }

    // an expired tape goes with its track, and a live tape stays
    #[tokio::test]
    async fn sweeps_expired() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let (expired, expired_track) = put_tape(&store, EpochNumber(2));
        let (live, live_track) = put_tape(&store, EpochNumber(10));
        let mut sweep = ExpiredTapeSweep::default();

        let stats = sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), TAPENET_PROFILE)
            .await
            .expect("sweep")
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
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), TAPENET_PROFILE)
            .await
            .expect("first sweep");
        let (tape, _) = put_tape(&store, EpochNumber(2));

        let repeat = sweep
            .sweep(&store, &config, EpochNumber(3), SlotNumber(0), TAPENET_PROFILE)
            .await
            .expect("repeat sweep");
        assert!(repeat.is_none());
        assert!(store.get_tape(tape).expect("get tape").is_some());

        sweep
            .sweep(&store, &config, EpochNumber(4), SlotNumber(0), TAPENET_PROFILE)
            .await
            .expect("next epoch sweep");
        assert!(store.get_tape(tape).expect("get tape").is_none());
    }
}
