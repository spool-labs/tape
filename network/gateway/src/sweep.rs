//! Epoch sweep for a gateway's store, run from its maintenance tick

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::types::EpochNumber;
use tape_node::config::store::GcConfig;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::features::gc::manager::reached_epoch;
use tape_node::features::gc::sweep::{sweep_expired_tapes, sweep_left_tracks, GcSweepStats};
use tape_protocol::Api;
use tape_store::TapeStore;
use tracing::{debug, warn};

/// Once per epoch the store has reached, deletes the tracks of expired tapes
/// and the tracks that left a tape closed, rewound or renewed, with the node
/// GC's passes
#[derive(Debug, Default)]
pub struct EpochSweep {
    swept_epoch: Option<EpochNumber>,
}

impl EpochSweep {
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

        let profile = context.params().profile;
        let swept = self.sweep(context.store.as_ref(), config, epoch, profile).await;
        match swept {
            Ok(Some(stats)) => debug!(
                %epoch,
                tapes_cleared = stats.tapes_cleared,
                tracks_deleted = stats.tracks_deleted,
                "gateway swept its store for the epoch"
            ),
            Ok(None) => {}
            Err(error) => warn!(%error, "gateway epoch sweep failed"),
        }
    }

    /// Sweep unless this epoch is already swept
    pub async fn sweep<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        config: &GcConfig,
        epoch: EpochNumber,
        profile: ErasureCode,
    ) -> Result<Option<GcSweepStats>, NodeError> {
        let is_swept = self.swept_epoch.is_some_and(|swept| swept >= epoch);
        if is_swept {
            return Ok(None);
        }

        let mut stats = sweep_expired_tapes(store, config, epoch, profile).await?;
        stats += sweep_left_tracks(store, config, profile).await?;
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
    use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_node::config::store::GcConfig;
    use tape_store::ops::{TapeOps, TrackOps};
    use tape_store::types::TapeInfo;
    use tape_store::TapeStore;

    use super::EpochSweep;

    /// A track at one number on a tape, returned by its address
    fn put_track(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        tape_number: TapeNumber,
        track_number: TrackNumber,
    ) -> Address {
        let info = CompressedTrack {
            tape,
            tape_number,
            key: Hash::new_unique(),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group: VirtualGroup(1),
            value_hash: Hash::new_unique(),
        };
        let track = info.address();
        store.put_track(track, info).expect("put track");
        track
    }

    /// A tape ending at `end_epoch` holding one track, returned as (tape, track)
    fn put_tape(
        store: &TapeStore<MemoryStore>,
        tape_number: TapeNumber,
        end_epoch: EpochNumber,
    ) -> (Address, Address) {
        let tape = Address::new_unique();
        let track = put_track(store, tape, tape_number, TrackNumber(0));

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: tape_number,
                    flags: 0,
                    end_epoch,
                    next_track_number: TrackNumber(1),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .expect("put tape");

        (tape, track)
    }

    // tracks that left a rewound, a closed and a reserved-again tape go, and
    // the tracks those tapes hold now stay
    #[tokio::test]
    async fn sweeps_left() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let live_until = EpochNumber(10);

        // Rewound: the tree starts at track 1.
        let (rewound, before_rewind) = put_tape(&store, TapeNumber(1), live_until);
        let mut row = store.get_tape(rewound).expect("get tape").expect("tape row");
        row.tree_start = TrackNumber(1);
        row.next_track_number = TrackNumber(2);
        store.put_tape(rewound, row).expect("put tape");
        let after_rewind = put_track(&store, rewound, TapeNumber(1), TrackNumber(1));

        // Closed: the row is gone.
        let (closed, on_closed) = put_tape(&store, TapeNumber(2), live_until);
        store.delete_tape(closed).expect("delete tape");

        // Reserved again: the address holds tape 4, and tape 3's track is left over.
        let (reserved, on_new) = put_tape(&store, TapeNumber(4), live_until);
        let on_old = put_track(&store, reserved, TapeNumber(3), TrackNumber(0));

        let stats = EpochSweep::default()
            .sweep(&store, &config, EpochNumber(3), TAPENET_PROFILE)
            .await
            .expect("sweep")
            .expect("epoch swept");

        assert_eq!(stats.tracks_deleted, 3);
        for gone in [before_rewind, on_closed, on_old] {
            assert!(store.get_track(gone).expect("get track").is_none());
        }
        for kept in [after_rewind, on_new] {
            assert!(store.get_track(kept).expect("get track").is_some());
        }
    }

    // an expired tape loses its track, and a live tape keeps its own
    #[tokio::test]
    async fn sweeps_expired() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let (expired, expired_track) = put_tape(&store, TapeNumber(1), EpochNumber(2));
        let (live, live_track) = put_tape(&store, TapeNumber(2), EpochNumber(10));
        let mut sweep = EpochSweep::default();

        let stats = sweep
            .sweep(&store, &config, EpochNumber(3), TAPENET_PROFILE)
            .await
            .expect("sweep")
            .expect("epoch swept");

        assert_eq!((stats.tapes_cleared, stats.tracks_deleted), (1, 1));
        // The row stays for a renew, and goes when the tape is closed.
        assert!(store.get_tape(expired).expect("get tape").is_some());
        assert!(store.get_track(expired_track).expect("get track").is_none());
        assert!(store.get_tape(live).expect("get tape").is_some());
        assert!(store.get_track(live_track).expect("get track").is_some());
    }

    // a swept epoch is not swept again, and the next epoch is
    #[tokio::test]
    async fn once_per_epoch() {
        let store = TapeStore::new(MemoryStore::new());
        let config = GcConfig::default();
        let mut sweep = EpochSweep::default();
        sweep
            .sweep(&store, &config, EpochNumber(3), TAPENET_PROFILE)
            .await
            .expect("first sweep");
        let (_, track) = put_tape(&store, TapeNumber(1), EpochNumber(2));

        let repeat = sweep
            .sweep(&store, &config, EpochNumber(3), TAPENET_PROFILE)
            .await
            .expect("repeat sweep");
        assert!(repeat.is_none());
        assert!(store.get_track(track).expect("get track").is_some());

        sweep
            .sweep(&store, &config, EpochNumber(4), TAPENET_PROFILE)
            .await
            .expect("next epoch sweep");
        assert!(store.get_track(track).expect("get track").is_none());
    }
}
