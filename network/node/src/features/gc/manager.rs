use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::task::spawn_blocking;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::types::{EpochNumber, SlotNumber, SpoolIndex};
use tape_protocol::Api;
use tape_crypto::Address;
use tape_store::{TapeStore, ops::{ChallengeOps, MetaOps}};

use crate::config::store::GcConfig;
use crate::context::NodeContext;
use crate::features::challenge::fold::holds_spool;
use crate::features::store::cleanup::sweep_deleted_slices;
use crate::core::error::NodeError;
use crate::core::ingest::{AT_TIP_THRESHOLD_SLOTS, IngestState};
use crate::core::types::ServiceName;
use crate::features::gc::sweep::sweep_epoch;

pub struct GcManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: GcConfig,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api, Blockchain: Rpc> GcManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: GcConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            enabled = self.config.enabled,
            interval_secs = self.config.interval_secs,
            "gc manager started"
        );

        if !self.config.enabled {
            self.cancel.cancelled().await;
            return Ok(());
        }

        let mut state_rx = self.context.subscribe_state();
        let mut store_rx = self.context.store_epoch.subscribe();
        let mut observed_epoch = reached_epoch(&self.context);

        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let reached = reached_epoch(&self.context);
                    run_tick(&self.context, &self.config, reached).await?;
                    observed_epoch = reached;
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    observed_epoch = self.sweep_new_epochs(observed_epoch).await?;
                }
                changed = store_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    observed_epoch = self.sweep_new_epochs(observed_epoch).await?;
                }
            }
        }
    }

    /// Sweep the epochs reached since `observed`, and return the latest one reached
    async fn sweep_new_epochs(
        &self,
        observed: Option<EpochNumber>,
    ) -> Result<Option<EpochNumber>, NodeError> {
        let reached = reached_epoch(&self.context);
        let Some(epoch) = reached.filter(|epoch| Some(*epoch) > observed) else {
            return Ok(observed);
        };

        catch_up_epochs(&self.context, &self.config, epoch).await?;
        Ok(reached)
    }
}

/// The latest epoch the cleanup may sweep, which both the store and the
/// protocol state have reached. Once the store has applied the block that
/// begins an epoch it holds every event before it, and an expired tape cannot
/// be extended, so nothing later can save a tape that ends by then. Spool
/// ownership comes from the state, so the state has to be there too.
pub fn reached_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Option<EpochNumber> {
    let store_epoch = context.store_epoch.current()?;
    Some(store_epoch.min(context.state().epoch()))
}

async fn run_tick<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &GcConfig,
    reached: Option<EpochNumber>,
) -> Result<(), NodeError> {
    // every tick, since a deleted track only has to outlive the rounds that can still ask for it
    let slot = SlotNumber(context.ingest.progress().last_dispatched_slot());
    match sweep_deleted_slices(context.store.as_ref(), slot, context.params().profile) {
        Ok(swept) if swept.tracks > 0 => debug!(
            tracks = swept.tracks,
            slices = swept.slices,
            "gc: forgot deleted tracks past the round horizon"
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "gc: deleted-track sweep failed"),
    }

    // Only epochs the markers still owe. A completed epoch is never swept twice.
    match reached {
        Some(epoch) => catch_up_epochs(context, config, epoch).await,
        None => Ok(()),
    }
}

/// Run any epoch sweeps still owed according to the persisted GC progress markers.
async fn catch_up_epochs<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &GcConfig,
    target_epoch: EpochNumber,
) -> Result<(), NodeError> {
    let store = context.store.as_ref();

    while let Some(epoch) = next_pending_epoch(store, target_epoch)? {
        // a sweep behind the tip leaves its epoch owed, so the next tick retries it
        if !run_epoch_sweep(context, config, epoch).await? {
            break;
        }
    }

    Ok(())
}

/// Sweep one epoch and report whether it finished as a completed epoch.
async fn run_epoch_sweep<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &GcConfig,
    epoch: EpochNumber,
) -> Result<bool, NodeError> {
    let store = context.store.as_ref();
    let at_tip = at_durable_tip(context.as_ref())?;
    // Behind the tip a certify or a registration the chain holds may be missing
    // from the store, so the track and slice sweeps would act on stale rows; the
    // epoch stays owed and the next tick runs it once the store catches up.
    if !at_tip {
        return Ok(false);
    }

    store
        .set_gc_started_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_gc_started_epoch: {error}")))?;

    let owned_spools = context.my_spools();
    // Tombstones from this sweep carry the live epoch's start slot even when
    // catching up an older epoch: a later slot only keeps an entry in the
    // sample set longer, which is the safe direction.
    let epoch_start_slot = context.state().current.epoch.start_slot;
    let sweep_stats = sweep_epoch(
        store,
        config,
        epoch,
        epoch_start_slot,
        &owned_spools,
        context.pending.as_ref(),
        at_tip,
        context.params().profile,
    )
    .await?;
    prune_handed_off_records(context.as_ref())?;
    debug!(
        node_id = context.node_id().0,
        epoch = epoch.0,
        at_tip,
        tapes_deleted = sweep_stats.tapes_deleted,
        tracks_deleted = sweep_stats.tracks_deleted,
        slices_deleted = sweep_stats.slices_deleted,
        "gc sweep complete"
    );

    if should_reclaim(config, sweep_stats.slices_deleted) {
        context.set_reclaim_pending(true);
        let store = context.store.clone();
        let reclaim_result = spawn_blocking(move || store.inner().inner().reclaim_space()).await;
        context.set_reclaim_pending(false);

        match reclaim_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(epoch = epoch.0, error = %error, "gc reclaim failed");
            }
            Err(source) => {
                return Err(NodeError::ServiceJoin {
                    service: ServiceName::GcManager,
                    source,
                });
            }
        }
    }

    store
        .set_gc_completed_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_gc_completed_epoch: {error}")))?;

    Ok(true)
}

/// Whether the store has applied blocks to within the at-tip threshold of the finalized tip
pub fn at_durable_tip<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Result<bool, NodeError> {
    if matches!(context.ingest_state(), IngestState::Stalled { .. }) {
        return Ok(false);
    }

    let finalized_tip = context.ingest.progress().last_known_tip();
    if finalized_tip == u64::MAX {
        return Ok(false);
    }

    let Some(cursor) = context
        .store
        .get_sync_cursor()
        .map_err(|error| NodeError::Store(format!("get_sync_cursor: {error}")))?
    else {
        return Ok(false);
    };

    Ok(finalized_tip.saturating_sub(cursor.0) <= AT_TIP_THRESHOLD_SLOTS)
}

fn next_pending_epoch<Db: Store>(
    store: &TapeStore<Db>,
    current_epoch: EpochNumber,
) -> Result<Option<EpochNumber>, NodeError> {
    let started = store
        .get_gc_started_epoch()
        .map_err(|error| NodeError::Store(format!("get_gc_started_epoch: {error}")))?;
    let completed = store
        .get_gc_completed_epoch()
        .map_err(|error| NodeError::Store(format!("get_gc_completed_epoch: {error}")))?;

    let candidate = match (started, completed) {
        (Some(started), Some(completed)) if started.0 > completed.0 => started,
        (Some(started), None) => started,
        (_, Some(completed)) => completed.next(),
        (None, None) => current_epoch,
    };

    if candidate > current_epoch {
        Ok(None)
    } else {
        Ok(Some(candidate))
    }
}

fn should_reclaim(config: &GcConfig, deleted_slices: usize) -> bool {
    deleted_slices >= config.reclaim_min_deleted_slices
}

/// Drop the records of spools their owners have handed on.
///
/// A record is keyed by spool, so a reassignment leaves the old owner's history
/// behind. Reads already ignore those, but nothing would ever remove them, and
/// the column would grow by a row per spool per owner it ever had.
fn prune_handed_off_records<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Result<(), NodeError> {
    let state = context.state();
    let stale: Vec<(Address, SpoolIndex)> = context
        .store
        .iter_peer_records()
        .map_err(|error| NodeError::Store(format!("iter_peer_records: {error}")))?
        .into_iter()
        .map(|((peer, spool), _)| (peer, spool))
        .filter(|(peer, spool)| !holds_spool(&state, *peer, *spool))
        .collect();

    for (peer, spool) in &stale {
        context
            .store
            .delete_spool_record(*peer, *spool)
            .map_err(|error| NodeError::Store(format!("delete_spool_record: {error}")))?;
    }
    if !stale.is_empty() {
        debug!(dropped = stale.len(), "gc: dropped records for handed-off spools");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytemuck::Zeroable;
    use tape_store_memory::MemoryStore;
    use tape_core::challenge::PeerRecord;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent};
    use tape_core::system::NodePreferences;
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        EpochNumber, SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber,
    };
    use tape_crypto::tx::Txid;
    use tape_crypto::{Address, Hash};
    use tape_store::types::TapeInfo;
    use tape_store::{TapeStore, ops::{ChallengeOps, MetaOps, TapeOps}};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{GcManager, next_pending_epoch, reached_epoch, run_epoch_sweep, run_tick};
    use crate::config::store::GcConfig;
    use crate::features::replay::types::ReplayBatch;
    use crate::features::store::manager::StoreManager;
    use crate::harness::{NodeHarness, TestContext};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    /// Put the node's ingest and store cursor at the finalized tip.
    fn put_at_durable_tip(context: &TestContext) {
        let tip = SlotNumber(100);
        context.ingest.progress().record_tip(tip.0);
        context.store.set_sync_cursor(tip, None).expect("set sync cursor");
    }

    fn test_config() -> GcConfig {
        GcConfig {
            enabled: true,
            interval_secs: 60,
            track_batch: 2,
            slice_batch: 2,
            reclaim_min_deleted_slices: 20,
        }
    }

    #[test]
    fn no_markers() {
        let store = test_store();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(8))
        );
    }

    #[test]
    fn prefers_started() {
        let store = test_store();

        store.set_gc_started_epoch(EpochNumber(5)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(4)).unwrap();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(5))
        );
    }

    #[test]
    fn from_completed() {
        let store = test_store();

        store.set_gc_started_epoch(EpochNumber(6)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(6)).unwrap();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(7))
        );
    }

    #[tokio::test]
    async fn marks_complete() {
        let context = test_context().await;
        let config = test_config();
        put_at_durable_tip(&context);

        run_epoch_sweep(&context, &config, EpochNumber(3)).await.unwrap();

        assert_eq!(context.store.get_gc_started_epoch().unwrap(), Some(EpochNumber(3)));
        assert_eq!(context.store.get_gc_completed_epoch().unwrap(), Some(EpochNumber(3)));
    }

    #[tokio::test]
    async fn sweep_behind_tip_leaves_epoch_owed() {
        let context = test_context().await;
        let config = test_config();
        let epoch = EpochNumber(3);
        let peer = Address::new_unique();

        // a record no owner answers for is dropped by a sweep that ran
        context
            .store
            .put_peer_record(peer, SpoolIndex(0), PeerRecord::default())
            .expect("put peer record");

        run_epoch_sweep(&context, &config, epoch)
            .await
            .expect("sweep behind the tip");

        assert_eq!(context.store.get_gc_started_epoch().expect("started"), None);
        assert_eq!(context.store.get_gc_completed_epoch().expect("completed"), None);
        assert_eq!(context.store.iter_peer_records().expect("records").len(), 1);
        assert_eq!(
            next_pending_epoch(&context.store, epoch).expect("pending"),
            Some(epoch)
        );

        put_at_durable_tip(&context);
        run_epoch_sweep(&context, &config, epoch)
            .await
            .expect("sweep at the tip");

        assert_eq!(context.store.get_gc_completed_epoch().expect("completed"), Some(epoch));
        assert!(context.store.iter_peer_records().expect("records").is_empty());
        assert_eq!(next_pending_epoch(&context.store, epoch).expect("pending"), None);
    }

    #[tokio::test]
    async fn repeat_tick_in_one_epoch_runs_no_sweep() {
        let context = test_context().await;
        let config = test_config();
        let epoch = context.state().epoch();
        put_at_durable_tip(&context);
        let peer = Address::new_unique();
        let spool = SpoolIndex(0);

        // a record no owner answers for survives a tick only when that tick ran no sweep
        context
            .store
            .put_peer_record(peer, spool, PeerRecord::default())
            .expect("put peer record");

        run_tick(&context, &config, Some(epoch)).await.expect("first tick");

        assert_eq!(context.store.get_gc_completed_epoch().expect("completed"), Some(epoch));
        assert_eq!(next_pending_epoch(&context.store, epoch).expect("pending"), None);
        assert!(context.store.iter_peer_records().expect("records").is_empty());

        context
            .store
            .put_peer_record(peer, spool, PeerRecord::default())
            .expect("put peer record again");

        run_tick(&context, &config, Some(epoch)).await.expect("second tick");

        assert_eq!(context.store.iter_peer_records().expect("records").len(), 1);
    }

    #[tokio::test]
    async fn startup_defers_catch_up_sweep_until_tick_or_epoch_change() {
        let context = test_context().await;
        let config = test_config();
        let cancel = CancellationToken::new();

        let task = tokio::spawn(
            GcManager::new(context.clone(), config, cancel.clone()).run(),
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        task.await.unwrap().unwrap();

        assert_eq!(context.store.get_gc_started_epoch().unwrap(), None);
        assert_eq!(context.store.get_gc_completed_epoch().unwrap(), None);
    }

    // the cleanup has no epoch until the store's is known, and then takes the
    // lower of the store's and the state's
    #[tokio::test]
    async fn reached_is_the_lower() {
        let context = test_context().await;
        let epoch = context.state().epoch();

        assert_eq!(reached_epoch(&context), None);

        context.store_epoch.reach(epoch.prev());
        assert_eq!(reached_epoch(&context), Some(epoch.prev()));

        context.store_epoch.reach(epoch.next());
        assert_eq!(reached_epoch(&context), Some(epoch));
    }

    fn put_tape(context: &TestContext, end_epoch: EpochNumber) -> Address {
        let tape = Address::new_unique();
        context
            .store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch,
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("put tape");
        tape
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord { tx_id: Txid::default(), actor: None, event }
    }

    // a tape extended in the block that turns the epoch outlives the cleanup,
    // which waits for the store to apply that block
    #[tokio::test]
    async fn waits_for_the_store() {
        let context = test_context().await;
        let cancel = CancellationToken::new();
        let before = context.state().epoch();
        let epoch = before.next();
        let later = epoch.next();
        put_at_durable_tip(&context);
        context.store_epoch.reach(before);

        let extended = put_tape(&context, epoch);
        let expired = put_tape(&context, epoch);

        let (batch_tx, batch_rx) = mpsc::channel(1);
        let store = tokio::spawn(
            StoreManager::new(context.clone(), batch_rx, cancel.clone()).run(),
        );
        let cleanup = tokio::spawn(
            GcManager::new(context.clone(), test_config(), cancel.clone()).run(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The state shows the new epoch before the store has applied its block.
        let mut state = (*context.state()).clone();
        state.current.epoch.id = epoch;
        context.set_state(state).expect("publish");
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(context.store.get_tape(extended).expect("get tape").is_some());
        assert!(context.store.get_tape(expired).expect("get tape").is_some());

        // The block holds the extension and then the advance.
        let block = ReplayBatch {
            slot: SlotNumber(100),
            blockhash: Hash::default(),
            block_time: None,
            records: vec![
                record(ReplayableEvent::ExtendTape {
                    tape: extended,
                    capacity: StorageUnits::mb(1),
                    expiry_epoch: later,
                    cost: TAPE(0),
                    burned: TAPE(0),
                    scheduled: TAPE(0),
                }),
                record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: before,
                    new_epoch: epoch,
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
            ],
            raw_tracks: Vec::new(),
            advanced_epoch: Some(epoch),
        };
        batch_tx.send(block).await.expect("send the block");

        for _ in 0..40 {
            if context.store.get_tape(expired).expect("get tape").is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel.cancel();
        store.await.expect("store joins").expect("store ok");
        cleanup.await.expect("cleanup joins").expect("cleanup ok");

        assert!(context.store.get_tape(expired).expect("get tape").is_none());
        let kept = context.store.get_tape(extended).expect("get tape").expect("tape kept");
        assert_eq!(kept.end_epoch, later);
    }
}
