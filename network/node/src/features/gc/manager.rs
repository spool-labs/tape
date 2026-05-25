use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::task::spawn_blocking;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::{TapeStore, ops::MetaOps};

use crate::config::store::GcConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
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
        let mut observed_epoch = state_rx.borrow().epoch();

        catch_up_epochs(&self.context, &self.config, observed_epoch).await?;

        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let current_epoch = self.context.state().epoch();

                    if next_pending_epoch(self.context.store.as_ref(), current_epoch)?.is_some() {
                        catch_up_epochs(&self.context, &self.config, current_epoch).await?;
                    } else {
                        run_epoch_sweep(&self.context, &self.config, current_epoch).await?;
                    }

                    observed_epoch = current_epoch;
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    let current_epoch = state_rx.borrow().epoch();
                    if current_epoch > observed_epoch {
                        catch_up_epochs(&self.context, &self.config, current_epoch).await?;
                        observed_epoch = current_epoch;
                    }
                }
            }
        }
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
        run_epoch_sweep(context, config, epoch).await?;
    }

    Ok(())
}

async fn run_epoch_sweep<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &GcConfig,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    let store = context.store.as_ref();
    store
        .set_gc_started_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_gc_started_epoch: {error}")))?;

    let owned_spools = context.my_spools();
    let sweep_stats = sweep_epoch(store, config, epoch, &owned_spools).await?;

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
        .map_err(|error| NodeError::Store(format!("set_gc_completed_epoch: {error}")))
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

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::EpochNumber;
    use tape_store::{TapeStore, ops::MetaOps};

    use super::{next_pending_epoch, run_epoch_sweep};
    use crate::config::store::GcConfig;
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

        run_epoch_sweep(&context, &config, EpochNumber(3)).await.unwrap();

        assert_eq!(context.store.get_gc_started_epoch().unwrap(), Some(EpochNumber(3)));
        assert_eq!(context.store.get_gc_completed_epoch().unwrap(), Some(EpochNumber(3)));
    }
}
