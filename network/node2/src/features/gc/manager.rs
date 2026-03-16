use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tape_store::TapeStore;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::config::GcConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::gc::sweep::sweep_epoch;

pub struct GcManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: GcConfig,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> GcManager<Db, Cluster, Blockchain> {
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
            scan_interval = ?self.config.scan_interval,
            "gc manager started"
        );

        if !self.config.enabled {
            self.cancel.cancelled().await;
            return Ok(());
        }

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;

        catch_up_epochs(self.context.store.as_ref(), &self.config, observed_epoch).await?;

        let mut ticker = interval(self.config.scan_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let current_epoch = self.context.state().epoch;

                    if next_pending_epoch(self.context.store.as_ref(), current_epoch)?.is_some() {
                        catch_up_epochs(self.context.store.as_ref(), &self.config, current_epoch).await?;
                    } else {
                        run_epoch_sweep(self.context.store.as_ref(), &self.config, current_epoch).await?;
                    }

                    observed_epoch = current_epoch;
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    let current_epoch = state_rx.borrow().epoch;
                    if current_epoch > observed_epoch {
                        catch_up_epochs(self.context.store.as_ref(), &self.config, current_epoch).await?;
                        observed_epoch = current_epoch;
                    }
                }
            }
        }
    }
}

/// Run any epoch sweeps still owed according to the persisted GC progress markers.
async fn catch_up_epochs<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    target_epoch: EpochNumber,
) -> Result<(), NodeError> {

    while let Some(epoch) = next_pending_epoch(store, target_epoch)? {
        run_epoch_sweep(store, config, epoch).await?;
    }

    Ok(())
}

async fn run_epoch_sweep<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    store
        .set_gc_started_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_gc_started_epoch: {error}")))?;

    sweep_epoch(store, config, epoch).await?;

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
        (_, Some(completed)) => EpochNumber(completed.0.saturating_add(1)),
        (None, None) => current_epoch,
    };

    if candidate > current_epoch {
        Ok(None)
    } else {
        Ok(Some(candidate))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use store_memory::MemoryStore;
    use tape_core::types::EpochNumber;
    use tape_store::ops::MetaOps;
    use tape_store::TapeStore;

    use super::{next_pending_epoch, run_epoch_sweep};
    use crate::core::config::GcConfig;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn test_config() -> GcConfig {
        GcConfig {
            enabled: true,
            scan_interval: Duration::from_secs(60),
            track_batch_size: 2,
            slice_batch_size: 2,
        }
    }

    #[test]
    fn next_pending_epoch_without_markers_starts_at_current_epoch() {
        let store = test_store();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(8))
        );
    }

    #[test]
    fn next_pending_epoch_prefers_incomplete_started_epoch() {
        let store = test_store();

        store.set_gc_started_epoch(EpochNumber(5)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(4)).unwrap();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(5))
        );
    }

    #[test]
    fn next_pending_epoch_advances_from_completed_epoch() {
        let store = test_store();

        store.set_gc_started_epoch(EpochNumber(6)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(6)).unwrap();

        assert_eq!(
            next_pending_epoch(&store, EpochNumber(8)).unwrap(),
            Some(EpochNumber(7))
        );
    }

    #[tokio::test]
    async fn gc_completed_epoch_advances_after_successful_sweep() {
        let store = test_store();
        let config = test_config();

        run_epoch_sweep(&store, &config, EpochNumber(3)).await.unwrap();

        assert_eq!(store.get_gc_started_epoch().unwrap(), Some(EpochNumber(3)));
        assert_eq!(store.get_gc_completed_epoch().unwrap(), Some(EpochNumber(3)));
    }
}
