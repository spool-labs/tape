use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Pubkey;
use tape_protocol::{Api, fetch::fetch_state};
use tape_retry::retry_if;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::EpochManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;

pub struct EpochHandlers<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochManagerConfig,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> EpochHandlers<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: EpochManagerConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn handle_advance_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        let previous_epoch = self.context.state().epoch;
        let context = self.context.clone();
        let state = retry_if(
            self.config.state_retry.clone(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move { fetch_state(&context.rpc).await }
            },
            |error| error.is_retriable() && !error.is_skipped_slot(),
        )
        .await
        .map_err(NodeError::from)?;

        if state.epoch < epoch {
            return Err(NodeError::StateUnavailable {
                expected_epoch: epoch,
            });
        }

        self.context.set_state(state)?;
        if epoch > previous_epoch {
            self.context.metrics.inc_epoch_transitions();
        }

        if let Err(error) = self.context.refresh_peers().await {
            warn!(error = %error, epoch = epoch.0, "peer refresh failed after epoch advance");
        }

        info!(epoch = epoch.0, "published protocol state");
        Ok(())
    }

    pub async fn handle_sync_epoch(&self, epoch: EpochNumber, phase: u64) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.epoch == epoch {
            if let Ok(phase) = EpochPhase::try_from(phase) {
                if phase != state.phase {
                    self.context.update_phase(phase)?;
                }
            }
        }

        debug!(epoch = epoch.0, "received sync epoch");
        Ok(())
    }

    pub async fn handle_advance_pool(
        &self,
        node: Pubkey,
        epoch: EpochNumber,
        phase: u64,
    ) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.epoch == epoch {
            if let Ok(phase) = EpochPhase::try_from(phase) {
                if phase != state.phase {
                    self.context.update_phase(phase)?;
                }
            }
        }

        debug!(node = %node, epoch = epoch.0, "received advance pool");
        Ok(())
    }

    pub async fn handle_join_network(&self, node_id: NodeId) -> Result<(), NodeError> {
        debug!(node_id = node_id.0, "received join network");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;
    use tape_retry::RetryConfig;
    use tokio_util::sync::CancellationToken;

    use super::EpochHandlers;
    use crate::chain::submit_advance_epoch;
    use crate::core::config::EpochManagerConfig;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn publishes_state() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(20)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = EpochHandlers::new(ctx.clone(), manager_config(), CancellationToken::new());

        submit_advance_epoch(&ctx)
            .await
            .expect("submit advance epoch");

        handlers
            .handle_advance_epoch(EPOCH + EpochNumber(1))
            .await
            .expect("handle advance epoch");

        let state = ctx.state();
        assert_eq!(state.epoch, EPOCH + EpochNumber(1));
        assert_eq!(state.phase, EpochPhase::Syncing);
    }

    #[tokio::test]
    async fn sync_phase() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = EpochHandlers::new(ctx.clone(), manager_config(), CancellationToken::new());

        handlers
            .handle_sync_epoch(EPOCH, EpochPhase::Settling as u64)
            .await
            .expect("handle sync epoch");
        assert_eq!(ctx.state().phase, EpochPhase::Settling);

        handlers
            .handle_sync_epoch(EPOCH + EpochNumber(1), EpochPhase::Active as u64)
            .await
            .expect("ignore mismatched epoch");
        assert_eq!(ctx.state().phase, EpochPhase::Settling);
    }

    #[tokio::test]
    async fn pool_phase() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = EpochHandlers::new(ctx.clone(), manager_config(), CancellationToken::new());

        handlers
            .handle_advance_pool(
                harness.node(NODE).node_address,
                EPOCH,
                EpochPhase::Active as u64,
            )
            .await
            .expect("handle advance pool");
        assert_eq!(ctx.state().phase, EpochPhase::Active);

        handlers
            .handle_advance_pool(
                harness.node(NODE).node_address,
                EPOCH + EpochNumber(1),
                EpochPhase::Syncing as u64,
            )
            .await
            .expect("ignore mismatched epoch");
        assert_eq!(ctx.state().phase, EpochPhase::Active);
    }

    fn manager_config() -> EpochManagerConfig {
        EpochManagerConfig {
            state_retry: RetryConfig::none(),
        }
    }
}
