use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::event::NodeJoinedCommittee;
use tape_core::erasure::MEMBER_COUNT;
use tape_core::system::{Committee, CommitteeMember, EpochPhase};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::EpochNumber;
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
        node: tape_crypto::Pubkey,
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

    pub async fn handle_join_network(&self, event: NodeJoinedCommittee) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();
        let expected_activation_epoch = state.epoch + EpochNumber(1);

        if event.activation_epoch != expected_activation_epoch {
            debug!(
                node_id = event.id.0,
                current_epoch = state.epoch.0,
                activation_epoch = event.activation_epoch.0,
                "ignoring join network for stale epoch"
            );
            return Ok(());
        }

        let member = CommitteeMember {
            id: event.id,
            stake: Coin::<TAPE>::new(u64::from_le_bytes(event.stake)),
            key: event.key,
            blacklist: event.blacklist,
            preferences: event.preferences,
            weight: 0,
        };

        if let Some((_, existing)) = state.find_member_next(event.id) {
            if *existing == member {
                debug!(node_id = event.id.0, "join network already applied");
                return Ok(());
            }
        }

        let mut committee_next = Committee::<MEMBER_COUNT>::from_members(&state.committee_next);
        if let Err(error) = committee_next.try_join(&member) {
            warn!(?error, node_id = event.id.0, "join network state diverged, refetching");
            let context = self.context.clone();
            let fresh = retry_if(
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
            self.context.set_state(fresh)?;
            return Ok(());
        }

        state.committee_next = committee_next.iter().copied().collect();
        self.context.set_state(state)?;

        if let Err(error) = self.context.refresh_peers().await {
            warn!(error = %error, node_id = event.id.0, "peer refresh failed after join network");
        }

        debug!(node_id = event.id.0, "received join network");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tape_api::event::NodeJoinedCommittee;
    use tape_core::system::NodePreferences;
    use tape_core::system::EpochPhase;
    use tape_core::types::coin::TAPE;
    use tape_core::types::EpochNumber;
    use tape_core::types::StorageUnits;
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

    #[tokio::test]
    async fn join_updates_committee_next() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .next_committee_size(0)
            .build()
            .await
            .expect("build harness");

        let ctx = harness.ctx_for(NODE);

        let handlers = EpochHandlers::new(ctx.clone(), manager_config(), CancellationToken::new());

        let joined = harness.node(NODE);
        let key = joined
            .bls_keypair()
            .public_key()
            .expect("bls public key");

        let preferences = NodePreferences {
            storage_capacity: StorageUnits::mb(123),
            storage_price: TAPE(7),
        };

        handlers
            .handle_join_network(NodeJoinedCommittee {
                node: joined.node_address,
                id: joined.node_id,
                stake: 500u64.to_le_bytes(),
                key,
                blacklist: StorageUnits::mb(11),
                preferences,
                activation_epoch: EPOCH + EpochNumber(1),
            })
            .await
            .expect("handle join network");

        let state = ctx.state();
        let (_, member) = state
            .find_member_next(joined.node_id)
            .expect("member added to committee_next");

        assert_eq!(member.stake, TAPE(500));
        assert_eq!(member.key, key);
        assert_eq!(member.blacklist, StorageUnits::mb(11));
        assert_eq!(member.preferences, preferences);
        assert_eq!(member.weight, 0);
    }

    fn manager_config() -> EpochManagerConfig {
        EpochManagerConfig {
            state_retry: RetryConfig::none(),
        }
    }
}
