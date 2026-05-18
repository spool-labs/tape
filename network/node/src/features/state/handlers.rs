use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::event::{AssignmentGroupFinalized, NodeJoinedCommittee, VoteRecorded};
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_protocol::{Api, fetch::fetch_state};
use tape_retry::{retry_if, RetryConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub struct ProtocolStateHandlers<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ProtocolStateHandlers<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            cancel,
        }
    }

    pub async fn handle_advance_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        let previous_epoch = self.context.state().epoch();
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await
                        .map_err(NodeError::from)?;

                    if state.epoch() < epoch {
                        return Err(NodeError::StateUnavailable { expected_epoch: epoch });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => *expected_epoch == epoch,
                _ => false,
            },
        )
        .await?;

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

    pub async fn handle_commit_epoch(
        &self,
        epoch: EpochNumber,
        next_nonce: Hash,
    ) -> Result<(), NodeError> {
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await.map_err(NodeError::from)?;

                    let next_epoch = epoch.saturating_add(EpochNumber(1));
                    let committed = state.epoch() == epoch
                        && state.phase() == EpochPhase::Closing
                        && state
                            .next_epoch
                            .as_ref()
                            .is_some_and(|next| next.id == next_epoch && next.nonce == next_nonce);

                    if !committed {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: next_epoch,
                        });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => {
                    *expected_epoch == epoch.saturating_add(EpochNumber(1))
                }
                _ => false,
            },
        )
        .await?;

        self.context.set_state(state)?;

        info!(
            epoch = epoch.0,
            next_epoch = epoch.saturating_add(EpochNumber(1)).0,
            "published committed epoch state"
        );
        Ok(())
    }

    pub async fn handle_sync_spool(
        &self,
        node: Address,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        debug!(node = %node, epoch = epoch.0, "received sync spool");
        Ok(())
    }

    pub async fn handle_advance_pool(
        &self,
        node: Address,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        debug!(node = %node, epoch = epoch.0, "received advance pool");
        Ok(())
    }

    pub async fn handle_join_committee(&self, event: NodeJoinedCommittee) -> Result<(), NodeError> {
        debug!(node = %event.node, "received join committee");

        let expected_activation_epoch = self.context.state().epoch() + EpochNumber(1);

        if event.activation_epoch != expected_activation_epoch {
            debug!(
                node = %event.node,
                current_epoch = self.context.state().epoch().0,
                activation_epoch = event.activation_epoch.0,
                "ignoring join committee for stale epoch"
            );
            return Ok(());
        }

        let node = event.node;
        let activation_epoch = event.activation_epoch;
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await
                        .map_err(NodeError::from)?;

                    if state.find_member_next(node).is_none() {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: activation_epoch,
                        });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => {
                    *expected_epoch == activation_epoch
                }
                _ => false,
            },
        )
        .await?;

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_snapshot_vote(&self, event: VoteRecorded) -> Result<(), NodeError> {
        if event.kind != VoteKind::Snapshot as u64
            || u64::from_le_bytes(event.signed_groups) != u64::from_le_bytes(event.total_groups)
        {
            return Ok(());
        }

        let target_epoch = event.target_epoch;
        let hash = event.hash;
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await.map_err(NodeError::from)?;

                    let landed = state
                        .previous
                        .as_ref()
                        .is_some_and(|previous| {
                            previous.epoch.id == target_epoch
                                && previous.epoch.snapshot_hash == hash
                        });
                    if !landed {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: target_epoch,
                        });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => *expected_epoch == target_epoch,
                _ => false,
            },
        )
        .await?;

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_assignment_vote(&self, event: VoteRecorded) -> Result<(), NodeError> {
        if event.kind != VoteKind::Assignment as u64
            || u64::from_le_bytes(event.signed_groups) != u64::from_le_bytes(event.total_groups)
        {
            return Ok(());
        }

        let target_epoch = event.target_epoch;
        let hash = event.hash;
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await.map_err(NodeError::from)?;

                    let landed = state
                        .next_epoch
                        .as_ref()
                        .is_some_and(|next| {
                            next.id == target_epoch
                                && next.assignment_hash == hash
                        });
                    if !landed {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: target_epoch,
                        });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => *expected_epoch == target_epoch,
                _ => false,
            },
        )
        .await?;

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_finalize_group(
        &self,
        event: AssignmentGroupFinalized,
    ) -> Result<(), NodeError> {
        let target_epoch = event.epoch;
        let expected_groups = u64::from_le_bytes(event.total_groups);
        let hash = event.hash;
        let context = self.context.clone();

        let state = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move {
                    let state = fetch_state(&context.rpc).await.map_err(NodeError::from)?;

                    let landed = state
                        .next_epoch
                        .as_ref()
                        .is_some_and(|next| {
                            next.id == target_epoch
                                && next.assignment_hash == hash
                                && next.total_groups >= expected_groups
                        });
                    if !landed {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: target_epoch,
                        });
                    }

                    Ok(state)
                }
            },
            |error| match error {
                NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
                NodeError::StateUnavailable { expected_epoch } => *expected_epoch == target_epoch,
                _ => false,
            },
        )
        .await?;

        self.context.set_state(state)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;
    use tokio_util::sync::CancellationToken;

    use super::ProtocolStateHandlers;
    use crate::chain::submit_advance_epoch;
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
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        submit_advance_epoch(&ctx)
            .await
            .expect("submit advance epoch");

        handlers
            .handle_advance_epoch(EPOCH + EpochNumber(1))
            .await
            .expect("handle advance epoch");

        let state = ctx.state();
        assert_eq!(state.epoch(), EPOCH + EpochNumber(1));
        assert_eq!(state.phase(), EpochPhase::Sync);
    }

    #[tokio::test]
    async fn sync_spool_does_not_mutate_protocol_state() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        handlers
            .handle_sync_spool(harness.node(NODE).node_address.into(), EPOCH)
            .await
            .expect("handle sync spool");
        assert_eq!(ctx.state().phase(), EpochPhase::Sync);
    }

    #[tokio::test]
    async fn advance_pool_does_not_mutate_protocol_state() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Settle)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        handlers
            .handle_advance_pool(harness.node(NODE).node_address.into(), EPOCH)
            .await
            .expect("handle advance pool");
        assert_eq!(ctx.state().phase(), EpochPhase::Settle);
    }
}
