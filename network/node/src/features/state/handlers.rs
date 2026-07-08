use std::sync::Arc;

use bytemuck::Zeroable;
use rpc::Rpc;
use store::Store;
use tape_api::event::{
    AssignmentFinalized, CommitteeCreated, CommitteeResized, EpochCreated,
    NodeJoinedCommittee, PeerSetResized, SnapshotFinalized, SpoolSynced, VoteRecorded,
};
use tape_api::state::Epoch;
use tape_core::system::{EpochPhase, NodePreferences, VoteKind};
use tape_core::types::{BitmapRead, BitmapWrite, EpochNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_protocol::{fetch::fetch_state, Api};
use tape_retry::{retry_if, RetryConfig};
#[cfg(feature = "metrics")]
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::state::events::apply_join_committee_event;
use crate::features::vote::all_vote_groups_signed;

pub struct ProtocolStateHandlers<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> 
ProtocolStateHandlers<Db, Cluster, Blockchain> {

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
            #[cfg(feature = "metrics")]
            {
                // The rolled counters cover the epoch that just ended, not the
                // one starting now.
                crate::observe::roll_epoch(epoch.0.saturating_sub(1));
                if let Ok(bytes) = serde_json::to_vec(&crate::observe::last_epoch()) {
                    let _ = self.context.store.set_observe_last_epoch(&bytes);
                }
                if let Ok(bytes) = serde_json::to_vec(&crate::observe::lifetime()) {
                    let _ = self.context.store.set_observe_lifetime(&bytes);
                }
            }
        }

        if let Err(error) = self.context.refresh_peers().await {
            warn!(error = %error, epoch = epoch.0, "peer refresh failed after epoch advance");
        }

        info!(epoch = epoch.0, "published protocol state");
        Ok(())
    }

    /// Genesis epoch transition. `start_network` moves the chain from epoch 0 to
    /// the genesis epoch, but a node that booted before it ran is still cached at
    /// epoch 0 (and `StartNetwork` emits no event of its own). Treat it like an
    /// epoch advance so the node re-fetches and catches up to the live committee.
    pub async fn handle_start_network(&self) -> Result<(), NodeError> {
        if self.context.state().epoch().is_zero() {
            self.handle_advance_epoch(EpochNumber(1)).await?;
        }

        Ok(())
    }

    pub async fn handle_commit_epoch(
        &self,
        epoch: EpochNumber,
        next_nonce: Hash,
        preferences: NodePreferences,
    ) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();
        if state.epoch() != epoch {
            debug!(
                event_epoch = epoch.0,
                local_epoch = state.epoch().0,
                "ignoring commit epoch for non-current epoch"
            );
            return Ok(());
        }

        state.current.epoch.state.phase = EpochPhase::Closing as u64;

        let next_epoch = epoch.next();
        let Some(next) = state.next_epoch.as_mut() else {
            warn!(
                epoch = epoch.0,
                next_epoch = next_epoch.0,
                "commit epoch observed but next epoch is missing from local protocol state"
            );
            return Ok(());
        };

        if next.id != next_epoch {
            warn!(
                epoch = epoch.0,
                expected_next_epoch = next_epoch.0,
                local_next_epoch = next.id.0,
                "commit epoch observed but local next epoch does not match"
            );
            return Ok(());
        }

        next.nonce = next_nonce;
        next.preferences = preferences;

        self.context.set_state(state)?;

        info!(
            epoch = epoch.0,
            next_epoch = epoch.next().0,
            "published committed epoch state"
        );
        Ok(())
    }

    pub async fn handle_create_epoch(&self, event: EpochCreated) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();
        let expected_next = state.epoch().next();
        let expected_candidate = state.epoch().saturating_add(EpochNumber(2));

        if event.epoch == expected_next {
            if !state
                .next_epoch
                .as_ref()
                .is_some_and(|epoch| epoch.id == event.epoch)
            {
                let mut epoch = Epoch::zeroed();
                epoch.id = event.epoch;
                state.next_epoch = Some(epoch);
            }
        } else if event.epoch == expected_candidate {
            if !state
                .candidate_epoch
                .as_ref()
                .is_some_and(|epoch| epoch.id == event.epoch)
            {
                let mut epoch = Epoch::zeroed();
                epoch.id = event.epoch;
                state.candidate_epoch = Some(epoch);
            }
        } else {
            debug!(
                event_epoch = event.epoch.0,
                current_epoch = state.epoch().0,
                "ignoring epoch creation outside tracked epochs"
            );
            return Ok(());
        }

        self.context.set_state(state)?;
        Ok(())
    }

    pub async fn handle_create_committee(
        &self,
        event: CommitteeCreated,
    ) -> Result<(), NodeError> {
        self.publish_tracked_committee_capacity(
            event.epoch,
            event.capacity,
        ).await
    }

    pub async fn handle_resize_committee(
        &self,
        event: CommitteeResized,
    ) -> Result<(), NodeError> {
        self.publish_tracked_committee_capacity(
            event.epoch,
            event.capacity,
        ).await
    }

    pub async fn handle_resize_peer_set(
        &self,
        event: PeerSetResized,
    ) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();
        state.peer_capacity = event.capacity;
        self.context.set_state(state)?;
        Ok(())
    }

    async fn publish_tracked_committee_capacity(
        &self,
        epoch: EpochNumber,
        capacity: u64,
    ) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();
        let expected_next = state.epoch().next();
        let expected_candidate = state.epoch().saturating_add(EpochNumber(2));

        if epoch == expected_next {
            if state.next_committee.is_none() {
                state.next_committee = Some(Vec::new());
            }
            state.next_committee_capacity = Some(capacity);
        } else if epoch == expected_candidate {
            state.candidate_committee_capacity = Some(capacity);
        } else {
            debug!(
                event_epoch = epoch.0,
                current_epoch = state.epoch().0,
                "ignoring committee setup outside tracked epochs"
            );
            return Ok(());
        }

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_sync_spool(&self, event: SpoolSynced) -> Result<(), NodeError> {
        debug!(
            node = %event.node,
            epoch = event.epoch.0,
            group = event.group.0,
            "received sync spool"
        );

        let mut state = (*self.context.state()).clone();
        if state.epoch() != event.epoch {
            return Ok(());
        }

        let spool = event.spool;
        let Some(position) = event.group.position_of(spool) else {
            return Ok(());
        };

        let Some(group) = state
            .current
            .groups
            .iter_mut()
            .find(|group| group.id == event.group)
        else {
            return Ok(());
        };

        if !group.synced.is_set(position) {
            group.synced.set(position);
        }

        apply_event_phase(&mut state, event.phase);

        self.context.set_state(state)?;
        Ok(())
    }

    pub async fn handle_advance_pool(
        &self,
        node: Address,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        debug!(node = %node, epoch = epoch.0, "received advance pool");
        self.refresh_registered_nodes("pool advance").await;
        Ok(())
    }

    pub async fn handle_register_node(&self, node: Address) -> Result<(), NodeError> {
        debug!(node = %node, "received node registration");
        self.refresh_registered_nodes("node registration").await;
        Ok(())
    }

    async fn refresh_registered_nodes(&self, reason: &'static str) {
        if let Err(error) = self
            .context
            .peer_manager
            .refresh_registered_nodes(&self.context.rpc)
            .await
        {
            warn!(error = %error, reason, "registered peer refresh failed");
        }
    }

    pub async fn handle_join_committee(&self, event: NodeJoinedCommittee) -> Result<(), NodeError> {
        debug!(node = %event.node, "received join committee");

        let mut state = (*self.context.state()).clone();
        let expected_activation_epoch = state.epoch().next();

        if event.activation_epoch != expected_activation_epoch {
            debug!(
                node = %event.node,
                current_epoch = state.epoch().0,
                activation_epoch = event.activation_epoch.0,
                "ignoring join committee for stale epoch"
            );
            return Ok(());
        }

        if let Err(error) = apply_join_committee_event(&mut state, event) {
            warn!(error = %error, "join committee event could not be applied locally");
            return Ok(());
        }

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_snapshot_vote(&self, event: VoteRecorded) -> Result<(), NodeError> {
        if event.kind != VoteKind::Snapshot as u64 || !all_vote_groups_signed(&event) {
            return Ok(());
        }

        let target_epoch = event.target_epoch;
        let mut state = (*self.context.state()).clone();
        if state.epoch() != event.voting_epoch {
            return Ok(());
        }

        // Record the canonical hash only; FinalizeSnapshot moves the epoch to Active.
        if let Some(previous) = state
            .previous
            .as_mut()
            .filter(|previous| previous.epoch.id == target_epoch)
        {
            previous.epoch.snapshot_hash = event.hash;
        }

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_snapshot_finalized(
        &self,
        event: SnapshotFinalized,
    ) -> Result<(), NodeError> {
        let voting_epoch = event.epoch.next();
        let mut state = (*self.context.state()).clone();
        if state.epoch() != voting_epoch {
            return Ok(());
        }

        state.current.epoch.state.phase = EpochPhase::Active as u64;
        if let Some(previous) = state
            .previous
            .as_mut()
            .filter(|previous| previous.epoch.id == event.epoch)
        {
            previous.epoch.snapshot_hash = event.hash;
        }

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_assignment_vote(&self, event: VoteRecorded) -> Result<(), NodeError> {
        if event.kind != VoteKind::Assignment as u64 || !all_vote_groups_signed(&event) {
            return Ok(());
        }

        let target_epoch = event.target_epoch;
        let mut state = (*self.context.state()).clone();
        if state.epoch() != event.voting_epoch {
            return Ok(());
        }

        if let Some(next) = state
            .next_epoch
            .as_mut()
            .filter(|next| next.id == target_epoch)
        {
            next.assignment_hash = event.hash;
        }

        self.context.set_state(state)?;

        Ok(())
    }

    pub async fn handle_finalize_group(
        &self,
        event: AssignmentFinalized,
    ) -> Result<(), NodeError> {
        let mut state = (*self.context.state()).clone();

        if let Some(next) = state
            .next_epoch
            .as_mut()
            .filter(|next| next.id == event.epoch)
        {
            next.assignment_hash = event.hash;
            next.total_groups = event.total_groups;
            next.total_assigned = event.total_assigned;
        }

        self.context.set_state(state)?;

        Ok(())
    }
}

fn apply_event_phase(state: &mut tape_protocol::ProtocolState, phase: u64) {
    let Ok(event_phase) = EpochPhase::try_from(phase) else {
        return;
    };

    if event_phase >= state.phase() {
        state.current.epoch.state.phase = phase;
    }
}

#[cfg(test)]
mod tests {
    use tape_api::event::SpoolSynced;
    use tape_core::system::EpochPhase;
    use tape_core::types::{BitmapRead, EpochNumber};
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
            .phase(EpochPhase::Closing)
            .next_committee_size(20)
            .advance_ready()
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        submit_advance_epoch(&ctx)
            .await
            .expect("submit advance epoch");

        handlers
            .handle_advance_epoch(EPOCH.next())
            .await
            .expect("handle advance epoch");

        let state = ctx.state();
        assert_eq!(state.epoch(), EPOCH.next());
        assert_eq!(state.phase(), EpochPhase::Sync);
    }

    #[tokio::test]
    async fn sync_spool_updates_group_and_phase() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        let group = ctx.state().current.groups[0];
        let spool = group.id.spool_at(0);
        handlers
            .handle_sync_spool(SpoolSynced {
                node: group.spools[0].node,
                epoch: EPOCH,
                group: group.id,
                spool,
                phase: EpochPhase::Snapshot as u64,
            })
            .await
            .expect("handle sync spool");

        let state = ctx.state();
        assert_eq!(state.phase(), EpochPhase::Snapshot);
        assert!(state.current.groups[0].synced.is_set(0));
    }

    #[tokio::test]
    async fn advance_pool_does_not_mutate_protocol_state() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Snapshot)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        handlers
            .handle_advance_pool(harness.node(NODE).node_address.into(), EPOCH)
            .await
            .expect("handle advance pool");
        assert_eq!(ctx.state().phase(), EpochPhase::Snapshot);
    }

    #[tokio::test]
    async fn start_network_is_noop_after_genesis() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Snapshot)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let handlers = ProtocolStateHandlers::new(ctx.clone(), CancellationToken::new());

        handlers
            .handle_start_network()
            .await
            .expect("handle start network");

        let state = ctx.state();
        assert_eq!(state.epoch(), EPOCH);
        assert_eq!(state.phase(), EpochPhase::Snapshot);
    }

}
