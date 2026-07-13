use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_blocks::ParsedInstruction;
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::Hash;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::{EventLogOps, SnapshotOps, TapeOps, TrackDataOps, TrackOps, VoteOps};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::context::NodeContext;
use crate::core::chain_tx::spawn_guarded;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::bootstrap::fetch::{fetch_and_decode_epoch, persist_snapshot_metadata};
use crate::features::snapshot::build::{
    build_snapshot, persist_snapshot_candidate, SnapshotCandidate,
};
use crate::features::snapshot::fanout::fanout_snapshot_votes;
use crate::features::snapshot::submit::{
    submit_ready_snapshot_votes, submit_snapshot_finalization, submit_snapshot_proposal,
};
use crate::features::snapshot::vote::create_snapshot_votes;
use crate::features::vote::all_vote_groups_signed;

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(30);

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    vote_round: Option<JoinHandle<()>>,
    finalize: Option<JoinHandle<()>>,
    proposed: Arc<Mutex<HashSet<EpochNumber>>>,
}

impl<Db, Cluster, Blockchain> SnapshotManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            block_rx,
            cancel,
            vote_round: None,
            finalize: None,
            proposed: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut heartbeat = tokio::time::interval(SNAPSHOT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    if let Some(handle) = self.vote_round.take() {
                        handle.abort();
                    }
                    if let Some(handle) = self.finalize.take() {
                        handle.abort();
                    }
                    return Ok(());
                }
                received = self.block_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SnapshotManager })
                        };
                    };
                    self.on_block(block).await?;
                }
                _ = heartbeat.tick() => {
                    self.try_progress_snapshot().await?;
                }
            }
        }
    }

    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.on_advance_epoch(event.old_epoch, event.new_epoch).await?;
                }

                ParsedInstruction::VoteSnapshot { event, .. } => {

                    if event.kind != VoteKind::Snapshot as u64 {
                        continue;
                    }

                    if all_vote_groups_signed(event) {

                        let state = self.context.state();
                        let state = if state.epoch() >= event.voting_epoch {
                            state
                        } else {
                            self.context
                                .state
                                .wait_for_epoch(event.voting_epoch, &self.cancel)
                                .await?
                        };


                        if !validate_block_state(
                            state.as_ref(),
                            event.voting_epoch,
                            event.target_epoch,
                        ) {
                            continue;
                        }

                        self.on_snapshot_canonical(state, event.target_epoch, event.hash)
                            .await?;
                    }
                }

                ParsedInstruction::FinalizeSnapshot { event, .. } => {
                    let voting_epoch = event.epoch.next();

                    let state = self.context.state();
                    let state = if state.epoch() >= voting_epoch {
                        state
                    } else {
                        self.context
                            .state
                            .wait_for_epoch(voting_epoch, &self.cancel)
                            .await?
                    };

                    if !validate_block_state(
                        state.as_ref(),
                        voting_epoch,
                        event.epoch
                    ) {
                        continue;
                    }

                    self.on_snapshot_finalized(state, event.epoch, event.hash)
                        .await?;
                }
                ParsedInstruction::FinalizeGroup { .. } => {
                    self.try_progress_snapshot().await?;
                }
                ParsedInstruction::ProposeSnapshot { event, .. } => {
                    if let Ok(mut seen) = self.proposed.lock() {
                        seen.insert(event.voting_epoch);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_advance_epoch(
        &self,
        old: EpochNumber,
        new: EpochNumber,
    ) -> Result<(), NodeError> {

        self.context
            .store
            .delete_snapshot_epochs_except(old)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epochs_except: {e}")))?;
        self.context
            .store
            .delete_vote_epochs_except(new)
            .map_err(|e| NodeError::Store(format!("delete_vote_epochs_except: {e}")))?;

        if let Ok(mut seen) = self.proposed.lock() {
            seen.retain(|&epoch| epoch >= new);
        }

        Ok(())
    }

    async fn on_snapshot_canonical(
        &mut self,
        state: Arc<ProtocolState>,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let Some(candidate) = self.build_candidate(state, epoch).await? else {
            return Ok(());
        };

        if candidate.hash != hash {
            warn!(
                epoch = epoch.0,
                local_hash = %candidate.hash,
                canonical_hash = %hash,
                "snapshot: local candidate does not match canonical hash"
            );
            return Ok(());
        }

        // Detach the finalize so its rank stagger never blocks the block loop.
        let ctx = self.context.clone();
        let cancel = self.cancel.clone();
        spawn_guarded(&mut self.finalize, async move {
            if let Err(error) = submit_snapshot_finalization(&ctx, &candidate, &cancel).await {
                warn!(%error, epoch = epoch.0, "snapshot: finalization task failed");
            }
        });
        Ok(())
    }

    async fn on_snapshot_finalized(
        &self,
        state: Arc<ProtocolState>,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let mut materialized = false;

        if let Some(candidate) = self.build_candidate(state, epoch).await? {
            if candidate.hash == hash {
                persist_snapshot_candidate(self.context.as_ref(), &candidate)?;
                materialized = true;
            } else {
                warn!(
                    epoch = epoch.0,
                    local_hash = %candidate.hash,
                    finalized_hash = %hash,
                    "snapshot: finalized hash does not match local candidate"
                );
            }
        }

        if !materialized {
            if let Err(error) = self.ensure_snapshot_metadata(epoch).await {
                warn!(
                    epoch = epoch.0,
                    %error,
                    "snapshot: finalized metadata materialization failed"
                );
            }
        }

        self.context
            .store
            .delete_epoch_events(epoch)
            .map_err(|e| NodeError::Store(format!("delete_epoch_events: {e}")))?;
        self.context
            .store
            .delete_snapshot_epoch(epoch)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epoch: {e}")))?;
        self.context
            .store
            .delete_vote_epoch(epoch.next())
            .map_err(|e| NodeError::Store(format!("delete_vote_epoch: {e}")))?;

        debug!(epoch = epoch.0, "snapshot: finalized local cleanup complete");
        Ok(())
    }

    async fn try_progress_snapshot(&mut self) -> Result<(), NodeError> {
        let state = self.context.state();

        if state.epoch().is_zero() {
            return Ok(());
        }

        if state.phase() == EpochPhase::Active {
            if let Some(previous) = state
                .previous
                .as_ref()
                .filter(|previous| previous.epoch.has_snapshot_hash())
            {
                if let Err(error) = self.ensure_snapshot_metadata(previous.epoch.id).await {
                    warn!(
                        epoch = previous.epoch.id.0,
                        %error,
                        "snapshot: active-epoch metadata repair failed"
                    );
                }
            }
            return Ok(());
        }

        if state.phase() != EpochPhase::Snapshot {
            return Ok(());
        }

        let snapshot_epoch = state.epoch().prev();

        let Some(previous) = state.previous.as_ref() else {
            return Ok(());
        };

        if previous.epoch.id != snapshot_epoch {
            return Ok(());
        }

        if let Some(hash) = canonical_snapshot_hash(&state, snapshot_epoch) {
            self.on_snapshot_canonical(state, snapshot_epoch, hash).await?;
            return Ok(());
        }

        let build = self.build_candidate(state.clone(), snapshot_epoch).await?;
        let Some(candidate) = build else {
            return Ok(());
        };

        self.spawn_vote_round(state, candidate);
        Ok(())
    }

    // Detach the proposal and vote submits so their rank staggers never block the
    // block loop. The in-flight guard drops the per-block and per-heartbeat
    // re-fire while a round is still running.
    fn spawn_vote_round(&mut self, state: Arc<ProtocolState>, candidate: SnapshotCandidate) {
        let ctx = self.context.clone();
        let cancel = self.cancel.clone();
        let proposed = self.proposed.clone();
        spawn_guarded(&mut self.vote_round, async move {
            if let Err(error) = run_vote_round(ctx, state, candidate, cancel, proposed).await {
                warn!(%error, "snapshot: vote round task failed");
            }
        });
    }

    async fn build_candidate(
        &self,
        state: Arc<ProtocolState>,
        epoch: EpochNumber,
    ) -> Result<Option<SnapshotCandidate>, NodeError> {
        build_snapshot(&self.context, state, epoch, &self.cancel).await
    }

    async fn ensure_snapshot_metadata(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        if self.snapshot_metadata_complete(epoch)? {
            return Ok(());
        }

        let decoded = fetch_and_decode_epoch(&self.context, epoch, &self.cancel).await?;
        persist_snapshot_metadata(self.context.as_ref(), epoch, &decoded)?;
        info!(
            epoch = epoch.0,
            tracks = decoded.tracks.len(),
            "snapshot: finalized metadata materialized"
        );
        Ok(())
    }

    fn snapshot_metadata_complete(&self, epoch: EpochNumber) -> Result<bool, NodeError> {
        let snapshot_tape = snapshot_tape_pda(epoch).0;
        let Some(tape) = self
            .context
            .store
            .get_tape(snapshot_tape)
            .map_err(|error| NodeError::Store(format!("get_tape: {error}")))?
        else {
            return Ok(false);
        };

        if tape.next_track_number.0 == 0 {
            return Ok(false);
        }

        for number in 0..tape.next_track_number.0 {
            let track = track_pda(snapshot_tape, TrackNumber(number)).0;
            if self
                .context
                .store
                .get_track(track)
                .map_err(|error| NodeError::Store(format!("get_track: {error}")))?
                .is_none()
            {
                return Ok(false);
            }
            if self
                .context
                .store
                .get_track_data(track)
                .map_err(|error| NodeError::Store(format!("get_track_data: {error}")))?
                .is_none()
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

}

// Drive one snapshot vote round to completion: propose, sign local votes, fan
// them out to peers, then submit the aggregated group votes. Each on-chain
// submit staggers by committee rank internally, so this runs detached.
async fn run_vote_round<Db, Cluster, Blockchain>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: Arc<ProtocolState>,
    candidate: SnapshotCandidate,
    cancel: CancellationToken,
    proposed: Arc<Mutex<HashSet<EpochNumber>>>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    submit_snapshot_proposal(&ctx, &candidate, &cancel, &proposed).await?;
    create_snapshot_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;
    fanout_snapshot_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;
    submit_ready_snapshot_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;

    Ok(())
}

fn canonical_snapshot_hash(
    state: &ProtocolState,
    snapshot_epoch: EpochNumber,
) -> Option<Hash> {
    let previous = state.previous.as_ref()?;
    if previous.epoch.id != snapshot_epoch || !previous.epoch.has_snapshot_hash() {
        return None;
    }
    Some(previous.epoch.snapshot_hash)
}

fn validate_block_state(
    state: &ProtocolState,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> bool {
    let phase = state.phase();

    if phase != EpochPhase::Snapshot {
        // Allow this node to try and create the snapshot even if the onchain state has already
        // advanced to Active. This reduces the need for repair if we assume our snapshot log
        // produces the same snapshot hash as the canonical one.
        if phase != EpochPhase::Active {
            return false;
        }
    }

    if state.epoch() != voting_epoch {
        return false;
    }

    if target_epoch.next() != voting_epoch {
        return false;
    }

    let Some(previous) = state.previous.as_ref() else {
        return false;
    };

    if previous.epoch.id != target_epoch {
        return false;
    }

    true
}
