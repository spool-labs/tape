use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{blacklist_pda, track_pda};
use tape_blocks::ParsedInstruction;
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::EpochNumber;
use tape_crypto::{Address, Hash};
use tape_protocol::api::GetTrackDataReq;
use tape_protocol::{Api, ProtocolState};
use tape_retry::RetryConfig;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::types::ObjectInfo;
use tracing::{debug, warn};

use crate::context::NodeContext;
use crate::core::chain_tx::spawn_guarded;
use crate::core::error::NodeError;
use crate::core::peer_call::call_peer;
use crate::core::types::ChannelName;
use crate::features::assignment::build::{AssignmentCandidate, build_assignment};
use crate::features::assignment::fanout::fanout_assignment_votes;
use crate::features::assignment::submit::{
    submit_assignment_finalization, submit_assignment_proposal, submit_ready_assignment_votes,
};
use crate::features::assignment::vote::create_assignment_votes;
use crate::features::blacklist::decode_blacklist_entry;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::vote::all_vote_groups_signed;

const ASSIGNMENT_HEARTBEAT: Duration = Duration::from_secs(30);
const BLACKLIST_BATCH: usize = 256;

pub struct AssignmentManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    vote_round: Option<JoinHandle<()>>,
    finalize: Option<JoinHandle<()>>,
    proposed: Arc<Mutex<HashSet<EpochNumber>>>,
}

impl<Db, Cluster, Blockchain> AssignmentManager<Db, Cluster, Blockchain>
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
        let mut heartbeat = tokio::time::interval(ASSIGNMENT_HEARTBEAT);

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
                            Err(NodeError::ChannelClosed { channel: ChannelName::AssignmentManager })
                        };
                    };
                    self.on_block(block).await?;
                }
                _ = heartbeat.tick() => {
                    self.try_progress_assignment().await?;
                }
            }
        }
    }

    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::VoteAssignment { event, .. } => {

                    if event.kind != VoteKind::Assignment as u64 {
                        continue;
                    }

                    if all_vote_groups_signed(event) {

                        let state = self.context.state();
                        let state = if state.epoch() >= event.voting_epoch {
                            state
                        } else {
                            self.context.state
                                .wait_for_epoch(event.voting_epoch, &self.cancel).await?
                        };

                        if !validate_assignment_state(
                            state.as_ref(),
                            event.voting_epoch,
                            event.target_epoch,
                        ) {
                            continue;
                        }

                        self.on_assignment_canonical(state, event.target_epoch, event.hash)
                            .await?;
                    }
                }
                ParsedInstruction::CommitEpoch { .. } => {
                    self.try_progress_assignment().await?;
                }
                ParsedInstruction::ProposeAssignment { event, .. } => {
                    if let Ok(mut seen) = self.proposed.lock() {
                        seen.insert(event.voting_epoch);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn try_progress_assignment(&mut self) -> Result<(), NodeError> {
        let state = self.context.state();

        if let Ok(mut seen) = self.proposed.lock() {
            seen.retain(|&epoch| epoch >= state.epoch());
        }

        let Some(target_epoch) = heartbeat_assignment_target(state.as_ref()) else {
            return Ok(());
        };

        if let Some(hash) = canonical_assignment_hash(&state, target_epoch) {
            self.on_assignment_canonical(state, target_epoch, hash).await?;
            return Ok(());
        }

        let Some(candidate) = self.build_candidate(state.clone()).await? else {
            return Ok(());
        };

        self.spawn_vote_round(state, candidate);
        Ok(())
    }

    // Detach the proposal and vote submits so their rank staggers never block the
    // block loop. The in-flight guard drops the per-block and per-heartbeat
    // re-fire while a round is still running.
    fn spawn_vote_round(&mut self, state: Arc<ProtocolState>, candidate: AssignmentCandidate) {
        let ctx = self.context.clone();
        let cancel = self.cancel.clone();
        let proposed = self.proposed.clone();
        spawn_guarded(&mut self.vote_round, async move {
            if let Err(error) = run_vote_round(ctx, state, candidate, cancel, proposed).await {
                warn!(%error, "assignment: vote round task failed");
            }
        });
    }

    async fn on_assignment_canonical(
        &mut self,
        state: Arc<ProtocolState>,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let Some(candidate) = self.build_candidate(state).await? else {
            return Ok(());
        };

        if candidate.target_epoch != epoch || candidate.hash != hash {
            warn!(
                epoch = epoch.0,
                local_epoch = candidate.target_epoch.0,
                local_hash = %candidate.hash,
                canonical_hash = %hash,
                "assignment: local candidate does not match canonical assignment"
            );
            return Ok(());
        }

        // Detach the finalize so its rank stagger never blocks the block loop.
        let ctx = self.context.clone();
        let cancel = self.cancel.clone();
        spawn_guarded(&mut self.finalize, async move {
            if let Err(error) = submit_assignment_finalization(&ctx, &candidate, &cancel).await {
                warn!(%error, epoch = epoch.0, "assignment: finalization task failed");
            }
        });
        Ok(())
    }

    async fn build_candidate(
        &self,
        state: Arc<ProtocolState>,
    ) -> Result<Option<AssignmentCandidate>, NodeError> {
        if !self.sync_blacklists(state.as_ref()).await? {
            return Ok(None);
        }

        build_assignment(&self.context, state, &self.cancel).await
    }

    async fn sync_blacklists(&self, state: &ProtocolState) -> Result<bool, NodeError> {
        let Some(next_epoch) = state.next_epoch.as_ref() else {
            return Ok(true);
        };
        let Some(next_committee) = state.next_committee.as_ref() else {
            return Ok(true);
        };
        let target_epoch = next_epoch.id;
        let voting_epoch = state.epoch();
        let members = next_committee.clone();

        let mut complete = true;
        for member in members {
            let blacklist = blacklist_pda(member.node).0;
            let Some(tape) = self
                .context
                .store
                .get_tape(blacklist)
                .map_err(store_error("blacklist tape lookup"))?
            else {
                continue;
            };

            if tape.end_epoch <= target_epoch {
                continue;
            }

            let mut cursor = None;
            loop {
                let tracks = self
                    .context
                    .store
                    .iter_tracks_by_tape_from(blacklist, cursor, BLACKLIST_BATCH)
                    .map_err(store_error("blacklist track scan"))?;
                if tracks.is_empty() {
                    break;
                }

                for track in &tracks {
                    let track_address = track_pda(blacklist, track.track_number).0;
                    if !self.track_before_cutoff(track_address, voting_epoch)? {
                        continue;
                    }

                    if self
                        .context
                        .store
                        .get_track_data(track_address)
                        .map_err(store_error("blacklist track data lookup"))?
                        .is_some()
                    {
                        continue;
                    }

                    match self
                        .fetch_entry(state, blacklist, track_address, track)
                        .await?
                    {
                        Some(data) => {
                            self.context
                                .store
                                .put_track_data(track_address, data)
                                .map_err(store_error("put blacklist track data"))?;
                        }
                        None => {
                            complete = false;
                            debug!(
                                node = %member.node,
                                track = %track_address,
                                "assignment: waiting for blacklist track data"
                            );
                        }
                    }
                }

                cursor = tracks.last().map(|track| track.track_number);
            }
        }

        Ok(complete)
    }

    fn track_before_cutoff(
        &self,
        track: Address,
        voting_epoch: EpochNumber,
    ) -> Result<bool, NodeError> {
        let Some(info) = self
            .context
            .store
            .get_object_info(track)
            .map_err(store_error("blacklist object info lookup"))?
        else {
            return Ok(true);
        };

        let ObjectInfo::Valid {
            registered_epoch,
            certified_epoch,
            ..
        } = info
        else {
            return Ok(true);
        };

        let Some(certified_epoch) = certified_epoch else {
            return Ok(true);
        };

        Ok(registered_epoch < voting_epoch && certified_epoch < voting_epoch)
    }

    async fn fetch_entry(
        &self,
        state: &ProtocolState,
        blacklist: Address,
        track_address: Address,
        track: &CompressedTrack,
    ) -> Result<Option<BlobData>, NodeError> {
        let peers = state.group_peers(track.group);
        if peers.is_empty() {
            return Ok(None);
        }

        let req = GetTrackDataReq { track: track_address };
        for (_, node) in peers {
            if node == self.context.node_address() {
                continue;
            }

            let result = call_peer(
                &self.context.peer_manager,
                RetryConfig::three(),
                node,
                Some(&self.cancel),
                || self.context.api.get_track_data(node, &req),
            )
            .await;

            let Ok(res) = result else {
                continue;
            };

            if let Err(error) = decode_blacklist_entry(track_address, blacklist, track, &res.data) {
                warn!(
                    node = %node,
                    track = %track_address,
                    %error,
                    "assignment: peer returned invalid blacklist track data"
                );
                continue;
            }

            return Ok(Some(res.data));
        }

        Ok(None)
    }

}

// Drive one assignment vote round to completion: propose, sign local votes, fan
// them out to peers, then submit the aggregated group votes. Each on-chain
// submit staggers by committee rank internally, so this runs detached.
async fn run_vote_round<Db, Cluster, Blockchain>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: Arc<ProtocolState>,
    candidate: AssignmentCandidate,
    cancel: CancellationToken,
    proposed: Arc<Mutex<HashSet<EpochNumber>>>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    submit_assignment_proposal(&ctx, &candidate, &cancel, &proposed).await?;
    create_assignment_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;
    fanout_assignment_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;
    submit_ready_assignment_votes(&ctx, state.as_ref(), &candidate, &cancel).await?;

    Ok(())
}

fn canonical_assignment_hash(
    state: &tape_protocol::ProtocolState,
    target_epoch: EpochNumber,
) -> Option<Hash> {
    let next_epoch = state.next_epoch.as_ref()?;
    if next_epoch.id != target_epoch || !next_epoch.has_assignment_hash() {
        return None;
    }

    Some(next_epoch.assignment_hash)
}

fn heartbeat_assignment_target(state: &ProtocolState) -> Option<EpochNumber> {
    let target_epoch = state.next_epoch.as_ref()?.id;
    if !validate_assignment_state(state, state.epoch(), target_epoch) {
        return None;
    }

    Some(target_epoch)
}

fn validate_assignment_state(
    state: &ProtocolState,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> bool {
    if state.epoch() != voting_epoch {
        return false;
    }

    if state.phase() != EpochPhase::Closing {
        return false;
    }

    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return false;
    };

    if next_epoch.id != target_epoch {
        return false;
    }

    true
}

fn store_error(label: &'static str) -> impl FnOnce(tape_store::error::TapeStoreError) -> NodeError {
    move |error| NodeError::Store(format!("{label}: {error}"))
}
