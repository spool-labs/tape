use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{blacklist_pda, track_pda};
use tape_blocks::ParsedInstruction;
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::EpochNumber;
use tape_crypto::{Address, Hash};
use tape_protocol::api::GetTrackDataReq;
use tape_protocol::Api;
use tape_retry::RetryConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::types::ObjectInfo;
use tracing::{debug, warn};

use crate::context::NodeContext;
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

const ASSIGNMENT_HEARTBEAT: Duration = Duration::from_secs(30);
const BLACKLIST_BATCH: usize = 256;

pub struct AssignmentManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
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
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut heartbeat = tokio::time::interval(ASSIGNMENT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
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
                    self.on_heartbeat().await?;
                }
            }
        }
    }

    async fn on_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::VoteAssignment { event, .. } => {
                    if is_landed_assignment_vote(event) {
                        self.on_assignment_canonical(event.target_epoch, event.hash).await?;
                    }
                }
                ParsedInstruction::FinalizeGroup { event, .. } => {
                    debug!(
                        epoch = event.epoch.0,
                        group = event.group.0,
                        total_groups = u64::from_le_bytes(event.total_groups),
                        "assignment: observed finalized group"
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_heartbeat(&self) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.phase() != EpochPhase::Closing {
            return Ok(());
        }

        let Some(next_epoch) = state.next_epoch.as_ref() else {
            return Ok(());
        };
        let target_epoch = next_epoch.id;

        if let Some(hash) = canonical_assignment_hash(&state, target_epoch) {
            drop(state);
            self.on_assignment_canonical(target_epoch, hash).await?;
            return Ok(());
        }

        drop(state);
        let Some(candidate) = self.build_candidate().await? else {
            return Ok(());
        };

        self.run_vote_round(&candidate).await
    }

    async fn on_assignment_canonical(
        &self,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let Some(candidate) = self.build_candidate().await? else {
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

        submit_assignment_finalization(&self.context, &candidate, &self.cancel).await
    }

    async fn build_candidate(&self) -> Result<Option<AssignmentCandidate>, NodeError> {
        if !self.sync_blacklists().await? {
            return Ok(None);
        }

        build_assignment(&self.context, &self.cancel).await
    }

    async fn sync_blacklists(&self) -> Result<bool, NodeError> {
        let state = self.context.state();
        let Some(next_epoch) = state.next_epoch.as_ref() else {
            return Ok(true);
        };
        let Some(next_committee) = state.next_committee.as_ref() else {
            return Ok(true);
        };
        let target_epoch = next_epoch.id;
        let voting_epoch = state.epoch();
        let members = next_committee.clone();
        drop(state);

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

                    match self.fetch_entry(blacklist, track_address, track).await? {
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
        blacklist: Address,
        track_address: Address,
        track: &CompressedTrack,
    ) -> Result<Option<TrackData>, NodeError> {
        let peers = self.context.state().group_peers(track.group);
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

    async fn run_vote_round(&self, candidate: &AssignmentCandidate) -> Result<(), NodeError> {
        submit_assignment_proposal(&self.context, candidate, &self.cancel).await?;
        create_assignment_votes(&self.context, candidate, &self.cancel).await?;
        fanout_assignment_votes(&self.context, candidate, &self.cancel).await?;
        submit_ready_assignment_votes(&self.context, candidate, &self.cancel).await?;
        Ok(())
    }
}

fn store_error(label: &'static str) -> impl FnOnce(tape_store::error::TapeStoreError) -> NodeError {
    move |error| NodeError::Store(format!("{label}: {error}"))
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

fn is_landed_assignment_vote(event: &tape_api::event::VoteRecorded) -> bool {
    event.kind == VoteKind::Assignment as u64
        && u64::from_le_bytes(event.signed_groups) == u64::from_le_bytes(event.total_groups)
}
