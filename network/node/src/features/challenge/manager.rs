//! The challenge task: follows ingested blocks, sends this node's proofs and
//! records the ones its peers send

use std::sync::Arc;
use std::time::Instant;

use tape_blocks::ParsedInstruction;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_protocol::{Api, ProtocolState};
use tape_rpc::Rpc;
use tape_store_api::Store;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::challenge::challenger::Challenger;
use crate::features::challenge::handle::Arrival;
use crate::features::challenge::round::Batch;
use crate::features::challenge::schedule::Schedule;
use crate::features::challenge::wire::to_req;

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    blocks: broadcast::Receiver<Arc<ParsedBlock>>,
    arrivals: mpsc::Receiver<Arrival>,
    cancel: CancellationToken,

    sends: JoinSet<()>,
    epoch: Option<EpochNumber>,
}

impl<Db, Cluster, Blockchain> ChallengeManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        blocks: broadcast::Receiver<Arc<ParsedBlock>>,
        arrivals: mpsc::Receiver<Arrival>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            blocks,
            arrivals,
            cancel,
            sends: JoinSet::new(),
            epoch: None,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        info!("challenge rounds enabled");

        loop {
            tokio::select! {
                // Safe: cancelled holds no state
                _ = self.cancel.cancelled() => break,
                // Safe: a broadcast recv that is dropped leaves the block in the feed
                received = self.blocks.recv() => match received {
                    Ok(block) => self.on_block(&block),
                    Err(RecvError::Lagged(lost)) => self.on_lag(lost),
                    Err(RecvError::Closed) => break,
                },
                // Safe: an mpsc recv that is dropped leaves the batch in the queue
                Some(arrival) = self.arrivals.recv() => self.on_arrival(arrival),
                // Safe: join_next only reaps a send that has finished
                Some(_) = self.sends.join_next() => {}
            }
        }

        // The feed closes when ingest stops, which ends this task with it.
        self.sends.shutdown().await;
        Ok(())
    }

    fn on_block(&mut self, block: &ParsedBlock) {
        let at = Instant::now();
        let context = Arc::clone(&self.context);
        let state = context.state();
        let mut challenger = context.challenge.challenger();

        self.follow_epoch(&mut challenger, &state, block);
        let batches = challenger.on_block(
            &context.store,
            &state,
            block.slot,
            block.blockhash,
            self.is_taking_part(),
            at,
        );
        drop(challenger);

        for (peer, batch) in batches {
            self.send(peer, batch);
        }
    }

    /// Keep the challenger on the chain's epoch
    ///
    /// An epoch begins at the block that advances it, and its own holders
    /// take over at the block that turns it Active. A node that starts inside
    /// an epoch, or lost blocks, takes both from its state.
    fn follow_epoch(&mut self, challenger: &mut Challenger, state: &ProtocolState, block: &ParsedBlock) {
        if self.epoch.is_none_or(|epoch| epoch < state.epoch()) {
            let epoch = &state.current.epoch;
            let schedule = Schedule::new(epoch.start_slot, epoch.preferences.challenge_cadence);
            self.begin(challenger, epoch.id, schedule, has_turned_active(state.phase()));
        }

        for instruction in &block.instructions {
            if let ParsedInstruction::AdvanceEpoch { event } = instruction {
                let schedule = Schedule::new(block.slot, event.preferences.challenge_cadence);
                self.begin(challenger, event.new_epoch, schedule, false);
            }
            if let ParsedInstruction::FinalizeSnapshot { .. } = instruction {
                challenger.turn_active();
            }
        }
    }

    fn begin(
        &mut self,
        challenger: &mut Challenger,
        epoch: EpochNumber,
        schedule: Schedule,
        has_turned_active: bool,
    ) {
        challenger.begin_epoch(epoch, schedule, has_turned_active);
        self.epoch = Some(epoch);
    }

    /// Whether this node takes part in a round now
    ///
    /// It has to be at the chain tip, and its store has to have reached the
    /// epoch, so that every block of the epoch before is applied and the set
    /// of tracks is whole.
    fn is_taking_part(&self) -> bool {
        let has_store_reached = match (self.epoch, self.context.store_epoch.current()) {
            (Some(epoch), Some(reached)) => reached >= epoch,
            (Some(_), None) | (None, Some(_)) | (None, None) => false,
        };

        has_store_reached && self.context.is_at_tip()
    }

    /// Send a batch once. A send that fails leaves this node's entries empty on the peer's board.
    fn send(&mut self, peer: Address, batch: Batch) {
        let api = Arc::clone(&self.context.api);
        let req = to_req(batch);

        self.sends.spawn(async move {
            let _ = api.challenge(peer, &req).await;
        });
    }

    fn on_arrival(&mut self, arrival: Arrival) {
        let state = self.context.state();

        self.context.challenge.challenger().on_proofs(
            &self.context.store,
            &state,
            arrival.sender,
            arrival.batch,
            arrival.at,
        );
    }

    /// Sit out the rounds whose blocks were lost, and take the epoch from the state again
    fn on_lag(&mut self, lost: u64) {
        warn!(lost, "challenge task fell behind the block feed");
        self.epoch = None;
    }
}

/// Whether an epoch in this phase has turned Active
fn has_turned_active(phase: EpochPhase) -> bool {
    match phase {
        EpochPhase::Active | EpochPhase::Closing | EpochPhase::Completed => true,
        EpochPhase::Unknown | EpochPhase::Sync | EpochPhase::Snapshot => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use bytemuck::Zeroable;
    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::event::{EpochAdvanced, SnapshotFinalized};
    use tape_core::system::NodePreferences;
    use tape_core::types::{RoundNumber, SlotNumber};
    use tape_crypto::Hash;
    use tape_protocol::api::{ApiError, ChallengeReq, ChallengeRequest, ChallengeRes, PeerReq, PeerRes};
    use tape_store_memory::MemoryStore;

    use crate::core::ingest::IngestState;
    use crate::features::challenge::board::{BoardRound, Outcome};
    use crate::features::challenge::fixture::{every_case, fill, EPOCH, PREVIOUS};
    use crate::features::challenge::wire::from_request;
    use crate::harness::{NodeHarness, TestContext};

    use super::*;

    const NODES: usize = 20;
    const CADENCE: u64 = 5;
    const START: SlotNumber = SlotNumber(1_000);
    const FILLER_TAPE: u8 = 100;
    const FILLER_TRACKS: u64 = 24;
    const WAIT: Duration = Duration::from_secs(10);

    type Sent = Arc<Mutex<Vec<(Address, ChallengeReq)>>>;
    type Manager = ChallengeManager<MemoryStore, MemoryApi, LiteSvmRpc>;

    /// A 20-node group in `phase`, where every node keeps what it is asked to send
    async fn cluster(phase: EpochPhase) -> (NodeHarness, Sent) {
        let sent: Sent = Arc::default();
        let kept = Arc::clone(&sent);
        let api = MemoryApi::new(move |peer, req| match req {
            PeerReq::Challenge(req) => {
                kept.lock().expect("sent lock").push((peer, req));
                PeerRes::Challenge(Ok(ChallengeRes))
            }
            _ => PeerRes::Challenge(Err(ApiError::Other("unexpected".into()))),
        });

        let harness = NodeHarness::builder()
            .nodes(NODES)
            .epoch(EPOCH)
            .phase(phase)
            .current_group_count(1)
            .prev_committee_size(NODES)
            .prev_group_count(1)
            .api(api)
            .challenge(true)
            .build()
            .await
            .expect("build harness");

        (harness, sent)
    }

    /// Put a node at the tip of an epoch that began at `START`, with tracks to prove
    fn ready(ctx: &TestContext) {
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = START;
        state.current.epoch.preferences.challenge_cadence = CADENCE;
        ctx.set_state(state).expect("state set");

        ctx.ingest.publish(IngestState::AtTip);
        ctx.store_epoch.reach(EPOCH);
        every_case(&ctx.store);
        fill(&ctx.store, FILLER_TAPE, FILLER_TRACKS);
    }

    fn manager(ctx: &TestContext) -> (Manager, broadcast::Sender<Arc<ParsedBlock>>) {
        let (feed, blocks) = broadcast::channel(16);
        let arrivals = ctx.challenge.attach();
        let manager = ChallengeManager::new(ctx.clone(), blocks, arrivals, CancellationToken::new());

        (manager, feed)
    }

    fn slot(round: u64) -> SlotNumber {
        SlotNumber(START.0 + round * CADENCE)
    }

    fn block(slot: SlotNumber, instructions: Vec<ParsedInstruction>) -> ParsedBlock {
        ParsedBlock {
            slot,
            blockhash: Hash::from([slot.0 as u8; 32]),
            instructions,
            ..ParsedBlock::default()
        }
    }

    fn seed_block(round: u64) -> ParsedBlock {
        block(slot(round), Vec::new())
    }

    fn round_at(ctx: &TestContext, slot: SlotNumber) -> Option<BoardRound> {
        ctx.challenge.challenger().board().round(slot).cloned()
    }

    async fn drain(manager: &mut Manager) {
        while manager.sends.join_next().await.is_some() {}
    }

    // a round sends one batch to each peer, once, and expects one proof from each
    #[tokio::test]
    async fn one_batch_per_peer() {
        let (harness, sent) = cluster(EpochPhase::Active).await;
        let ctx = harness.ctx_for(0);
        ready(&ctx);
        let (mut manager, _feed) = manager(&ctx);

        manager.on_block(&seed_block(1));
        manager.on_block(&seed_block(1));
        drain(&mut manager).await;

        let sent = sent.lock().expect("sent lock");
        let mut peers: Vec<Address> = sent.iter().map(|(peer, _)| *peer).collect();
        peers.sort();
        peers.dedup();
        assert_eq!(sent.len(), NODES - 1);
        assert_eq!(peers.len(), NODES - 1);
        assert!(!peers.contains(&ctx.node_address()));
        for (_, req) in sent.iter() {
            assert_eq!((req.epoch, req.round), (EPOCH, RoundNumber(1)));
            assert_eq!(req.proofs.len(), 1);
        }

        let round = round_at(&ctx, slot(1)).expect("round on the board");
        assert!(round.opened.is_some());
        assert_eq!(round.entries.len(), NODES - 1);
    }

    // a peer's batch goes over the wire form, through the queue, and is recorded as valid
    #[tokio::test]
    async fn peer_proof() {
        let (harness, sent) = cluster(EpochPhase::Active).await;
        let (observer, prover) = (harness.ctx_for(0), harness.ctx_for(1));
        ready(&observer);
        ready(&prover);
        let (mut watching, _feed) = manager(&observer);
        let (mut proving, _other_feed) = manager(&prover);

        watching.on_block(&seed_block(1));
        proving.on_block(&seed_block(1));
        drain(&mut proving).await;

        let req = sent
            .lock()
            .expect("sent lock")
            .iter()
            .find(|(peer, req)| *peer == observer.node_address() && req.proofs.len() == 1)
            .map(|(_, req)| req.clone())
            .expect("a batch for the observer");
        let spool = req.proofs[0].spool;
        let wire = ChallengeRequest { epoch: req.epoch, round: req.round, proofs: req.proofs };
        let arrival = Arrival {
            sender: prover.node_address(),
            batch: from_request(wire),
            at: Instant::now(),
        };
        assert!(observer.challenge.submit(arrival));
        let queued = watching.arrivals.try_recv().expect("queued batch");
        watching.on_arrival(queued);

        let round = round_at(&observer, slot(1)).expect("round on the board");
        let entry = round
            .entries
            .iter()
            .find(|entry| entry.spool == spool)
            .expect("an entry for the prover's spool");
        assert_eq!(entry.seen.map(|seen| seen.outcome), Some(Outcome::Ok));
    }

    // rounds keep opening through every phase, the holders switch at the block
    // that turns the epoch Active, and the next epoch counts from its advance block
    #[tokio::test]
    async fn through_phases() {
        let (harness, _sent) = cluster(EpochPhase::Sync).await;
        let ctx = harness.ctx_for(0);
        ready(&ctx);
        let (mut manager, _feed) = manager(&ctx);
        let answering = |slot: SlotNumber| {
            round_at(&ctx, slot)
                .and_then(|round| round.opened)
                .map(|opened| (opened.answering, opened.round.number))
        };

        manager.on_block(&seed_block(1));
        ctx.update_phase(EpochPhase::Snapshot).expect("phase set");
        manager.on_block(&seed_block(2));
        let finalize = ParsedInstruction::FinalizeSnapshot {
            epoch: PREVIOUS,
            event: SnapshotFinalized::zeroed(),
        };
        manager.on_block(&block(slot(3), vec![finalize]));
        ctx.update_phase(EpochPhase::Closing).expect("phase set");
        manager.on_block(&seed_block(4));

        assert_eq!(answering(slot(1)), Some((PREVIOUS, RoundNumber(1))));
        assert_eq!(answering(slot(2)), Some((PREVIOUS, RoundNumber(2))));
        assert_eq!(answering(slot(3)), Some((EPOCH, RoundNumber(3))));
        assert_eq!(answering(slot(4)), Some((EPOCH, RoundNumber(4))));

        let next = EPOCH.next();
        let advance = ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch: EPOCH,
                new_epoch: next,
                preferences: NodePreferences { challenge_cadence: 3, ..NodePreferences::zeroed() },
                ..EpochAdvanced::zeroed()
            },
        };
        let advanced_at = SlotNumber(slot(4).0 + 1);
        manager.on_block(&block(advanced_at, vec![advance]));
        let first = SlotNumber(advanced_at.0 + 3);
        let second = SlotNumber(advanced_at.0 + 6);

        // The store has yet to apply the advance block, so the node sits the round out.
        manager.on_block(&block(first, Vec::new()));
        manager.on_block(&seed_block(5));
        ctx.store_epoch.reach(next);
        manager.on_block(&block(second, Vec::new()));

        assert!(round_at(&ctx, advanced_at).is_none());
        assert!(round_at(&ctx, slot(5)).is_none());
        assert!(round_at(&ctx, first).expect("round on the board").opened.is_none());
        assert_eq!(answering(second), Some((EPOCH, RoundNumber(2))));
        drain(&mut manager).await;
    }

    // off the tip a node sends nothing and marks the round as not observed
    #[tokio::test]
    async fn off_tip() {
        let (harness, sent) = cluster(EpochPhase::Active).await;
        let ctx = harness.ctx_for(0);
        ready(&ctx);
        ctx.ingest.publish(IngestState::Catching { lag_slots: 100 });
        let (mut manager, _feed) = manager(&ctx);

        manager.on_block(&seed_block(1));
        drain(&mut manager).await;

        let round = round_at(&ctx, slot(1)).expect("round on the board");
        assert!(round.opened.is_none());
        assert!(sent.lock().expect("sent lock").is_empty());
    }

    // a task that lost blocks has no rounds for them and carries on from its state
    #[tokio::test]
    async fn lost_blocks() {
        let (harness, _sent) = cluster(EpochPhase::Active).await;
        let ctx = harness.ctx_for(0);
        ready(&ctx);
        let (mut manager, _feed) = manager(&ctx);

        manager.on_block(&seed_block(1));
        manager.on_lag(40);
        manager.on_block(&seed_block(5));
        drain(&mut manager).await;

        assert!(round_at(&ctx, slot(1)).is_some());
        assert!(round_at(&ctx, slot(3)).is_none());
        assert!(round_at(&ctx, slot(5)).expect("round on the board").opened.is_some());
    }

    // the task follows the feed, and a cancel ends it with its sends
    #[tokio::test]
    async fn runs_until_cancel() {
        let (harness, sent) = cluster(EpochPhase::Active).await;
        let ctx = harness.ctx_for(0);
        ready(&ctx);
        let (manager, feed) = manager(&ctx);
        let cancel = manager.cancel.clone();
        let task = tokio::spawn(manager.run());

        feed.send(Arc::new(seed_block(1))).expect("task subscribed");
        let all_sent = async {
            while sent.lock().expect("sent lock").len() < NODES - 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(WAIT, all_sent).await.expect("round sent");
        cancel.cancel();

        let ended = tokio::time::timeout(WAIT, task).await.expect("task ends");
        assert!(ended.expect("task joins").is_ok());
        assert!(round_at(&ctx, slot(1)).is_some());
    }
}
