use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;
use tape_crypto::hash::Hash;

/// What the challenge lane sees of the chain.
///
/// Rounds run on produced blocks, so this lane carries a block the moment it is
/// confirmed rather than when it finalizes, plus the two later verdicts on it.
/// A round's evidence is only standing once its entropy block finalizes, and
/// evidence gathered under a candidate that lost has to go rather than settle.
#[derive(Debug)]
pub enum ChainEvent {
    /// A confirmed block, which may seed a round.
    Produced(Arc<ParsedBlock>),
    /// Blocks that lost, newest first. Any round they seeded is void.
    Rolled(Vec<Hash>),
    /// A block that survived to finality.
    Finalized(Hash),
}

const PARSED_BLOCK_CHANNEL_CAPACITY: usize = 256;
const REPLAY_BATCH_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub state: mpsc::Sender<Arc<ParsedBlock>>,
    pub assignment: mpsc::Sender<Arc<ParsedBlock>>,
    pub challenge: mpsc::Sender<ChainEvent>,
    pub eviction: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub state: mpsc::Receiver<Arc<ParsedBlock>>,
    pub assignment: mpsc::Receiver<Arc<ParsedBlock>>,
    pub challenge: mpsc::Receiver<ChainEvent>,
    pub eviction: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels() -> (DownstreamSenders, DownstreamReceivers) {
    let (state_tx, state_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (assignment_tx, assignment_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (challenge_tx, challenge_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (eviction_tx, eviction_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (replay_tx, replay_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);

    (
        DownstreamSenders {
            state: state_tx,
            assignment: assignment_tx,
            challenge: challenge_tx,
            eviction: eviction_tx,
            replay: replay_tx,
            snapshot: snapshot_tx,
        },
        DownstreamReceivers {
            state: state_rx,
            assignment: assignment_rx,
            challenge: challenge_rx,
            eviction: eviction_rx,
            replay: replay_rx,
            snapshot: snapshot_rx,
        },
    )
}

pub async fn send_block(
    sender: &mpsc::Sender<Arc<ParsedBlock>>,
    channel: ChannelName,
    block: Arc<ParsedBlock>,
) -> Result<(), NodeError> {
    sender
        .send(block)
        .await
        .map_err(|_| NodeError::ChannelSend { channel })
}

pub fn store_channel() -> (mpsc::Sender<ReplayBatch>, mpsc::Receiver<ReplayBatch>) {
    mpsc::channel(REPLAY_BATCH_CHANNEL_CAPACITY)
}

pub async fn send_replay_batch(
    sender: &mpsc::Sender<ReplayBatch>,
    batch: ReplayBatch,
) -> Result<(), NodeError> {
    sender
        .send(batch)
        .await
        .map_err(|_| NodeError::ChannelSend {
            channel: ChannelName::StoreManager,
        })
}

/// Consume a downstream lane nothing in this process reads.
///
/// The lanes are bounded and the ingestor sends to every one of them, so a lane
/// with no consumer stops block ingest outright. A runtime that skips a consumer
/// drains its lane instead. Generic because the challenge lane carries chain
/// events rather than blocks.
pub async fn drain_block_channel<Item>(
    mut rx: mpsc::Receiver<Item>,
    cancel: CancellationToken,
    channel: ChannelName,
) -> Result<(), NodeError> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            received = rx.recv() => {
                if received.is_none() {
                    return if cancel.is_cancelled() {
                        Ok(())
                    } else {
                        Err(NodeError::ChannelClosed { channel })
                    };
                }

                debug!(channel = ?channel, "drained unused block channel");
            }
        }
    }
}
