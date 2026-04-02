use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;

const PARSED_BLOCK_CHANNEL_CAPACITY: usize = 256;
const REPLAY_BATCH_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub state: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub state: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels() -> (DownstreamSenders, DownstreamReceivers) {
    let (state_tx, state_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (replay_tx, replay_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);

    (
        DownstreamSenders {
            state: state_tx,
            replay: replay_tx,
            snapshot: snapshot_tx,
        },
        DownstreamReceivers {
            state: state_rx,
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
