use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::ChannelConfig;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub state: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub state: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels(config: &ChannelConfig) -> (DownstreamSenders, DownstreamReceivers) {
    let (state_tx, state_rx) = mpsc::channel(config.parsed_block_capacity);
    let (replay_tx, replay_rx) = mpsc::channel(config.parsed_block_capacity);

    (
        DownstreamSenders {
            state: state_tx,
            replay: replay_tx,
        },
        DownstreamReceivers {
            state: state_rx,
            replay: replay_rx,
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

pub fn store_channel(config: &ChannelConfig) -> (mpsc::Sender<ReplayBatch>, mpsc::Receiver<ReplayBatch>) {
    mpsc::channel(config.replay_batch_capacity)
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
