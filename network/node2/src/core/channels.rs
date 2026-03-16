use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::config::ChannelConfig;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;
use crate::features::spool::types::SpoolEvent;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub epoch: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub epoch: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels(config: &ChannelConfig) -> (DownstreamSenders, DownstreamReceivers) {
    let (epoch_tx, epoch_rx) = mpsc::channel(config.parsed_block_capacity);
    let (replay_tx, replay_rx) = mpsc::channel(config.parsed_block_capacity);

    (
        DownstreamSenders {
            epoch: epoch_tx,
            replay: replay_tx,
        },
        DownstreamReceivers {
            epoch: epoch_rx,
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

pub fn state_channel(config: &ChannelConfig) -> (mpsc::Sender<ReplayBatch>, mpsc::Receiver<ReplayBatch>) {
    mpsc::channel(config.replay_batch_capacity)
}

pub fn spool_event_channel(
    config: &ChannelConfig,
) -> (mpsc::Sender<SpoolEvent>, mpsc::Receiver<SpoolEvent>) {
    mpsc::channel(config.spool_event_capacity)
}

pub async fn send_replay_batch(
    sender: &mpsc::Sender<ReplayBatch>,
    batch: ReplayBatch,
) -> Result<(), NodeError> {
    sender
        .send(batch)
        .await
        .map_err(|_| NodeError::ChannelSend {
            channel: ChannelName::StateManager,
        })
}

pub async fn send_spool_event(
    sender: &mpsc::Sender<SpoolEvent>,
    event: SpoolEvent,
) -> Result<(), NodeError> {
    sender
        .send(event)
        .await
        .map_err(|_| NodeError::ChannelSend {
            channel: ChannelName::SpoolManager,
        })
}
