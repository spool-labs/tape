use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::config::ChannelConfig;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub epoch: mpsc::Sender<Arc<ParsedBlock>>,
    pub spool: mpsc::Sender<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub epoch: mpsc::Receiver<Arc<ParsedBlock>>,
    pub spool: mpsc::Receiver<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels(config: &ChannelConfig) -> (DownstreamSenders, DownstreamReceivers) {
    let (epoch_tx, epoch_rx) = mpsc::channel(config.parsed_block_capacity);
    let (spool_tx, spool_rx) = mpsc::channel(config.parsed_block_capacity);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(config.parsed_block_capacity);
    let (replay_tx, replay_rx) = mpsc::channel(config.parsed_block_capacity);

    (
        DownstreamSenders {
            epoch: epoch_tx,
            spool: spool_tx,
            snapshot: snapshot_tx,
            replay: replay_tx,
        },
        DownstreamReceivers {
            epoch: epoch_rx,
            spool: spool_rx,
            snapshot: snapshot_rx,
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
