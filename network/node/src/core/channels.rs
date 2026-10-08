use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;

const PARSED_BLOCK_CHANNEL_CAPACITY: usize = 256;
const REPLAY_BATCH_CHANNEL_CAPACITY: usize = 256;
const BLOCK_FEED_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct DownstreamSenders {
    pub state: mpsc::Sender<Arc<ParsedBlock>>,
    pub assignment: mpsc::Sender<Arc<ParsedBlock>>,
    pub replay: mpsc::Sender<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Sender<Arc<ParsedBlock>>,

    /// Every ingested block, for readers that must never hold ingest up. A
    /// reader that falls behind loses blocks and is told how many.
    pub feed: broadcast::Sender<Arc<ParsedBlock>>,
}

pub struct DownstreamReceivers {
    pub state: mpsc::Receiver<Arc<ParsedBlock>>,
    pub assignment: mpsc::Receiver<Arc<ParsedBlock>>,
    pub replay: mpsc::Receiver<Arc<ParsedBlock>>,
    pub snapshot: mpsc::Receiver<Arc<ParsedBlock>>,
}

pub fn downstream_channels() -> (DownstreamSenders, DownstreamReceivers) {
    let (state_tx, state_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (assignment_tx, assignment_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (replay_tx, replay_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(PARSED_BLOCK_CHANNEL_CAPACITY);
    let (feed_tx, _) = broadcast::channel(BLOCK_FEED_CAPACITY);

    (
        DownstreamSenders {
            state: state_tx,
            assignment: assignment_tx,
            replay: replay_tx,
            snapshot: snapshot_tx,
            feed: feed_tx,
        },
        DownstreamReceivers {
            state: state_rx,
            assignment: assignment_rx,
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

/// Consume a downstream lane that this process does not otherwise read.
///
/// Downstream lanes are bounded, so leaving one unread eventually blocks ingest.
pub async fn drain_block_channel(
    mut rx: mpsc::Receiver<Arc<ParsedBlock>>,
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

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast::error::TryRecvError;

    use super::*;

    // the feed takes every block at once while its reader is behind, and the
    // reader is told it lost some
    #[test]
    fn feed_never_waits() {
        let (senders, _receivers) = downstream_channels();
        let mut reader = senders.feed.subscribe();

        for _ in 0..BLOCK_FEED_CAPACITY * 4 {
            assert!(senders.feed.send(Arc::new(ParsedBlock::default())).is_ok());
        }

        assert!(matches!(reader.try_recv(), Err(TryRecvError::Lagged(_))));
        assert!(reader.try_recv().is_ok());
    }

    // with nothing subscribed the feed drops the block
    #[test]
    fn feed_without_reader() {
        let (senders, _receivers) = downstream_channels();

        assert!(senders.feed.send(Arc::new(ParsedBlock::default())).is_err());
    }
}
