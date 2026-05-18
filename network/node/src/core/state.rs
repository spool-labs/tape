use std::sync::Arc;

use tape_core::types::EpochNumber;
use tape_protocol::ProtocolState;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::core::error::NodeError;

#[derive(Debug)]
pub struct StateBus {
    tx: watch::Sender<Arc<ProtocolState>>,
}

impl StateBus {
    pub fn new(initial_state: ProtocolState) -> Self {
        let (tx, _rx) = watch::channel(Arc::new(initial_state));

        Self { tx }
    }

    pub fn current(&self) -> Arc<ProtocolState> {
        self.tx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<ProtocolState>> {
        self.tx.subscribe()
    }

    pub fn publish(&self, state: ProtocolState) -> Result<(), NodeError> {
        self.tx.send_replace(Arc::new(state));
        Ok(())
    }

    /// Block until published protocol state reaches the requested epoch.
    pub async fn wait_for_epoch(
        &self,
        epoch: EpochNumber,
        cancel: &CancellationToken,
    ) -> Result<Arc<ProtocolState>, NodeError> {
        let mut rx = self.subscribe();

        loop {
            let current = rx.borrow().clone();
            if current.epoch() >= epoch {
                return Ok(current);
            }

            tokio::select! {
                _ = cancel.cancelled() => {
                    return Err(NodeError::StateUnavailable {
                        expected_epoch: epoch,
                    });
                }
                changed = rx.changed() => {
                    if changed.is_err() {
                        return Err(NodeError::StateUnavailable {
                            expected_epoch: epoch,
                        });
                    }
                }
            }
        }
    }
}

impl Default for StateBus {
    fn default() -> Self {
        Self::new(ProtocolState::default())
    }
}
