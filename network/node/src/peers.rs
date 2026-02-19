use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Maximum number of queued peer commands.
///
/// Backpressure policy: senders await when full. This keeps memory bounded and
/// naturally slows down producers during sustained peer churn.
const PEER_COMMAND_CHANNEL_CAPACITY: usize = 512;

struct PeerState {
    failures: u32,
    last_failure: Option<Instant>,
}

pub struct PeerTracker {
    peers: HashMap<SocketAddr, PeerState>,
}

impl PeerTracker {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn is_cooling_down(&self, addr: &SocketAddr) -> bool {
        let Some(state) = self.peers.get(addr) else {
            return false;
        };
        let Some(last) = state.last_failure else {
            return false;
        };
        let cooldown_seconds = 1u64 << state.failures.min(6);
        last.elapsed().as_secs() < cooldown_seconds
    }

    pub fn record_success(&mut self, addr: &SocketAddr) {
        self.peers.remove(addr);
    }

    pub fn record_failure(&mut self, addr: &SocketAddr) {
        let state = self.peers.entry(*addr).or_insert(PeerState {
            failures: 0,
            last_failure: None,
        });
        state.failures = state.failures.saturating_add(1);
        state.last_failure = Some(Instant::now());
    }

    pub fn reset(&mut self) {
        self.peers.clear();
    }
}

enum PeerCommand {
    IsCoolingDown {
        address: SocketAddr,
        response: oneshot::Sender<bool>,
    },
    RecordSuccess {
        address: SocketAddr,
    },
    RecordFailure {
        address: SocketAddr,
    },
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerServiceError {
    ServiceClosed,
    ResponseClosed,
}

impl fmt::Display for PeerServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerServiceError::ServiceClosed => write!(f, "peer service command channel is closed"),
            PeerServiceError::ResponseClosed => write!(f, "peer service response channel is closed"),
        }
    }
}

impl std::error::Error for PeerServiceError {}

#[derive(Clone)]
pub struct PeerHandle {
    sender: mpsc::Sender<PeerCommand>,
}

impl PeerHandle {
    pub async fn is_cooling_down(&self, address: SocketAddr) -> Result<bool, PeerServiceError> {
        let (response_sender, response_receiver) = oneshot::channel();
        if self
            .sender
            .send(PeerCommand::IsCoolingDown {
                address,
                response: response_sender,
            })
            .await
            .is_err()
        {
            return Err(PeerServiceError::ServiceClosed);
        }
        response_receiver
            .await
            .map_err(|_| PeerServiceError::ResponseClosed)
    }

    pub async fn record_success(&self, address: SocketAddr) -> Result<(), PeerServiceError> {
        self
            .sender
            .send(PeerCommand::RecordSuccess { address })
            .await
            .map_err(|_| PeerServiceError::ServiceClosed)
    }

    pub async fn record_failure(&self, address: SocketAddr) -> Result<(), PeerServiceError> {
        self
            .sender
            .send(PeerCommand::RecordFailure { address })
            .await
            .map_err(|_| PeerServiceError::ServiceClosed)
    }

    pub async fn reset(&self) -> Result<(), PeerServiceError> {
        self
            .sender
            .send(PeerCommand::Reset)
            .await
            .map_err(|_| PeerServiceError::ServiceClosed)
    }
}

pub struct PeerService {
    tracker: PeerTracker,
    receiver: mpsc::Receiver<PeerCommand>,
}

impl PeerService {
    pub fn new() -> (Self, PeerHandle) {
        let (sender, receiver) = mpsc::channel(PEER_COMMAND_CHANNEL_CAPACITY);
        let service = Self {
            tracker: PeerTracker::new(),
            receiver,
        };
        let handle = PeerHandle { sender };
        (service, handle)
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                command = self.receiver.recv() => {
                    match command {
                        Some(PeerCommand::IsCoolingDown { address, response }) => {
                            if response.send(self.tracker.is_cooling_down(&address)).is_err() {
                                tracing::warn!("peer service response receiver dropped");
                            }
                        }
                        Some(PeerCommand::RecordSuccess { address }) => {
                            self.tracker.record_success(&address);
                        }
                        Some(PeerCommand::RecordFailure { address }) => {
                            self.tracker.record_failure(&address);
                        }
                        Some(PeerCommand::Reset) => {
                            self.tracker.reset();
                        }
                        None => break,
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }
    }
}
