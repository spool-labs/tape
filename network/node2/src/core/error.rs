use std::error::Error as StdError;

use rpc::RpcError;
use tape_blocks::ParseError;
use tape_core::types::EpochNumber;
use tape_retry::Retryable;
use tokio::task::JoinError;

use crate::core::types::{ChannelName, ServiceName};

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("tracing initialization failed: {0}")]
    TracingInit(#[source] Box<dyn StdError + Send + Sync>),

    #[error("failed to build tokio runtime: {0}")]
    RuntimeBuild(#[source] std::io::Error),

    #[error("failed to register shutdown signal: {0}")]
    SignalRegistration(#[source] std::io::Error),

    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("keypair error: {0}")]
    Keypair(String),

    #[error("storage error: {0}")]
    Store(String),

    #[error("failed to publish protocol state")]
    StatePublish,

    #[error("protocol state for epoch {expected_epoch} is not available")]
    StateUnavailable { expected_epoch: EpochNumber },

    #[error("rpc error: {0}")]
    Rpc(#[from] RpcError),

    #[error("block parse error: {0}")]
    BlockParse(#[from] ParseError),

    #[error("failed to send on channel {channel:?}")]
    ChannelSend { channel: ChannelName },

    #[error("channel {channel:?} closed unexpectedly")]
    ChannelClosed { channel: ChannelName },

    #[error("service {service:?} exited before shutdown")]
    UnexpectedServiceExit { service: ServiceName },

    #[error("service {service:?} join failed: {source}")]
    ServiceJoin {
        service: ServiceName,
        #[source]
        source: JoinError,
    },
}

impl NodeError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Rpc(error) if error.is_retriable() && !error.is_skipped_slot())
    }
}

impl Retryable for NodeError {
    fn is_retryable(&self) -> bool {
        self.is_retryable()
    }
}
