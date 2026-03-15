use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceName {
    Unknown,
    HttpServer,
    BlockIngestor,
    EpochManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelName {
    EpochManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    CtrlC,
    SigTerm,
}

