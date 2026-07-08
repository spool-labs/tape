#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceName {
    Unknown,
    HttpServer,
    BlockIngestor,
    IngestMonitor,
    AssignmentManager,
    LifecycleManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StoreManager,
    StateManager,
    GcManager,
    PeerAggregator,
}

impl ServiceName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::HttpServer => "HttpServer",
            Self::BlockIngestor => "BlockIngestor",
            Self::IngestMonitor => "IngestMonitor",
            Self::AssignmentManager => "AssignmentManager",
            Self::LifecycleManager => "LifecycleManager",
            Self::SpoolManager => "SpoolManager",
            Self::SnapshotManager => "SnapshotManager",
            Self::ReplayManager => "ReplayManager",
            Self::StoreManager => "StoreManager",
            Self::StateManager => "StateManager",
            Self::GcManager => "GcManager",
            Self::PeerAggregator => "PeerAggregator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelName {
    StateManager,
    AssignmentManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StoreManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    CtrlC,
    SigTerm,
}
