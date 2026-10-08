#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceName {
    Unknown,
    HttpServer,
    S3Server,
    S3AdminServer,
    S3WriteDrain,
    BlockIngestor,
    IngestMonitor,
    AssignmentManager,
    ChallengeManager,
    EvictionManager,
    LifecycleManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StoreManager,
    StateManager,
    GcManager,
    PeerAggregator,
    AtlasObserve,
    BalanceMonitor,
    ObserveStream,
}

impl ServiceName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::HttpServer => "HttpServer",
            Self::S3Server => "S3Server",
            Self::S3AdminServer => "S3AdminServer",
            Self::S3WriteDrain => "S3WriteDrain",
            Self::BlockIngestor => "BlockIngestor",
            Self::IngestMonitor => "IngestMonitor",
            Self::AssignmentManager => "AssignmentManager",
            Self::ChallengeManager => "ChallengeManager",
            Self::EvictionManager => "EvictionManager",
            Self::LifecycleManager => "LifecycleManager",
            Self::SpoolManager => "SpoolManager",
            Self::SnapshotManager => "SnapshotManager",
            Self::ReplayManager => "ReplayManager",
            Self::StoreManager => "StoreManager",
            Self::StateManager => "StateManager",
            Self::GcManager => "GcManager",
            Self::PeerAggregator => "PeerAggregator",
            Self::AtlasObserve => "AtlasObserve",
            Self::BalanceMonitor => "BalanceMonitor",
            Self::ObserveStream => "ObserveStream",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelName {
    StateManager,
    AssignmentManager,
    ChallengeManager,
    EvictionManager,
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
