#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceName {
    Unknown,
    HttpServer,
    BlockIngestor,
    EpochManager,
    EpochLifecycle,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StateManager,
    GcManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelName {
    EpochManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StateManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    CtrlC,
    SigTerm,
}
