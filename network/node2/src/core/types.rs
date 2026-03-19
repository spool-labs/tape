#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceName {
    Unknown,
    HttpServer,
    BlockIngestor,
    LifecycleManager,
    SpoolManager,
    SnapshotManager,
    ReplayManager,
    StoreManager,
    StateManager,
    GcManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelName {
    StateManager,
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
