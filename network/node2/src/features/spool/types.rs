use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Sync,
    Scan,
    Repair,
    Recover,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncResult {
    Done { synced: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanResult {
    Done { gaps: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairResult {
    Done { unrepairable: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverResult {
    Done { remaining: usize },
}

/// Result of a single spool worker completing.
/// Carries enough info for the manager to apply the FSM transition.
#[derive(Debug)]
pub enum WorkerDone {
    Sync(SpoolIndex, EpochNumber, SyncResult),
    Scan(SpoolIndex, EpochNumber, ScanResult),
    Repair(SpoolIndex, EpochNumber, RepairResult),
    Recover(SpoolIndex, EpochNumber, RecoverResult),
}
