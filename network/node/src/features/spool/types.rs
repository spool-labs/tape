use tape_core::types::SpoolIndex;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Sync { spool: SpoolIndex, epoch: EpochNumber },
    Scan { spool: SpoolIndex, epoch: EpochNumber },
    Repair { spool: SpoolIndex, epoch: EpochNumber },
    Recover { spool: SpoolIndex, epoch: EpochNumber },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskDone {
    Done(Action, TaskResult),
    Cancelled(Action, TaskResult),
    Rejected(Action, TaskResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncResult {
    Done {
        synced_tracks: usize,
        synced_slices: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanResult {
    Done { gaps: usize },
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairResult {
    Done { unrepairable: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverResult {
    Done { remaining: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskResult {
    Sync(SyncResult),
    Scan(ScanResult),
    Repair(RepairResult),
    Recover(RecoverResult),
}

impl Action {
    pub fn spool(self) -> SpoolIndex {
        match self {
            Action::Sync { spool, .. }
            | Action::Scan { spool, .. }
            | Action::Repair { spool, .. }
            | Action::Recover { spool, .. } => spool,
        }
    }

    pub fn epoch(self) -> EpochNumber {
        match self {
            Action::Sync { epoch, .. }
            | Action::Scan { epoch, .. }
            | Action::Repair { epoch, .. }
            | Action::Recover { epoch, .. } => epoch,
        }
    }
}

impl TaskDone {
    pub fn action(self) -> Action {
        match self {
            TaskDone::Done(action, _)
            | TaskDone::Cancelled(action, _)
            | TaskDone::Rejected(action, _) => action,
        }
    }
}
