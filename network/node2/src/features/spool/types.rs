use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Sync,
    Scan,
    Repair,
    Recover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskDone {
    Done(Action, TaskResult),
    Cancelled(Action, TaskResult),
    Rejected(Action, TaskResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskResult {
    action: Action,
    epoch: EpochNumber,
    spool: SpoolIndex,
    processed: usize,
    remaining: usize,
}
