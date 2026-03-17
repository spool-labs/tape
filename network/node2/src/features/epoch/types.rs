use tape_core::types::EpochNumber;

/// Actions the lifecycle worker can run. Each maps to a single on-chain instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    SyncEpoch,
    AdvancePool,
    JoinNetwork,
    AdvanceEpoch,
}

/// Outcome of a lifecycle task completing.
///
/// Tasks loop internally with retry. They only return when:
///   - The tx landed successfully (Done)
///   - The cancel token fired (Cancelled)
///   - The tx was rejected for a reason the task cannot recover from (Rejected)
///
/// Rejected does NOT mean the manager gives up — the manager re-evaluates
/// and may respawn the same or a different task depending on current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskDone {
    Done(Action, EpochNumber),
    Cancelled(Action, EpochNumber),
    Rejected(Action, EpochNumber),
}

impl TaskDone {
    pub fn action(&self) -> Action {
        match self {
            TaskDone::Done(a, _) => *a,
            TaskDone::Cancelled(a, _) => *a,
            TaskDone::Rejected(a, _) => *a,
        }
    }

    pub fn epoch(&self) -> EpochNumber {
        match self {
            TaskDone::Done(_, e) => *e,
            TaskDone::Cancelled(_, e) => *e,
            TaskDone::Rejected(_, e) => *e,
        }
    }
}
