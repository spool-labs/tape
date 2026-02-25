use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCategory {
    SolanaTx,
    PeerHttp,
    CpuHeavy,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Task {
    /// Advance the on-chain epoch.
    AdvanceEpoch { epoch: EpochNumber },
    /// Sync this node's epoch state on-chain.
    SyncEpoch { epoch: EpochNumber },
    /// Join the network on-chain.
    JoinNetwork { epoch: EpochNumber },
    /// Advance a staking pool on-chain.
    AdvancePool { epoch: EpochNumber },
    /// Register a snapshot commitment on-chain.
    RegisterSnapshot { epoch: EpochNumber },
    /// Submit a snapshot certification transaction on-chain.
    SnapshotSubmit { epoch: EpochNumber },
    /// Invalidate a track on-chain.
    InvalidateTrack { track: Pubkey },
    /// Sync a spool from a peer.
    SpoolSync { spool: SpoolIndex },
    /// Scan for missing slices in a spool.
    RecoveryScan { spool: SpoolIndex },
    /// Recover missing slices for a spool.
    SpoolRecovery { spool: SpoolIndex },
    /// Build a snapshot for the current epoch.
    SnapshotBuild { epoch: EpochNumber },
    /// Collect snapshot signatures for certification.
    SnapshotCollect { epoch: EpochNumber },
    /// Bootstrap from a snapshot (new node joining).
    SnapshotBootstrap,
}

impl Task {
    pub fn category(&self) -> TaskCategory {
        match self {
            Task::AdvanceEpoch { .. }
            | Task::SyncEpoch { .. }
            | Task::JoinNetwork { .. }
            | Task::AdvancePool { .. }
            | Task::RegisterSnapshot { .. }
            | Task::SnapshotSubmit { .. }
            | Task::InvalidateTrack { .. } => TaskCategory::SolanaTx,
            Task::SpoolSync { .. } | Task::SpoolRecovery { .. } | Task::RecoveryScan { .. } => {
                TaskCategory::PeerHttp
            }
            Task::SnapshotBuild { .. } | Task::SnapshotCollect { .. } => TaskCategory::CpuHeavy,
            Task::SnapshotBootstrap => TaskCategory::PeerHttp,
        }
    }

    pub fn scheduled_epoch(&self) -> Option<EpochNumber> {
        match self {
            Task::AdvanceEpoch { epoch }
            | Task::SyncEpoch { epoch }
            | Task::JoinNetwork { epoch }
            | Task::AdvancePool { epoch }
            | Task::RegisterSnapshot { epoch }
            | Task::SnapshotSubmit { epoch }
            | Task::SnapshotBuild { epoch }
            | Task::SnapshotCollect { epoch } => Some(*epoch),
            _ => None,
        }
    }

    pub fn is_epoch_scoped(&self) -> bool {
        self.scheduled_epoch().is_some()
    }

    pub fn spool_id(&self) -> Option<SpoolIndex> {
        match self {
            Task::SpoolSync { spool }
            | Task::RecoveryScan { spool }
            | Task::SpoolRecovery { spool } => Some(*spool),
            _ => None,
        }
    }
}

/// Outcome of a single task execution attempt.
#[derive(Debug)]
pub enum TaskOutcome {
    Success,
    Retryable(String),
    /// Expected wait state with explicit retry delay.
    /// Used to avoid warning/error noise for non-failure polling.
    Pending(Duration),
    Permanent(String),
}

/// Result of a completed task, returned to the scheduler.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully.
    Success(Task),
    /// Task was explicitly canceled.
    Canceled(Task),
    /// Task failed with a retryable error.
    RetryableError(Task, String),
    /// Task failed permanently.
    PermanentError(Task, String),
}
