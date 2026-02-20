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
pub enum TaskKey {
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
    /// Refresh cached on-chain state.
    RefreshOnchainState,
}

impl TaskKey {
    pub fn category(&self) -> TaskCategory {
        match self {
            TaskKey::AdvanceEpoch { .. }
            | TaskKey::SyncEpoch { .. }
            | TaskKey::JoinNetwork { .. }
            | TaskKey::AdvancePool { .. }
            | TaskKey::RegisterSnapshot { .. }
            | TaskKey::SnapshotSubmit { .. }
            | TaskKey::InvalidateTrack { .. } => TaskCategory::SolanaTx,
            TaskKey::SpoolSync { .. } | TaskKey::SpoolRecovery { .. } | TaskKey::RecoveryScan { .. } => {
                TaskCategory::PeerHttp
            }
            TaskKey::SnapshotBuild { .. } | TaskKey::SnapshotCollect { .. } => TaskCategory::CpuHeavy,
            TaskKey::SnapshotBootstrap => TaskCategory::PeerHttp,
            TaskKey::RefreshOnchainState => TaskCategory::Internal,
        }
    }

    pub fn scheduled_epoch(&self) -> Option<EpochNumber> {
        match self {
            TaskKey::AdvanceEpoch { epoch }
            | TaskKey::SyncEpoch { epoch }
            | TaskKey::JoinNetwork { epoch }
            | TaskKey::AdvancePool { epoch }
            | TaskKey::RegisterSnapshot { epoch }
            | TaskKey::SnapshotSubmit { epoch }
            | TaskKey::SnapshotBuild { epoch }
            | TaskKey::SnapshotCollect { epoch } => Some(*epoch),
            _ => None,
        }
    }

    pub fn is_epoch_scoped(&self) -> bool {
        self.scheduled_epoch().is_some()
    }

    pub fn is_one_shot(&self) -> bool {
        matches!(
            self,
            TaskKey::AdvanceEpoch { .. }
                | TaskKey::SyncEpoch { .. }
                | TaskKey::JoinNetwork { .. }
                | TaskKey::AdvancePool { .. }
                | TaskKey::RegisterSnapshot { .. }
                | TaskKey::SnapshotSubmit { .. }
                | TaskKey::InvalidateTrack { .. }
                | TaskKey::RefreshOnchainState
                | TaskKey::SnapshotBuild { .. }
                | TaskKey::SnapshotCollect { .. }
                | TaskKey::SnapshotBootstrap
        )
    }

    /// Spool-oriented tasks are rerunnable while their owning spool remains in
    /// active work, so keep them as continuous tasks.
    pub fn spool_id(&self) -> Option<SpoolIndex> {
        match self {
            TaskKey::SpoolSync { spool }
            | TaskKey::RecoveryScan { spool }
            | TaskKey::SpoolRecovery { spool } => Some(*spool),
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
    Success(TaskKey),
    /// Task was explicitly canceled.
    Canceled(TaskKey),
    /// Task failed with a retryable error.
    RetryableError(TaskKey, String),
    /// Task failed permanently.
    PermanentError(TaskKey, String),
}
