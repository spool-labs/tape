use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use tape_core::spooler::SpoolIndex;

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
    AdvanceEpoch,
    /// Sync this node's epoch state on-chain.
    SyncEpoch,
    /// Join the network on-chain.
    JoinNetwork,
    /// Advance a staking pool on-chain.
    AdvancePool,
    /// Register a snapshot commitment on-chain.
    RegisterSnapshot,
    /// Certify a snapshot with BLS aggregate on-chain.
    CertifySnapshot,
    /// Invalidate a track on-chain.
    InvalidateTrack { track: Pubkey },
    /// Sync a spool from a peer.
    SpoolSync { spool: SpoolIndex },
    /// Scan for missing slices in a spool.
    RecoveryScan { spool: SpoolIndex },
    /// Recover missing slices for a spool.
    SpoolRecovery { spool: SpoolIndex },
    /// Build a snapshot for the current epoch.
    SnapshotBuild,
    /// Certify a snapshot by collecting BLS signatures.
    SnapshotCertify,
    /// Bootstrap from a snapshot (new node joining).
    SnapshotBootstrap,
    /// Refresh cached on-chain state.
    RefreshOnchainState,
}

impl TaskKey {
    pub fn category(&self) -> TaskCategory {
        match self {
            TaskKey::AdvanceEpoch
            | TaskKey::SyncEpoch
            | TaskKey::JoinNetwork
            | TaskKey::AdvancePool
            | TaskKey::RegisterSnapshot
            | TaskKey::CertifySnapshot
            | TaskKey::InvalidateTrack { .. } => TaskCategory::SolanaTx,
            TaskKey::SpoolSync { .. } | TaskKey::SpoolRecovery { .. } | TaskKey::RecoveryScan { .. } => {
                TaskCategory::PeerHttp
            }
            TaskKey::SnapshotBuild | TaskKey::SnapshotCertify => TaskCategory::CpuHeavy,
            TaskKey::SnapshotBootstrap => TaskCategory::PeerHttp,
            TaskKey::RefreshOnchainState => TaskCategory::Internal,
        }
    }

    pub fn is_one_shot(&self) -> bool {
        matches!(
            self,
            TaskKey::AdvanceEpoch
                | TaskKey::SyncEpoch
                | TaskKey::JoinNetwork
                | TaskKey::AdvancePool
                | TaskKey::RegisterSnapshot
                | TaskKey::CertifySnapshot
                | TaskKey::InvalidateTrack { .. }
                | TaskKey::RefreshOnchainState
                | TaskKey::RecoveryScan { .. }
                | TaskKey::SpoolRecovery { .. }
                | TaskKey::SnapshotBuild
                | TaskKey::SnapshotCertify
                | TaskKey::SnapshotBootstrap
                | TaskKey::SpoolSync { .. }
        )
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

