use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolIndex;
use tape_core::types::{EpochNumber, NodeId};
use tape_store::types::Pubkey;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpoolTaskKind {
    Sync,
    Scan,
    Recover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpoolWorkItem {
    pub spool_id: SpoolIndex,
    pub epoch: EpochNumber,
    pub kind: SpoolTaskKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolTaskSummary {
    SyncDone,
    SyncUnavailable,
    ScanDone { gaps: usize },
    RecoverDone { remaining: usize },
}

#[derive(Debug, Clone)]
pub struct SpoolAssignment {
    pub work: SpoolWorkItem,
    pub cancel: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolEvent {
    EpochReconcile {
        spool_id: SpoolIndex,
        epoch: EpochNumber,
        owned: bool,
        prev_owner: Option<NodeId>,
        prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
    },
    TaskSummary {
        work: SpoolWorkItem,
        summary: SpoolTaskSummary,
    },
    MissingCertifiedSlice {
        spool_id: SpoolIndex,
        track: Pubkey,
    },
}
