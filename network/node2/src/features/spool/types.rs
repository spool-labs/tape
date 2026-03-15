use tokio_util::sync::CancellationToken;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone)]
pub struct SpoolAssignment {
    pub epoch: EpochNumber,
    pub spool_id: SpoolIndex,
    pub cancel: CancellationToken,
}

#[derive(Debug)]
pub struct SpoolWorkerExit {
    pub spool_id: SpoolIndex,
}
