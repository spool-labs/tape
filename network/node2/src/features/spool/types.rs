use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct SpoolAssignment {
    pub epoch: EpochId,
    pub spool_id: SpoolId,
    pub cancel: CancellationToken,
}

#[derive(Debug)]
pub struct SpoolWorkerExit {
    pub spool_id: SpoolId,
}
