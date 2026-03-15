use tape_core::snapshot::ReplayableEvent;
use tape_core::types::SlotNumber;

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub events: Vec<ReplayableEvent>,
}
