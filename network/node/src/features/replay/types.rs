use tape_core::snapshot::replay::ReplayRecord;
use tape_core::types::SlotNumber;

pub use tape_blocks::RawTrack;

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub records: Vec<ReplayRecord>,
    pub raw_tracks: Vec<RawTrack>,
}
