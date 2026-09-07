use tape_core::snapshot::replay::ReplayRecord;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;

pub use tape_blocks::RawTrack;

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub blockhash: Hash,
    pub block_time: Option<i64>,
    pub records: Vec<ReplayRecord>,
    pub raw_tracks: Vec<RawTrack>,
}
