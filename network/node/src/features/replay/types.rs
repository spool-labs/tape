use tape_core::spooler::GroupIndex;
use tape_core::snapshot::replay::ReplayRecord;
use tape_core::types::SlotNumber;
use tape_crypto::address::Address;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTrack {
    pub track: Address,
    pub group: GroupIndex,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub records: Vec<ReplayRecord>,
    pub raw_tracks: Vec<RawTrack>,
}
