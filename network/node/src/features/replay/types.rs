use tape_core::spooler::SpoolGroup;
use tape_core::snapshot::types::ReplayableEvent;
use tape_core::types::SlotNumber;
use tape_crypto::address::Address;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTrack {
    pub track: Address,
    pub spool_group: SpoolGroup,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub events: Vec<ReplayableEvent>,
    pub raw_tracks: Vec<RawTrack>,
}
