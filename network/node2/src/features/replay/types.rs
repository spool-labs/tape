use tape_core::spooler::SpoolGroup;
use tape_core::snapshot::ReplayableEvent;
use tape_core::types::SlotNumber;
use tape_store::types::Pubkey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTrack {
    pub track: Pubkey,
    pub spool_group: SpoolGroup,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReplayBatch {
    pub slot: SlotNumber,
    pub events: Vec<ReplayableEvent>,
    pub raw_tracks: Vec<RawTrack>,
}
