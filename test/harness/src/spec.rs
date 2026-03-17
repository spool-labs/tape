use tape_api::program::EPOCH_DURATION;
use tape_core::spooler::SpoolCount;
use tape_core::system::{EpochPhase, EpochState};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};

#[derive(Clone, Debug)]
pub struct HarnessSpec {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
    pub last_epoch: i64,
    pub nodes: Vec<HarnessNodeSpec>,
    pub current_committee_nodes: Vec<usize>,
    pub prev_committee_nodes: Vec<usize>,
    pub next_committee_nodes: Vec<usize>,
    pub current_spool_counts: Vec<SpoolCount>,
    pub prev_spool_counts: Vec<SpoolCount>,
}

impl HarnessSpec {
    pub(crate) fn epoch_state(&self) -> EpochState {
        phase_to_epoch_state(self.phase)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessNodeSpec {
    pub stake: Coin<TAPE>,
    pub registered_epoch: EpochNumber,
    pub latest_sync_epoch: EpochNumber,
    pub latest_advance_epoch: EpochNumber,
    pub commission_rate: BasisPoints,
    pub storage_capacity: StorageUnits,
    pub storage_price: Coin<TAPE>,
}

impl HarnessNodeSpec {
    pub fn default_for(epoch: EpochNumber) -> Self {
        Self {
            stake: TAPE(1_000),
            registered_epoch: EpochNumber(1),
            latest_sync_epoch: previous_epoch(epoch),
            latest_advance_epoch: previous_epoch(epoch),
            commission_rate: BasisPoints(0),
            storage_capacity: StorageUnits::mb(1_000_000),
            storage_price: TAPE(10),
        }
    }
}

pub(crate) fn previous_epoch(epoch: EpochNumber) -> EpochNumber {
    EpochNumber(epoch.0.saturating_sub(1))
}

pub(crate) fn phase_to_epoch_state(phase: EpochPhase) -> EpochState {
    match phase {
        EpochPhase::Unknown => EpochState::new(),
        EpochPhase::Syncing => EpochState::syncing(),
        EpochPhase::Settling => EpochState::settling(),
        EpochPhase::Active => EpochState::active(),
    }
}

pub(crate) fn default_last_epoch() -> i64 {
    0
}

pub(crate) fn elapsed_last_epoch(now: i64) -> i64 {
    now - EPOCH_DURATION - 1
}

pub(crate) fn onchain_elapsed_last_epoch(value: i64) -> i64 {
    -(value + 1)
}
