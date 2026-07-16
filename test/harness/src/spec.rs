use tape_api::program::tapedrive::DEFAULT_SUBSIDY_DECAY_BPS;

use crate::TEST_EPOCH_DURATION;
use tape_core::system::{EpochPhase, EpochState};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use bytemuck::Zeroable;

#[derive(Clone, Debug)]
pub struct HarnessSpec {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
    pub last_epoch: i64,
    pub nodes: Vec<HarnessNodeSpec>,
    pub current_committee_nodes: Vec<usize>,
    pub prev_committee_nodes: Vec<usize>,
    pub next_committee_nodes: Vec<usize>,
    pub current_group_count: u64,
    pub prev_group_count: u64,
    /// Whether to seed the next epoch as having a finalized assignment.
    /// This is useful for exercising `AdvanceEpoch`, which now requires the
    /// assignment manager to have completed before lifecycle can advance.
    pub next_assignment_ready: bool,
    /// Whether to seed the candidate E+2 epoch and committee accounts required
    /// before `AdvanceEpoch` can enter E+1.
    pub candidate_ready: bool,
    /// Whether to seed the previous epoch's finalized snapshot tape.
    /// `true` (the default) lets `commit_epoch` pass its previous-snapshot
    /// gate without first running the snapshot pipeline.
    pub seed_prev_snapshot_tape: bool,
}

impl HarnessSpec {
    pub fn epoch_state(&self) -> EpochState {
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
    pub burn_fee_bps: BasisPoints,
    pub subsidy_decay_bps: BasisPoints,
}

impl HarnessNodeSpec {
    pub fn default_for(epoch: EpochNumber) -> Self {
        Self {
            stake: TAPE(1_000),
            registered_epoch: EpochNumber(1),
            latest_sync_epoch: previous_epoch(epoch),
            latest_advance_epoch: previous_epoch(previous_epoch(epoch)),
            commission_rate: BasisPoints(0),
            storage_capacity: StorageUnits::mb(1_000_000),
            storage_price: TAPE(10),
            burn_fee_bps: BasisPoints(1_000),
            subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
        }
    }
}

pub fn previous_epoch(epoch: EpochNumber) -> EpochNumber {
    epoch.prev()
}

pub fn phase_to_epoch_state(phase: EpochPhase) -> EpochState {
    EpochState {
        phase: phase as u64,
        ..EpochState::zeroed()
    }
}

pub fn default_last_epoch() -> i64 {
    0
}

pub fn elapsed_last_epoch(now: i64) -> i64 {
    now - TEST_EPOCH_DURATION.0 as i64 - 1
}

pub fn onchain_elapsed_last_epoch(value: i64) -> i64 {
    -(value + 1)
}
