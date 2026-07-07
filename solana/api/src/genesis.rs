use tape_core::erasure::GROUP_SIZE;
use tape_core::system::NodePreferences;
use tape_core::types::{BasisPoints, EpochDuration, StorageUnits, VersionId};
use tape_core::types::coin::{Coin, TAPE};

use crate::program::token::ONE_TAPE;
use crate::program::tapedrive::{
    DEFAULT_BURN_FEE_BPS, DEFAULT_STORAGE_CAPACITY, DEFAULT_STORAGE_PRICE,
    DEFAULT_SUBSIDY_DECAY_BPS,
};

/// Genesis params for bringing up a fresh network.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GenesisConfig {
    pub committee_size: u64,
    pub spool_groups: u64,
    pub min_version: VersionId,
    pub min_epoch_duration: EpochDuration,
    pub max_epoch_duration: EpochDuration,
    pub epoch_duration: EpochDuration,
    pub storage_capacity: StorageUnits,
    pub storage_price: Coin<TAPE>,
    pub burn_fee_bps: BasisPoints,
    pub subsidy_decay_bps: BasisPoints,
    pub subsidy_amount: Coin<TAPE>,
}

impl GenesisConfig {
    /// Production profile: 1-week epochs.
    #[inline(always)]
    pub const fn mainnet() -> Self {
        MAINNET
    }

    /// Deployed fleet on Solana devnet: 1-hour epochs.
    #[inline(always)]
    pub const fn devnet() -> Self {
        DEVNET
    }

    /// Local-validator e2e harness: 100-second epochs.
    #[inline(always)]
    pub const fn localnet() -> Self {
        LOCALNET
    }

    /// In-memory simnet harness: 20-second epochs.
    #[inline(always)]
    pub const fn simnet() -> Self {
        SIMNET
    }
}

const MAINNET: GenesisConfig = GenesisConfig {
    committee_size: GROUP_SIZE as u64,
    spool_groups: 1,
    min_version: VersionId(0),
    min_epoch_duration: EpochDuration(86_400),     // 1 day
    max_epoch_duration: EpochDuration(1_209_600),  // 2 weeks
    epoch_duration: EpochDuration(604_800),        // 1 week
    storage_capacity: DEFAULT_STORAGE_CAPACITY,
    storage_price: DEFAULT_STORAGE_PRICE,
    burn_fee_bps: DEFAULT_BURN_FEE_BPS,
    subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
    subsidy_amount: TAPE(0),
};

const DEVNET: GenesisConfig = GenesisConfig {
    epoch_duration: EpochDuration(3_600),          // 1 hour
    ..LOCALNET
};

const LOCALNET: GenesisConfig = GenesisConfig {
    min_epoch_duration: EpochDuration(60),         // 60 seconds
    max_epoch_duration: EpochDuration(1_209_600),  // 2 weeks
    epoch_duration: EpochDuration(100),            // 100 seconds
    spool_groups: 10,
    subsidy_amount: TAPE(50_000 * ONE_TAPE),       // 50000 TAPE
    ..MAINNET
};

const SIMNET: GenesisConfig = GenesisConfig {
    min_epoch_duration: EpochDuration(10),         // 10 seconds
    max_epoch_duration: EpochDuration(200),        // 200 seconds
    epoch_duration: EpochDuration(20),             // 20 seconds
    ..MAINNET
};

impl From<&GenesisConfig> for NodePreferences {
    fn from(config: &GenesisConfig) -> Self {
        NodePreferences {
            storage_capacity: config.storage_capacity,
            storage_price: config.storage_price,
            committee_size: config.committee_size,
            spool_groups: config.spool_groups,
            min_version: config.min_version,
            burn_fee_bps: config.burn_fee_bps,
            subsidy_decay_bps: config.subsidy_decay_bps,
            epoch_duration: config.epoch_duration,
            access_threshold: TAPE(0),
        }
    }
}
