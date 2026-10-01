use tape_core::encoding::{ErasureCode, TAPENET_PROFILE};
use tape_core::system::NodePreferences;
use tape_core::thresholds::{validate_profile, ThresholdError, TrustModel};
use tape_core::types::{BasisPoints, EpochDuration, StorageUnits};
use tape_core::types::coin::{Coin, TAPE};

use crate::program::token::ONE_TAPE;
use crate::program::tapedrive::{
    DEFAULT_BURN_FEE_BPS, DEFAULT_STORAGE_CAPACITY, DEFAULT_STORAGE_PRICE,
    DEFAULT_SUBSIDY_DECAY_BPS,
};

/// Genesis params for bringing up a fresh network.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GenesisConfig {
    /// Erasure code every track uses, fixed for the network's life.
    pub profile: ErasureCode,
    /// What the network assumes about its nodes, fixed for its life.
    pub trust_model: TrustModel,
    /// Committee capacity at genesis: one node per spool of the first group.
    pub committee_size: u64,
    pub spool_groups: u64,
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

    /// A private cluster: the operator's profile and trust model, devnet's
    /// values for everything else.
    pub fn try_custom(profile: ErasureCode, trust_model: TrustModel) -> Result<Self, ThresholdError> {
        let profile = validate_profile(trust_model, profile.into())?;

        Ok(Self {
            profile,
            trust_model,
            committee_size: profile.n() as u64,
            ..DEVNET
        })
    }
}

const MAINNET: GenesisConfig = GenesisConfig {
    profile: TAPENET_PROFILE,
    trust_model: TrustModel::Permissionless,
    committee_size: TAPENET_PROFILE.n() as u64,
    spool_groups: 1,
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
    spool_groups: 5,
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
            network_capacity: config.storage_capacity,
            storage_price: config.storage_price,
            committee_size: config.committee_size,
            spool_groups: config.spool_groups,
            burn_fee_bps: config.burn_fee_bps,
            subsidy_decay_bps: config.subsidy_decay_bps,
            epoch_duration: config.epoch_duration,
            access_threshold: TAPE(0),
            node_capacity: StorageUnits::zero(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{RSParams, RepParams};
    use tape_core::thresholds::Thresholds;

    const PRESETS: [GenesisConfig; 4] = [MAINNET, DEVNET, LOCALNET, SIMNET];

    // every tapenet preset is a permissionless network on tapenet's profile
    #[test]
    fn tapenet_presets() {
        for preset in PRESETS {
            assert_eq!(preset.profile, TAPENET_PROFILE);
            assert_eq!(preset.trust_model, TrustModel::Permissionless);
            assert_eq!(preset.committee_size, 20);
            assert_eq!(
                validate_profile(preset.trust_model, preset.profile.into()),
                Ok(preset.profile),
            );
        }
    }

    // every tapenet preset has tapenet's thresholds
    #[test]
    fn tapenet_thresholds() {
        for preset in PRESETS {
            let thresholds = Thresholds::try_new(
                preset.trust_model,
                u64::from(preset.profile.n()),
                u64::from(preset.profile.k()),
            )
            .expect("tapenet");

            assert_eq!((thresholds.certify, thresholds.snapshot, thresholds.synced), (14, 7, 7));
        }
    }

    // a custom cluster takes devnet's values and sizes its committee to n
    #[test]
    fn custom_values() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));

        let custom = GenesisConfig::try_custom(profile, TrustModel::Trusted).expect("3 of 4");

        assert_eq!(
            custom,
            GenesisConfig {
                profile,
                trust_model: TrustModel::Trusted,
                committee_size: 4,
                ..DEVNET
            },
        );
    }

    // a custom cluster refuses a profile its code or trust model refuses
    #[test]
    fn custom_refused() {
        let no_parity = ErasureCode::ReedSolomon(RSParams::new(4, 0));
        let three_of_four = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let too_wide = ErasureCode::Replication(RepParams::new(21));

        assert!(GenesisConfig::try_custom(no_parity, TrustModel::Trusted).is_err());
        assert!(GenesisConfig::try_custom(too_wide, TrustModel::Trusted).is_err());
        assert!(GenesisConfig::try_custom(three_of_four, TrustModel::Permissionless).is_err());
    }
}
