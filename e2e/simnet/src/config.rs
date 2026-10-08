use std::time::Duration;
use solana_pubkey::Pubkey;
use tape_api::genesis::GenesisConfig;
use tape_core::encoding::{ErasureCode, TAPENET_PROFILE};
use tape_core::thresholds::{validate_profile, ThresholdError, TrustModel};
use tape_node::config::challenge::ChallengeConfig;

use crate::force_challenge;

/// A serialized account payload to seed into the simulated on-chain state.
#[derive(Debug, Clone)]
pub struct SeededAccount {
    pub address: Pubkey,
    pub owner: Pubkey,
    pub data: Vec<u8>,
}

impl SeededAccount {
    pub fn new(
        address: impl Into<Pubkey>,
        owner: impl Into<Pubkey>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            address: address.into(),
            owner: owner.into(),
            data,
        }
    }
}

/// Runtime mode for spawned node fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum NodeRuntimeMode {
    /// Spawn full runtime (`ingestor + fsm + scheduler + task_runner + http`).
    Full,
    /// Do not spawn runtime tasks automatically.
    #[default]
    Disabled,
}


/// Top-level simnet configuration.
#[derive(Debug, Clone)]
pub struct SimnetConfig {
    /// Erasure code the simulated chain is created with.
    pub profile: ErasureCode,
    /// What the simulated chain assumes about its nodes.
    pub trust_model: TrustModel,
    /// Number of nodes to create.
    pub node_count: usize,
    /// Runtime mode for each node.
    pub runtime_mode: NodeRuntimeMode,
    /// Shutdown timeout for runtime tasks.
    pub stop_timeout: Duration,
    /// Deterministic slot advancement after each successful transaction.
    pub slot_advance_per_tx: u64,
    /// Enable writing simnet logs to `target/sim-e2e/sim.log`.
    pub file_log: bool,
    /// Arbitrary serialized accounts to inject into LiteSVM before nodes start.
    pub seed_accounts: Vec<SeededAccount>,
    /// Challenge rounds, shared by every node unless a node is set otherwise.
    pub challenge: ChallengeConfig,
}

impl SimnetConfig {
    /// The genesis config the simulated chain starts from: simnet's values on
    /// this config's profile and trust model, with one committee seat per spool
    /// of a group.
    pub fn genesis(&self) -> Result<GenesisConfig, ThresholdError> {
        let profile = validate_profile(self.trust_model, self.profile.into())?;

        Ok(GenesisConfig {
            profile,
            trust_model: self.trust_model,
            committee_size: u64::from(profile.n()),
            ..GenesisConfig::simnet()
        })
    }
}

impl Default for SimnetConfig {
    fn default() -> Self {
        Self {
            profile: TAPENET_PROFILE,
            trust_model: TrustModel::Permissionless,
            node_count: usize::from(TAPENET_PROFILE.n()),
            runtime_mode: NodeRuntimeMode::Disabled,
            stop_timeout: Duration::from_secs(5),
            slot_advance_per_tx: 1,
            file_log: false,
            seed_accounts: Vec::new(),
            challenge: ChallengeConfig { enabled: force_challenge() },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::FORCE_CHALLENGE_VAR;

    use super::*;

    // a cluster starts with challenge rounds on exactly when the run forces them on
    #[test]
    fn challenge_switch() {
        let is_forced = std::env::var(FORCE_CHALLENGE_VAR).is_ok_and(|value| value == "1");

        assert_eq!(SimnetConfig::default().challenge.enabled, is_forced);
    }
}
