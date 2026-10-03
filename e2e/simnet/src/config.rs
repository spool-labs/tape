use std::time::Duration;
use solana_pubkey::Pubkey;
use tape_api::genesis::GenesisConfig;
use tape_core::encoding::{ErasureCode, TAPENET_PROFILE};
use tape_core::thresholds::{validate_profile, ThresholdError, TrustModel};
use tape_node::config::challenge::ChallengeConfig;
use tape_node::config::eviction::EvictionConfig;

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
    /// First TCP port used for node bind/public addresses.
    pub base_port: u16,
    /// Shutdown timeout for runtime tasks.
    pub stop_timeout: Duration,
    /// Deterministic slot advancement after each successful transaction.
    pub slot_advance_per_tx: u64,
    /// Enable writing simnet logs to `target/sim-e2e/sim.log`.
    pub file_log: bool,
    /// Arbitrary serialized accounts to inject into LiteSVM before nodes start.
    pub seed_accounts: Vec<SeededAccount>,
    /// Eviction participation and evidence policy, shared by every node.
    pub eviction: EvictionConfig,
    /// Explicit challenge settings applied to every node in this simulation.
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
            base_port: 19_000,
            stop_timeout: Duration::from_secs(5),
            slot_advance_per_tx: 1,
            file_log: false,
            seed_accounts: Vec::new(),
            eviction: Default::default(),
            challenge: Default::default(),
        }
    }
}
