use std::collections::HashMap;
use std::path::PathBuf;

use tape_api::genesis::GenesisConfig;
use tape_core::encoding::{ErasureCode, TAPENET_PROFILE};
use tape_core::thresholds::{validate_profile, ThresholdError, TrustModel};

pub struct LocalnetConfig {
    pub rpc_url: String,
    pub node_binary: PathBuf,
    /// Nodes that run on another binary, by local id
    pub node_binary_overrides: HashMap<usize, PathBuf>,
    pub data_dir: PathBuf,
    pub base_port: u16,
    pub sol_airdrop: u64,
    pub stake_amount: u64,
    pub spool_groups: u64,
    pub is_admission_open: bool,
    pub is_challenge_enabled: bool,
    /// Erasure code the local chain is created with
    pub profile: ErasureCode,
    /// What the local chain assumes about its nodes
    pub trust_model: TrustModel,
}

impl LocalnetConfig {
    /// The genesis config the local chain starts from: localnet's values on
    /// this config's profile and trust model, with one committee seat per
    /// spool of a group.
    pub fn genesis(&self) -> Result<GenesisConfig, ThresholdError> {
        let profile = validate_profile(self.trust_model, self.profile.into())?;

        Ok(GenesisConfig {
            profile,
            trust_model: self.trust_model,
            committee_size: u64::from(profile.n()),
            ..GenesisConfig::localnet()
        })
    }
}

impl Default for LocalnetConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".into(),
            node_binary: PathBuf::from("target/debug/tape-node"),
            node_binary_overrides: HashMap::new(),
            data_dir: PathBuf::from("target/localnet"),
            base_port: 4000,
            sol_airdrop: 50_000_000_000,
            stake_amount: 100_000,
            spool_groups: 1,
            is_admission_open: false,
            is_challenge_enabled: false,
            profile: TAPENET_PROFILE,
            trust_model: TrustModel::Permissionless,
        }
    }
}
