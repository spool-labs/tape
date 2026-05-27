use std::path::PathBuf;

use tape_core::types::EpochDuration;

pub struct TestnetConfig {
    pub rpc_url: String,
    pub node_binary: PathBuf,
    pub data_dir: PathBuf,
    pub base_port: u16,
    pub sol_airdrop: u64,
    pub stake_amount: u64,
    pub spool_groups: u64,
    pub epoch_duration: EpochDuration,
    pub min_epoch_duration: EpochDuration,
    pub max_epoch_duration: EpochDuration,
}

impl Default for TestnetConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".into(),
            node_binary: PathBuf::from("target/debug/tape-node"),
            data_dir: PathBuf::from("target/testnet"),
            base_port: 4000,
            sol_airdrop: 50_000_000_000,
            stake_amount: 1_000_000,
            spool_groups: 1,
            epoch_duration: EpochDuration(100),
            min_epoch_duration: EpochDuration(60),
            max_epoch_duration: EpochDuration(14 * 24 * 60 * 60),
        }
    }
}
