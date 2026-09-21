use std::path::PathBuf;

pub struct LocalnetConfig {
    pub rpc_url: String,
    pub node_binary: PathBuf,
    pub data_dir: PathBuf,
    pub base_port: u16,
    pub sol_airdrop: u64,
    pub stake_amount: u64,
    pub spool_groups: u64,
}

impl Default for LocalnetConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".into(),
            node_binary: PathBuf::from("target/debug/tape-node"),
            data_dir: PathBuf::from("target/localnet"),
            base_port: 4000,
            sol_airdrop: 50_000_000_000,
            stake_amount: 100_000,
            spool_groups: 1,
        }
    }
}
