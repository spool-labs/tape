use std::path::PathBuf;

pub struct ProdnetConfig {
    pub rpc_url: String,
    pub node_binary: PathBuf,
    pub data_dir: PathBuf,
    pub base_port: u16,
    pub node_count: usize,
    pub sol_airdrop: u64,
    pub stake_amount: u64,
}

impl Default for ProdnetConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".into(),
            node_binary: PathBuf::from("target/debug/tape-node2"),
            data_dir: PathBuf::from("target/prodnet"),
            base_port: 4000,
            node_count: 3,
            sol_airdrop: 50_000_000_000,
            stake_amount: 1_000_000,
        }
    }
}
