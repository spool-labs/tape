use std::time::Duration;
use solana_sdk::pubkey::Pubkey;

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
pub enum NodeRuntimeMode {
    /// Spawn full runtime (`ingestor + fsm + scheduler + task_runner + http`).
    Full,
    /// Do not spawn runtime tasks automatically.
    Disabled,
}

impl Default for NodeRuntimeMode {
    fn default() -> Self {
        Self::Disabled
    }
}

/// Top-level simnet configuration.
#[derive(Debug, Clone)]
pub struct SimnetConfig {
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
}

impl Default for SimnetConfig {
    fn default() -> Self {
        Self {
            node_count: 20,
            runtime_mode: NodeRuntimeMode::Disabled,
            base_port: 19_000,
            stop_timeout: Duration::from_secs(5),
            slot_advance_per_tx: 1,
            file_log: false,
            seed_accounts: Vec::new(),
        }
    }
}
