use std::time::Duration;
use solana_pubkey::Pubkey;

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
    /// Whether nodes propose and sign evictions.
    ///
    /// On by default, as a fleet runs it. A fixture that is not about eviction
    /// turns it off: a node too busy to answer a probe reads as one that is
    /// gone, and a committee sized at the group floor has no seat to spare for
    /// the eviction that follows.
    pub eviction: bool,
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
            eviction: true,
        }
    }
}
