use std::time::Duration;

/// Runtime mode for spawned node fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRuntimeMode {
    /// Spawn full runtime (`ingestor + fsm + reconciler + supervisor + http`).
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
        }
    }
}
