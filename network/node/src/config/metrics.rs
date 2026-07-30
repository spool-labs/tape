use serde::Deserialize;

/// Metrics configuration for Prometheus integration.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MetricsConfig {
    /// Whether Prometheus metrics should be initialized and exposed.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Probe committee peers in the background for the dashboard's network
    /// table. Off by default; enable it only on nodes that serve a dashboard.
    #[serde(default)]
    pub aggregate_peers: bool,

    /// How often, in seconds, to refresh peer liveness.
    #[serde(default = "default_aggregate_interval_secs")]
    pub aggregate_interval_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            aggregate_peers: false,
            aggregate_interval_secs: default_aggregate_interval_secs(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

fn default_aggregate_interval_secs() -> u64 {
    15
}
