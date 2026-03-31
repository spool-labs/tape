use serde::Deserialize;

/// Metrics configuration for Prometheus integration.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MetricsConfig {
    /// Whether Prometheus metrics should be initialized and exposed.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
