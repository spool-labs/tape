use serde::Deserialize;

/// Logging configuration for the node runtime.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct LoggingConfig {
    /// Default tracing filter when no environment override is present.
    #[serde(default = "default_filter")]
    pub filter: String,

    /// Log output format.
    #[serde(default)]
    pub format: LoggingFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: default_filter(),
            format: LoggingFormat::default(),
        }
    }
}

/// Supported log output formats.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoggingFormat {
    /// Compact human-readable logs.
    #[default]
    Compact,
    /// Structured JSON logs.
    Json,
}

fn default_filter() -> String {
    "info".to_string()
}
