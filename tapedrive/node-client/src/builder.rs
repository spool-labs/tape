//! Builder pattern for StorageNodeClient.

#[cfg(feature = "metrics")]
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use url::Url;

use crate::client::StorageNodeClient;
use crate::error::NodeError;

#[cfg(feature = "metrics")]
use crate::metrics::NodeClientMetrics;

/// Builder for creating StorageNodeClient instances.
pub struct StorageNodeClientBuilder {
    connect_timeout: Duration,
    request_timeout: Duration,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<NodeClientMetrics>>,
}

impl Default for StorageNodeClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageNodeClientBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            #[cfg(feature = "metrics")]
            metrics: None,
        }
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Enable metrics collection.
    #[cfg(feature = "metrics")]
    pub fn with_metrics(mut self, metrics: Arc<NodeClientMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build a client for the given node address.
    ///
    /// # Arguments
    /// * `address` - The node address (host:port or full URL)
    pub fn build(self, address: &str) -> Result<StorageNodeClient, NodeError> {
        // Parse address - add https:// if no scheme
        let base_url = if address.starts_with("http://") || address.starts_with("https://") {
            Url::parse(address)?
        } else {
            Url::parse(&format!("https://{}", address))?
        };

        let client = Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .build()
            .map_err(NodeError::Request)?;

        Ok(StorageNodeClient {
            inner: client,
            base_url,
            #[cfg(feature = "metrics")]
            metrics: self.metrics,
        })
    }

    /// Build a client for the given URL.
    ///
    /// # Arguments
    /// * `url` - The base URL for the node
    pub fn build_with_url(self, url: Url) -> Result<StorageNodeClient, NodeError> {
        let client = Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .build()
            .map_err(NodeError::Request)?;

        Ok(StorageNodeClient {
            inner: client,
            base_url: url,
            #[cfg(feature = "metrics")]
            metrics: self.metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let builder = StorageNodeClientBuilder::new();
        assert_eq!(builder.connect_timeout, Duration::from_secs(5));
        assert_eq!(builder.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_builder_custom_timeouts() {
        let builder = StorageNodeClientBuilder::new()
            .connect_timeout(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(60));

        assert_eq!(builder.connect_timeout, Duration::from_secs(10));
        assert_eq!(builder.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_build_with_address() {
        let client = StorageNodeClientBuilder::new()
            .build("localhost:8080")
            .unwrap();

        assert_eq!(client.base_url().as_str(), "https://localhost:8080/");
    }

    #[test]
    fn test_build_with_full_url() {
        let client = StorageNodeClientBuilder::new()
            .build("http://localhost:8080")
            .unwrap();

        assert_eq!(client.base_url().as_str(), "http://localhost:8080/");
    }

    #[test]
    fn test_build_with_url_object() {
        let url = Url::parse("https://node1.tapedrive.io:443").unwrap();
        let client = StorageNodeClientBuilder::new()
            .build_with_url(url)
            .unwrap();

        assert_eq!(client.base_url().as_str(), "https://node1.tapedrive.io/");
    }
}
