//! Factory for creating node clients.

use std::time::Duration;
use tape_node_client::{NodeError, StorageNodeClient, StorageNodeClientBuilder};

/// Factory for creating storage node clients.
#[derive(Clone)]
pub struct NodeCommunicationFactory {
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl Default for NodeCommunicationFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeCommunicationFactory {
    /// Create a new factory with default settings.
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the request timeout.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Create a client for the given node address.
    pub fn client_for_address(&self, address: &str) -> Result<StorageNodeClient, NodeError> {
        StorageNodeClientBuilder::new()
            .connect_timeout(self.connect_timeout)
            .request_timeout(self.request_timeout)
            .build(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_defaults() {
        let factory = NodeCommunicationFactory::new();
        assert_eq!(factory.connect_timeout, Duration::from_secs(5));
        assert_eq!(factory.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_factory_custom_timeouts() {
        let factory = NodeCommunicationFactory::new()
            .with_connect_timeout(Duration::from_secs(10))
            .with_request_timeout(Duration::from_secs(60));

        assert_eq!(factory.connect_timeout, Duration::from_secs(10));
        assert_eq!(factory.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_create_client() {
        let factory = NodeCommunicationFactory::new();
        let client = factory.client_for_address("localhost:8080").unwrap();
        assert_eq!(client.base_url().host_str(), Some("localhost"));
    }
}
