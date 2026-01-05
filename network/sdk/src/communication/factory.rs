//! Factory for creating node clients.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tape_node_client::{NodeError, StorageNodeClient, StorageNodeClientBuilder};

/// Factory for creating storage node clients.
///
/// Caches clients per address to reuse HTTP connections.
/// This is critical for performance - creating a new reqwest::Client
/// per request is ~100x slower than reusing connections.
#[derive(Clone)]
pub struct NodeCommunicationFactory {
    connect_timeout: Duration,
    request_timeout: Duration,
    /// Cache of clients by address. Uses Arc<RwLock> for thread-safe sharing.
    cache: Arc<RwLock<HashMap<String, StorageNodeClient>>>,
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
            cache: Arc::new(RwLock::new(HashMap::new())),
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

    /// Get or create a client for the given node address.
    ///
    /// Clients are cached and reused to maintain HTTP connection pools.
    pub fn client_for_address(&self, address: &str) -> Result<StorageNodeClient, NodeError> {
        // Fast path: check cache with read lock
        {
            let cache = self.cache.read().unwrap();
            if let Some(client) = cache.get(address) {
                return Ok(client.clone());
            }
        }

        // Slow path: create new client and cache it
        let client = StorageNodeClientBuilder::new()
            .connect_timeout(self.connect_timeout)
            .request_timeout(self.request_timeout)
            .build(address)?;

        let mut cache = self.cache.write().unwrap();
        // Double-check in case another thread created it
        if let Some(existing) = cache.get(address) {
            return Ok(existing.clone());
        }
        cache.insert(address.to_string(), client.clone());

        Ok(client)
    }

    /// Clear the client cache.
    ///
    /// Useful for testing or when node addresses change.
    pub fn clear_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
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
