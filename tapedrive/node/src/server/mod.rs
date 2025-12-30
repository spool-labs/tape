//! Server module.

pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use store::Store;
use store_rocks::RocksStore;
use tokio::net::TcpListener;

use crate::config::NodeConfig;
use crate::metrics::NodeMetrics;
use crate::storage_service::StorageService;

pub use routes::*;

/// The storage node server.
pub struct Server<S: Store = RocksStore> {
    config: NodeConfig,
    metrics: Arc<NodeMetrics>,
    service: Arc<StorageService<S>>,
}

impl Server<RocksStore> {
    /// Create a new server with RocksDB storage.
    pub fn new(
        config: NodeConfig,
        metrics: Arc<NodeMetrics>,
        service: Arc<StorageService<RocksStore>>,
    ) -> Self {
        Self {
            config,
            metrics,
            service,
        }
    }
}

impl<S: Store + Send + Sync + 'static> Server<S> {
    /// Create a new server with a custom store backend.
    pub fn with_store(
        config: NodeConfig,
        metrics: Arc<NodeMetrics>,
        service: Arc<StorageService<S>>,
    ) -> Self {
        Self {
            config,
            metrics,
            service,
        }
    }

    /// Run the server.
    pub async fn run(self) -> anyhow::Result<()> {
        let state = routes::ApiState {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
        };

        // Merge with observability routes from tape-metrics
        let app = Router::new()
            .merge(routes::create_router(state))
            .merge(tape_metrics::observability_router());

        let listener = TcpListener::bind(self.config.bind_address).await?;

        tracing::info!("Server listening on {}", self.config.bind_address);

        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Get the bind address.
    pub fn bind_address(&self) -> SocketAddr {
        self.config.bind_address
    }
}
