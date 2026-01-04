//! Server module.

pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use store::Store;
use store_rocks::RocksStore;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::config::NodeConfig;
use crate::metrics::NodeMetrics;
use crate::storage_service::StorageService;

pub use routes::*;

/// Handle for controlling a running server.
pub struct ServerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ServerHandle {
    /// Signal the server to shut down gracefully.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

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

    /// Run the server (blocking).
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

    /// Start the server in the background and return a handle for control.
    pub async fn start(self) -> anyhow::Result<ServerHandle> {
        let state = routes::ApiState {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
        };

        // Merge with observability routes from tape-metrics
        let app = Router::new()
            .merge(routes::create_router(state))
            .merge(tape_metrics::observability_router());

        let listener = TcpListener::bind(self.config.bind_address).await?;
        let addr = self.config.bind_address;

        tracing::info!("Server listening on {}", addr);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server task
        tokio::spawn(async move {
            let server = axum::serve(listener, app);
            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "Server error");
                    }
                }
                _ = shutdown_rx => {
                    tracing::info!("Server shutdown signal received");
                }
            }
        });

        Ok(ServerHandle {
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Get the bind address.
    pub fn bind_address(&self) -> SocketAddr {
        self.config.bind_address
    }
}
