//! Server setup and lifecycle management.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use store::Store;
use store_rocks::RocksStore;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::core::config::NodeConfig;
use crate::control_plane::ControlPlane;
use crate::metrics::NodeMetrics;
use crate::features::storage::StorageService;
use tape_core::bls::BlsPrivateKey;

use super::{create_router, ApiState};

/// Handle for controlling a running server.
pub struct ServerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// Signal the server to shut down gracefully and await completion.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join_handle.await;
    }
}

/// The storage node server.
pub struct Server<S: Store = RocksStore> {
    config: NodeConfig,
    metrics: Arc<NodeMetrics>,
    service: Arc<StorageService<S>>,
    bls_keypair: Arc<BlsPrivateKey>,
    control_plane: Arc<ControlPlane>,
}

impl Server<RocksStore> {
    /// Create a new server with RocksDB storage.
    pub fn new(
        config: NodeConfig,
        metrics: Arc<NodeMetrics>,
        service: Arc<StorageService<RocksStore>>,
        bls_keypair: Arc<BlsPrivateKey>,
        control_plane: Arc<ControlPlane>,
    ) -> Self {
        Self {
            config,
            metrics,
            service,
            bls_keypair,
            control_plane,
        }
    }
}

impl<S: Store + Send + Sync + 'static> Server<S> {
    /// Create a new server with a custom store backend.
    pub fn with_store(
        config: NodeConfig,
        metrics: Arc<NodeMetrics>,
        service: Arc<StorageService<S>>,
        bls_keypair: Arc<BlsPrivateKey>,
        control_plane: Arc<ControlPlane>,
    ) -> Self {
        Self {
            config,
            metrics,
            service,
            bls_keypair,
            control_plane,
        }
    }

    /// Run the server (blocking).
    pub async fn run(self) -> anyhow::Result<()> {
        let state = ApiState {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
            bls_keypair: self.bls_keypair.clone(),
            control_plane: self.control_plane.clone(),
            insecure: self.config.insecure,
        };

        // Merge with observability routes from tape-metrics
        let app = Router::new()
            .merge(create_router(state))
            .merge(tape_metrics::observability_router());

        let listener = TcpListener::bind(self.config.bind_address).await?;

        tracing::info!("Server listening on {}", self.config.bind_address);

        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Start the server in the background and return a handle for control.
    pub async fn start(self) -> anyhow::Result<ServerHandle> {
        let state = ApiState {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
            bls_keypair: self.bls_keypair.clone(),
            control_plane: self.control_plane.clone(),
            insecure: self.config.insecure,
        };

        // Merge with observability routes from tape-metrics
        let app = Router::new()
            .merge(create_router(state))
            .merge(tape_metrics::observability_router());

        let listener = TcpListener::bind(self.config.bind_address).await?;
        let addr = self.config.bind_address;

        tracing::info!("Server listening on {}", addr);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server task
        let span = tracing::Span::current();
        let join_handle = tokio::spawn(tracing::Instrument::instrument(async move {
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
        }, span));

        Ok(ServerHandle {
            shutdown_tx: Some(shutdown_tx),
            join_handle,
        })
    }

    /// Get the bind address.
    pub fn bind_address(&self) -> SocketAddr {
        self.config.bind_address
    }
}
