//! Server module.

pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;

use crate::config::NodeConfig;
use crate::metrics::NodeMetrics;

pub use routes::*;

/// The storage node server.
pub struct Server {
    config: NodeConfig,
    metrics: Arc<NodeMetrics>,
}

impl Server {
    /// Create a new server with the given configuration.
    pub fn new(config: NodeConfig, metrics: Arc<NodeMetrics>) -> Self {
        Self { config, metrics }
    }

    /// Run the server.
    pub async fn run(self) -> anyhow::Result<()> {
        let state = routes::ApiState {
            metrics: self.metrics.clone(),
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
