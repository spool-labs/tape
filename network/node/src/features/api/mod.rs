//! API feature module.
//!
//! This module provides the HTTP API server and route handlers for the storage node.

pub mod error;
mod info;
mod metadata;
mod repair;
mod verify;
mod sign;
mod slices;
mod snapshot;
mod status;
mod sync;
pub mod server;

use std::str::FromStr;
use std::sync::Arc;

use axum::{routing::{get, post}, Router};
use store::Store;
use tape_crypto::Pubkey;

use crate::control_plane::ControlPlane;
use crate::metrics::NodeMetrics;
use crate::features::storage::StorageService;
use tape_core::bls::BlsPrivateKey;

pub use error::ApiError;
pub use server::{Server, ServerHandle};

// Re-export handlers
pub use info::{get_info, get_stats};
pub use metadata::{get_metadata, put_metadata, get_metadata_status};
pub use repair::post_repair;
pub use verify::post_inconsistency;
pub use sign::get_sign;
pub use slices::{get_slice, put_slice, get_slice_status};
pub use snapshot::get_snapshot_sign;
pub use status::{get_track_status, health_check};
pub use sync::sync_spool;

// Re-export shared constants from tape-core and tape-node-api
pub use tape_core::erasure::{MAX_SLICE_SIZE, SPOOL_COUNT};
pub use tape_node_api::{
    HEALTH_PATH as HEALTH_ENDPOINT, INFO_PATH as INFO_ENDPOINT,
    METADATA_PATH as METADATA_ENDPOINT, SLICE_PATH as SLICE_ENDPOINT,
    SLICE_STATUS_PATH as SLICE_STATUS_ENDPOINT,
    METADATA_STATUS_PATH as METADATA_STATUS_ENDPOINT,
    STATS_PATH as STATS_ENDPOINT, TRACK_STATUS_PATH as TRACK_STATUS_ENDPOINT,
    REPAIR_PATH as REPAIR_ENDPOINT,
    INCONSISTENCY_PATH as INCONSISTENCY_ENDPOINT,
    SYNC_SPOOL_PATH as SYNC_SPOOL_ENDPOINT, SIGN_PATH as SIGN_ENDPOINT,
    SNAPSHOT_SIGN_PATH as SNAPSHOT_SIGN_ENDPOINT,
};

/// Shared state for API handlers.
pub struct ApiState<S: Store = store_rocks::RocksStore> {
    pub metrics: Arc<NodeMetrics>,
    pub service: Arc<StorageService<S>>,
    /// BLS private key for signing track certifications.
    pub bls_keypair: Arc<BlsPrivateKey>,
    /// Control plane for committee membership.
    pub control_plane: Arc<ControlPlane>,
    /// Whether to accept invalid TLS certificates (for local/test).
    pub insecure: bool,
}

// Manual Clone impl since Arc<T> is Clone regardless of T
impl<S: Store> Clone for ApiState<S> {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
            bls_keypair: self.bls_keypair.clone(),
            control_plane: self.control_plane.clone(),
            insecure: self.insecure,
        }
    }
}

/// Create the API router.
pub fn create_router<S: Store + Send + Sync + 'static>(state: ApiState<S>) -> Router {
    Router::new()
        // Slice operations
        .route(SLICE_ENDPOINT, get(get_slice::<S>).put(put_slice::<S>))
        .route(SLICE_STATUS_ENDPOINT, get(get_slice_status::<S>))
        // Metadata
        .route(METADATA_ENDPOINT, get(get_metadata::<S>).put(put_metadata::<S>))
        .route(METADATA_STATUS_ENDPOINT, get(get_metadata_status::<S>))
        // Track status
        .route(TRACK_STATUS_ENDPOINT, get(get_track_status::<S>))
        // Certification signature
        .route(SIGN_ENDPOINT, get(get_sign::<S>))
        // Repair
        .route(REPAIR_ENDPOINT, post(post_repair::<S>))
        .route(INCONSISTENCY_ENDPOINT, post(post_inconsistency::<S>))
        // Health check
        .route(HEALTH_ENDPOINT, get(health_check))
        // Node info
        .route(INFO_ENDPOINT, get(get_info::<S>))
        // Node stats (block processor metrics)
        .route(STATS_ENDPOINT, get(get_stats::<S>))
        // Snapshot certification signature
        .route(SNAPSHOT_SIGN_ENDPOINT, get(get_snapshot_sign::<S>))
        // Spool sync (node-to-node)
        .route(SYNC_SPOOL_ENDPOINT, post(sync_spool::<S>))
        .with_state(state)
}

/// Parse track_id string to Pubkey.
///
/// The track_id is the base58-encoded on-chain track address.
pub(crate) fn parse_track_id(track_id: &str) -> Result<Pubkey, ApiError> {
    Pubkey::from_str(track_id).map_err(|_| ApiError::InvalidTrackId)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rpc_client::prelude::Zeroable;
    use store_memory::MemoryStore;
    use tape_api::state::{Epoch, Node, System};
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::Committee;
    use tape_core::types::NodeId;
    use tape_metrics::MetricsRegistry;
    use tape_store::TapeStore;
    use tower::ServiceExt;
    use axum::http::StatusCode;

    fn create_test_state() -> ApiState<MemoryStore> {
        let _registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = Arc::new(NodeMetrics::new());
        let service = Arc::new(StorageService::new(TapeStore::new(MemoryStore::new())));
        let bls_keypair = Arc::new(BlsPrivateKey::from_random());

        let system = System::zeroed();
        let epoch = Epoch::zeroed();
        let node = Node::zeroed();
        let control_plane = Arc::new(ControlPlane::new(system, epoch, node, Default::default()));

        ApiState { metrics, service, bls_keypair, control_plane, insecure: true }
    }

    #[tokio::test]
    async fn test_health_check() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_info() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/info")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_slice_index() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tracks/11111111111111111111111111111111/slices/9999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_slice_not_found() {
        let state = create_test_state();
        let app = create_router(state);

        let track_id = Pubkey::new_unique().to_string();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tracks/{}/slices/0", track_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Stub returns NOT_FOUND
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_invalid_track_id() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tracks/not-a-valid-pubkey/slices/0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
