//! API routes and handlers.

use std::str::FromStr;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use tape_crypto::Pubkey;
use store::Store;
use tape_metrics::OperationTimer;
use tape_node_api::{SlicePayload, SignResponse};

use crate::control_plane::ControlPlane;
use crate::error::ApiError;
use crate::metrics::NodeMetrics;
use crate::storage::service::{Compression, SliceMeta, StorageService, TrackInfo};
use tape_core::bls::BlsPrivateKey;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;

// Re-export shared constants from tape-core and tape-node-api
pub use tape_core::erasure::{MAX_SLICE_SIZE, SLICE_COUNT};
pub use tape_node_api::{
    HEALTH_PATH as HEALTH_ENDPOINT, INFO_PATH as INFO_ENDPOINT,
    METADATA_PATH as METADATA_ENDPOINT, SLICE_PATH as SLICE_ENDPOINT,
    STATUS_PATH as STATUS_ENDPOINT, SYNC_SPOOL_PATH as SYNC_SPOOL_ENDPOINT,
    SIGN_PATH as SIGN_ENDPOINT,
};

/// Shared state for API handlers.
pub struct ApiState<S: Store = store_rocks::RocksStore> {
    pub metrics: Arc<NodeMetrics>,
    pub service: Arc<StorageService<S>>,
    /// BLS private key for signing track certifications.
    pub bls_keypair: Arc<BlsPrivateKey>,
    /// Control plane for committee membership.
    pub control_plane: Arc<ControlPlane>,
}

// Manual Clone impl since Arc<T> is Clone regardless of T
impl<S: Store> Clone for ApiState<S> {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
            service: self.service.clone(),
            bls_keypair: self.bls_keypair.clone(),
            control_plane: self.control_plane.clone(),
        }
    }
}

/// Create the API router.
pub fn create_router<S: Store + Send + Sync + 'static>(state: ApiState<S>) -> Router {
    Router::new()
        // Slice operations
        .route(SLICE_ENDPOINT, get(get_slice::<S>).put(put_slice::<S>))
        // Metadata
        .route(METADATA_ENDPOINT, get(get_metadata::<S>).put(put_metadata::<S>))
        // Status
        .route(STATUS_ENDPOINT, get(get_status::<S>))
        // Certification signature
        .route(SIGN_ENDPOINT, get(get_sign::<S>))
        // Health check
        .route(HEALTH_ENDPOINT, get(health_check))
        // Node info
        .route(INFO_ENDPOINT, get(get_info::<S>))
        // Spool sync (node-to-node)
        .route(SYNC_SPOOL_ENDPOINT, post(sync_spool::<S>))
        .with_state(state)
}

/// Parse track_id string to Pubkey.
///
/// The track_id is the base58-encoded on-chain track address.
fn parse_track_id(track_id: &str) -> Result<Pubkey, ApiError> {
    Pubkey::from_str(track_id).map_err(|_| ApiError::InvalidTrackId)
}

/// GET /v1/tracks/:track_id/slices/:slice_index
pub async fn get_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("get_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // spool_idx == slice_index (always - by definition)
    let spool_idx = slice_index;

    // Retrieve from storage
    match state.service.get_slice(spool_idx, track_address) {
        Ok(Some((data, _meta))) => {
            state
                .metrics
                .record_request("get_slice", "success", timer.elapsed_secs());
            Ok((StatusCode::OK, data).into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_slice", "not_found", timer.elapsed_secs());
            Err(ApiError::NotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get slice");
            state
                .metrics
                .record_request("get_slice", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// PUT /v1/tracks/:track_id/slices/:slice_index
pub async fn put_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // Deserialize SlicePayload from wincode
    let payload = SlicePayload::from_bytes(&body).map_err(|e| {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid slice payload: {}", e))
    })?;

    // Validate data size
    if payload.data.len() > MAX_SLICE_SIZE {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidBody("slice too large".into()));
    }

    // Parse track_id to Pubkey
    let track_address = parse_track_id(&track_id)?;
    let spool_idx = slice_index; // Always identical by definition

    // Build metadata from payload
    let meta = SliceMeta {
        len: payload.data.len() as u32,
        leaf_hash: payload.leaf_hash,
        merkle_proof: payload.merkle_proof,
        compression: Compression::None,
        received_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };

    // Store
    match state
        .service
        .put_slice(spool_idx, track_address, payload.data, meta)
    {
        Ok(()) => {
            state
                .metrics
                .record_request("put_slice", "success", timer.elapsed_secs());
            Ok(StatusCode::CREATED.into_response())
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to put slice");
            state
                .metrics
                .record_request("put_slice", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Retrieve track info from storage
    match state.service.get_track_info(track_address) {
        Ok(Some(info)) => {
            let response = serde_json::json!({
                "commitment_hash": hex::encode(info.commitment_hash.0),
                "certified_epoch": info.certified_epoch.0,
                "slice_count": info.slice_count
            });

            state
                .metrics
                .record_request("get_metadata", "success", timer.elapsed_secs());

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&response).unwrap_or_default(),
            )
                .into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_metadata", "not_found", timer.elapsed_secs());
            Err(ApiError::TrackNotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track metadata");
            state
                .metrics
                .record_request("get_metadata", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// PUT /v1/tracks/:track_id/metadata
pub async fn put_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Parse JSON body to extract commitment_hash
    let json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid JSON: {}", e))
    })?;

    let commitment_hex = json
        .get("commitment_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            state
                .metrics
                .record_request("put_metadata", "error", timer.elapsed_secs());
            ApiError::InvalidBody("missing commitment_hash field".into())
        })?;

    // Decode hex to bytes
    let commitment_bytes = hex::decode(commitment_hex).map_err(|e| {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid hex in commitment_hash: {}", e))
    })?;

    // Validate length is 32 bytes
    if commitment_bytes.len() != 32 {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidBody(format!(
            "commitment_hash must be 32 bytes, got {}",
            commitment_bytes.len()
        )));
    }

    // Convert to Hash type
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&commitment_bytes);
    let commitment_hash = Hash(hash_bytes);

    // Create TrackInfo with commitment_hash, certified_epoch=0, slice_count=0
    let info = TrackInfo {
        commitment_hash,
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    };

    // Store track metadata
    match state.service.put_track_info(track_address, info) {
        Ok(()) => {
            state
                .metrics
                .record_request("put_metadata", "success", timer.elapsed_secs());
            Ok(StatusCode::CREATED.into_response())
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to put track metadata");
            state
                .metrics
                .record_request("put_metadata", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// GET /v1/tracks/:track_id/status
pub async fn get_status<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Retrieve track info from storage
    match state.service.get_track_info(track_address) {
        Ok(Some(info)) => {
            let is_certified = info.certified_epoch.0 > 0;
            let mut response = serde_json::json!({
                "track_id": track_id,
                "slice_count": info.slice_count,
                "is_certified": is_certified
            });

            // Include certified_epoch only if certified
            if is_certified {
                response["certified_epoch"] = serde_json::json!(info.certified_epoch.0);
            }

            state
                .metrics
                .record_request("get_status", "success", timer.elapsed_secs());

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&response).unwrap_or_default(),
            )
                .into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_status", "not_found", timer.elapsed_secs());
            Err(ApiError::TrackNotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track status");
            state
                .metrics
                .record_request("get_status", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// GET /v1/tracks/:track_id/sign
///
/// Returns a BLS signature over the track address for certification.
/// Returns 404 if the node doesn't have any slice data for the track.
/// Returns 403 if the node is not in the current committee.
pub async fn get_sign<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Check if node is in committee
    if !state.control_plane.is_in_committee() {
        state
            .metrics
            .record_request("get_sign", "forbidden", timer.elapsed_secs());
        return Err(ApiError::Unauthorized);
    }

    // Get our member index for the bitmap
    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system.committee.index_of(&node_id)
        .ok_or_else(|| {
            state
                .metrics
                .record_request("get_sign", "error", timer.elapsed_secs());
            ApiError::Internal("Node is in committee but index_of failed".to_string())
        })? as u8;

    // Check if we have any slice data for this track
    // We just need to verify track metadata exists (node has received at least some data)
    match state.service.get_track_info(track_address) {
        Ok(Some(_)) => {
            // We have track info, proceed to sign
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_sign", "not_found", timer.elapsed_secs());
            return Err(ApiError::TrackNotFound);
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track info for signing");
            state
                .metrics
                .record_request("get_sign", "error", timer.elapsed_secs());
            return Err(ApiError::Storage(e.to_string()));
        }
    }

    // Sign the track address (32 bytes)
    // Message format: track_address.as_ref() (32 bytes) - no epoch binding
    let message = track_address.as_ref();
    let signature = state.bls_keypair.sign(message).map_err(|e| {
        tracing::error!(error = ?e, "BLS signing failed");
        state
            .metrics
            .record_request("get_sign", "error", timer.elapsed_secs());
        ApiError::Internal(format!("BLS signing failed: {:?}", e))
    })?;

    // Build response
    let response = SignResponse {
        signature: signature.0.0,
        node_id: node_id.as_u64(),
        member_index,
    };

    state
        .metrics
        .record_request("get_sign", "success", timer.elapsed_secs());

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&response).unwrap_or_default(),
    )
        .into_response())
}

/// GET /v1/health
pub async fn health_check() -> Response {
    StatusCode::OK.into_response()
}

/// GET /v1/info
pub async fn get_info<S: Store>(State(_state): State<ApiState<S>>) -> Response {
    // TODO: Return node info (version, pubkey, etc.)
    let info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running"
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&info).unwrap_or_default(),
    )
        .into_response()
}

/// POST /v1/migrate/sync_spool
pub async fn sync_spool<S: Store>(
    State(state): State<ApiState<S>>,
    _body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // TODO: Implement spool sync (Ed25519 signed request verification)
    state
        .metrics
        .record_request("sync_spool", "error", timer.elapsed_secs());
    Err(ApiError::Unauthorized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rpc_client::prelude::Zeroable;
    use store_memory::MemoryStore;
    use tape_api::state::{Epoch, Node, System};
    use tape_crypto::Hash;
    use tape_metrics::MetricsRegistry;
    use tape_store::TapeStore;
    use tower::ServiceExt;

    fn create_test_state() -> ApiState<MemoryStore> {
        // Initialize metrics for API state (routes need metrics for recording)
        let _registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = Arc::new(NodeMetrics::new());
        let service = Arc::new(StorageService::new(TapeStore::new(MemoryStore::new())));

        // Create a mock BLS keypair
        let bls_keypair = Arc::new(BlsPrivateKey::from_random());

        // Create a mock control plane with default/zeroed state
        let system = System::zeroed();
        let epoch = Epoch::zeroed();
        let node = Node::zeroed();
        let control_plane = Arc::new(ControlPlane::new(system, epoch, node));

        ApiState { metrics, service, bls_keypair, control_plane }
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

        // Use a valid base58 pubkey
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

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_put_and_get_slice() {
        let state = create_test_state();
        let track_id = Pubkey::new_unique().to_string();

        // Create a valid SlicePayload
        let payload = SlicePayload::new(
            vec![0xAB; 1024],
            Hash::default(),
            [Hash::default(); tape_node_api::MERKLE_HEIGHT],
        );
        let body = payload.to_bytes();

        // PUT the slice
        let app = create_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/tracks/{}/slices/0", track_id))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // GET the slice
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tracks/{}/slices/0", track_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify body content
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &[0xAB; 1024]);
    }

    #[tokio::test]
    async fn test_put_slice_invalid_payload() {
        let state = create_test_state();
        let app = create_router(state);
        let track_id = Pubkey::new_unique().to_string();

        // Send invalid wincode data
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/tracks/{}/slices/0", track_id))
                    .body(Body::from(vec![0u8; 100])) // Invalid payload
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_track_id() {
        let state = create_test_state();
        let app = create_router(state);

        // Use an invalid base58 string
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
