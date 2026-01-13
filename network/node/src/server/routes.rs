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
use crate::storage::service::{Compression, SliceMeta, StorageService, TrackInfo, MERKLE_HEIGHT};
use crate::sync::types::{
    SignedSyncRequest, SyncSlice, SyncSpoolRequest, SyncSpoolResponse, track_id_from_pubkey,
};
use tape_core::bls::BlsPrivateKey;
use tape_core::cert::CertifyMessage;
use tape_core::types::EpochNumber;
use tape_crypto::ed25519::sig_verify;
use tape_crypto::merkle::verify_proof;
use tape_crypto::Hash;
use tape_store::ops::SliceOps;

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

    // Verify spool ownership - only accept slices for spools we're assigned to
    let spool_idx = slice_index; // slice_index == spool_index by definition
    if !state.control_plane.owns_spool(spool_idx) {
        state
            .metrics
            .record_request("put_slice", "not_responsible", timer.elapsed_secs());
        return Err(ApiError::NotResponsible);
    }

    // Parse track_id to Pubkey (needed early for metadata lookup)
    let track_address = parse_track_id(&track_id)?;

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

    // If track metadata exists (commitment_hash uploaded), verify merkle proof
    if let Ok(Some(track_info)) = state.service.get_track_info(track_address) {
        // Verify the merkle proof against the stored commitment (merkle root)
        let is_valid = verify_proof(
            &payload.data,
            &track_info.commitment_hash,
            &payload.merkle_proof,
            spool_idx as u64,
            MERKLE_HEIGHT,
        );
        if !is_valid {
            state
                .metrics
                .record_request("put_slice", "merkle_failed", timer.elapsed_secs());
            return Err(ApiError::MerkleVerificationFailed);
        }
    }
    // Note: If no track metadata yet, verification happens at signing time

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

    // Get track metadata (contains commitment_hash for verification)
    let track_info = match state.service.get_track_info(track_address) {
        Ok(Some(info)) => info,
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
    };

    // Get our assigned spools and verify we have valid slice data for each
    let our_spools = state.control_plane.get_our_spools();
    for spool_idx in &our_spools {
        // Check if we have this slice
        let (data, meta) = match state.service.get_slice(*spool_idx, track_address) {
            Ok(Some((d, m))) => (d, m),
            Ok(None) => {
                tracing::warn!(
                    track = %track_id,
                    spool = spool_idx,
                    "Missing slice data for owned spool"
                );
                state
                    .metrics
                    .record_request("get_sign", "incomplete", timer.elapsed_secs());
                return Err(ApiError::IncompleteSliceData);
            }
            Err(e) => {
                tracing::error!(error = %e, spool = spool_idx, "Failed to get slice for signing");
                state
                    .metrics
                    .record_request("get_sign", "error", timer.elapsed_secs());
                return Err(ApiError::Storage(e.to_string()));
            }
        };

        // Verify merkle proof against commitment_hash
        let is_valid = verify_proof(
            &data,
            &track_info.commitment_hash,
            &meta.merkle_proof,
            *spool_idx as u64,
            MERKLE_HEIGHT,
        );
        if !is_valid {
            tracing::warn!(
                track = %track_id,
                spool = spool_idx,
                "Merkle proof verification failed for slice"
            );
            state
                .metrics
                .record_request("get_sign", "merkle_failed", timer.elapsed_secs());
            return Err(ApiError::MerkleVerificationFailed);
        }
    }

    // Build certification message with domain separation and epoch binding
    // Format: DOMAIN_TAG (8) || EPOCH (8 LE) || TRACK_ADDRESS (32) = 48 bytes
    let current_epoch = state.control_plane.current_epoch();
    let certify_message = CertifyMessage::new(current_epoch, track_address.to_bytes());
    let message_bytes = certify_message.to_bytes();

    let signature = state.bls_keypair.sign(&message_bytes).map_err(|e| {
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
        epoch: current_epoch.0,
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
pub async fn get_info<S: Store>(State(state): State<ApiState<S>>) -> Response {
    let node_id = state.control_plane.our_node_id();
    let info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running",
        "node_id": node_id.as_u64(),
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&info).unwrap_or_default(),
    )
        .into_response()
}

/// POST /v1/migrate/sync_spool
///
/// Node-to-node spool synchronization endpoint.
/// Accepts a signed request and returns slices for the requested spool.
pub async fn sync_spool<S: Store>(
    State(state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // 1. Deserialize SignedSyncRequest
    let signed_request: SignedSyncRequest = serde_json::from_slice(&body).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid sync request: {}", e))
    })?;

    // 2. Verify Ed25519 signature over the serialized request
    let request_bytes = serde_json::to_vec(&signed_request.request).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Internal(format!("request serialization failed: {}", e))
    })?;

    sig_verify(
        &signed_request.signer_pubkey,
        &signed_request.signature,
        &request_bytes,
    )
    .map_err(|_| {
        state
            .metrics
            .record_request("sync_spool", "unauthorized", timer.elapsed_secs());
        ApiError::Unauthorized
    })?;

    // 3. Extract request details
    let (spool_idx, starting_track, batch_size) = match signed_request.request {
        SyncSpoolRequest::V1(ref v1) => {
            (v1.spool_index, v1.starting_track_id.clone(), v1.batch_size)
        }
    };

    // 4. Get all slices for the requested spool
    let all_slices = state.service.store.get_spool_slices(spool_idx).map_err(|e| {
        tracing::error!(spool_idx, error = %e, "Failed to get spool slices");
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Storage(e.to_string())
    })?;

    // 5. Build response with pagination
    let mut result_slices = Vec::new();
    let mut found_start = starting_track.is_empty();

    for (track_pubkey, meta) in all_slices {
        // Convert store Pubkey to base58 track ID
        let track_address = Pubkey::new_from_array(track_pubkey.0);
        let track_id = track_id_from_pubkey(&track_address);

        // Skip until we find the starting track (for pagination)
        if !found_start {
            if track_id == starting_track {
                found_start = true;
            }
            continue;
        }

        // Get the actual slice data
        let (data, _) = state
            .service
            .get_slice(spool_idx, track_address)
            .map_err(|e| {
                tracing::error!(spool_idx, track = %track_id, error = %e, "Failed to get slice data");
                state
                    .metrics
                    .record_request("sync_spool", "error", timer.elapsed_secs());
                ApiError::Storage(e.to_string())
            })?
            .ok_or_else(|| {
                // Slice disappeared between listing and fetching
                state
                    .metrics
                    .record_request("sync_spool", "error", timer.elapsed_secs());
                ApiError::Storage("slice disappeared during enumeration".into())
            })?;

        result_slices.push(SyncSlice {
            track_id,
            slice_index: spool_idx,
            data,
            leaf_hash: meta.leaf_hash,
            merkle_proof: meta.merkle_proof,
        });

        // Respect batch size limit
        if result_slices.len() >= batch_size {
            break;
        }
    }

    // 6. Build and return response
    let response = SyncSpoolResponse::new_v1(result_slices);
    let response_bytes = serde_json::to_vec(&response).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Internal(format!("response serialization failed: {}", e))
    })?;

    state
        .metrics
        .record_request("sync_spool", "success", timer.elapsed_secs());

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        response_bytes,
    )
        .into_response())
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
    use tape_core::system::{Committee, CommitteeMember};
    use tape_core::types::{Coin, NodeId, TAPE};
    use tape_crypto::Hash;
    use tape_metrics::MetricsRegistry;
    use tape_store::TapeStore;
    use tower::ServiceExt;

    /// Create test state with zeroed system (no spool ownership).
    fn create_test_state() -> ApiState<MemoryStore> {
        create_test_state_with_spools(false)
    }

    /// Create test state where our node owns all spools.
    fn create_test_state_with_ownership() -> ApiState<MemoryStore> {
        create_test_state_with_spools(true)
    }

    fn create_test_state_with_spools(owns_spools: bool) -> ApiState<MemoryStore> {
        // Initialize metrics for API state (routes need metrics for recording)
        let _registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = Arc::new(NodeMetrics::new());
        let service = Arc::new(StorageService::new(TapeStore::new(MemoryStore::new())));

        // Create a mock BLS keypair
        let bls_keypair = Arc::new(BlsPrivateKey::from_random());

        // Create a mock control plane
        let (system, node) = if owns_spools {
            // Set up a committee with our node owning all spools
            let mut system = System::zeroed();
            let mut node = Node::zeroed();
            node.id = NodeId::new(1);

            // Create committee with our node
            let mut committee: Committee<128> = Committee::new();
            let member = CommitteeMember::new(NodeId::new(1), Coin::<TAPE>::new(1000));
            let _ = committee.try_join(&member);
            system.committee = committee;

            // Assign all spools to member 0 (our node)
            system.spools = SpoolAssignment::new([0u8; SLICE_COUNT]);

            (system, node)
        } else {
            (System::zeroed(), Node::zeroed())
        };

        let epoch = Epoch::zeroed();
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
        // Use state where our node owns all spools
        let state = create_test_state_with_ownership();
        let track_id = Pubkey::new_unique().to_string();

        // Create a valid SlicePayload (no track metadata yet, so no merkle verification)
        let payload = SlicePayload::new(
            vec![0xAB; 1024],
            Hash::default(),
            [Hash::default(); tape_node_api::MERKLE_HEIGHT],
        );
        let body = payload.to_bytes();

        // PUT the slice - should succeed since we own spool 0 and no metadata exists yet
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
    async fn test_put_slice_not_responsible() {
        // Use state where our node owns NO spools
        let state = create_test_state();
        let track_id = Pubkey::new_unique().to_string();

        let payload = SlicePayload::new(
            vec![0xAB; 1024],
            Hash::default(),
            [Hash::default(); tape_node_api::MERKLE_HEIGHT],
        );
        let body = payload.to_bytes();

        // PUT should fail with 421 MISDIRECTED_REQUEST
        let app = create_router(state);
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

        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
    }

    #[tokio::test]
    async fn test_put_slice_invalid_payload() {
        // Use state where our node owns spools, so we test payload validation
        let state = create_test_state_with_ownership();
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
