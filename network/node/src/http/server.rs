//! HTTP server — axum-based API for node-to-node and public endpoints.
//!
//! Serves slice data, metadata, BLS signing, repair, sync, and health routes.
//! Uses tower middleware for body limits, concurrency throttling, and load shedding.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::Router;
use rpc::Rpc;
use tape_protocol::api::routes;
use tape_protocol::Api;
use store::Store;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;

use crate::core::NodeContext;
use crate::fsm::UserEvent;
use crate::http::handlers::{
    health, inconsistency, metadata, metrics, repair, sign, slice, snapshot, status, sync,
};
use crate::http::state::AppState;

/// The HTTP server serving the node API.
pub struct HttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    user_event_tx: Option<mpsc::Sender<UserEvent>>,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static> HttpServer<Db, Cluster, Blockchain> {
    pub fn new(context: Arc<NodeContext<Db, Cluster, Blockchain>>, user_event_tx: Option<mpsc::Sender<UserEvent>>) -> Self {
        Self { context, user_event_tx }
    }

    /// Build the axum router with all routes and middleware.
    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
            user_event_tx: self.user_event_tx.clone(),
        };
        let limits = &self.context.config.node_api.ingress_limits;

        // Observability routes (no body limits needed)
        let observability = Router::new()
            .route(routes::HEALTH_PATH, get(health::health::<Db, Cluster, Blockchain>))
            .route(routes::INFO_PATH, get(health::info::<Db, Cluster, Blockchain>))
            .route(routes::STATS_PATH, get(health::stats::<Db, Cluster, Blockchain>))
            .route(routes::METRICS_PATH, get(metrics::get_metrics))
            .route(
                routes::SNAPSHOT_COMMITMENTS_PATH,
                get(snapshot::get_commitments::<Db, Cluster, Blockchain>),
            );

        // Status routes (lightweight checks)
        let status = Router::new()
            .route(
                routes::SLICE_STATUS_PATH,
                get(status::slice_status::<Db, Cluster, Blockchain>),
            )
            .route(
                routes::METADATA_STATUS_PATH,
                get(status::metadata_status::<Db, Cluster, Blockchain>),
            )
            .route(
                routes::TRACK_STATUS_PATH,
                get(status::track_status::<Db, Cluster, Blockchain>),
            );

        // Slice read
        let slice_read = Router::new().route(
            routes::SLICE_PATH,
            get(slice::get_slice::<Db, Cluster, Blockchain>),
        );

        // Sign routes (read-only BLS signing)
        let sign = Router::new()
            .route(
                routes::SIGN_PATH,
                get(sign::get_signature::<Db, Cluster, Blockchain>),
            )
            .route(
                routes::SNAPSHOT_SIG_PATH,
                post(sign::post_snapshot_signature::<Db, Cluster, Blockchain>),
            );

        // Metadata read
        let metadata_read = Router::new().route(
            routes::METADATA_PATH,
            get(metadata::get_metadata::<Db, Cluster, Blockchain>),
        );

        // Slice ingestion
        let slice_ingest = Router::new()
            .route(
                routes::SLICE_PATH,
                put(slice::put_slice::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(limits.slice_body_max));

        // Sync spool
        let mut sync = Router::new()
            .route(routes::SYNC_SPOOL_PATH, post(sync::sync_spool::<Db, Cluster, Blockchain>))
            .layer(DefaultBodyLimit::max(limits.sync_body_max));

        if let Some(limit) = limits.sync_spool_limit {
            sync = sync.layer(ConcurrencyLimitLayer::new(limit));
        }

        // Repair
        let mut repair = Router::new()
            .route(
                routes::REPAIR_PATH,
                post(repair::post_repair::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(limits.repair_body_max));

        if let Some(limit) = limits.repair_limit {
            repair = repair.layer(ConcurrencyLimitLayer::new(limit));
        }

        // Inconsistency
        let mut inconsistency = Router::new()
            .route(
                routes::INCONSISTENCY_PATH,
                post(inconsistency::post_inconsistency::<Db, Cluster, Blockchain>),
            )
            .layer(DefaultBodyLimit::max(limits.inconsistency_body_max));

        if let Some(limit) = limits.inconsistency_limit {
            inconsistency = inconsistency.layer(ConcurrencyLimitLayer::new(limit));
        }

        // Assemble all route groups
        let app = Router::new()
            .merge(observability)
            .merge(status)
            .merge(slice_read)
            .merge(metadata_read)
            .merge(sign)
            .merge(slice_ingest)
            .merge(sync)
            .merge(repair)
            .merge(inconsistency);

        app.with_state(state)
    }

    /// Start the HTTP server and run until the cancellation token fires.
    pub async fn serve(self, cancel: CancellationToken) -> Result<(), anyhow::Error> {
        let addr = self.context.config.bind_address;
        let router = self.build_router();

        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("HTTP server listening on {addr}");

        axum::serve(listener, router)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_core::bls::{BlsPrivateKey, BlsPubkey};
    use tape_core::erasure::{spool_for_slice, COMMITMENT_TREE_HEIGHT};
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{EpochNumber, NodeId};

    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::system::CommitteeMember;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::spooler::SpoolAssignment;
    use tape_protocol::state::ProtocolState;
    use peer_memory::MemoryApi;
    use tape_crypto::merkle::{create_merkle_proof, hash_leaf};
    use tape_crypto::Hash;
    use tape_protocol::api::{
        RepairRequest, SnapshotSignatureSubmission, SlicePayload, StripeSubChunkRequest,
        SyncSpoolRequest, SyncSpoolResponse,
    };
    use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};
    use tape_store::types::{ChunkIndex, Pubkey, SpoolState, SpoolStatus, TrackInfo};
    use tape_core::cert::snapshot::SnapshotMessage;
    use tape_store::{MemoryStore, TapeStore};
    use tower::ServiceExt;

    use crate::core::NodeContext;
    use crate::core::test_utils::{test_config, test_context};

    fn test_router(ctx: Arc<NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>>) -> Router {
        HttpServer::new(ctx, None).build_router()
    }

    fn make_track_with_data(
        spool_group: SpoolGroup,
        slice_data: &[&[u8]],
    ) -> (TrackInfo, Vec<Hash>) {
        let leaf_hashes: Vec<Hash> = slice_data.iter().map(|d| hash_leaf(d)).collect();
        // Pad to SPOOL_GROUP_SIZE with hash_leaf(&[]) so the tree matches
        // proofs built via create_merkle_proof with empty slices.
        let empty_leaf = hash_leaf(&[]);
        let mut padded = leaf_hashes.clone();
        while padded.len() < tape_core::erasure::SPOOL_GROUP_SIZE {
            padded.push(empty_leaf);
        }
        let info = TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group,
            original_size: 0,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 1,
            encoding_params: 0,
            commitment: padded,
        };
        (info, leaf_hashes)
    }

    #[tokio::test]
    async fn health() {
        let ctx = test_context();
        let app = test_router(ctx);

        let resp = app
            .oneshot(Request::get("/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_slice_missing() {
        let ctx = test_context();
        // Set up a spool so we can route requests
        let app = test_router(ctx);

        let resp = app
            .oneshot(
                Request::get("/v1/tracks/11111111111111111111111111111111/slices/0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_and_get_slice() {
        let ctx = test_context();
        let spool_group = SpoolGroup(0);
        let spool_id = 0u16;
        let spool_id = spool_for_slice(spool_group, spool_id as usize);

        // Register the spool
        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        // Create track with proper commitment
        let data = vec![0xABu8; 100];
        let (track_info, _leaf_hashes) = make_track_with_data(spool_group, &[&data]);

        let track_address = Pubkey::new_unique();
        let track_b58 = solana_sdk::pubkey::Pubkey::from(track_address.0).to_string();
        ctx.store
            .put_track(track_address, track_info.clone())
            .unwrap();
        ctx.peer_manager.state_handle().store(ProtocolState {
            epoch: EpochNumber(1),
            ..Default::default()
        });

        // Create a valid SlicePayload with merkle proof
        let leaf_hash = hash_leaf(&data);
        let mut padded_data: Vec<&[u8]> = vec![&data];
        let empty = vec![0u8; 0];
        while padded_data.len() < tape_core::erasure::SPOOL_GROUP_SIZE {
            padded_data.push(&empty);
        }
        let proof = create_merkle_proof(&padded_data, 0, COMMITMENT_TREE_HEIGHT);
        let payload = SlicePayload::new(data.clone(), leaf_hash, proof);

        // PUT slice via internal route
        let app = test_router(ctx.clone());
        let put_resp = app
            .oneshot(
                Request::put(format!(
                    "/v1/tracks/{track_b58}/slices/{spool_id}"
                ))
                .header("content-type", "application/octet-stream")
                .body(Body::from(wincode::serialize(&payload).unwrap()))
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(put_resp.status(), StatusCode::OK);

        // GET slice
        let app = test_router(ctx);
        let get_resp = app
            .oneshot(
                Request::get(format!(
                    "/v1/tracks/{track_b58}/slices/{spool_id}"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn put_bad_proof() {
        let ctx = test_context();
        let spool_group = SpoolGroup(0);
        let spool_id = 0u16;
        let spool_id = spool_for_slice(spool_group, spool_id as usize);

        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        let data = vec![0xABu8; 100];
        let (track_info, _) = make_track_with_data(spool_group, &[&data]);

        let track_address = Pubkey::new_unique();
        let track_b58 = solana_sdk::pubkey::Pubkey::from(track_address.0).to_string();
        ctx.store
            .put_track(track_address, track_info)
            .unwrap();

        // Create payload with bad proof
        let leaf_hash = hash_leaf(&data);
        let bad_proof = vec![Hash::from([0xFF; 32]); COMMITMENT_TREE_HEIGHT];
        let payload = SlicePayload::new(data, leaf_hash, bad_proof);

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::put(format!(
                    "/v1/tracks/{track_b58}/slices/{spool_id}"
                ))
                .header("content-type", "application/octet-stream")
                .body(Body::from(wincode::serialize(&payload).unwrap()))
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn slice_status_check() {
        let ctx = test_context();
        let spool_group = SpoolGroup(0);
        let spool_id = 0u16;
        let spool_id = spool_for_slice(spool_group, spool_id as usize);

        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        let track_address = Pubkey::new_unique();
        let track_b58 = solana_sdk::pubkey::Pubkey::from(track_address.0).to_string();
        let (track_info, _) = make_track_with_data(spool_group, &[&[1u8; 10]]);
        ctx.store
            .put_track(track_address, track_info)
            .unwrap();

        // Check status when slice doesn't exist
        let app = test_router(ctx.clone());
        let resp = app
            .oneshot(
                Request::get(format!(
                    "/v1/tracks/{track_b58}/slices/{spool_id}/status"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Store a slice
        ctx.store
            .put_slice(spool_id, track_address, vec![1u8; 10])
            .unwrap();

        // Check status when slice exists
        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::get(format!(
                    "/v1/tracks/{track_b58}/slices/{spool_id}/status"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn repair_extract() {
        use tape_core::encoding::{ClayParams, EncodingProfile};
        use tape_slicer::ClayCoder;

        let ctx = test_context();
        let spool_group = SpoolGroup(0);
        let helper_spool_id = 1u16;
        let helper_spool = spool_for_slice(spool_group, helper_spool_id as usize);

        ctx.store
            .set_spool_state(helper_spool, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        // Create Clay-encoded track
        let clay_params = ClayParams::new(20, 7, 16);
        let coder = ClayCoder::from_params(clay_params);
        let alpha = coder.alpha();
        let k = clay_params.k() as usize;
        let stripe_size = k * alpha * 16;
        let chunk_size = coder.chunk_size_for(stripe_size);
        let sub_chunk_size = chunk_size / alpha;
        let stripe_count = 2u64;
        let original_size = stripe_size as u64 * stripe_count;

        let profile = EncodingProfile::clay(clay_params);
        let mut track_info = TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group,
            original_size,
            stripe_size: stripe_size as u64,
            stripe_count,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![Hash::default(); 20],
        };
        track_info.set_profile(profile);

        let track_address = Pubkey::new_unique();
        let track_b58 = solana_sdk::pubkey::Pubkey::from(track_address.0).to_string();
        ctx.store
            .put_track(track_address, track_info)
            .unwrap();

        // Store a slice with known data pattern
        let total_size = chunk_size * stripe_count as usize;
        let mut slice_data = vec![0u8; total_size];
        for (i, b) in slice_data.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        ctx.store
            .put_slice(helper_spool, track_address, slice_data.clone())
            .unwrap();

        // Request sub-chunks 0 and 1 from stripe 0
        let request = RepairRequest {
            helper_spool,
            stripes: vec![StripeSubChunkRequest {
                stripe: 0,
                sub_chunks: vec![0, 1],
            }],
        };

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post(format!("/v1/tracks/{track_b58}/repair"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();

        // Verify: sub-chunk 0 = bytes [0..sub_chunk_size], sub-chunk 1 = [sub_chunk_size..2*sub_chunk_size]
        assert_eq!(body.len(), 2 * sub_chunk_size);
        assert_eq!(&body[..sub_chunk_size], &slice_data[..sub_chunk_size]);
        assert_eq!(
            &body[sub_chunk_size..2 * sub_chunk_size],
            &slice_data[sub_chunk_size..2 * sub_chunk_size]
        );
    }

    #[tokio::test]
    async fn repair_missing_track() {
        let ctx = test_context();

        let request = RepairRequest {
            helper_spool: 1,
            stripes: vec![],
        };

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post("/v1/tracks/11111111111111111111111111111111/repair")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sync_roundtrip() {
        let ctx = test_context();
        let spool_id = 42u16;

        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        ctx.store
            .put_slice(spool_id, track1, vec![1, 2, 3])
            .unwrap();
        ctx.store
            .put_slice(spool_id, track2, vec![4, 5, 6])
            .unwrap();
        ctx.store
            .put_slice(spool_id, track3, vec![7, 8, 9])
            .unwrap();

        let request = SyncSpoolRequest {
            spool_index: spool_id,
            cursor: None,
            limit: 100,
        };

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post("/v1/sync/spool")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let response: SyncSpoolResponse = wincode::deserialize(&body).unwrap();

        assert_eq!(response.entries.len(), 3);
        assert!(response.next_cursor.is_none());
    }

    #[tokio::test]
    async fn sync_pagination() {
        let ctx = test_context();
        let spool_id = 10u16;

        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        // Insert 50 slices
        let mut tracks = Vec::new();
        for i in 0..50u8 {
            let track = Pubkey::new_unique();
            ctx.store
                .put_slice(spool_id, track, vec![i])
                .unwrap();
            tracks.push(track);
        }

        // Request with limit=10
        let request = SyncSpoolRequest {
            spool_index: spool_id,
            cursor: None,
            limit: 10,
        };

        let app = test_router(ctx.clone());
        let resp = app
            .oneshot(
                Request::post("/v1/sync/spool")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let page1: SyncSpoolResponse = wincode::deserialize(&body).unwrap();

        assert_eq!(page1.entries.len(), 10);
        assert!(page1.next_cursor.is_some());

        // Fetch next page using cursor
        let request2 = SyncSpoolRequest {
            spool_index: spool_id,
            cursor: page1.next_cursor,
            limit: 10,
        };
        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post("/v1/sync/spool")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request2).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let page2: SyncSpoolResponse = wincode::deserialize(&body).unwrap();

        assert_eq!(page2.entries.len(), 10);
        // No overlap between pages
        assert_ne!(page1.entries[9].track_address, page2.entries[0].track_address);
    }

    #[tokio::test]
    async fn sync_empty_spool() {
        let ctx = test_context();
        let spool_id = 77u16;

        ctx.store
            .set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0), prev_owner: None })
            .unwrap();

        let request = SyncSpoolRequest {
            spool_index: spool_id,
            cursor: None,
            limit: 100,
        };

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post("/v1/sync/spool")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let response: SyncSpoolResponse = wincode::deserialize(&body).unwrap();

        assert!(response.entries.is_empty());
        assert!(response.next_cursor.is_none());
    }

    #[tokio::test]
    async fn commitments_found() {
        use tape_core::erasure::SPOOL_GROUP_COUNT;
        use tape_core::types::ChunkIndex;

        let ctx = test_context();
        let epoch = EpochNumber(5);
        for i in 0..SPOOL_GROUP_COUNT {
            ctx.store
                .set_snapshot_commitment(epoch, ChunkIndex(i as u64), Hash::new_unique())
                .unwrap();
        }

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::get("/v1/snapshots/5/commitments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let commitments: Vec<Hash> = wincode::deserialize(&body).unwrap();
        assert_eq!(commitments.len(), SPOOL_GROUP_COUNT);
    }

    #[tokio::test]
    async fn commitments_missing() {
        let ctx = test_context();
        let app = test_router(ctx);

        let resp = app
            .oneshot(
                Request::get("/v1/snapshots/99/commitments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn signature_submission() {
        let ctx = test_context();
        let epoch = 12;
        let chunk = 0u64;
        let committee_epoch = EpochNumber(epoch);
        let signer = BlsPrivateKey::from_random();
        let signer_pk = signer.public_key().unwrap();
        let mut member = CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000));
        member.key = signer_pk;
        let mut spool_map = [255u8; SPOOL_COUNT];
        spool_map[0] = 0;
        ctx.peer_manager.state_handle().store(ProtocolState {
            epoch: EpochNumber(epoch),
            committee: vec![member],
            spools: SpoolAssignment::new(spool_map),
            ..Default::default()
        });

        let commitment = Hash::new_unique();
        ctx.store
            .set_snapshot_commitment(committee_epoch, ChunkIndex(chunk), commitment)
            .unwrap();

        let msg = SnapshotMessage::new(committee_epoch, commitment.0).to_bytes();
        let signature = signer.sign(msg).unwrap();

        let payload = SnapshotSignatureSubmission {
            signature,
            member_index: 0,
            epoch: EpochNumber(epoch),
        };

        let app = test_router(ctx.clone());
        let resp = app
            .oneshot(
                Request::post(format!("/v1/snapshots/{epoch}/{chunk}/partial_signature"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            ctx.store
                .get_snapshot_partial_signature(committee_epoch, chunk, 0)
                .unwrap()
                .unwrap()
                .member_index,
            0
        );
    }

    #[tokio::test]
    async fn signature_wrong_member() {
        let ctx = test_context();
        let epoch = 12;
        let chunk = 1u64;
        let committee_epoch = EpochNumber(epoch);
        let commitment = Hash::new_unique();

        ctx.store
            .set_snapshot_commitment(committee_epoch, ChunkIndex(chunk), commitment)
            .unwrap();
        let mut member = CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000));
        member.key = BlsPubkey::new_unique();
        let mut spool_map = [255u8; SPOOL_COUNT];
        spool_map[0] = 0;
        ctx.peer_manager.state_handle().store(ProtocolState {
            epoch: EpochNumber(epoch),
            committee: vec![member],
            spools: SpoolAssignment::new(spool_map),
            ..Default::default()
        });

        let signer = BlsPrivateKey::from_random();
        let msg = SnapshotMessage::new(committee_epoch, commitment.0).to_bytes();
        let signature = signer.sign(msg).unwrap();

        let payload = SnapshotSignatureSubmission {
            signature,
            member_index: 5,
            epoch: EpochNumber(epoch),
        };

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::post(format!("/v1/snapshots/{epoch}/{chunk}/partial_signature"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(wincode::serialize(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn body_limit() {
        let mut config = test_config();
        config.node_api.ingress_limits.slice_body_max = 10;
        use tape_protocol::peer::{PeerManager, TrustedPeers};
        let rpc = std::sync::Arc::new(RpcClient::from_rpc(LiteSvmRpc::new()));
        let api = std::sync::Arc::new(MemoryApi::noop());
        let peers = std::sync::Arc::new(TrustedPeers::new());
        let peer_manager = std::sync::Arc::new(PeerManager::new(rpc.clone(), api, peers));
        let ctx = NodeContext::new(
            config,
            Keypair::new(),
            BlsPrivateKey::from_random(),
            TapeStore::new(MemoryStore::new()),
            RpcClient::from_rpc(LiteSvmRpc::new()),
            peer_manager,
        );
        let track_address = Pubkey::new_unique();
        let track_b58 = solana_sdk::pubkey::Pubkey::from(track_address.0).to_string();

        let app = test_router(ctx);
        let resp = app
            .oneshot(
                Request::put(format!("/v1/tracks/{track_b58}/slices/0"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(vec![0u8; 100]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
