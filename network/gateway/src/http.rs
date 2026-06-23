use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::error_handling::HandleErrorLayer;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::Stream;
use rpc::Rpc;
use store::{Column, Direction, Store};
use tape_api::program::tapedrive::track_pda;
use tape_core::erasure::GROUP_SIZE;
use tape_core::object::object_etag;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::track::data::BlobData;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::{ContentType, SpoolIndex, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_crypto::merkle::{create_proof_from_leaf_hashes, hash_leaf};
use tape_node::config::http::HttpConfig;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, FindTrackRequest, GetSliceReq, ListObjectsRequest, ListObjectsResponse,
    ListTracksByTapeRequest, ListTracksByTapeResponse, NodeStats, ObjectListItem,
    TrackDataResponse, TrackProofResponse, TrackResponse,
};
use tape_sdk::codec::decoder::BlobDecoder;
use tape_sdk::stream::manifest::ChunkManifest;
use tape_store::TapeStore;
use tape_store::columns::SliceCol;
use tape_store::ops::{
    MetaOps, ObjectListOps, ObjectMetadataOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::types::SliceValue;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

use crate::cache::{CacheRead, CacheSource, GatewayCacheError, GatewaySliceCache};
use crate::meter::{GatewayMeter, GatewayMeterDecision, rate_limited_response};

const MAX_TRACK_SCAN_LIMIT: usize = u32::MAX as usize;
const MAX_OBJECT_LIST_LIMIT: usize = 1_000;
const OBJECT_PATH: &str = "/object/{track_id}";
const TRACK_BYTES_PATH: &str = "/track/{track_id}";

pub struct GatewayHttpServer<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    slice_cache: Arc<GatewaySliceCache<Db>>,
    meter: Arc<GatewayMeter>,
    http_config: HttpConfig,
    cancel: CancellationToken,
}

pub(crate) struct AppState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub(crate) context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    pub(crate) slice_cache: Arc<GatewaySliceCache<Db>>,
    pub(crate) meter: Arc<GatewayMeter>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Clone for AppState<Db, Cluster, Blockchain> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            slice_cache: self.slice_cache.clone(),
            meter: self.meter.clone(),
        }
    }
}

#[derive(Debug)]
enum RouteError {
    NotFound,
    BadRequest(String),
    BadGateway(String),
    Internal(String),
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message).into_response(),
            Self::BadGateway(message) => {
                tracing::warn!("gateway upstream error: {message}");
                (StatusCode::BAD_GATEWAY, "bad gateway").into_response()
            }
            Self::Internal(message) => {
                tracing::error!("gateway internal error: {message}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

impl Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::BadRequest(message) => write!(f, "bad request: {message}"),
            Self::BadGateway(message) => write!(f, "bad gateway: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for RouteError {}

impl From<GatewayCacheError> for RouteError {
    fn from(error: GatewayCacheError) -> Self {
        Self::Internal(error.to_string())
    }
}

impl<Db, Cluster, Blockchain> GatewayHttpServer<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        http_config: HttpConfig,
        cancel: CancellationToken,
    ) -> Result<Self, NodeError> {
        let slice_cache = Arc::new(
            GatewaySliceCache::new(context.store.clone(), context.config.gateway.cache.clone())
                .map_err(|error| NodeError::Store(error.to_string()))?,
        );
        let meter = Arc::new(GatewayMeter::new(context.config.gateway.metering.clone()));

        Ok(Self {
            context,
            slice_cache,
            meter,
            http_config,
            cancel,
        })
    }

    fn build_router(&self) -> Router {
        let state = AppState {
            context: self.context.clone(),
            slice_cache: self.slice_cache.clone(),
            meter: self.meter.clone(),
        };
        let peer_body_limit = DefaultBodyLimit::max(self.http_config.peer_max_bytes);

        Router::new()
            .route(
                OBJECT_PATH,
                get(get_object::<Db, Cluster, Blockchain>).layer(from_fn_with_state(
                    state.clone(),
                    crate::meter::object_read_metering::<Db, Cluster, Blockchain>,
                )),
            )
            .route(
                TRACK_BYTES_PATH,
                get(get_track_bytes::<Db, Cluster, Blockchain>).layer(from_fn_with_state(
                    state.clone(),
                    crate::meter::object_read_metering::<Db, Cluster, Blockchain>,
                )),
            )
            .route(tape_protocol::api::NODE_HEALTH_PATH, get(health))
            .route(
                tape_protocol::api::NODE_STATS_PATH,
                get(stats::<Db, Cluster, Blockchain>),
            )
            .route(tape_protocol::api::TRACK_PATH, get(get_track::<Db, Cluster, Blockchain>))
            .route(
                tape_protocol::api::TRACK_DATA_PATH,
                get(get_track_data::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TRACK_PROOF_PATH,
                get(get_track_proof::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TRACK_SLICE_PATH,
                get(get_slice::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_PATH,
                get(get_track_by_number::<Db, Cluster, Blockchain>),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_FIND_PATH,
                post(find_track::<Db, Cluster, Blockchain>).layer(peer_body_limit.clone()),
            )
            .route(
                tape_protocol::api::TAPE_TRACK_LIST_PATH,
                post(list_tracks_by_tape::<Db, Cluster, Blockchain>)
                    .layer(peer_body_limit.clone()),
            )
            .route(
                tape_protocol::api::TAPE_OBJECT_LIST_PATH,
                post(list_objects::<Db, Cluster, Blockchain>).layer(peer_body_limit),
            )
            .with_state(state.clone())
            .layer(from_fn_with_state(
                state,
                count_requests::<Db, Cluster, Blockchain>,
            ))
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_http_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(self.http_config.concurrency))
                    .layer(TimeoutLayer::new(Duration::from_secs(
                        self.http_config.timeout_secs,
                    ))),
            )
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let listen = self.http_config.listen;
        let router = self.build_router();
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(NodeError::Io)?;

        info!(listen = %listen, "gateway http listener bound");

        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                self.cancel.cancelled().await;
            })
            .await
            .map_err(NodeError::Io)
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn stats<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Result<Json<NodeStats>, RouteError> {
    let store = state.context.store.as_ref();
    let current_state = state.context.state();
    let metrics = state.context.metrics.snapshot();
    let last_processed_slot = store
        .get_sync_cursor()
        .map_err(store_error)?
        .map(|slot| slot.0)
        .unwrap_or(0);

    let ingest_progress = state.context.ingest.progress();
    let ingest_tip_raw = ingest_progress.last_known_tip();
    let ingest_dispatched = ingest_progress.last_dispatched_slot();
    let ingest_tip_slot = if ingest_tip_raw == u64::MAX {
        0
    } else {
        ingest_tip_raw
    };
    let ingest_lag_slots = if ingest_tip_raw == u64::MAX {
        0
    } else {
        ingest_tip_raw.saturating_sub(ingest_dispatched)
    };
    let ingest_state = state.context.ingest_state().label().to_string();
    let (slices_stored, slice_payload_bytes) = cached_slice_stats(store)?;
    let store_disk_bytes = store
        .inner()
        .inner()
        .actual_size_bytes()
        .map_err(store_error)?;
    let free_disk_bytes = store
        .inner()
        .inner()
        .available_disk_bytes()
        .map_err(store_error)?;

    Ok(Json(NodeStats {
        last_processed_slot,
        blocks_processed: metrics.blocks_processed_total,
        epoch_transitions: metrics.epoch_transitions_total,
        current_epoch: current_state.epoch().0,
        owned_spools: 0,
        tracks_stored: store.count_tracks().map_err(store_error)? as u64,
        slice_payload_bytes,
        store_disk_bytes,
        free_disk_bytes,
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        requests_total: metrics.requests_total,
        ingest_state,
        ingest_lag_slots,
        ingest_tip_slot,
    }))
}

async fn get_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

async fn get_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    let data_addr = track_pda(track.tape, track.track_number).0.into();
    let data = track_data_with_pending(&state, data_addr)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackDataResponse { data })
}

async fn get_track_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    let tape = state
        .context
        .store
        .get_tape(track.tape)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let pending_tracks = state.context.pending.registered_tracks_by_tape(track.tape);
    let pending_leaf_count = pending_tracks
        .iter()
        .map(|(_, track)| track.track_number.next().as_usize())
        .max()
        .unwrap_or(0);
    let leaf_count = (tape.next_track_number.0 as usize)
        .max(pending_leaf_count)
        .max(track.track_number.next().as_usize());
    let track_index = track.track_number.0 as usize;

    if leaf_count == 0
        || leaf_count > (1usize << TRACK_TREE_HEIGHT)
        || track_index >= leaf_count
    {
        return Err(RouteError::Internal("invalid tape track numbering".into()));
    }

    let empty_hash = hash_leaf(&[]);
    let mut leaves = vec![empty_hash; leaf_count];
    let disk_tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(track.tape, None, leaf_count)
        .map_err(store_error)?;

    for tape_track in merge_pending_tape_tracks(&state, track.tape, disk_tracks, pending_tracks) {
        let index = tape_track.track_number.0 as usize;
        if index < leaf_count {
            leaves[index] = tape_track.get_hash();
        }
    }

    let proof: [Hash; TRACK_TREE_HEIGHT] =
        create_proof_from_leaf_hashes::<{ TRACK_TREE_HEIGHT }>(&leaves, track_index)
            .map_err(|_| RouteError::Internal("invalid track proof".into()))?
            .try_into()
            .map_err(|_| RouteError::Internal("invalid track proof length".into()))?;

    binary_response(&TrackProofResponse {
        proof: CompressedTrackProof { state: track, proof }.pack(),
    })
}

async fn get_track_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number)): Path<(String, u64)>,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let track_addr = track_pda(tape, TrackNumber(track_number)).0.into();
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

async fn find_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let request: FindTrackRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("find track request: {error}")))?;

    let pending_tracks = state.context.pending.registered_tracks_by_tape(tape);
    let disk_tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(tape, None, MAX_TRACK_SCAN_LIMIT)
        .map_err(store_error)?;
    let mut matches = merge_pending_tape_tracks(&state, tape, disk_tracks, pending_tracks)
        .into_iter()
        .filter(|track| track.key == request.key)
        .collect::<Vec<_>>();
    matches.sort_by_key(|track| track.track_number.0);

    let track = match request.version {
        tape_protocol::api::ops::FindTrackVersion::Latest => matches.pop(),
        tape_protocol::api::ops::FindTrackVersion::Number(track_number) => matches
            .into_iter()
            .find(|track| track.track_number == track_number),
    }
    .ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

async fn list_tracks_by_tape<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let request: ListTracksByTapeRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list tracks request: {error}")))?;
    let limit = (request.limit as usize).clamp(1, MAX_TRACK_SCAN_LIMIT);

    let pending_tracks = state.context.pending.registered_tracks_by_tape(tape);
    let disk_limit = limit
        .saturating_add(pending_tracks.len())
        .saturating_add(1)
        .min(MAX_TRACK_SCAN_LIMIT);
    let disk_tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(tape, request.cursor, disk_limit)
        .map_err(store_error)?;

    let mut tracks = merge_pending_tape_tracks(&state, tape, disk_tracks, pending_tracks);
    if let Some(cursor) = request.cursor {
        tracks.retain(|track| track.track_number > cursor);
    }

    tracks.sort_by_key(|track| track.track_number.0);
    let next_cursor = tracks.get(limit).map(|track| track.track_number);
    let tracks = tracks
        .into_iter()
        .take(limit)
        .map(|track| track.pack())
        .collect();

    binary_response(&ListTracksByTapeResponse {
        tracks,
        next_cursor,
    })
}

async fn list_objects<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let bucket = parse_address(&tape_id, "tape id")?;
    let request: ListObjectsRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list objects request: {error}")))?;
    let limit = (request.limit as usize).clamp(1, MAX_OBJECT_LIST_LIMIT);
    let delimiter = request
        .delimiter
        .as_deref()
        .filter(|delimiter| !delimiter.is_empty());

    let page = state
        .context
        .store
        .list_objects(
            bucket,
            &request.prefix,
            delimiter,
            request.cursor.as_deref(),
            limit,
        )
        .map_err(store_error)?;

    let objects = page
        .objects
        .into_iter()
        .map(|(name, entry)| ObjectListItem {
            name,
            size: entry.size,
            etag: entry.etag,
            block_time: entry.block_time,
            slot: entry.slot,
            data_tape: entry.data_tape,
            track_number: entry.track_number,
            kind: entry.kind,
            content_type: entry.content_type,
        })
        .collect();

    binary_response(&ListObjectsResponse {
        objects,
        common_prefixes: page.common_prefixes,
        next_cursor: page.next,
        is_truncated: page.is_truncated,
    })
}

async fn get_object<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Path(track_id): Path<String>,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    match state
        .meter
        .check_object_bytes(remote.ip(), track.size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited_response(retry_after));
        }
    }

    let content_type = object_content_type(&state, track_addr)?;
    let decoded = decode_track_bytes(&state, track_addr, track).await?;
    let Ok(manifest) = ChunkManifest::from_bytes(&decoded.bytes) else {
        state
            .context
            .metrics
            .add_downloaded(decoded.bytes.len() as u64);
        return object_response(decoded.bytes, content_type, decoded.etag);
    };

    match state
        .meter
        .check_object_bytes(remote.ip(), manifest.total_size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited_response(retry_after));
        }
    }

    let chunks = manifest_chunks(&state, track.tape, &manifest)?;
    object_stream_response(
        state,
        chunks,
        content_type,
        decoded.etag,
        manifest.total_size.to_bytes(),
    )
}

async fn get_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Path(track_id): Path<String>,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    match state
        .meter
        .check_object_bytes(remote.ip(), track.size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited_response(retry_after));
        }
    }

    let content_type = object_content_type(&state, track_addr)?;
    let decoded = decode_track_bytes(&state, track_addr, track).await?;
    state
        .context
        .metrics
        .add_downloaded(decoded.bytes.len() as u64);

    object_response(decoded.bytes, content_type, decoded.etag)
}

struct DecodedObject {
    bytes: Vec<u8>,
    etag: Hash,
}

#[derive(Clone, Copy)]
struct ManifestChunk {
    index: usize,
    track_addr: Address,
    track: CompressedTrack,
    expected_size: u64,
}

fn manifest_chunks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    manifest: &ChunkManifest,
) -> Result<Vec<ManifestChunk>, RouteError> {
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for (chunk_index, entry) in manifest.chunks.iter().enumerate() {
        let chunk_addr = track_pda(tape, entry.track_number).0.into();
        let chunk = track_with_pending(state, chunk_addr)?.ok_or(RouteError::NotFound)?;
        if !chunk.is_certified() {
            return Err(RouteError::BadGateway(format!(
                "manifest chunk {chunk_index} is not certified"
            )));
        }

        chunks.push(ManifestChunk {
            index: chunk_index,
            track_addr: chunk_addr,
            track: chunk,
            expected_size: entry.size.to_bytes(),
        });
    }

    Ok(chunks)
}

async fn decode_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
) -> Result<DecodedObject, RouteError> {
    let track_data = track_data_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;

    if track.is_inline() {
        let BlobData::Inline(bytes) = track_data else {
            return Err(RouteError::BadRequest("track data is not inline".into()));
        };
        if hash(&bytes) != track.value_hash {
            return Err(RouteError::Internal("inline track hash mismatch".into()));
        }

        return Ok(DecodedObject {
            bytes,
            etag: object_etag(&track, None),
        });
    }

    if !track.is_coded() {
        return Err(RouteError::BadRequest("unsupported track kind".into()));
    }

    let BlobData::Coded(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };
    if blob.get_hash() != track.value_hash || blob.commitment_root() != blob.commitment {
        return Err(RouteError::Internal("blob commitment mismatch".into()));
    }

    let slices = fetch_decoding_slices(state, track_addr, track, blob).await?;
    let mut decoder = BlobDecoder::with_profile(blob.profile);
    let mut bytes = decoder
        .decode(slices)
        .map_err(|error| RouteError::BadGateway(format!("decode object: {error}")))?;
    let logical_size = usize::try_from(blob.size.to_bytes())
        .map_err(|_| RouteError::Internal("object is too large for this platform".into()))?;
    if bytes.len() < logical_size {
        return Err(RouteError::BadGateway("decoded object is truncated".into()));
    }
    bytes.truncate(logical_size);

    Ok(DecodedObject {
        bytes,
        etag: object_etag(&track, Some(&blob)),
    })
}

async fn fetch_decoding_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
    blob: BlobEncoding,
) -> Result<Vec<(SpoolIndex, Vec<u8>)>, RouteError> {
    let k = blob.profile.k() as usize;
    if k == 0 || k > GROUP_SIZE {
        return Err(RouteError::Internal("invalid blob encoding profile".into()));
    }

    let peers = state.context.state().group_peers(track.group);
    if peers.is_empty() {
        return Err(RouteError::BadGateway("no peers for track group".into()));
    }

    let mut fetches = FuturesUnordered::new();
    for (spool_id, _) in peers {
        fetches.push(async move {
            let read = read_cached_slice(state, track_addr, spool_id).await?;
            Ok::<_, RouteError>((spool_id, read.data))
        });
    }

    let mut slices = Vec::with_capacity(k);
    while let Some(result) = fetches.next().await {
        match result {
            Ok((spool_id, data)) => {
                let Some(position) = track.group.position_of(spool_id) else {
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice outside track group");
                    continue;
                };
                if position >= GROUP_SIZE || hash_leaf(&data) != blob.leaves[position] {
                    warn!(spool = %spool_id, track = %track_addr, "gateway skipped slice with mismatched leaf hash");
                    continue;
                }

                slices.push((SpoolIndex(position as u64), data));
                if slices.len() >= k {
                    return Ok(slices);
                }
            }
            Err(error) => {
                debug!(track = %track_addr, ?error, "gateway object slice fetch failed");
            }
        }
    }

    Err(RouteError::BadGateway(
        "insufficient verified slices for object decode".into(),
    ))
}

fn object_content_type<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<ContentType, RouteError> {
    let content_type = state
        .context
        .store
        .get_object_metadata(track_addr)
        .map_err(store_error)?
        .map(|metadata| metadata.content_type)
        .unwrap_or(ContentType::Unknown);
    Ok(content_type)
}

fn object_response(
    bytes: Vec<u8>,
    content_type: ContentType,
    etag: Hash,
) -> Result<Response, RouteError> {
    let headers = object_headers(bytes.len() as u64, content_type, etag)?;
    Ok((StatusCode::OK, headers, bytes).into_response())
}

fn object_stream_response<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
    content_type: ContentType,
    etag: Hash,
    content_length: u64,
) -> Result<Response, RouteError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let headers = object_headers(content_length, content_type, etag)?;
    let body = Body::from_stream(manifest_chunk_stream(state, chunks));
    Ok((StatusCode::OK, headers, body).into_response())
}

fn object_headers(
    content_length: u64,
    content_type: ContentType,
    etag: Hash,
) -> Result<HeaderMap, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type.to_str()),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| RouteError::Internal(format!("content length header: {error}")))?,
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );

    Ok(headers)
}

fn manifest_chunk_stream<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
) -> impl Stream<Item = Result<Bytes, RouteError>> + Send + 'static
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    futures::stream::try_unfold(
        ObjectStreamState {
            state,
            chunks,
            next: 0,
        },
        |mut stream| async move {
            let Some(chunk) = stream.chunks.get(stream.next).copied() else {
                return Ok(None);
            };
            stream.next += 1;

            let decoded =
                decode_track_bytes(&stream.state, chunk.track_addr, chunk.track).await?;
            if decoded.bytes.len() as u64 != chunk.expected_size {
                return Err(RouteError::BadGateway(format!(
                    "manifest chunk {} size mismatch",
                    chunk.index
                )));
            }

            stream
                .state
                .context
                .metrics
                .add_downloaded(decoded.bytes.len() as u64);

            Ok(Some((Bytes::from(decoded.bytes), stream)))
        },
    )
}

struct ObjectStreamState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
    next: usize,
}

async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, SpoolIndex)>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let read = read_cached_slice(&state, track_addr, spool_id).await?;
    let data = read.data;
    state.context.metrics.add_downloaded(data.len() as u64);
    if read.source == CacheSource::Hit {
        debug!(track = %track_addr, spool = spool_id.0, bytes = data.len(), "gateway served cached slice");
    }

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], data))
}

async fn read_cached_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool_id: SpoolIndex,
) -> Result<CacheRead, RouteError> {
    state
        .slice_cache
        .get_or_insert_with(spool_id, track_addr, || {
            fetch_slice_from_owner(state, track_addr, spool_id)
        })
        .await
}

async fn fetch_slice_from_owner<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool_id: SpoolIndex,
) -> Result<Vec<u8>, RouteError> {
    let track = track_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_coded() {
        return Err(RouteError::BadRequest("track is not coded".into()));
    }

    let position = track
        .group
        .position_of(spool_id)
        .ok_or_else(|| RouteError::BadRequest("spool is not in track group".into()))?;

    let data = track_data_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;
    let BlobData::Coded(blob) = data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    let owner = state
        .context
        .state()
        .group_peers(track.group)
        .into_iter()
        .find_map(|(spool, node)| (spool == spool_id).then_some(node))
        .ok_or_else(|| RouteError::BadGateway("spool owner not found".into()))?;

    let response = state
        .context
        .api
        .get_slice(
            owner,
            &GetSliceReq {
                track: track_addr,
                spool: spool_id,
            },
        )
        .await
        .map_err(|error| RouteError::BadGateway(format!("get_slice: {error}")))?;

    if position >= GROUP_SIZE || hash_leaf(&response.data) != blob.leaves[position] {
        return Err(RouteError::BadGateway("slice leaf hash mismatch".into()));
    }

    debug!(
        track = %track_addr,
        spool = spool_id.0,
        owner = %owner,
        bytes = response.data.len(),
        "gateway fetched slice from owner"
    );

    Ok(response.data)
}

fn track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .get_track(track_addr)
        .map_err(store_error)?;
    Ok(state.context.pending.apply_to_track(track_addr, in_store))
}

fn track_data_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<BlobData>, RouteError> {
    match state.context.pending.track_data(track_addr) {
        Some(data) => Ok(Some(data)),
        None => state
            .context
            .store
            .get_track_data(track_addr)
            .map_err(store_error),
    }
}

fn merge_pending_tape_tracks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    disk_tracks: Vec<CompressedTrack>,
    pending_tracks: Vec<(Address, CompressedTrack)>,
) -> Vec<CompressedTrack> {
    let mut by_number = BTreeMap::new();

    for disk_track in disk_tracks {
        let track_addr = track_pda(disk_track.tape, disk_track.track_number).0.into();
        if let Some(track) = state
            .context
            .pending
            .apply_to_track(track_addr, Some(disk_track))
        {
            by_number.insert(track.track_number, track);
        }
    }

    for (_, track) in pending_tracks {
        if track.tape == tape {
            by_number.insert(track.track_number, track);
        }
    }

    by_number.into_values().collect()
}

fn parse_address(value: &str, label: &str) -> Result<Address, RouteError> {
    value
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid {label}: {error}")))
}

fn cached_slice_stats<Db: Store>(store: &TapeStore<Db>) -> Result<(u64, u64), RouteError> {
    let iter = store
        .inner()
        .inner()
        .iter_from(SliceCol::CF_NAME, &[], Direction::Asc)
        .map_err(store_error)?;
    let mut slices_stored = 0u64;
    let mut slice_payload_bytes = 0u64;

    for (_key, value_bytes) in iter {
        let data: SliceValue = wincode::deserialize(&value_bytes)
            .map_err(|error| RouteError::Internal(format!("slice value: {error}")))?;
        slices_stored = slices_stored.saturating_add(1);
        slice_payload_bytes = slice_payload_bytes.saturating_add(data.0.len() as u64);
    }

    Ok((slices_stored, slice_payload_bytes))
}

fn binary_response<T: wincode::SchemaWrite<Src = T>>(
    value: &T,
) -> Result<impl IntoResponse, RouteError> {
    let body = wincode::serialize(value)
        .map_err(|error| RouteError::Internal(format!("serialize response: {error}")))?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

async fn handle_http_error(error: axum::BoxError) -> StatusCode {
    if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::REQUEST_TIMEOUT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn count_requests<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response {
    state.context.metrics.inc_requests_total();
    next.run(req).await
}
