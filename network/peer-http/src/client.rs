use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use peer_manager::{PeerManager, PeerNode};
use tape_protocol::api::*;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair;

use crate::builder::HttpApiBuilder;
use crate::metrics::ApiMetrics;

/// Per-request timeout for snapshot vote calls.
const SNAPSHOT_VOTE_TIMEOUT: Duration = Duration::from_secs(3);

/// A reqwest client pinned to a specific peer's TLS public key. Cached on the
/// `HttpApi` and invalidated when PeerManager observes a change to the peer's
/// on-chain `network_tls` field.
#[derive(Clone)]
pub struct PinnedPeerClient {
    pub client: reqwest::Client,
    pub tls_pubkey: Address,
    pub network_address: NetworkAddress,
}

pub struct HttpApi {
    pub peer_manager: Arc<PeerManager>,
    pub clients: Arc<DashMap<NodeId, PinnedPeerClient>>,
    pub metrics: Option<Arc<ApiMetrics>>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub local_identity: Option<Arc<Keypair>>,
}

impl HttpApi {
    /// Build an HttpApi with default timeouts. Equivalent to
    /// `HttpApiBuilder::new().build(peer_manager)` and cannot fail in practice
    /// since the builder installs the rustls crypto provider once.
    pub fn with_default_timeouts(peer_manager: Arc<PeerManager>) -> Self {
        HttpApiBuilder::new()
            .build(peer_manager)
            .expect("default peer HTTP client config should build")
    }

    /// Get-or-build a pinned HTTPS client for the given peer. Rebuilds when the
    /// cached entry's TLS pubkey or network address differ from the current
    /// PeerNode snapshot (which is how we handle peer key rotations).
    fn client_for(&self, peer: &PeerNode) -> Result<(reqwest::Client, String), ApiError> {
        if let Some(entry) = self.clients.get(&peer.node_id) {
            if entry.tls_pubkey == peer.tls_pubkey
                && entry.network_address == peer.network_address
            {
                let url = https_base_url(entry.network_address)?;
                return Ok((entry.client.clone(), url));
            }
        }

        let client = self.build_pinned_client(peer.tls_pubkey)?;
        let pinned = PinnedPeerClient {
            client: client.clone(),
            tls_pubkey: peer.tls_pubkey,
            network_address: peer.network_address,
        };
        self.clients.insert(peer.node_id, pinned);

        let url = https_base_url(peer.network_address)?;
        Ok((client, url))
    }

    fn build_pinned_client(&self, tls_pubkey: Address) -> Result<reqwest::Client, ApiError> {
        let builder = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);
        let builder = match &self.local_identity {
            Some(identity) => {
                peer_tls::apply_pinned_tls_with_identity(builder, tls_pubkey, identity.as_ref())
                    .map_err(|e| ApiError::Other(format!("tls build: {e}")))?
            }
            None => peer_tls::apply_pinned_tls(builder, tls_pubkey)
                .map_err(|e| ApiError::Other(format!("tls build: {e}")))?,
        };
        builder
            .build()
            .map_err(|e| ApiError::Other(format!("client build: {e}")))
    }

    fn resolve(&self, node_id: NodeId) -> Result<(reqwest::Client, String), ApiError> {
        let peer = self.resolve_peer(node_id)?;
        self.client_for(&peer)
    }

    fn resolve_peer(&self, node_id: NodeId) -> Result<PeerNode, ApiError> {
        self.peer_manager
            .get(node_id)
            .ok_or(ApiError::NodeUnresolved(node_id))
    }

    fn record(&self, op: &str, resp: &reqwest::Response, start: Instant, bytes_sent: u64) {
        if let Some(m) = &self.metrics {
            let duration = start.elapsed().as_secs_f64();
            let status = resp.status().as_u16().to_string();
            m.record_request(op, &status, duration);
            if bytes_sent > 0 {
                m.record_bytes_sent(op, bytes_sent);
            }
        }
    }

    fn record_rx(&self, op: &str, bytes: u64) {
        if let Some(m) = &self.metrics {
            m.record_bytes_received(op, bytes);
        }
    }
}

fn https_base_url(addr: NetworkAddress) -> Result<String, ApiError> {
    let sa = addr
        .to_socket_addr()
        .map_err(|e| ApiError::ConnectionFailed(e.to_string()))?;
    Ok(format!("https://{sa}"))
}

#[async_trait]
impl Api for HttpApi {
    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", slice_url(&track_id, req.spool));
        let body =
            wincode::serialize(&req.payload)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .put(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("put_slice", &resp, start, bytes_sent);

        check_status(resp).await?;

        Ok(PutSliceRes)
    }

    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", slice_url(&track_id, req.spool));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_slice", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_slice", bytes.len() as u64);
        Ok(GetSliceRes {
            data: bytes.to_vec(),
        })
    }

    async fn get_track(&self, node: NodeId, req: &GetTrackReq) -> Result<GetTrackRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_track", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_track", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn get_track_by_number(
        &self,
        node: NodeId,
        req: &GetTrackByNumberReq,
    ) -> Result<GetTrackByNumberRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", tape_track_url(&tape_id, req.track_number));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_track_by_number", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_track_by_number", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackByNumberRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn find_track(&self, node: NodeId, req: &FindTrackReq) -> Result<FindTrackRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", find_track_url(&tape_id));
        let wire_req = FindTrackRequest {
            key: req.key,
            version: req.version.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("find_track", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("find_track", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(FindTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn list_tracks_by_tape(
        &self,
        node: NodeId,
        req: &ListTracksByTapeReq,
    ) -> Result<ListTracksByTapeRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", list_tracks_by_tape_url(&tape_id));
        let wire_req = ListTracksByTapeRequest {
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("list_tracks_by_tape", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("list_tracks_by_tape", bytes.len() as u64);
        let wire: ListTracksByTapeResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(ListTracksByTapeRes {
            tracks: wire
                .tracks
                .into_iter()
                .map(CompressedTrack::unpack)
                .collect(),
            next_cursor: wire.next_cursor,
        })
    }

    async fn get_track_data(
        &self,
        node: NodeId,
        req: &GetTrackDataReq,
    ) -> Result<GetTrackDataRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_data_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_track_data", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_track_data", bytes.len() as u64);
        let wire: TrackDataResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetTrackDataRes { data: wire.data })
    }

    async fn get_track_proof(
        &self,
        node: NodeId,
        req: &GetTrackProofReq,
    ) -> Result<GetTrackProofRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_proof_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_track_proof", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_track_proof", bytes.len() as u64);
        let wire: TrackProofResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetTrackProofRes {
            proof: CompressedTrackProof::unpack(wire.proof),
        })
    }

    async fn sync_slices(&self, node: NodeId, req: &SyncSlicesReq) -> Result<SyncSlicesRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", SYNC_SLICES_PATH);
        let wire_req = SyncSlicesRequest {
            spool_index: req.spool_index,
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("sync_slices", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("sync_slices", bytes.len() as u64);
        let wire_res: SyncSlicesResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(SyncSlicesRes {
            entries: wire_res.entries,
            next_cursor: wire_res.next_cursor,
        })
    }

    async fn sync_tracks(&self, node: NodeId, req: &SyncTracksReq) -> Result<SyncTracksRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", SYNC_TRACKS_PATH);
        let wire_req = SyncTracksRequest {
            spool_index: req.spool_index,
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("sync_tracks", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("sync_tracks", bytes.len() as u64);
        let wire_res: SyncTracksResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(SyncTracksRes {
            entries: wire_res.entries,
            next_cursor: wire_res.next_cursor,
        })
    }

    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", repair_url(&track_id));
        let wire_req = RepairRequest {
            helper_spool: req.helper_spool,
            stripes: req.stripes.clone(),
        };

        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("repair", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("repair", bytes.len() as u64);
        Ok(RepairRes {
            data: bytes.to_vec(),
        })
    }

    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", sign_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("certify", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("certify", bytes.len() as u64);
        let wire: BlsSignResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(CertifyRes {
            signature: wire.signature,
            node_id: wire.node_id,
            epoch: wire.epoch,
        })
    }

    async fn snapshot_vote(
        &self,
        node: NodeId,
        req: &SnapshotVoteReq,
    ) -> Result<SnapshotVoteRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{SNAPSHOT_VOTE_PATH}");
        let wire_req = SnapshotVoteRequest {
            node_id: req.node_id,
            kind: req.kind,
            message: req.message.clone(),
            signature: req.signature,
        };
        let body = wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .timeout(SNAPSHOT_VOTE_TIMEOUT)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("snapshot_vote", &resp, start, bytes_sent);
        check_status(resp).await?;
        Ok(SnapshotVoteRes)
    }

    async fn invalidate(
        &self,
        node: NodeId,
        req: &InvalidateReq,
    ) -> Result<InvalidateRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", inconsistency_url(&track_id));
        let wire_req = InconsistencyRequest {
            proof: req.proof.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("invalidate", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("invalidate", bytes.len() as u64);
        let wire: BlsInconsistencyResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(InvalidateRes {
            signature: wire.signature,
            node_id: wire.node_id,
            epoch: wire.epoch,
        })
    }

    async fn get_health(
        &self,
        node: NodeId,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", NODE_HEALTH_PATH);

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_health", &resp, start, 0);
        Ok(GetHealthRes {
            ok: resp.status().is_success(),
        })
    }

    async fn get_stats(
        &self,
        node: NodeId,
        _req: &GetStatsReq,
    ) -> Result<GetStatsRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", NODE_STATS_PATH);

        let start = Instant::now();
        let resp = client
            .get(&url)
            .header("accept", JSON_CONTENT)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_stats", &resp, start, 0);
        let resp = check_status(resp).await?;
        let stats = resp
            .json()
            .await
            .map_err(|e| ApiError::Serialization(format!("json: {e}")))?;
        Ok(GetStatsRes { stats })
    }
}

fn map_reqwest(e: reqwest::Error) -> ApiError {
    let msg = error_chain(&e);
    if e.is_timeout() {
        ApiError::Timeout
    } else if e.is_connect() {
        ApiError::ConnectionFailed(msg)
    } else {
        ApiError::Other(msg)
    }
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    match status.as_u16() {
        404 => Err(ApiError::NotFound),
        403 => {
            let body = resp.text().await.unwrap_or_default();
            if body.contains("not responsible") {
                Err(ApiError::NotResponsible)
            } else if body.contains("not in committee") {
                Err(ApiError::NotInCommittee)
            } else {
                Err(ApiError::ServerError {
                    status: 403,
                    message: body,
                })
            }
        }
        s => {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiError::ServerError {
                status: s,
                message: body,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use axum_server::tls_rustls::RustlsConfig;
    use peer_manager::PeerNode;
    use peer_tls::{build_server_config, install_default_provider};
    use rand::thread_rng;
    use tape_core::bls::{BlsPrivateKey, BlsPubkey};
    use tape_core::cert::{SNAPSHOT_SIGN_MESSAGE_SIZE, SNAPSHOT_WRITE_MESSAGE_SIZE};
    use tape_crypto::ed25519::Keypair as EdKeypair;
    use tokio::net::TcpListener;

    fn make_peer(id: u64, port: u16, tls_pubkey: Address) -> PeerNode {
        PeerNode {
            node_id: NodeId(id),
            authority: Address::new_unique(),
            state_address: Address::new_unique(),
            bls_pubkey: BlsPubkey::new_unique(),
            tls_pubkey,
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
        }
    }

    async fn serve_tls(
        tls_keypair: EdKeypair,
        router: Router,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();

        let server_config =
            build_server_config(&tls_keypair, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("cfg");
        let rustls = RustlsConfig::from_config(server_config);

        let handle = tokio::spawn(async move {
            axum_server::from_tcp_rustls(std_listener, rustls)
                .serve(router.into_make_service())
                .await
                .unwrap();
        });
        (addr, handle)
    }

    #[test]
    fn resolves_peers_added_after_api_construction() {
        install_default_provider();
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::with_default_timeouts(peer_manager.clone());
        let node_id = NodeId(7);

        assert!(matches!(
            api.resolve_peer(node_id),
            Err(ApiError::NodeUnresolved(id)) if id == node_id
        ));

        peer_manager.add_peer(make_peer(7, 8080, Address::new_unique()));
        let peer = api.resolve_peer(node_id).expect("resolve");
        assert_eq!(peer.node_id, node_id);
    }

    #[test]
    fn default_timeout_builder_constructs_http_api() {
        install_default_provider();
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::with_default_timeouts(peer_manager.clone());
        assert!(Arc::ptr_eq(&api.peer_manager, &peer_manager));
    }

    #[tokio::test]
    async fn snapshot_vote_write_roundtrip_over_tls() {
        install_default_provider();
        let mut rng = thread_rng();
        let tls = EdKeypair::new(&mut rng);
        let tls_pubkey = tls.address();

        let request = SnapshotVoteRequest {
            node_id: NodeId(9),
            kind: SnapshotVoteKind::WriteChunk,
            message: vec![0xAB; SNAPSHOT_WRITE_MESSAGE_SIZE],
            signature: BlsPrivateKey::from_random()
                .sign(b"snapshot-write")
                .unwrap(),
        };
        let api_request = SnapshotVoteReq {
            node_id: request.node_id,
            kind: request.kind,
            message: request.message.clone(),
            signature: request.signature,
        };

        let expected_request = Arc::new(request.clone());
        let router = Router::new().route(
            SNAPSHOT_VOTE_PATH,
            post({
                let expected_request = Arc::clone(&expected_request);
                move |body: Bytes| {
                    let expected_request = Arc::clone(&expected_request);
                    async move {
                        let decoded: SnapshotVoteRequest = wincode::deserialize(&body).unwrap();
                        assert_eq!(decoded, *expected_request);
                        StatusCode::OK
                    }
                }
            }),
        );

        let (addr, _handle) = serve_tls(tls, router).await;

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(7, addr.port(), tls_pubkey));
        let api = HttpApi::with_default_timeouts(peer_manager);

        api.snapshot_vote(NodeId(7), &api_request).await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_vote_complete_group_roundtrip_over_tls() {
        install_default_provider();
        let mut rng = thread_rng();
        let tls = EdKeypair::new(&mut rng);
        let tls_pubkey = tls.address();

        let request = SnapshotVoteRequest {
            node_id: NodeId(9),
            kind: SnapshotVoteKind::CompleteGroup,
            message: vec![0xCD; SNAPSHOT_SIGN_MESSAGE_SIZE],
            signature: BlsPrivateKey::from_random()
                .sign(b"snapshot-complete")
                .unwrap(),
        };
        let api_request = SnapshotVoteReq {
            node_id: request.node_id,
            kind: request.kind,
            message: request.message.clone(),
            signature: request.signature,
        };

        let expected_request = Arc::new(request.clone());
        let router = Router::new().route(
            SNAPSHOT_VOTE_PATH,
            post({
                let expected_request = Arc::clone(&expected_request);
                move |body: Bytes| {
                    let expected_request = Arc::clone(&expected_request);
                    async move {
                        let decoded: SnapshotVoteRequest = wincode::deserialize(&body).unwrap();
                        assert_eq!(decoded, *expected_request);
                        StatusCode::OK
                    }
                }
            }),
        );

        let (addr, _handle) = serve_tls(tls, router).await;

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(7, addr.port(), tls_pubkey));
        let api = HttpApi::with_default_timeouts(peer_manager);

        api.snapshot_vote(NodeId(7), &api_request).await.unwrap();
    }

    #[tokio::test]
    async fn rebuilds_client_when_peer_rotates_tls_key() {
        install_default_provider();
        let mut rng = thread_rng();
        let original_tls = EdKeypair::new(&mut rng);
        let new_tls = EdKeypair::new(&mut rng);

        let router = Router::new().route(
            SNAPSHOT_VOTE_PATH,
            post(|_: Bytes| async move { StatusCode::OK }),
        );

        let (addr, _handle) = serve_tls(original_tls, router).await;

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(7, addr.port(), Address::new_unique()));
        let api = HttpApi::with_default_timeouts(peer_manager.clone());

        // Snapshot the client with a wrong pin — request should fail.
        let request = SnapshotVoteReq {
            node_id: NodeId(9),
            kind: SnapshotVoteKind::WriteChunk,
            message: vec![0u8; SNAPSHOT_WRITE_MESSAGE_SIZE],
            signature: BlsPrivateKey::from_random().sign(b"x").unwrap(),
        };
        assert!(api.snapshot_vote(NodeId(7), &request).await.is_err());

        // Rotate the peer's tls_pubkey to an also-wrong key; cache must
        // reflect the rotation (still mismatched, but the cached entry is
        // fresh).
        peer_manager.add_peer(make_peer(7, addr.port(), new_tls.address()));
        let peer = peer_manager.get(NodeId(7)).unwrap();
        api.client_for(&peer).expect("build fresh client");
        let cached = api.clients.get(&NodeId(7)).unwrap();
        assert_eq!(cached.tls_pubkey, new_tls.address());
    }

    // Plain-TcpListener warning suppression — tokio's import can otherwise go
    // unused in test-only builds where only axum_server is used.
    #[allow(dead_code)]
    fn _tcp_listener_ref(_: TcpListener) {}
}
