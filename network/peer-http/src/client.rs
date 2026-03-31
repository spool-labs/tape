use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tape_protocol::api::{
    self, Api, ApiError, BlsInconsistencyResponse, BlsSignResponse, CertifyReq, CertifyRes,
    CONTENT_TYPE_JSON, BINARY_CONTENT, FindTrackReq, FindTrackRes, FindTrackRequest,
    GetHealthReq, GetHealthRes, GetSliceReq, GetSliceRes, GetSnapshotReq, GetSnapshotRes,
    GetStatsReq, GetStatsRes, GetTrackByNumberReq, GetTrackByNumberRes, GetTrackDataReq,
    GetTrackDataRes, GetTrackProofReq, GetTrackProofRes, GetTrackReq, GetTrackRes, InconsistencyRequest, InvalidateReq,
    InvalidateRes, ListTracksByTapeReq, ListTracksByTapeRequest, ListTracksByTapeRes,
    ListTracksByTapeResponse, PutSliceReq, PutSliceRes, PutSnapshotReq, PutSnapshotRes,
    RepairReq, RepairRequest, RepairRes, SyncSlicesReq, SyncSlicesRes, SyncSlicesRequest,
    SyncSlicesResponse, SyncTracksReq, SyncTracksRes, SyncTracksRequest, SyncTracksResponse,
    TrackDataResponse, TrackProofResponse, TrackResponse,
};
use peer_manager::PeerManager;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_crypto::Hash;

use crate::builder::HttpApiBuilder;
use crate::metrics::ApiMetrics;

pub struct HttpApi {
    pub peer_manager: Arc<PeerManager>,
    pub client: reqwest::Client,
    pub metrics: Option<Arc<ApiMetrics>>,
    pub scheme: &'static str,
}

impl HttpApi {
    pub fn new(http: reqwest::Client, peer_manager: Arc<PeerManager>) -> Self {
        Self {
            peer_manager,
            client: http,
            metrics: None,
            scheme: "http",
        }
    }

    pub fn with_default_timeouts(peer_manager: Arc<PeerManager>) -> Self {
        HttpApiBuilder::new()
            .build(peer_manager)
            .expect("default peer HTTP client config should build")
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

fn base_url(scheme: &str, addr: NetworkAddress) -> Result<String, ApiError> {
    let sa = addr
        .to_socket_addr()
        .map_err(|e| ApiError::ConnectionFailed(e.to_string()))?;
    Ok(format!("{scheme}://{sa}"))
}

fn resolve(scheme: &str, pm: &PeerManager, node: NodeId) -> Result<String, ApiError> {
    let addr = pm.resolve(node).ok_or(ApiError::NodeUnresolved(node))?;
    base_url(scheme, addr)
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
    use std::sync::Arc;

    use tape_core::bls::BlsPubkey;
    use tape_crypto::Pubkey;
    use peer_manager::PeerNode;

    fn make_peer(id: u64, port: u16) -> PeerNode {
        PeerNode {
            node_id: NodeId(id),
            authority: Pubkey::new_unique(),
            state_address: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::new_unique(),
            tls_pubkey: Pubkey::new_unique(),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
        }
    }

    #[test]
    fn resolves_peers_added_after_api_construction() {
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::new(reqwest::Client::new(), peer_manager.clone());
        let node_id = NodeId(7);

        assert!(matches!(
            resolve(api.scheme, api.peer_manager.as_ref(), node_id),
            Err(ApiError::NodeUnresolved(id)) if id == node_id
        ));

        peer_manager.add_peer(make_peer(7, 8080));

        let base = resolve(api.scheme, api.peer_manager.as_ref(), node_id).unwrap();
        assert_eq!(base, "http://127.0.0.1:8080");
    }

    #[test]
    fn default_timeout_builder_constructs_http_api() {
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::with_default_timeouts(peer_manager.clone());
        assert_eq!(api.scheme, "http");
        assert!(Arc::ptr_eq(&api.peer_manager, &peer_manager));
    }
}

#[async_trait]
impl Api for HttpApi {
    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::slice_url(&track_id, req.spool));
        let body =
            wincode::serialize(&req.payload)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::slice_url(&track_id, req.spool));

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::track_url(&track_id));

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", api::tape_track_url(&tape_id, req.track_number));

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", api::find_track_url(&tape_id));
        let wire_req = FindTrackRequest {
            key: req.key,
            version: req.version.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", api::list_tracks_by_tape_url(&tape_id));
        let wire_req = ListTracksByTapeRequest {
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::track_data_url(&track_id));

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::track_proof_url(&track_id));

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!("{base}{}", api::SYNC_SLICES_PATH);
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
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!("{base}{}", api::SYNC_TRACKS_PATH);
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
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::repair_url(&track_id));
        let wire_req = RepairRequest {
            helper_spool: req.helper_spool,
            stripes: req.stripes.clone(),
        };

        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::sign_url(&track_id));

        let start = Instant::now();
        let resp = self
            .client
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

    async fn invalidate(
        &self,
        node: NodeId,
        req: &InvalidateReq,
    ) -> Result<InvalidateRes, ApiError> {
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", api::inconsistency_url(&track_id));
        let wire_req = InconsistencyRequest {
            proof: req.proof.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
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

    async fn put_snapshot(
        &self,
        node: NodeId,
        req: &PutSnapshotReq,
    ) -> Result<PutSnapshotRes, ApiError> {
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!(
            "{base}{}",
            api::snapshot_signature_url(req.epoch.0, req.chunk_index)
        );
        let body = wincode::serialize(&req.submission)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = self
            .client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("put_snapshot", &resp, start, bytes_sent);
        check_status(resp).await?;
        Ok(PutSnapshotRes)
    }

    async fn get_snapshot(
        &self,
        node: NodeId,
        req: &GetSnapshotReq,
    ) -> Result<GetSnapshotRes, ApiError> {
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!(
            "{base}{}",
            api::snapshot_commitments_url(req.epoch.0)
        );

        let start = Instant::now();
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_snapshot", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_snapshot", bytes.len() as u64);
        let commitments: Vec<Hash> =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetSnapshotRes { commitments })
    }

    async fn get_health(
        &self,
        node: NodeId,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, ApiError> {
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!("{base}{}", api::HEALTH_PATH);

        let start = Instant::now();
        let resp = self
            .client
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
        let base = resolve(self.scheme, &self.peer_manager, node)?;
        let url = format!("{base}{}", api::STATS_PATH);

        let start = Instant::now();
        let resp = self
            .client
            .get(&url)
            .header("accept", CONTENT_TYPE_JSON)
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
