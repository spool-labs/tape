use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tape_protocol::api::{
    Api, ApiError,
    CertifyReq, CertifyRes, GetHealthReq, GetHealthRes, GetMetadataReq, GetMetadataRes,
    GetSliceReq, GetSliceRes, GetSnapshotReq, GetSnapshotRes, GetStatsReq, GetStatsRes,
    InvalidateReq, InvalidateRes, PutSliceReq, PutSliceRes,
    PutSnapshotReq, PutSnapshotRes, RepairReq, RepairRes, SyncReq, SyncRes,
    BlsInconsistencyResponse, BlsSignResponse, InconsistencyRequest, RepairRequest,
    SyncSpoolRequest, SyncSpoolResponse, BINARY_CONTENT, CONTENT_TYPE_JSON,
};
use tape_protocol::peer::TrustedPeers;
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;

use crate::metrics::ApiMetrics;

pub struct HttpApi {
    pub(crate) peers: TrustedPeers,
    pub(crate) client: reqwest::Client,
    pub(crate) metrics: Option<Arc<ApiMetrics>>,
    pub(crate) scheme: &'static str,
}

impl Default for HttpApi {
    fn default() -> Self {
        Self {
            peers: TrustedPeers::new(),
            client: reqwest::Client::new(),
            metrics: None,
            scheme: "http",
        }
    }
}

impl HttpApi {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            peers: TrustedPeers::new(),
            client: http,
            metrics: None,
            scheme: "http",
        }
    }

    pub fn peers(&self) -> &TrustedPeers {
        &self.peers
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

fn resolve(scheme: &str, peers: &TrustedPeers, node: NodeId) -> Result<String, ApiError> {
    let addr = peers.resolve(node).ok_or(ApiError::NodeUnresolved(node))?;
    base_url(scheme, addr)
}

fn map_reqwest(e: reqwest::Error) -> ApiError {
    if e.is_timeout() {
        ApiError::Timeout
    } else if e.is_connect() {
        ApiError::ConnectionFailed(e.to_string())
    } else {
        ApiError::Other(e.to_string())
    }
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

#[async_trait]
impl Api for HttpApi {
    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::slice_url(&track_id, req.spool));
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
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::slice_url(&track_id, req.spool));

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

    async fn get_metadata(
        &self,
        node: NodeId,
        req: &GetMetadataReq,
    ) -> Result<GetMetadataRes, ApiError> {
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::metadata_url(&track_id));

        let start = Instant::now();
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record("get_metadata", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("get_metadata", bytes.len() as u64);
        Ok(GetMetadataRes {
            data: bytes.to_vec(),
        })
    }

    async fn sync(&self, node: NodeId, req: &SyncReq) -> Result<SyncRes, ApiError> {
        let base = resolve(self.scheme, &self.peers, node)?;
        let url = format!("{base}{}", tape_protocol::api::SYNC_SPOOL_PATH);
        let wire_req = SyncSpoolRequest {
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

        self.record("sync", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx("sync", bytes.len() as u64);
        let wire_res: SyncSpoolResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(SyncRes {
            entries: wire_res.entries,
            next_cursor: wire_res.next_cursor,
        })
    }

    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, ApiError> {
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::repair_url(&track_id));
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
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::sign_url(&track_id));

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
        let base = resolve(self.scheme, &self.peers, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_protocol::api::inconsistency_url(&track_id));
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
        let base = resolve(self.scheme, &self.peers, node)?;
        let url = format!(
            "{base}{}",
            tape_protocol::api::snapshot_signature_url(req.epoch.0, req.chunk_index)
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
        let base = resolve(self.scheme, &self.peers, node)?;
        let url = format!(
            "{base}{}",
            tape_protocol::api::snapshot_commitments_url(req.epoch.0)
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
        let commitments: Vec<tape_crypto::Hash> =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetSnapshotRes { commitments })
    }

    async fn get_health(
        &self,
        node: NodeId,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, ApiError> {
        let base = resolve(self.scheme, &self.peers, node)?;
        let url = format!("{base}{}", tape_protocol::api::HEALTH_PATH);

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
        let base = resolve(self.scheme, &self.peers, node)?;
        let url = format!("{base}{}", tape_protocol::api::STATS_PATH);

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
