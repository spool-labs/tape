//! HTTP implementation of the `Peer` trait for production node-to-node communication.

use async_trait::async_trait;
use peer::{
    CertifyReq, CertifyRes, GetHealthReq, GetHealthRes, GetMetadataReq, GetMetadataRes,
    GetSliceReq, GetSliceRes, GetSnapshotReq, GetSnapshotRes, GetStatsReq, GetStatsRes,
    InvalidateReq, InvalidateRes, Peer, PeerDirectory, PeerError, PutSliceReq, PutSliceRes,
    PutSnapshotReq, PutSnapshotRes, RepairReq, RepairRes, SyncReq, SyncRes,
};
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_node_api::{
    BlsInconsistencyResponse, BlsSignResponse, InconsistencyRequest, RepairRequest,
    SyncSpoolRequest, SyncSpoolResponse, BINARY_CONTENT, CONTENT_TYPE_JSON,
};

pub struct HttpPeerClient {
    directory: PeerDirectory,
    http: reqwest::Client,
}

impl HttpPeerClient {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            directory: PeerDirectory::new(),
            http,
        }
    }
}

fn base_url(addr: NetworkAddress) -> Result<String, PeerError> {
    let sa = addr
        .to_socket_addr()
        .map_err(|e| PeerError::ConnectionFailed(e.to_string()))?;
    Ok(format!("http://{sa}"))
}

fn resolve(dir: &PeerDirectory, node: NodeId) -> Result<String, PeerError> {
    let addr = dir.resolve(node).ok_or(PeerError::NodeUnresolved(node))?;
    base_url(addr)
}

fn map_reqwest(e: reqwest::Error) -> PeerError {
    if e.is_timeout() {
        PeerError::Timeout
    } else if e.is_connect() {
        PeerError::ConnectionFailed(e.to_string())
    } else {
        PeerError::Other(e.to_string())
    }
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response, PeerError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    match status.as_u16() {
        404 => Err(PeerError::NotFound),
        403 => {
            let body = resp.text().await.unwrap_or_default();
            if body.contains("not responsible") {
                Err(PeerError::NotResponsible)
            } else if body.contains("not in committee") {
                Err(PeerError::NotInCommittee)
            } else {
                Err(PeerError::ServerError {
                    status: 403,
                    message: body,
                })
            }
        }
        s => {
            let body = resp.text().await.unwrap_or_default();
            Err(PeerError::ServerError {
                status: s,
                message: body,
            })
        }
    }
}

#[async_trait]
impl Peer for HttpPeerClient {
    fn directory(&self) -> &PeerDirectory {
        &self.directory
    }

    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::internal_slice_url(&track_id, req.spool));
        let body =
            wincode::serialize(&req.payload).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let resp = self
            .http
            .put(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        check_status(resp).await?;
        Ok(PutSliceRes)
    }

    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::slice_url(&track_id, req.spool));

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        Ok(GetSliceRes {
            data: bytes.to_vec(),
        })
    }

    async fn get_metadata(
        &self,
        node: NodeId,
        req: &GetMetadataReq,
    ) -> Result<GetMetadataRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::metadata_url(&track_id));

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        Ok(GetMetadataRes {
            data: bytes.to_vec(),
        })
    }

    async fn sync(&self, node: NodeId, req: &SyncReq) -> Result<SyncRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let url = format!("{base}{}", tape_node_api::SYNC_SPOOL_PATH);
        let wire_req = SyncSpoolRequest {
            spool_index: req.spool_index,
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let resp = self
            .http
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        let wire_res: SyncSpoolResponse =
            wincode::deserialize(&bytes).map_err(|e| PeerError::Serialization(e.to_string()))?;
        Ok(SyncRes {
            entries: wire_res.entries,
            next_cursor: wire_res.next_cursor,
        })
    }

    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::repair_url(&track_id));
        let wire_req = RepairRequest {
            helper_spool: req.helper_spool,
            stripes: req.stripes.clone(),
        };
        let body =
            wincode::serialize(&wire_req).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let resp = self
            .http
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        Ok(RepairRes {
            data: bytes.to_vec(),
        })
    }

    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::sign_url(&track_id));

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        let wire: BlsSignResponse =
            wincode::deserialize(&bytes).map_err(|e| PeerError::Serialization(e.to_string()))?;
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
    ) -> Result<InvalidateRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", tape_node_api::inconsistency_url(&track_id));
        let wire_req = InconsistencyRequest {
            proof: req.proof.clone(),
        };
        let body =
            wincode::serialize(&wire_req).map_err(|e| PeerError::Serialization(e.to_string()))?;

        let resp = self
            .http
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        let wire: BlsInconsistencyResponse =
            wincode::deserialize(&bytes).map_err(|e| PeerError::Serialization(e.to_string()))?;
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
    ) -> Result<PutSnapshotRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let url = format!(
            "{base}{}",
            tape_node_api::snapshot_signature_url(req.epoch.0, req.chunk_index)
        );
        let body = wincode::serialize(&req.submission)
            .map_err(|e| PeerError::Serialization(e.to_string()))?;

        let resp = self
            .http
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        check_status(resp).await?;
        Ok(PutSnapshotRes)
    }

    async fn get_snapshot(
        &self,
        node: NodeId,
        req: &GetSnapshotReq,
    ) -> Result<GetSnapshotRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let url = format!(
            "{base}{}",
            tape_node_api::snapshot_commitments_url(req.epoch.0)
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        let commitments: Vec<tape_crypto::Hash> =
            wincode::deserialize(&bytes).map_err(|e| PeerError::Serialization(e.to_string()))?;
        Ok(GetSnapshotRes { commitments })
    }

    async fn get_health(
        &self,
        node: NodeId,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let url = format!("{base}{}", tape_node_api::HEALTH_PATH);

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        Ok(GetHealthRes {
            ok: resp.status().is_success(),
        })
    }

    async fn get_stats(
        &self,
        node: NodeId,
        _req: &GetStatsReq,
    ) -> Result<GetStatsRes, PeerError> {
        let base = resolve(&self.directory, node)?;
        let url = format!("{base}{}", tape_node_api::STATS_PATH);

        let resp = self
            .http
            .get(&url)
            .header("accept", CONTENT_TYPE_JSON)
            .send()
            .await
            .map_err(map_reqwest)?;

        let resp = check_status(resp).await?;
        let stats = resp
            .json()
            .await
            .map_err(|e| PeerError::Serialization(format!("json: {e}")))?;
        Ok(GetStatsRes { stats })
    }
}
