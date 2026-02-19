//! HTTP client for communicating with a single storage node.

use std::sync::Arc;
use std::time::Instant;

use reqwest::Client;
use tape_store::types::Pubkey;
use url::Url;

use tape_node_api::{
    InconsistencyRequest, BlsInconsistencyResponse, NodeStats, RepairRequest,
    SnapshotSignatureSubmission, BlsSignResponse, SignedMessage, SlicePayload, CONTENT_TYPE_JSON,
    BINARY_CONTENT,
};

use crate::error::NodeError;
use crate::metrics::NodeClientMetrics;

/// Client for communicating with a single storage node.
///
/// This struct is cheap to clone — the inner `reqwest::Client` uses `Arc`
/// internally, so clones share the same connection pool.
#[derive(Clone)]
pub struct NodeClient {
    pub(crate) inner: Client,
    pub(crate) base_url: Url,
    pub(crate) metrics: Option<Arc<NodeClientMetrics>>,
}

impl NodeClient {
    /// Get the base URL of this client.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// PUT a slice via the public (authority-signed) route.
    pub async fn put_slice(
        &self,
        track: Pubkey,
        slice_index: u16,
        payload: &SignedMessage,
    ) -> Result<(), NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::slice_url(&track_id, slice_index))?;
        let body =
            wincode::serialize(payload).map_err(|e| NodeError::Serialization(e.to_string()))?;
        let len = body.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .put(url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("put_slice", &resp, start, len, 0);
        self.check_status(resp).await?;
        Ok(())
    }

    /// PUT a slice via the internal (peer-authenticated) route.
    pub async fn put_slice_internal(
        &self,
        track: Pubkey,
        slice_index: u16,
        payload: &SlicePayload,
    ) -> Result<(), NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::internal_slice_url(&track_id, slice_index))?;
        let body =
            wincode::serialize(payload).map_err(|e| NodeError::Serialization(e.to_string()))?;
        let len = body.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .put(url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("put_slice_internal", &resp, start, len, 0);
        self.check_status(resp).await?;
        Ok(())
    }

    /// GET a slice's raw data.
    pub async fn get_slice(
        &self,
        track: Pubkey,
        slice_index: u16,
    ) -> Result<Vec<u8>, NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::slice_url(&track_id, slice_index))?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_slice", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("get_slice", bytes.len() as u64);
        Ok(bytes.to_vec())
    }

    /// GET track metadata.
    pub async fn get_metadata(&self, track: Pubkey) -> Result<Vec<u8>, NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::metadata_url(&track_id))?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_metadata", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("get_metadata", bytes.len() as u64);
        Ok(bytes.to_vec())
    }

    /// PUT track metadata via the public route.
    pub async fn put_metadata(
        &self,
        track: Pubkey,
        metadata: Vec<u8>,
    ) -> Result<(), NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::metadata_url(&track_id))?;
        let len = metadata.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .put(url)
            .header("content-type", BINARY_CONTENT)
            .body(metadata)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("put_metadata", &resp, start, len, 0);
        self.check_status(resp).await?;
        Ok(())
    }

    /// PUT track metadata via the internal route.
    pub async fn put_metadata_internal(
        &self,
        track: Pubkey,
        metadata: Vec<u8>,
    ) -> Result<(), NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::internal_metadata_url(&track_id))?;
        let len = metadata.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .put(url)
            .header("content-type", BINARY_CONTENT)
            .body(metadata)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("put_metadata_internal", &resp, start, len, 0);
        self.check_status(resp).await?;
        Ok(())
    }

    /// GET /v1/health — returns true if the node responds 200.
    pub async fn health_check(&self) -> Result<bool, NodeError> {
        let url = self.url(tape_node_api::HEALTH_PATH)?;
        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        Ok(resp.status().is_success())
    }

    /// GET /v1/stats — node statistics (JSON).
    pub async fn get_stats(&self) -> Result<NodeStats, NodeError> {
        let url = self.url(tape_node_api::STATS_PATH)?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .header("accept", CONTENT_TYPE_JSON)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_stats", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        resp.json::<NodeStats>()
            .await
            .map_err(|e| NodeError::InvalidResponse(format!("json: {e}")))
    }

    /// GET /v1/info — node info (JSON).
    pub async fn get_info(&self) -> Result<serde_json::Value, NodeError> {
        let url = self.url(tape_node_api::INFO_PATH)?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_info", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| NodeError::InvalidResponse(format!("json: {e}")))
    }

    /// POST spool sync — exchange raw wincode bytes.
    pub async fn sync_spool(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>, NodeError> {
        let url = self.url(tape_node_api::SYNC_SPOOL_PATH)?;
        let len = request_bytes.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .post(url)
            .header("content-type", BINARY_CONTENT)
            .body(request_bytes)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("sync_spool", &resp, start, len, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("sync_spool", bytes.len() as u64);
        Ok(bytes.to_vec())
    }

    /// POST repair request.
    pub async fn request_repair(
        &self,
        track: Pubkey,
        request: &RepairRequest,
    ) -> Result<Vec<u8>, NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::repair_url(&track_id))?;
        let body =
            wincode::serialize(request).map_err(|e| NodeError::Serialization(e.to_string()))?;
        let len = body.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .post(url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("request_repair", &resp, start, len, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("request_repair", bytes.len() as u64);
        Ok(bytes.to_vec())
    }

    /// GET track BLS signature.
    pub async fn get_signature(&self, track: Pubkey) -> Result<BlsSignResponse, NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::sign_url(&track_id))?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_signature", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("get_signature", bytes.len() as u64);
        wincode::deserialize(&bytes).map_err(|e| NodeError::InvalidResponse(e.to_string()))
    }

    /// POST snapshot partial signature.
    pub async fn post_snapshot_signature(
        &self,
        target_epoch: u64,
        chunk_index: u64,
        request: &SnapshotSignatureSubmission,
    ) -> Result<(), NodeError> {
        let url = self.url(&tape_node_api::snapshot_signature_url(
            target_epoch,
            chunk_index,
        ))?;
        let body =
            wincode::serialize(request).map_err(|e| NodeError::Serialization(e.to_string()))?;
        let len = body.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .post(url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("post_snapshot_signature", &resp, start, len, 0);
        self.check_status(resp).await?;
        Ok(())
    }

    /// GET /v1/snapshots/:epoch/commitments — fetch snapshot chunk commitments.
    pub async fn get_snapshot_commitments(
        &self,
        epoch: u64,
    ) -> Result<Vec<tape_crypto::Hash>, NodeError> {
        let url = self.url(&tape_node_api::snapshot_commitments_url(epoch))?;
        let start = Instant::now();

        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("get_snapshot_commitments", &resp, start, 0, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("get_snapshot_commitments", bytes.len() as u64);
        wincode::deserialize(&bytes).map_err(|e| NodeError::InvalidResponse(e.to_string()))
    }

    /// POST inconsistency attestation.
    pub async fn post_inconsistency(
        &self,
        track: Pubkey,
        request: &InconsistencyRequest,
    ) -> Result<BlsInconsistencyResponse, NodeError> {
        let track_id = track.to_string();
        let url = self.url(&tape_node_api::inconsistency_url(&track_id))?;
        let body =
            wincode::serialize(request).map_err(|e| NodeError::Serialization(e.to_string()))?;
        let len = body.len() as u64;
        let start = Instant::now();

        let resp = self
            .inner
            .post(url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;

        self.record("post_inconsistency", &resp, start, len, 0);
        let resp = self.check_status_return(resp).await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error(e))?;
        self.record_rx("post_inconsistency", bytes.len() as u64);
        wincode::deserialize(&bytes).map_err(|e| NodeError::InvalidResponse(e.to_string()))
    }

    // --- helpers ---

    fn url(&self, path: &str) -> Result<Url, NodeError> {
        self.base_url.join(path).map_err(NodeError::Url)
    }

    fn map_reqwest_error(&self, e: reqwest::Error) -> NodeError {
        if e.is_timeout() {
            NodeError::Timeout
        } else if e.is_connect() {
            NodeError::Connection(e.to_string())
        } else {
            NodeError::Request(e)
        }
    }

    async fn check_status(&self, resp: reqwest::Response) -> Result<(), NodeError> {
        self.check_status_return(resp).await?;
        Ok(())
    }

    async fn check_status_return(
        &self,
        resp: reqwest::Response,
    ) -> Result<reqwest::Response, NodeError> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        match status.as_u16() {
            404 => Err(NodeError::NotFound),
            403 => {
                let body = resp.text().await.unwrap_or_default();
                if body.contains("not responsible") {
                    Err(NodeError::NotResponsible)
                } else if body.contains("not in committee") {
                    Err(NodeError::NotInCommittee)
                } else {
                    Err(NodeError::server_error(403, body))
                }
            }
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(NodeError::server_error(status.as_u16(), body))
            }
        }
    }

    fn record(
        &self,
        operation: &str,
        resp: &reqwest::Response,
        start: Instant,
        bytes_sent: u64,
        _bytes_recv: u64,
    ) {
        if let Some(m) = &self.metrics {
            let duration = start.elapsed().as_secs_f64();
            let status = resp.status().as_u16().to_string();
            m.record_request(operation, &status, duration);
            if bytes_sent > 0 {
                m.record_bytes_sent(operation, bytes_sent);
            }
        }
    }

    fn record_rx(&self, operation: &str, bytes: u64) {
        if let Some(m) = &self.metrics {
            m.record_bytes_received(operation, bytes);
        }
    }
}
