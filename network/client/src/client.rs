//! Storage node client implementation.

#[cfg(feature = "metrics")]
use std::sync::Arc;
#[cfg(feature = "metrics")]
use std::time::Instant;
use reqwest::Client;
use url::Url;

use tape_node_api::{InconsistencyRequest, InconsistencyResponse, SlicePayload, SignResponse, CONTENT_TYPE_WINCODE};

use crate::error::NodeError;

#[cfg(feature = "metrics")]
use crate::metrics::NodeClientMetrics;

/// Client for communicating with a single storage node.
///
/// This struct is cheap to clone - the inner reqwest::Client uses Arc
/// internally, so clones share the same connection pool.
#[derive(Clone)]
pub struct NodeClient {
    /// HTTP client.
    pub(crate) inner: Client,
    /// Base URL for this node.
    pub(crate) base_url: Url,
    /// Optional metrics collector.
    #[cfg(feature = "metrics")]
    pub(crate) metrics: Option<Arc<NodeClientMetrics>>,
}

impl NodeClient {
    /// Get the base URL for this client.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Store a slice on this node.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slice_index` - The slice index (0-1023)
    /// * `payload` - The slice payload (data + merkle proof)
    pub async fn put_slice(
        &self,
        track_id: &str,
        slice_index: u16,
        payload: &SlicePayload,
    ) -> Result<(), NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let body = payload.to_bytes();

        #[cfg(feature = "metrics")]
        let body_len = body.len();

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/slices/{}", track_id, slice_index))?;

        let response = self.inner
            .put(url)
            .header("Content-Type", CONTENT_TYPE_WINCODE)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("put_slice", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("put_slice", "success", start.elapsed().as_secs_f64());
            metrics.record_bytes_sent("put_slice", body_len as u64);
        }

        Ok(())
    }

    /// Retrieve a slice from this node.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slice_index` - The slice index (0-1023)
    pub async fn get_slice(
        &self,
        track_id: &str,
        slice_index: u16,
    ) -> Result<Vec<u8>, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/slices/{}", track_id, slice_index))?;

        let response = self.inner
            .get(url)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_slice", "not_found", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::NotFound);
        }

        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_slice", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let bytes = response.bytes().await?.to_vec();

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("get_slice", "success", start.elapsed().as_secs_f64());
            metrics.record_bytes_received("get_slice", bytes.len() as u64);
        }

        Ok(bytes)
    }

    /// Get track metadata from this node.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    pub async fn get_metadata(&self, track_id: &str) -> Result<Vec<u8>, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/metadata", track_id))?;

        let response = self.inner
            .get(url)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_metadata", "not_found", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::NotFound);
        }

        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_metadata", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let bytes = response.bytes().await?.to_vec();

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("get_metadata", "success", start.elapsed().as_secs_f64());
        }

        Ok(bytes)
    }

    /// Store track metadata on this node.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `metadata` - The metadata bytes
    pub async fn put_metadata(&self, track_id: &str, metadata: Vec<u8>) -> Result<(), NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/metadata", track_id))?;

        let response = self.inner
            .put(url)
            .body(metadata)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("put_metadata", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("put_metadata", "success", start.elapsed().as_secs_f64());
        }

        Ok(())
    }

    /// Check if the node is healthy.
    pub async fn health_check(&self) -> Result<bool, NodeError> {
        let url = self.base_url.join("/v1/health")?;

        let response = self.inner
            .get(url)
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    /// Get node info.
    pub async fn get_info(&self) -> Result<serde_json::Value, NodeError> {
        let url = self.base_url.join("/v1/info")?;

        let response = self.inner
            .get(url)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let info: serde_json::Value = response
            .json()
            .await
            .map_err(|e| NodeError::InvalidResponse(e.to_string()))?;

        Ok(info)
    }

    /// Send a spool sync request (node-to-node, wincode encoded).
    ///
    /// # Arguments
    /// * `request_bytes` - The wincode-encoded and signed sync request
    pub async fn sync_spool(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let url = self.base_url.join("/v1/migrate/sync_spool")?;

        let response = self.inner
            .post(url)
            .header("Content-Type", "application/x-wincode")
            .body(request_bytes)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("sync_spool", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let bytes = response.bytes().await?.to_vec();

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("sync_spool", "success", start.elapsed().as_secs_f64());
        }

        Ok(bytes)
    }

    /// Request repair sub-chunks from this node.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `request` - The repair request (wincode-serialized)
    pub async fn request_repair(
        &self,
        track_id: &str,
        request: &tape_node_api::RepairRequest,
    ) -> Result<Vec<u8>, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let body = wincode::serialize(request)
            .map_err(|e| NodeError::Serialization(e.to_string()))?;

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/repair", track_id))?;

        let response = self.inner
            .post(url)
            .header("Content-Type", CONTENT_TYPE_WINCODE)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("request_repair", "not_found", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::NotFound);
        }

        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("request_repair", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let bytes = response.bytes().await?.to_vec();

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("request_repair", "success", start.elapsed().as_secs_f64());
            metrics.record_bytes_received("request_repair", bytes.len() as u64);
        }

        Ok(bytes)
    }

    /// Request a BLS signature for track certification.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier (base58-encoded pubkey)
    ///
    /// # Returns
    /// * `Ok(SignResponse)` - The signature, node_id, and member_index
    /// * `Err(NodeError::NotFound)` - Node doesn't have data for this track
    /// * `Err(NodeError::Unauthorized)` - Node is not in the committee
    pub async fn get_signature(&self, track_id: &str) -> Result<SignResponse, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/sign", track_id))?;

        let response = self.inner
            .get(url)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_signature", "not_found", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::NotFound);
        }

        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_signature", "unauthorized", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), "Node is not in committee"));
        }

        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("get_signature", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let sign_response: SignResponse = response
            .json()
            .await
            .map_err(|e| NodeError::InvalidResponse(format!("Failed to parse SignResponse: {}", e)))?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("get_signature", "success", start.elapsed().as_secs_f64());
        }

        Ok(sign_response)
    }

    /// Request an inconsistency attestation from this node.
    ///
    /// Sends the computed root and receives a BLS signature if the node
    /// independently verifies the inconsistency.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier (base58-encoded pubkey)
    /// * `request` - The inconsistency request with computed root
    pub async fn post_inconsistency(
        &self,
        track_id: &str,
        request: &InconsistencyRequest,
    ) -> Result<InconsistencyResponse, NodeError> {
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        let body = wincode::serialize(request)
            .map_err(|e| NodeError::Serialization(e.to_string()))?;

        let url = self.base_url
            .join(&format!("/v1/tracks/{}/inconsistency", track_id))?;

        let response = self.inner
            .post(url)
            .header("Content-Type", CONTENT_TYPE_WINCODE)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("post_inconsistency", "not_found", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::NotFound);
        }

        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_request("post_inconsistency", "error", start.elapsed().as_secs_f64());
            }
            return Err(NodeError::server_error(status.as_u16(), message));
        }

        let resp: InconsistencyResponse = response
            .json()
            .await
            .map_err(|e| NodeError::InvalidResponse(format!("Failed to parse InconsistencyResponse: {}", e)))?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_request("post_inconsistency", "success", start.elapsed().as_secs_f64());
        }

        Ok(resp)
    }
}
