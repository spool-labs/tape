//! Gateway-backed implementation of the storage-node `Api` trait.
//!
//! Read calls are sent to a single gateway base URL. The `node` argument is
//! accepted for compatibility with existing SDK read paths, but the gateway
//! performs the actual storage-node lookup and fetch.

use std::time::Duration;

use async_trait::async_trait;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::Address;
use tape_protocol::Api;
use tape_protocol::api::*;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct GatewayApi {
    client: reqwest::Client,
    base_url: String,
}

impl GatewayApi {
    pub fn new(base_url: impl Into<String>) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|error| ApiError::Other(format!("gateway client build: {error}")))?;
        Self::with_client(base_url, client)
    }

    pub fn with_client(
        base_url: impl Into<String>,
        client: reqwest::Client,
    ) -> Result<Self, ApiError> {
        let base_url = normalize_base_url(base_url.into())?;
        Ok(Self { client, base_url })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Fetch exact decoded bytes for one track from the gateway public API.
    pub async fn get_track_bytes(&self, track: Address) -> Result<Vec<u8>, ApiError> {
        self.get_bytes(gateway_track_url(&track.to_string())).await
    }

    /// Fetch logical object bytes for a representing track from the gateway
    /// public API. If the track is an SDK chunk manifest, the gateway follows it.
    pub async fn get_object_bytes(&self, track: Address) -> Result<Vec<u8>, ApiError> {
        self.get_bytes(gateway_object_url(&track.to_string())).await
    }

    /// Fetch a byte window of the logical object for a representing track:
    /// `len` bytes starting at `start`. A window whose end runs past the
    /// object is clamped, so the result can be shorter than `len`; a `start`
    /// at or past the object end surfaces the gateway's `416` as a
    /// `ServerError` with status 416.
    pub async fn get_object_bytes_range(
        &self,
        track: Address,
        start: u64,
        len: u64,
    ) -> Result<Vec<u8>, ApiError> {
        if len == 0 {
            return Err(ApiError::Other("empty byte range".to_string()));
        }
        let end_inclusive = start.saturating_add(len - 1);

        let response = self
            .client
            .get(self.url(gateway_object_url(&track.to_string())))
            .header("range", format!("bytes={start}-{end_inclusive}"))
            .send()
            .await
            .map_err(map_reqwest)?;
        let response = check_status(response).await?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(ApiError::Other(format!(
                "expected 206 partial content, got {}",
                response.status()
            )));
        }
        let bytes = response.bytes().await.map_err(map_reqwest)?;
        Ok(bytes.to_vec())
    }

    fn url(&self, path: String) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn get_bytes(&self, path: String) -> Result<Vec<u8>, ApiError> {
        let response = self
            .client
            .get(self.url(path))
            .send()
            .await
            .map_err(map_reqwest)?;
        let response = check_status(response).await?;
        let bytes = response.bytes().await.map_err(map_reqwest)?;
        Ok(bytes.to_vec())
    }

    async fn post_wincode<Req: wincode::SchemaWrite<Src = Req>>(
        &self,
        path: String,
        request: &Req,
    ) -> Result<Vec<u8>, ApiError> {
        let body =
            wincode::serialize(request).map_err(|e| ApiError::Serialization(e.to_string()))?;
        let response = self
            .client
            .post(self.url(path))
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;
        let response = check_status(response).await?;
        let bytes = response.bytes().await.map_err(map_reqwest)?;
        Ok(bytes.to_vec())
    }
}

#[async_trait]
impl Api for GatewayApi {
    async fn put_slice(&self, _node: Address, _req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        Err(unsupported("put_slice"))
    }

    async fn get_slice(&self, _node: Address, req: &GetSliceReq) -> Result<GetSliceRes, ApiError> {
        let data = self
            .get_bytes(slice_url(&req.track.to_string(), req.spool))
            .await?;
        Ok(GetSliceRes { data })
    }

    async fn get_track(&self, _node: Address, req: &GetTrackReq) -> Result<GetTrackRes, ApiError> {
        let bytes = self.get_bytes(track_url(&req.track.to_string())).await?;
        let wire: TrackResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn get_track_by_number(
        &self,
        _node: Address,
        req: &GetTrackByNumberReq,
    ) -> Result<GetTrackByNumberRes, ApiError> {
        let bytes = self
            .get_bytes(tape_track_url(&req.tape.to_string(), req.track_number))
            .await?;
        let wire: TrackResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackByNumberRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn find_track(
        &self,
        _node: Address,
        req: &FindTrackReq,
    ) -> Result<FindTrackRes, ApiError> {
        let wire_req = FindTrackRequest {
            key: req.key,
            version: req.version.clone(),
        };
        let bytes = self
            .post_wincode(find_track_url(&req.tape.to_string()), &wire_req)
            .await?;
        let wire: TrackResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(FindTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn list_tracks_by_tape(
        &self,
        _node: Address,
        req: &ListTracksByTapeReq,
    ) -> Result<ListTracksByTapeRes, ApiError> {
        let wire_req = ListTracksByTapeRequest {
            cursor: req.cursor,
            limit: req.limit,
        };
        let bytes = self
            .post_wincode(list_tracks_by_tape_url(&req.tape.to_string()), &wire_req)
            .await?;
        let wire: ListTracksByTapeResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(ListTracksByTapeRes {
            tracks: wire
                .tracks
                .into_iter()
                .map(CompressedTrack::unpack)
                .collect(),
            next_cursor: wire.next_cursor,
        })
    }

    async fn list_objects(
        &self,
        _node: Address,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsRes, ApiError> {
        let wire_req = ListObjectsRequest {
            prefix: req.prefix.clone(),
            delimiter: req.delimiter.clone(),
            cursor: req.cursor.clone(),
            limit: req.limit,
        };
        let bytes = self
            .post_wincode(list_objects_url(&req.bucket.to_string()), &wire_req)
            .await?;
        let wire: ListObjectsResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(ListObjectsRes {
            objects: wire.objects,
            common_prefixes: wire.common_prefixes,
            next_cursor: wire.next_cursor,
            is_truncated: wire.is_truncated,
        })
    }

    async fn get_track_data(
        &self,
        _node: Address,
        req: &GetTrackDataReq,
    ) -> Result<GetTrackDataRes, ApiError> {
        let bytes = self
            .get_bytes(track_data_url(&req.track.to_string()))
            .await?;
        let wire: TrackDataResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackDataRes { data: wire.data })
    }

    async fn get_track_proof(
        &self,
        _node: Address,
        req: &GetTrackProofReq,
    ) -> Result<GetTrackProofRes, ApiError> {
        let bytes = self
            .get_bytes(track_proof_url(&req.track.to_string()))
            .await?;
        let wire: TrackProofResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackProofRes {
            proof: CompressedTrackProof::unpack(wire.proof),
        })
    }

    async fn sync_slices(
        &self,
        _node: Address,
        _req: &SyncSlicesReq,
    ) -> Result<SyncSlicesRes, ApiError> {
        Err(unsupported("sync_slices"))
    }

    async fn sync_tracks(
        &self,
        _node: Address,
        _req: &SyncTracksReq,
    ) -> Result<SyncTracksRes, ApiError> {
        Err(unsupported("sync_tracks"))
    }

    async fn repair(&self, _node: Address, _req: &RepairReq) -> Result<RepairRes, ApiError> {
        Err(unsupported("repair"))
    }

    async fn certify(&self, _node: Address, _req: &CertifyReq) -> Result<CertifyRes, ApiError> {
        Err(unsupported("certify"))
    }

    async fn invalidate(
        &self,
        _node: Address,
        _req: &InvalidateReq,
    ) -> Result<InvalidateRes, ApiError> {
        Err(unsupported("invalidate"))
    }

    async fn vote(&self, _node: Address, _req: &VoteReq) -> Result<VoteRes, ApiError> {
        Err(unsupported("vote"))
    }

    async fn get_health(
        &self,
        _node: Address,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, ApiError> {
        let response = self
            .client
            .get(self.url(NODE_HEALTH_PATH.to_string()))
            .send()
            .await
            .map_err(map_reqwest)?;
        Ok(GetHealthRes {
            ok: response.status().is_success(),
        })
    }

    async fn get_stats(&self, _node: Address, _req: &GetStatsReq) -> Result<GetStatsRes, ApiError> {
        let response = self
            .client
            .get(self.url(NODE_STATS_PATH.to_string()))
            .header("accept", JSON_CONTENT)
            .send()
            .await
            .map_err(map_reqwest)?;
        let response = check_status(response).await?;
        let stats = response
            .json()
            .await
            .map_err(|e| ApiError::Serialization(format!("json: {e}")))?;
        Ok(GetStatsRes { stats })
    }

    async fn get_observe_board(&self, _node: Address) -> Result<Vec<u8>, ApiError> {
        self.get_bytes(OBSERVE_BOARD_PATH.to_string()).await
    }
}

fn normalize_base_url(mut base_url: String) -> Result<String, ApiError> {
    while base_url.ends_with('/') {
        base_url.pop();
    }
    if base_url.is_empty() {
        return Err(ApiError::Other("gateway base URL is empty".into()));
    }
    Ok(base_url)
}

async fn check_status(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let code = status.as_u16();
    let message = response.text().await.unwrap_or_default();
    match code {
        403 if message.contains("not responsible") => Err(ApiError::NotResponsible),
        403 if message.contains("blacklisted object") => Err(ApiError::BlacklistedObject),
        403 if message.contains("not in committee") => Err(ApiError::NotInCommittee),
        404 => Err(ApiError::NotFound),
        _ => Err(ApiError::ServerError {
            status: code,
            message,
        }),
    }
}

fn map_reqwest(error: reqwest::Error) -> ApiError {
    if error.is_timeout() {
        ApiError::Timeout
    } else if error.is_connect() {
        ApiError::ConnectionFailed(error.to_string())
    } else {
        ApiError::Other(error.to_string())
    }
}

fn unsupported(op: &str) -> ApiError {
    ApiError::Other(format!("gateway api does not support {op}"))
}

fn gateway_track_url(track_id: &str) -> String {
    format!("/track/{track_id}")
}

fn gateway_object_url(track_id: &str) -> String {
    format!("/object/{track_id}")
}

#[cfg(test)]
mod tests {
    use super::{GatewayApi, gateway_object_url, gateway_track_url};

    #[test]
    fn normalizes_base_url() {
        let api = GatewayApi::new("http://127.0.0.1:3000///").unwrap();
        assert_eq!(api.base_url(), "http://127.0.0.1:3000");
    }

    #[test]
    fn decoded_byte_urls_are_unversioned() {
        assert_eq!(gateway_track_url("abc"), "/track/abc");
        assert_eq!(gateway_object_url("abc"), "/object/abc");
    }
}
