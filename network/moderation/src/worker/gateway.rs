//! Content and track records read through one gateway

use reqwest::header::{HeaderName, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use tape_core::track::types::CompressedTrack;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_protocol::api::{
    list_tracks_by_tape_url, track_url, ListTracksByTapeRequest, ListTracksByTapeResponse,
    TrackResponse, BINARY_CONTENT,
};

use crate::config::settings::GatewayConfig;
use crate::worker::body::read_bounded;
use crate::worker::error::WorkerError;

/// Room for one wincode track record
const TRACK_RESPONSE_MAX_BYTES: usize = 4 * 1_024;

/// Room per listed track, well over its packed size
const LISTED_TRACK_MAX_BYTES: usize = 512;

const LISTING_OVERHEAD_BYTES: usize = 1_024;

/// Reads from the gateway checks go through
pub struct GatewayClient {
    client: Client,
    base_url: String,
}

/// A read the gateway answered
#[derive(Clone, Debug)]
pub enum GatewayRead {
    /// 410: the gateway already refuses the track
    Gone,
    Content(ContentRead),
}

/// A prefix of content and what the gateway said about the whole
#[derive(Clone, Debug)]
pub struct ContentRead {
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub total_bytes: Option<u64>,
}

impl GatewayClient {
    pub fn try_new(config: &GatewayConfig) -> Result<Self, WorkerError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| WorkerError::Client(error.to_string()))?;
        Ok(Self {
            client,
            base_url: config.url.trim_end_matches('/').to_string(),
        })
    }

    /// Up to `limit` bytes of the object a track represents, streams followed
    pub async fn read_object(&self, track: Address, limit: u64) -> Result<GatewayRead, WorkerError> {
        self.read_prefix(format!("/object/{track}"), limit).await
    }

    /// Up to `limit` bytes the track itself holds, a stream's raw manifest
    pub async fn read_track(&self, track: Address, limit: u64) -> Result<GatewayRead, WorkerError> {
        self.read_prefix(format!("/track/{track}"), limit).await
    }

    pub async fn track_record(&self, track: Address) -> Result<CompressedTrack, WorkerError> {
        let path = track_url(&track.to_string());
        let response = self.send(&path, self.client.get(self.url(&path))).await?;
        let bytes = self.success_body(&path, response, TRACK_RESPONSE_MAX_BYTES).await?;
        let record: TrackResponse = wincode::deserialize(&bytes).map_err(|error| decode(&path, error.to_string()))?;
        Ok(CompressedTrack::unpack(record.track))
    }

    /// Up to `limit` of a tape's tracks in track order
    pub async fn list_tracks(&self, tape: Address, limit: u32) -> Result<Vec<CompressedTrack>, WorkerError> {
        let path = list_tracks_by_tape_url(&tape.to_string());
        let mut tracks = Vec::new();
        let mut cursor = None;
        while tracks.len() < limit as usize {
            let remaining = limit.saturating_sub(u32::try_from(tracks.len()).unwrap_or(u32::MAX));
            let page = self.list_page(&path, cursor, remaining).await?;
            let is_last = page.next_cursor.is_none() || page.tracks.is_empty();
            for packed in page.tracks {
                tracks.push(CompressedTrack::unpack(packed));
            }
            cursor = page.next_cursor;
            if is_last {
                break;
            }
        }
        tracks.truncate(limit as usize);
        Ok(tracks)
    }

    async fn list_page(
        &self,
        path: &str,
        cursor: Option<TrackNumber>,
        limit: u32,
    ) -> Result<ListTracksByTapeResponse, WorkerError> {
        let request = ListTracksByTapeRequest { cursor, limit };
        let body = wincode::serialize(&request).map_err(|error| decode(path, error.to_string()))?;
        let builder = self
            .client
            .post(self.url(path))
            .header(CONTENT_TYPE, BINARY_CONTENT)
            .body(body);
        let response = self.send(path, builder).await?;
        let max_bytes = (limit as usize)
            .saturating_mul(LISTED_TRACK_MAX_BYTES)
            .saturating_add(LISTING_OVERHEAD_BYTES);
        let bytes = self.success_body(path, response, max_bytes).await?;
        wincode::deserialize(&bytes).map_err(|error| decode(path, error.to_string()))
    }

    async fn read_prefix(&self, path: String, limit: u64) -> Result<GatewayRead, WorkerError> {
        let range = format!("bytes=0-{}", limit.saturating_sub(1));
        let builder = self.client.get(self.url(&path)).header(RANGE, range);
        let response = self.send(&path, builder).await?;
        let content_type = header_text(&response, CONTENT_TYPE);

        match response.status() {
            StatusCode::GONE => Ok(GatewayRead::Gone),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(GatewayRead::Content(ContentRead {
                bytes: Vec::new(),
                content_type,
                total_bytes: content_range_total(&response),
            })),
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => read_content(path, response, content_type, limit).await,
            status => Err(WorkerError::GatewayStatus {
                path,
                status: status.as_u16(),
            }),
        }
    }

    async fn success_body(&self, path: &str, response: Response, limit: usize) -> Result<Vec<u8>, WorkerError> {
        let status = response.status();
        if !status.is_success() {
            return Err(WorkerError::GatewayStatus {
                path: path.to_string(),
                status: status.as_u16(),
            });
        }
        let body = read_bounded(response, limit)
            .await
            .map_err(|source| request_error(path, source))?;
        match body.is_truncated {
            true => Err(decode(path, format!("response is larger than {limit} bytes"))),
            false => Ok(body.bytes),
        }
    }

    async fn send(&self, path: &str, builder: RequestBuilder) -> Result<Response, WorkerError> {
        builder
            .send()
            .await
            .map_err(|source| request_error(path, source))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl ContentRead {
    /// Whether the bytes are the whole content
    pub fn is_complete(&self) -> bool {
        self.total_bytes == Some(self.bytes.len() as u64)
    }
}

async fn read_content(
    path: String,
    response: Response,
    content_type: Option<String>,
    limit: u64,
) -> Result<GatewayRead, WorkerError> {
    let is_partial = response.status() == StatusCode::PARTIAL_CONTENT;
    let range_total = content_range_total(&response);
    let declared_length = response.content_length();
    let body = read_bounded(response, usize::try_from(limit).unwrap_or(usize::MAX))
        .await
        .map_err(|source| WorkerError::GatewayRequest { path, source })?;

    let total_bytes = match (is_partial, body.is_truncated) {
        (true, _) => range_total,
        (false, false) => Some(body.bytes.len() as u64),
        (false, true) => declared_length,
    };
    Ok(GatewayRead::Content(ContentRead {
        bytes: body.bytes,
        content_type,
        total_bytes,
    }))
}

/// The total length from `Content-Range: bytes 0-99/1000` or `bytes */0`
fn content_range_total(response: &Response) -> Option<u64> {
    let value = header_text(response, CONTENT_RANGE)?;
    let (_, total) = value.rsplit_once('/')?;
    total.trim().parse().ok()
}

fn header_text(response: &Response, name: HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn request_error(path: &str, source: reqwest::Error) -> WorkerError {
    WorkerError::GatewayRequest {
        path: path.to_string(),
        source,
    }
}

fn decode(path: &str, reason: String) -> WorkerError {
    WorkerError::GatewayDecode {
        path: path.to_string(),
        reason,
    }
}
