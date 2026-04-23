//! Upstream JSON-RPC client. Handles 429 cool-off (with `Retry-After`
//! priority) and an exponential-backoff path for other transient errors.

use std::time::Duration;

use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Caps the exponential-backoff retry loop so misbehaving upstream
/// doesn't deadlock inbound requests forever. A 429 cool-off is
/// separate and not counted here.
const MAX_TRANSIENT_RETRIES: u32 = 5;
const BASE_DELAY_MS: u64 = 500;
const MAX_DELAY_S: u64 = 5;

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("upstream returned non-success status {status}: {body}")]
    Status { status: u16, body: String },

    #[error("malformed upstream response: {0}")]
    BadResponse(String),

    #[error("gave up after {attempts} transient retries")]
    GaveUp { attempts: u32 },
}

/// Canonical JSON-RPC response envelope as returned by Solana RPC.
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

pub struct Upstream {
    client: Client,
    url: String,
    min_429_delay: Duration,
}

impl Upstream {
    pub fn new(url: String, min_429_delay: Duration) -> Self {
        let client = Client::builder()
            // Don't block inbound indefinitely on a stuck upstream.
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client build");
        Self {
            client,
            url,
            min_429_delay,
        }
    }

    /// Forward a JSON-RPC request body verbatim, returning the parsed
    /// envelope. Handles 429 cool-off and general transient retries
    /// internally.
    pub async fn forward(&self, body: &Value) -> Result<RpcEnvelope, UpstreamError> {
        let mut transient_attempts = 0u32;

        loop {
            let resp = self
                .client
                .post(&self.url)
                .json(body)
                .send()
                .await;

            let resp = match resp {
                Ok(r) => r,
                Err(e) if e.is_connect() || e.is_timeout() => {
                    warn!(error = %e, "upstream transport error");
                    if transient_attempts >= MAX_TRANSIENT_RETRIES {
                        return Err(UpstreamError::GaveUp {
                            attempts: transient_attempts,
                        });
                    }
                    sleep(backoff_delay(transient_attempts)).await;
                    transient_attempts += 1;
                    continue;
                }
                Err(e) => return Err(UpstreamError::Http(e)),
            };

            let status = resp.status();

            if status == StatusCode::TOO_MANY_REQUESTS {
                let wait = parse_retry_after(&resp).unwrap_or(self.min_429_delay);
                warn!(
                    wait_ms = wait.as_millis() as u64,
                    "upstream 429; cooling off"
                );
                sleep(wait).await;
                // 429s do NOT increment the transient_attempts counter —
                // they use a separate, independent floor (per the plan).
                continue;
            }

            if status.is_server_error() {
                warn!(status = status.as_u16(), "upstream 5xx; backing off");
                if transient_attempts >= MAX_TRANSIENT_RETRIES {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(UpstreamError::Status {
                        status: status.as_u16(),
                        body,
                    });
                }
                sleep(backoff_delay(transient_attempts)).await;
                transient_attempts += 1;
                continue;
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(UpstreamError::Status {
                    status: status.as_u16(),
                    body,
                });
            }

            // Success — parse envelope.
            let envelope: RpcEnvelope = resp
                .json()
                .await
                .map_err(|e| UpstreamError::BadResponse(e.to_string()))?;
            debug!(
                has_result = envelope.result.is_some(),
                has_error = envelope.error.is_some(),
                "upstream ok"
            );
            return Ok(envelope);
        }
    }
}

fn backoff_delay(attempt: u32) -> Duration {
    let base = BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(6));
    let capped = base.min(MAX_DELAY_S * 1000);
    Duration::from_millis(capped)
}

fn parse_retry_after(resp: &reqwest::Response) -> Option<Duration> {
    let val = resp.headers().get(RETRY_AFTER)?.to_str().ok()?;
    // Per RFC 7231 this is either seconds or an HTTP-date; we only
    // support the seconds form here (providers that ship dates are rare).
    let secs: u64 = val.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let d0 = backoff_delay(0);
        let d1 = backoff_delay(1);
        let d10 = backoff_delay(10);
        assert!(d1 > d0);
        assert!(d10 <= Duration::from_secs(MAX_DELAY_S));
    }
}
