//! Upstream JSON-RPC client. Every request waits out one shared 429
//! cool-off and holds one of a fixed number of in-flight permits; other
//! transient errors back off exponentially per request.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Caps the exponential-backoff retry loop so misbehaving upstream
/// doesn't deadlock inbound requests forever. Rate limiting is separate
/// and not counted here.
const MAX_TRANSIENT_RETRIES: u32 = 5;
const BASE_DELAY_MS: u64 = 500;
const MAX_DELAY_S: u64 = 5;

/// Rate limiting a request a node is waiting on sits out before answering
///
/// Under the node's 30 s request timeout, so the node hears back and fails
/// over instead of timing out.
pub const CALLER_RATE_LIMIT_WAIT: RateLimitWait = RateLimitWait::Bounded(Duration::from_secs(10));

/// How long a request waits out upstream rate limiting
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitWait {
    /// Background fills, which should wait as long as upstream asks
    Unbounded,
    /// Give up once waiting would pass this much in total
    Bounded(Duration),
}

impl RateLimitWait {
    /// Whether waiting this long in total is past what the request tolerates
    fn is_exceeded_by(self, total: Duration) -> bool {
        match self {
            RateLimitWait::Unbounded => false,
            RateLimitWait::Bounded(limit) => total > limit,
        }
    }
}

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("http error: {0}")]
    Http(String),

    #[error("upstream returned non-success status {status}: {body}")]
    Status { status: u16, body: String },

    #[error("malformed upstream response: {0}")]
    BadResponse(String),

    #[error("gave up after {attempts} transient retries: {cause}")]
    GaveUp { attempts: u32, cause: String },

    #[error("rate limited by upstream (429) after waiting {waited:?}")]
    RateLimited { waited: Duration },

    #[error("upstream client is closed")]
    Closed,
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

/// What one request attempt came to
enum Attempt {
    Done(Result<RpcEnvelope, UpstreamError>),
    /// Upstream answered 429 and wants this long a pause
    RateLimited(Duration),
    /// Worth retrying after a backoff; carries the cause
    Transient(String),
}

pub struct Upstream {
    client: Client,
    url: String,
    min_429_delay: Duration,

    /// Earliest instant any request may go upstream; each 429 pushes it later
    resume_at: Mutex<Instant>,
    /// Upstream requests allowed in flight at once
    in_flight: Semaphore,
}

impl Upstream {
    pub fn new(
        url: String,
        min_429_delay: Duration,
        headers: HashMap<String, String>,
        max_in_flight: usize,
    ) -> Self {
        let mut header_map = HeaderMap::new();
        for (name, value) in &headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .unwrap_or_else(|e| panic!("invalid upstream header name {name:?}: {e}"));
            let header_value = HeaderValue::from_str(value)
                .unwrap_or_else(|e| panic!("invalid upstream header value for {name:?}: {e}"));
            header_map.insert(header_name, header_value);
        }

        let client = Client::builder()
            // Don't block inbound indefinitely on a stuck upstream.
            .timeout(Duration::from_secs(30))
            // Force IPv4 outbound. Helius / Triton / etc DNS returns both
            // IPv4 and IPv6 (Cloudflare). reqwest's hyper-util connector
            // tries IPv6 first; on hosts without a global IPv6 route — like
            // a default DigitalOcean droplet — that IPv6 connect blocks the
            // full timeout window before falling back to IPv4, manifesting
            // as every other request appearing to hang. Binding the local
            // socket to 0.0.0.0 constrains the address family to IPv4 and
            // skips that detour.
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .default_headers(header_map)
            .build()
            .expect("reqwest client build");
        Self {
            client,
            url,
            min_429_delay,
            resume_at: Mutex::new(Instant::now()),
            // No permits would stall every request forever.
            in_flight: Semaphore::new(max_in_flight.max(1)),
        }
    }

    /// Forward a JSON-RPC request body verbatim, returning the parsed
    /// envelope
    ///
    /// Waits out the shared 429 cool-off, for as long as `wait` allows, and
    /// retries other transient errors with backoff.
    pub async fn forward(
        &self,
        body: &Value,
        wait: RateLimitWait,
    ) -> Result<RpcEnvelope, UpstreamError> {
        let mut transient_attempts = 0u32;
        let mut rate_limited_for = Duration::ZERO;

        loop {
            let cool_off = self.cool_off_remaining();
            if !cool_off.is_zero() {
                if wait.is_exceeded_by(rate_limited_for + cool_off) {
                    return Err(UpstreamError::RateLimited { waited: rate_limited_for });
                }
                sleep(cool_off).await;
                rate_limited_for += cool_off;
                continue;
            }

            let Ok(permit) = self.in_flight.acquire().await else {
                return Err(UpstreamError::Closed);
            };
            // A 429 may have landed while this request queued for a permit.
            if !self.cool_off_remaining().is_zero() {
                continue;
            }
            let attempt = self.attempt(body).await;
            drop(permit);

            match attempt {
                Attempt::Done(result) => return result,
                Attempt::RateLimited(delay) => self.cool_off_for(delay),
                Attempt::Transient(_) if transient_attempts < MAX_TRANSIENT_RETRIES => {
                    sleep(backoff_delay(transient_attempts)).await;
                    transient_attempts += 1;
                }
                Attempt::Transient(cause) => {
                    return Err(UpstreamError::GaveUp { attempts: transient_attempts, cause });
                }
            }
        }
    }

    /// Send the request once and sort the answer
    async fn attempt(&self, body: &Value) -> Attempt {
        let response = match self.client.post(&self.url).json(body).send().await {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_timeout() => {
                return transport_failure(error);
            }
            Err(error) => return Attempt::Done(Err(UpstreamError::Http(describe(error)))),
        };

        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            let delay = parse_retry_after(&response).unwrap_or(self.min_429_delay);
            return Attempt::RateLimited(delay);
        }
        if status.is_server_error() {
            warn!(status = status.as_u16(), "upstream 5xx; backing off");
            let text = response.text().await.unwrap_or_default();
            return Attempt::Transient(format!("status {}: {text}", status.as_u16()));
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            let error = UpstreamError::Status { status: status.as_u16(), body: text };
            return Attempt::Done(Err(error));
        }

        match response.json::<RpcEnvelope>().await {
            Ok(envelope) => {
                debug!(
                    has_result = envelope.result.is_some(),
                    has_error = envelope.error.is_some(),
                    "upstream ok"
                );
                Attempt::Done(Ok(envelope))
            }
            Err(error) => Attempt::Done(Err(UpstreamError::BadResponse(describe(error)))),
        }
    }

    /// Time left before any request may go upstream again
    fn cool_off_remaining(&self) -> Duration {
        let resume_at = *self.resume_at.lock().unwrap_or_else(PoisonError::into_inner);
        resume_at.saturating_duration_since(Instant::now())
    }

    /// Hold every request back for `delay`, unless an earlier 429 already holds them longer
    fn cool_off_for(&self, delay: Duration) {
        warn!(wait_ms = delay.as_millis() as u64, "upstream 429; cooling off");
        let until = Instant::now() + delay;
        let mut resume_at = self.resume_at.lock().unwrap_or_else(PoisonError::into_inner);
        *resume_at = (*resume_at).max(until);
    }
}

/// A connect or timeout failure, logged with its cause
fn transport_failure(error: reqwest::Error) -> Attempt {
    let is_timeout = error.is_timeout();
    let is_connect = error.is_connect();
    let cause = describe(error);
    warn!(is_timeout, is_connect, error = %cause, "upstream transport error");
    Attempt::Transient(cause)
}

/// The error and each source beneath it, without the url, whose query can carry an api key
fn describe(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut description = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(inner) = source {
        let text = inner.to_string();
        // hyper renders some sources inside their parent's text as well.
        if !description.contains(text.as_str()) {
            description.push_str(": ");
            description.push_str(&text);
        }
        source = inner.source();
    }
    description
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
    use std::sync::Arc;

    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;

    use crate::mock_upstream::{MockUpstream, Reply};

    use super::*;

    const COOL_OFF: Duration = Duration::from_millis(200);
    const POLL: Duration = Duration::from_millis(5);

    fn body() -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "method": "getSlot"})
    }

    // backoff grows, then caps
    #[test]
    fn backoff_growth() {
        let d0 = backoff_delay(0);
        let d1 = backoff_delay(1);
        let d10 = backoff_delay(10);
        assert!(d1 > d0);
        assert!(d10 <= Duration::from_secs(MAX_DELAY_S));
    }

    // a 429 holds back every request until the shared cool-off lapses
    #[tokio::test]
    async fn shared_cool_off() {
        let mock = MockUpstream::start(|index, _| {
            if index == 0 {
                Reply::status("429 Too Many Requests")
            } else {
                Reply::result(json!(1))
            }
        })
        .await;
        let upstream = Upstream::new(mock.url.clone(), COOL_OFF, HashMap::new(), 8);
        let request = body();

        let (first, second) = tokio::join!(
            upstream.forward(&request, RateLimitWait::Unbounded),
            async {
                while upstream.cool_off_remaining().is_zero() {
                    sleep(POLL).await;
                }
                upstream.forward(&request, RateLimitWait::Unbounded).await
            },
        );

        let arrivals = mock.arrivals();
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(arrivals.len(), 3);
        assert!(arrivals[1] >= arrivals[0] + COOL_OFF);
        assert!(arrivals[2] >= arrivals[0] + COOL_OFF);
    }

    // a request a node waits on gives up once its rate-limit wait runs out
    #[tokio::test]
    async fn bounded_wait() {
        let mock = MockUpstream::start(|_, _| Reply::status("429 Too Many Requests")).await;
        let upstream =
            Upstream::new(mock.url.clone(), Duration::from_millis(50), HashMap::new(), 8);

        let result = upstream
            .forward(&body(), RateLimitWait::Bounded(Duration::from_millis(120)))
            .await;

        assert!(matches!(result, Err(UpstreamError::RateLimited { .. })));
        assert_eq!(mock.arrivals().len(), 3);
    }

    // concurrent requests never exceed the in-flight cap
    #[tokio::test]
    async fn in_flight_cap() {
        let mock = MockUpstream::start(|_, _| Reply {
            delay: Duration::from_millis(50),
            ..Reply::result(json!(1))
        })
        .await;
        let upstream = Arc::new(Upstream::new(mock.url.clone(), COOL_OFF, HashMap::new(), 2));

        let mut requests = JoinSet::new();
        for _ in 0..6 {
            let upstream = upstream.clone();
            requests.spawn(async move {
                upstream.forward(&body(), RateLimitWait::Unbounded).await.is_ok()
            });
        }
        let mut succeeded = 0;
        while let Some(joined) = requests.join_next().await {
            if joined.expect("request task") {
                succeeded += 1;
            }
        }

        assert_eq!(succeeded, 6);
        assert_eq!(mock.max_in_flight(), 2);
    }

    // a transport error keeps the cause beneath it and leaves out the url
    #[tokio::test]
    async fn error_cause() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        drop(listener);

        let error = Client::new()
            .post(format!("http://{address}/?api-key=secret"))
            .send()
            .await
            .expect_err("nothing listens on a closed port");
        let description = describe(error);

        assert!(description.contains(": "));
        assert!(!description.contains("secret"));
    }
}
