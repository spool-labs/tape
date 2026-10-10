//! HTTP implementation of the `Api` trait for production node-to-node communication.

mod builder;
mod client;
mod gateway;
mod metrics;

pub use builder::HttpApiBuilder;
pub use client::{HttpApi, PeerTransfer, TransferSink};
pub use gateway::GatewayApi;
pub use metrics::ApiMetrics;

/// The rate limit error for a 429 answer, with any advertised retry delay.
pub(crate) fn rate_limited_error(response: &reqwest::Response) -> tape_protocol::ApiError {
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .map(std::time::Duration::from_secs);
    tape_protocol::ApiError::RateLimited { retry_after }
}
