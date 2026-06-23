use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_node::config::gateway::GatewayMeteringConfig;
use tape_protocol::Api;
use tracing::{debug, warn};

use crate::http::AppState;

const PRUNE_INTERVAL: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum MeterClass {
    ObjectRequest,
    ObjectBytes,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct MeterKey {
    class: MeterClass,
    ip: IpAddr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayMeterDecision {
    Allowed,
    RateLimited { retry_after: Duration },
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    blocked_until: Option<Instant>,
    last_seen: Instant,
}

pub struct GatewayMeter {
    config: GatewayMeteringConfig,
    buckets: Mutex<HashMap<MeterKey, BucketState>>,
    checks: AtomicUsize,
}

impl GatewayMeter {
    pub fn new(config: GatewayMeteringConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
            checks: AtomicUsize::new(0),
        }
    }

    pub fn check_object_request(&self, ip: IpAddr) -> GatewayMeterDecision {
        self.check_bucket(
            MeterClass::ObjectRequest,
            ip,
            1.0,
            self.config.object_read_per_sec as f64,
            self.config.object_read_burst as f64,
        )
    }

    pub fn check_object_bytes(&self, ip: IpAddr, bytes: u64) -> GatewayMeterDecision {
        let cost = (bytes as f64).max(1.0);
        self.check_bucket(
            MeterClass::ObjectBytes,
            ip,
            cost,
            self.config.object_read_bytes_per_sec as f64,
            self.config.object_read_byte_burst as f64,
        )
    }

    fn check_bucket(
        &self,
        class: MeterClass,
        ip: IpAddr,
        cost: f64,
        refill_per_sec: f64,
        burst: f64,
    ) -> GatewayMeterDecision {
        let now = Instant::now();
        let mut buckets = match self.lock_buckets() {
            Some(buckets) => buckets,
            None => return GatewayMeterDecision::Allowed,
        };

        self.maybe_prune(&mut buckets, now);

        let key = MeterKey { class, ip };
        let bucket = buckets.entry(key).or_insert_with(|| BucketState {
            tokens: burst,
            last_refill: now,
            blocked_until: None,
            last_seen: now,
        });

        bucket.last_seen = now;

        if let Some(blocked_until) = bucket.blocked_until {
            if blocked_until > now {
                return GatewayMeterDecision::RateLimited {
                    retry_after: blocked_until.saturating_duration_since(now),
                };
            }
            bucket.blocked_until = None;
        }

        let elapsed = now.saturating_duration_since(bucket.last_refill);
        if !elapsed.is_zero() {
            let refill = elapsed.as_secs_f64() * refill_per_sec;
            bucket.tokens = (bucket.tokens + refill).min(burst);
            bucket.last_refill = now;
        }

        if bucket.tokens >= cost {
            bucket.tokens -= cost;
            return GatewayMeterDecision::Allowed;
        }

        let retry_after = Duration::from_secs(self.config.over_budget_penalty_secs);
        bucket.blocked_until = Some(now + retry_after);
        GatewayMeterDecision::RateLimited { retry_after }
    }

    fn lock_buckets(&self) -> Option<MutexGuard<'_, HashMap<MeterKey, BucketState>>> {
        match self.buckets.lock() {
            Ok(buckets) => Some(buckets),
            Err(_) => {
                warn!("gateway meter lock poisoned; allowing request");
                None
            }
        }
    }

    fn maybe_prune(&self, buckets: &mut HashMap<MeterKey, BucketState>, now: Instant) {
        let checks = self.checks.fetch_add(1, Ordering::Relaxed);
        if checks % PRUNE_INTERVAL != 0 {
            return;
        }

        let stale_after = Duration::from_secs(self.config.stale_entry_secs);
        buckets.retain(|_, bucket| now.saturating_duration_since(bucket.last_seen) <= stale_after);
    }
}

pub(crate) async fn object_read_metering<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let ip = caller_ip(&req);
    match state.meter.check_object_request(ip) {
        GatewayMeterDecision::Allowed => next.run(req).await,
        GatewayMeterDecision::RateLimited { retry_after } => {
            debug!(%ip, retry_after_secs = retry_after.as_secs(), "gateway meter rejected object request");
            rate_limited_response(retry_after)
        }
    }
}

pub(crate) fn caller_ip(req: &Request) -> IpAddr {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub(crate) fn rate_limited_response(retry_after: Duration) -> Response {
    let retry_after_secs = retry_after.as_secs().max(1).to_string();
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, retry_after_secs)],
        "rate limited",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn test_config() -> GatewayMeteringConfig {
        GatewayMeteringConfig {
            object_read_per_sec: 1,
            object_read_burst: 1,
            object_read_bytes_per_sec: 10,
            object_read_byte_burst: 10,
            over_budget_penalty_secs: 30,
            stale_entry_secs: 60,
        }
    }

    #[test]
    fn object_requests_are_bucketed_per_ip() {
        let meter = GatewayMeter::new(test_config());
        let first = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let second = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        assert_eq!(meter.check_object_request(first), GatewayMeterDecision::Allowed);
        assert!(matches!(
            meter.check_object_request(first),
            GatewayMeterDecision::RateLimited { .. }
        ));
        assert_eq!(meter.check_object_request(second), GatewayMeterDecision::Allowed);
    }

    #[test]
    fn object_bytes_are_bucketed_per_ip() {
        let meter = GatewayMeter::new(test_config());
        let first = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let second = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        assert_eq!(
            meter.check_object_bytes(first, 6),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_bytes(first, 6),
            GatewayMeterDecision::RateLimited { .. }
        ));
        assert_eq!(
            meter.check_object_bytes(second, 6),
            GatewayMeterDecision::Allowed
        );
    }
}
