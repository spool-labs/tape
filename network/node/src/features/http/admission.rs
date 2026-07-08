use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_crypto::Address;
use tape_protocol::Api;
use tracing::{debug, warn};

use crate::config::http::AdmissionConfig;
use crate::features::http::auth::{ActivePeer, StakedPeer};
use crate::features::http::state::AppState;

const ADMISSION_COST: f64 = 1.0;
const PRUNE_INTERVAL: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdmissionCaller {
    Peer(Address),
    Anonymous(IpAddr),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BucketClass {
    AnonymousWrite,
    AnonymousRead,
    Probe,
    TrustedMetered,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct BucketKey {
    class: BucketClass,
    caller: AdmissionCaller,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionDecision {
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

pub struct AdmissionLimiter {
    config: AdmissionConfig,
    buckets: Mutex<HashMap<BucketKey, BucketState>>,
    checks: AtomicUsize,
}

impl AdmissionLimiter {
    pub fn new(config: AdmissionConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
            checks: AtomicUsize::new(0),
        }
    }

    pub fn check_direct_write(&self, caller: AdmissionCaller) -> AdmissionDecision {
        match caller {
            AdmissionCaller::Peer(_) => self.check_bucket(
                BucketClass::TrustedMetered,
                caller,
                self.config.trusted_metered_per_sec,
                self.config.trusted_metered_burst,
            ),
            AdmissionCaller::Anonymous(_) => self.check_bucket(
                BucketClass::AnonymousWrite,
                caller,
                self.config.anonymous_write_per_sec,
                self.config.anonymous_write_burst,
            ),
        }
    }

    pub fn check_metered(&self, caller: AdmissionCaller) -> AdmissionDecision {
        match caller {
            AdmissionCaller::Peer(_) => self.check_bucket(
                BucketClass::TrustedMetered,
                caller,
                self.config.trusted_metered_per_sec,
                self.config.trusted_metered_burst,
            ),
            AdmissionCaller::Anonymous(_) => self.check_bucket(
                BucketClass::AnonymousRead,
                caller,
                self.config.anonymous_read_per_sec,
                self.config.anonymous_read_burst,
            ),
        }
    }

    pub fn check_probe(&self, caller: AdmissionCaller) -> AdmissionDecision {
        self.check_bucket(
            BucketClass::Probe,
            caller,
            self.config.probe_per_sec,
            self.config.probe_burst,
        )
    }

    fn check_bucket(
        &self,
        class: BucketClass,
        caller: AdmissionCaller,
        refill_per_sec: u32,
        burst: u32,
    ) -> AdmissionDecision {
        let now = Instant::now();
        let mut buckets = match self.lock_buckets() {
            Some(buckets) => buckets,
            None => return AdmissionDecision::Allowed,
        };

        self.maybe_prune(&mut buckets, now);

        let key = BucketKey { class, caller };
        let bucket = buckets.entry(key).or_insert_with(|| BucketState {
            tokens: burst as f64,
            last_refill: now,
            blocked_until: None,
            last_seen: now,
        });

        bucket.last_seen = now;

        if let Some(blocked_until) = bucket.blocked_until {
            if blocked_until > now {
                return AdmissionDecision::RateLimited {
                    retry_after: blocked_until.saturating_duration_since(now),
                };
            }
            bucket.blocked_until = None;
        }

        let elapsed = now.saturating_duration_since(bucket.last_refill);
        if !elapsed.is_zero() {
            let refill = elapsed.as_secs_f64() * refill_per_sec as f64;
            bucket.tokens = (bucket.tokens + refill).min(burst as f64);
            bucket.last_refill = now;
        }

        if bucket.tokens >= ADMISSION_COST {
            bucket.tokens -= ADMISSION_COST;
            return AdmissionDecision::Allowed;
        }

        let retry_after = Duration::from_secs(self.config.over_budget_penalty_secs);
        bucket.blocked_until = Some(now + retry_after);
        AdmissionDecision::RateLimited { retry_after }
    }

    fn lock_buckets(&self) -> Option<MutexGuard<'_, HashMap<BucketKey, BucketState>>> {
        match self.buckets.lock() {
            Ok(buckets) => Some(buckets),
            Err(_) => {
                warn!("http admission limiter lock poisoned; allowing request");
                None
            }
        }
    }

    fn maybe_prune(&self, buckets: &mut HashMap<BucketKey, BucketState>, now: Instant) {
        let checks = self.checks.fetch_add(1, Ordering::Relaxed);
        if checks % PRUNE_INTERVAL != 0 {
            return;
        }

        let stale_after = Duration::from_secs(self.config.stale_entry_secs);
        buckets.retain(|_, bucket| now.saturating_duration_since(bucket.last_seen) <= stale_after);
    }
}

pub async fn direct_write_admission<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    admit_request(&state, req, next, AdmissionMode::DirectWrite).await
}

pub async fn slice_admission<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mode = if req.method() == Method::PUT {
        AdmissionMode::DirectWrite
    } else {
        AdmissionMode::Metered
    };
    admit_request(&state, req, next, mode).await
}

pub async fn metered_route_admission<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    admit_request(&state, req, next, AdmissionMode::Metered).await
}

pub async fn probe_admission<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    admit_request(&state, req, next, AdmissionMode::Probe).await
}

#[derive(Clone, Copy, Debug)]
enum AdmissionMode {
    DirectWrite,
    Metered,
    Probe,
}

async fn admit_request<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    req: Request,
    next: Next,
    mode: AdmissionMode,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let caller = caller_from_request(&req);
    let decision = match mode {
        AdmissionMode::DirectWrite => state.context.admission.check_direct_write(caller),
        AdmissionMode::Metered => state.context.admission.check_metered(caller),
        AdmissionMode::Probe => state.context.admission.check_probe(caller),
    };

    match decision {
        AdmissionDecision::Allowed => next.run(req).await,
        AdmissionDecision::RateLimited { retry_after } => {
            debug!(?caller, ?mode, retry_after_secs = retry_after.as_secs(), "http admission rejected request");
            rate_limited_response(retry_after)
        }
    }
}

fn caller_from_request(req: &Request) -> AdmissionCaller {
    if let Some(active) = req.extensions().get::<ActivePeer>() {
        return AdmissionCaller::Peer(active.node);
    }

    if let Some(staked) = req.extensions().get::<StakedPeer>() {
        return AdmissionCaller::Peer(staked.node);
    }

    let ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    AdmissionCaller::Anonymous(ip)
}

fn rate_limited_response(retry_after: Duration) -> Response {
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

    fn test_config() -> AdmissionConfig {
        AdmissionConfig {
            anonymous_write_per_sec: 1,
            anonymous_write_burst: 1,
            anonymous_read_per_sec: 1,
            anonymous_read_burst: 1,
            probe_per_sec: 1,
            probe_burst: 1,
            trusted_metered_per_sec: 1,
            trusted_metered_burst: 1,
            over_budget_penalty_secs: 30,
            stale_entry_secs: 60,
        }
    }

    #[test]
    fn anonymous_write_is_bucketed_per_ip() {
        let limiter = AdmissionLimiter::new(test_config());
        let first = AdmissionCaller::Anonymous(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let second = AdmissionCaller::Anonymous(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));

        assert_eq!(
            limiter.check_direct_write(first),
            AdmissionDecision::Allowed
        );
        assert!(matches!(
            limiter.check_direct_write(first),
            AdmissionDecision::RateLimited { .. }
        ));
        assert_eq!(
            limiter.check_direct_write(second),
            AdmissionDecision::Allowed
        );
    }

    #[test]
    fn trusted_metered_bucket_is_keyed_by_peer() {
        let limiter = AdmissionLimiter::new(test_config());
        let first = AdmissionCaller::Peer(Address::from([1u8; 32]));
        let second = AdmissionCaller::Peer(Address::from([2u8; 32]));

        assert_eq!(
            limiter.check_metered(first),
            AdmissionDecision::Allowed
        );
        assert!(matches!(
            limiter.check_metered(first),
            AdmissionDecision::RateLimited { .. }
        ));
        assert_eq!(
            limiter.check_metered(second),
            AdmissionDecision::Allowed
        );
    }

    #[test]
    fn anonymous_read_is_bucketed_per_ip() {
        let limiter = AdmissionLimiter::new(test_config());
        let first = AdmissionCaller::Anonymous(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let second = AdmissionCaller::Anonymous(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));

        assert_eq!(
            limiter.check_metered(first),
            AdmissionDecision::Allowed
        );
        assert!(matches!(
            limiter.check_metered(first),
            AdmissionDecision::RateLimited { .. }
        ));
        assert_eq!(
            limiter.check_metered(second),
            AdmissionDecision::Allowed
        );
    }

    #[test]
    fn probe_bucket_is_keyed_by_caller() {
        let limiter = AdmissionLimiter::new(test_config());
        let anonymous = AdmissionCaller::Anonymous(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let peer = AdmissionCaller::Peer(Address::from([3u8; 32]));

        assert_eq!(limiter.check_probe(anonymous), AdmissionDecision::Allowed);
        assert!(matches!(
            limiter.check_probe(anonymous),
            AdmissionDecision::RateLimited { .. }
        ));
        assert_eq!(
            limiter.check_probe(peer),
            AdmissionDecision::Allowed
        );
    }
}
