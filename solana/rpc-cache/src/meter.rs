//! Token buckets for a public instance: one per caller, one for upstream
//!
//! Callers are keyed by IPv4 address or IPv6 /64, since one IPv6 host
//! usually holds a whole /64.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Checks between sweeps of idle caller buckets
const PRUNE_INTERVAL: usize = 1024;
/// Leading bits of an IPv6 address kept in a caller key
const IPV6_PREFIX_BITS: u32 = 64;
/// One request costs one token
const REQUEST_COST: f64 = 1.0;

/// Whether a request may proceed
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Decision {
    Allowed,
    Limited { retry_after: Duration },
}

/// Refill rate and burst for one bucket
#[derive(Clone, Copy, Debug)]
pub struct Rate {
    pub per_second: f64,
    pub burst: f64,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    refilled_at: Instant,
}

impl Bucket {
    fn full(rate: Rate, now: Instant) -> Self {
        Self { tokens: rate.burst, refilled_at: now }
    }

    /// Take one request's worth of tokens, or say how long until there are enough
    fn take(&mut self, rate: Rate, now: Instant) -> Decision {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        self.tokens = (self.tokens + elapsed.as_secs_f64() * rate.per_second).min(rate.burst);
        self.refilled_at = now;

        if self.tokens >= REQUEST_COST {
            self.tokens -= REQUEST_COST;
            return Decision::Allowed;
        }
        let deficit = REQUEST_COST - self.tokens;
        Decision::Limited {
            retry_after: Duration::from_secs_f64(deficit / rate.per_second),
        }
    }

    /// Whether the bucket has refilled to its burst, which makes it the same as a fresh one
    fn is_full(&self, rate: Rate, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        self.tokens + elapsed.as_secs_f64() * rate.per_second >= rate.burst
    }
}

/// Per-caller buckets plus one shared bucket for requests that go upstream
pub struct Meter {
    caller_rate: Rate,
    upstream_rate: Rate,
    callers: Mutex<HashMap<IpAddr, Bucket>>,
    upstream: Mutex<Bucket>,
    checks: AtomicUsize,
}

impl Meter {
    /// Buckets at these rates, all starting full
    pub fn new(caller_rate: Rate, upstream_rate: Rate) -> Self {
        Self {
            caller_rate,
            upstream_rate,
            callers: Mutex::new(HashMap::new()),
            upstream: Mutex::new(Bucket::full(upstream_rate, Instant::now())),
            checks: AtomicUsize::new(0),
        }
    }

    /// Charge one request to the caller's bucket
    pub fn charge_caller(&self, caller: IpAddr) -> Decision {
        let now = Instant::now();
        let key = caller_key(caller);
        let mut callers = self.callers.lock().unwrap_or_else(PoisonError::into_inner);

        if self.checks.fetch_add(1, Ordering::Relaxed).is_multiple_of(PRUNE_INTERVAL) {
            let rate = self.caller_rate;
            callers.retain(|_, bucket| !bucket.is_full(rate, now));
        }
        callers
            .entry(key)
            .or_insert_with(|| Bucket::full(self.caller_rate, now))
            .take(self.caller_rate, now)
    }

    /// Charge one request to the shared upstream bucket
    pub fn charge_upstream(&self) -> Decision {
        let now = Instant::now();
        let mut upstream = self.upstream.lock().unwrap_or_else(PoisonError::into_inner);
        upstream.take(self.upstream_rate, now)
    }
}

/// Key IPv4 callers by address and IPv6 callers by /64
fn caller_key(caller: IpAddr) -> IpAddr {
    match caller {
        IpAddr::V4(_) => caller,
        IpAddr::V6(address) => match address.to_ipv4_mapped() {
            Some(mapped) => IpAddr::V4(mapped),
            None => {
                let mask = u128::MAX << (128 - IPV6_PREFIX_BITS);
                IpAddr::V6((u128::from(address) & mask).into())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(per_second: f64, burst: f64) -> Rate {
        Rate { per_second, burst }
    }

    fn address(value: &str) -> IpAddr {
        value.parse().expect("address")
    }

    // a caller gets its burst, then waits for the next token
    #[test]
    fn burst_then_wait() {
        let meter = Meter::new(rate(10.0, 3.0), rate(100.0, 100.0));
        let caller = address("198.51.100.7");

        for _ in 0..3 {
            assert_eq!(meter.charge_caller(caller), Decision::Allowed);
        }
        let limited = meter.charge_caller(caller);

        assert!(
            matches!(limited, Decision::Limited { retry_after } if retry_after <= Duration::from_millis(100)),
            "fourth request should wait for one token: {limited:?}"
        );
    }

    // callers have separate buckets
    #[test]
    fn separate_callers() {
        let meter = Meter::new(rate(1.0, 1.0), rate(100.0, 100.0));

        assert_eq!(meter.charge_caller(address("198.51.100.7")), Decision::Allowed);
        assert_eq!(meter.charge_caller(address("198.51.100.8")), Decision::Allowed);
        assert!(matches!(meter.charge_caller(address("198.51.100.7")), Decision::Limited { .. }));
    }

    // every address in one IPv6 /64 shares a bucket
    #[test]
    fn ipv6_prefix() {
        let meter = Meter::new(rate(1.0, 1.0), rate(100.0, 100.0));

        assert_eq!(meter.charge_caller(address("2001:db8:1:2::1")), Decision::Allowed);
        let same_prefix = meter.charge_caller(address("2001:db8:1:2:ffff::9"));
        let next_prefix = meter.charge_caller(address("2001:db8:1:3::1"));

        assert!(matches!(same_prefix, Decision::Limited { .. }));
        assert_eq!(next_prefix, Decision::Allowed);
    }

    // an IPv4-mapped IPv6 caller shares the IPv4 bucket
    #[test]
    fn mapped_ipv4() {
        assert_eq!(caller_key(address("::ffff:198.51.100.7")), address("198.51.100.7"));
    }

    // the upstream bucket is shared by every caller
    #[test]
    fn upstream_ceiling() {
        let meter = Meter::new(rate(100.0, 100.0), rate(1.0, 2.0));

        assert_eq!(meter.charge_upstream(), Decision::Allowed);
        assert_eq!(meter.charge_upstream(), Decision::Allowed);
        assert!(matches!(meter.charge_upstream(), Decision::Limited { .. }));
    }

    // a bucket that has refilled reads as full, so pruning it loses nothing
    #[test]
    fn refilled_bucket() {
        let start = Instant::now();
        let limits = rate(10.0, 2.0);
        let mut bucket = Bucket::full(limits, start);

        assert_eq!(bucket.take(limits, start), Decision::Allowed);
        assert!(!bucket.is_full(limits, start));
        assert!(bucket.is_full(limits, start + Duration::from_millis(100)));
    }
}
