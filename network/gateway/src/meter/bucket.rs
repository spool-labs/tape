use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tape_node::config::gateway::GatewayMeteringConfig;
use tracing::warn;

const PRUNE_INTERVAL: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum MeterClass {
    ObjectRequest,
    ObjectBytes,
}

/// What a bucket is keyed on: the resolved caller IP for the abuse layer, or
/// the verified access key for the per-credential quota layer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum MeterScope {
    Ip(IpAddr),
    AccessKey(String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MeterKey {
    class: MeterClass,
    scope: MeterScope,
}

/// The identity a read is metered against: the resolved caller IP always, plus
/// the verified access key and its assigned grade when the request was signed.
#[derive(Clone, Debug)]
pub struct MeterCaller {
    pub ip: IpAddr,
    pub access_key: Option<String>,
    pub grade: Option<String>,
}

/// The refill rate and burst cap of one bucket layer.
#[derive(Clone, Copy)]
struct BucketRates {
    refill_per_sec: f64,
    burst: f64,
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

    /// Meter one decoded-object request. The caller's IP bucket is charged
    /// first; a signed caller must then also clear its access-key bucket.
    pub fn check_object_request(&self, caller: &MeterCaller) -> GatewayMeterDecision {
        self.check_layered(MeterClass::ObjectRequest, caller, 1.0)
    }

    /// Meter a decoded-object read of `bytes`, charging the IP byte bucket and
    /// then the access-key byte bucket for signed callers.
    pub fn check_object_bytes(
        &self,
        caller: &MeterCaller,
        bytes: u64,
    ) -> GatewayMeterDecision {
        self.check_layered(MeterClass::ObjectBytes, caller, (bytes as f64).max(1.0))
    }

    /// Charge the caller's IP bucket, then the access-key bucket for signed
    /// callers; the first rejection wins. Both layers are charged under one
    /// lock, so the two-layer decision is atomic. The IP layer runs at the
    /// anonymous grade; the key layer at the credential's grade.
    fn check_layered(
        &self,
        class: MeterClass,
        caller: &MeterCaller,
        cost: f64,
    ) -> GatewayMeterDecision {
        let ip_rates = self.grade_rates(&self.config.anonymous_grade, class);
        let now = Instant::now();
        let mut buckets = match self.lock_buckets() {
            Some(buckets) => buckets,
            None => return GatewayMeterDecision::Allowed,
        };

        self.maybe_prune(&mut buckets, now);

        let decision = self.check_bucket(
            &mut buckets,
            now,
            MeterKey {
                class,
                scope: MeterScope::Ip(caller.ip),
            },
            cost,
            ip_rates,
        );
        match (decision, caller.access_key.as_ref()) {
            (GatewayMeterDecision::Allowed, Some(access_key)) => {
                let grade = caller.grade.as_deref().unwrap_or(&self.config.default_grade);
                let rates = self.grade_rates(grade, class);
                self.check_bucket(
                    &mut buckets,
                    now,
                    MeterKey {
                        class,
                        scope: MeterScope::AccessKey(access_key.clone()),
                    },
                    cost,
                    rates,
                )
            }
            _ => decision,
        }
    }

    /// Look up one grade's rates for a meter class, falling back to the
    /// default grade when a credential names a grade that no longer exists.
    fn grade_rates(&self, name: &str, class: MeterClass) -> BucketRates {
        let grade = self.config.grades.get(name).or_else(|| {
            warn!(grade = name, "unknown metering grade; using the default grade");
            self.config.grades.get(&self.config.default_grade)
        });
        let Some(grade) = grade else {
            // Startup validation guarantees the default grade exists; meter
            // at a crawl rather than panic if a broken config slips through.
            return BucketRates {
                refill_per_sec: 1.0,
                burst: 1.0,
            };
        };
        match class {
            MeterClass::ObjectRequest => BucketRates {
                refill_per_sec: grade.read_per_sec as f64,
                burst: grade.read_burst as f64,
            },
            MeterClass::ObjectBytes => BucketRates {
                refill_per_sec: grade.read_bytes_per_sec as f64,
                burst: grade.read_byte_burst as f64,
            },
        }
    }

    fn check_bucket(
        &self,
        buckets: &mut HashMap<MeterKey, BucketState>,
        now: Instant,
        key: MeterKey,
        cost: f64,
        rates: BucketRates,
    ) -> GatewayMeterDecision {
        let BucketRates {
            refill_per_sec,
            burst,
        } = rates;
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

        // A request larger than the burst cap is admitted on a full bucket
        // and runs it into debt; the caller is then blocked until the
        // deficit refills. Requiring tokens >= cost would make any object
        // larger than the burst permanently unfetchable.
        if bucket.tokens >= cost.min(burst) {
            bucket.tokens -= cost;
            return GatewayMeterDecision::Allowed;
        }

        let deficit_secs = (-bucket.tokens / refill_per_sec).ceil() as u64;
        let retry_after =
            Duration::from_secs(deficit_secs.max(self.config.over_budget_penalty_secs));
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};

    use tape_node::config::gateway::MeteringGrade;

    use super::*;

    fn grade(per_sec: u32, burst: u32, bytes_per_sec: u64, byte_burst: u64) -> MeteringGrade {
        MeteringGrade {
            read_per_sec: per_sec,
            read_burst: burst,
            read_bytes_per_sec: bytes_per_sec,
            read_byte_burst: byte_burst,
        }
    }

    fn test_config() -> GatewayMeteringConfig {
        GatewayMeteringConfig {
            grades: BTreeMap::from([
                ("anonymous".to_string(), grade(1, 2, 10, 20)),
                ("standard".to_string(), grade(1, 3, 10, 30)),
                ("firehose".to_string(), grade(1, 6, 10, 60)),
            ]),
            anonymous_grade: "anonymous".to_string(),
            default_grade: "standard".to_string(),
            over_budget_penalty_secs: 30,
            stale_entry_secs: 60,
            trusted_proxies: Vec::new(),
        }
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    fn anon(last: u8) -> MeterCaller {
        MeterCaller {
            ip: ip(last),
            access_key: None,
            grade: None,
        }
    }

    fn signed(last: u8, access_key: &str) -> MeterCaller {
        MeterCaller {
            ip: ip(last),
            access_key: Some(access_key.to_string()),
            grade: None,
        }
    }

    fn graded(last: u8, access_key: &str, grade: &str) -> MeterCaller {
        MeterCaller {
            ip: ip(last),
            access_key: Some(access_key.to_string()),
            grade: Some(grade.to_string()),
        }
    }

    #[test]
    fn anonymous_requests_are_bucketed_per_ip() {
        let meter = GatewayMeter::new(test_config());
        let first = anon(1);
        let second = anon(2);

        assert_eq!(
            meter.check_object_request(&first),
            GatewayMeterDecision::Allowed
        );
        assert_eq!(
            meter.check_object_request(&first),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_request(&first),
            GatewayMeterDecision::RateLimited { .. }
        ));
        assert_eq!(
            meter.check_object_request(&second),
            GatewayMeterDecision::Allowed
        );
    }

    // A signed caller shares the IP bucket with anonymous traffic, so a
    // blocked IP cannot buy more throughput by signing.
    #[test]
    fn signed_requests_share_the_ip_bucket() {
        let meter = GatewayMeter::new(test_config());
        let anonymous = anon(1);
        let caller = signed(1, "AKIDEXAMPLE");

        assert_eq!(
            meter.check_object_request(&anonymous),
            GatewayMeterDecision::Allowed
        );
        assert_eq!(
            meter.check_object_request(&anonymous),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_request(&caller),
            GatewayMeterDecision::RateLimited { .. }
        ));
    }

    // One access key spread across many IPs still drains a single quota
    // bucket, while an unrelated key from the same IPs is unaffected.
    #[test]
    fn access_key_quota_spans_ips() {
        let meter = GatewayMeter::new(test_config());

        assert_eq!(
            meter.check_object_request(&signed(1, "hot")),
            GatewayMeterDecision::Allowed
        );
        assert_eq!(
            meter.check_object_request(&signed(2, "hot")),
            GatewayMeterDecision::Allowed
        );
        assert_eq!(
            meter.check_object_request(&signed(3, "hot")),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_request(&signed(4, "hot")),
            GatewayMeterDecision::RateLimited { .. }
        ));
        assert_eq!(
            meter.check_object_request(&signed(5, "cold")),
            GatewayMeterDecision::Allowed
        );
    }

    // A key on a bigger grade gets that grade's burst; spread across IPs so
    // the anonymous-grade IP layer never interferes.
    #[test]
    fn assigned_grade_rates_apply() {
        let meter = GatewayMeter::new(test_config());

        for i in 1..=6 {
            assert_eq!(
                meter.check_object_request(&graded(i, "hot", "firehose")),
                GatewayMeterDecision::Allowed
            );
        }
        assert!(matches!(
            meter.check_object_request(&graded(7, "hot", "firehose")),
            GatewayMeterDecision::RateLimited { .. }
        ));
    }

    // A credential pointing at a deleted grade meters at the default grade,
    // never unlimited.
    #[test]
    fn unknown_grade_falls_back_to_default() {
        let meter = GatewayMeter::new(test_config());

        for i in 1..=3 {
            assert_eq!(
                meter.check_object_request(&graded(i, "key", "goldplated")),
                GatewayMeterDecision::Allowed
            );
        }
        assert!(matches!(
            meter.check_object_request(&graded(4, "key", "goldplated")),
            GatewayMeterDecision::RateLimited { .. }
        ));
    }

    #[test]
    fn object_bytes_are_bucketed_per_ip() {
        let meter = GatewayMeter::new(test_config());
        let first = anon(1);
        let second = anon(2);

        assert_eq!(
            meter.check_object_bytes(&first, 15),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_bytes(&first, 15),
            GatewayMeterDecision::RateLimited { .. }
        ));
        assert_eq!(
            meter.check_object_bytes(&second, 15),
            GatewayMeterDecision::Allowed
        );
    }

    // Byte reads debit the access-key bucket too, so the key-level byte quota
    // holds across IPs.
    #[test]
    fn object_bytes_drain_the_access_key_bucket() {
        let meter = GatewayMeter::new(test_config());

        assert_eq!(
            meter.check_object_bytes(&signed(1, "hot"), 18),
            GatewayMeterDecision::Allowed
        );
        assert!(matches!(
            meter.check_object_bytes(&signed(2, "hot"), 18),
            GatewayMeterDecision::RateLimited { .. }
        ));
    }

    // An object larger than the byte burst must serve once and then block
    // the caller while the debt refills, not stay unfetchable forever.
    #[test]
    fn oversized_object() {
        let meter = GatewayMeter::new(test_config());
        let caller = anon(1);

        assert_eq!(
            meter.check_object_bytes(&caller, 200),
            GatewayMeterDecision::Allowed
        );
        let GatewayMeterDecision::RateLimited { retry_after } =
            meter.check_object_bytes(&caller, 1)
        else {
            panic!("expected debt to rate limit the next read");
        };
        assert!(retry_after >= Duration::from_secs(18));
    }
}
