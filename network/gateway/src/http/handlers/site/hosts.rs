//! Self-serve domain bindings proven by TXT records
//!
//! A domain owner binds their hostname to a tape by publishing a TXT record
//! at the underscore label, for example `_tape.example.com` holding the
//! tape address, and pointing the hostname's traffic at a gateway that has
//! txt domains enabled. The gateway resolves and caches the binding, so
//! attaching a site needs no operator involvement.

use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::TokioResolver;
use hickory_resolver::lookup::Lookup;
use hickory_resolver::proto::rr::RData;
use moka::future::Cache;
use tape_crypto::address::Address;
use tracing::{debug, error};

/// Record TTLs steer refresh, clamped in the resolver's cache so a huge TTL
/// cannot pin a stale binding for hours and a tiny one cannot turn every
/// request into a DNS query.
const MIN_RECORD_TTL: Duration = Duration::from_secs(60);
const MAX_RECORD_TTL: Duration = Duration::from_secs(3600);

/// How long a parsed binding lives in the outer cache before re-asking the
/// resolver, whose own cache honors the record TTLs above. This layer exists
/// for single-flight coalescing, transport-error damping, and skipping the
/// TXT re-parse; freshness comes from the resolver cache.
const BINDING_TTL: Duration = Duration::from_secs(60);

/// Bound on remembered hosts; Host headers are caller-controlled.
const MAX_CACHED_HOSTS: u64 = 10_000;

/// Resolver plus a bounded, request-coalescing cache of proven bindings.
pub struct SiteHostBindings {
    resolver: TokioResolver,
    cache: Cache<String, Option<Address>>,
}

impl SiteHostBindings {
    /// Bindings for the host-serving layer when the operator opted in. A
    /// promised capability that cannot start is a loud error, not a warning
    /// buried at startup.
    pub fn from_config(txt_domains: bool) -> Option<Arc<Self>> {
        if !txt_domains {
            return None;
        }

        let resolver = TokioResolver::builder_tokio().and_then(|mut builder| {
            let options = builder.options_mut();
            options.validate = true;
            options.positive_min_ttl = Some(MIN_RECORD_TTL);
            options.positive_max_ttl = Some(MAX_RECORD_TTL);
            options.negative_min_ttl = Some(MIN_RECORD_TTL);
            options.negative_max_ttl = Some(MAX_RECORD_TTL);
            builder.build()
        });
        match resolver {
            Ok(resolver) => Some(Arc::new(Self {
                resolver,
                cache: Cache::builder()
                    .max_capacity(MAX_CACHED_HOSTS)
                    .time_to_live(BINDING_TTL)
                    .build(),
            })),
            Err(err) => {
                error!(%err, "txt domains enabled but no resolver; txt-bound sites will not serve");
                None
            }
        }
    }

    /// The tape a hostname is bound to, if its TXT record proves one.
    /// Concurrent requests for the same unknown host share one lookup.
    pub async fn tape_for_host(&self, host: &str) -> Option<Address> {
        if !looks_like_domain(host) {
            return None;
        }

        let host = host.to_ascii_lowercase();
        self.cache
            .get_with_by_ref(&host, self.resolve(&host))
            .await
    }

    async fn resolve(&self, host: &str) -> Option<Address> {
        let name = format!("_tape.{host}.");
        match self.resolver.txt_lookup(name).await {
            Ok(lookup) => tape_in_records(&lookup),
            Err(error) => {
                debug!(host, %error, "txt binding lookup failed");
                None
            }
        }
    }
}

/// Only hostnames can carry a binding; IP literals and dotless names skip
/// the resolver entirely, which keeps junk Host headers cheap.
fn looks_like_domain(host: &str) -> bool {
    host.contains('.') && host.parse::<std::net::IpAddr>().is_err()
}

/// The first TXT value across the answers that parses as a tape address.
fn tape_in_records(lookup: &Lookup) -> Option<Address> {
    for record in lookup.answers() {
        let RData::TXT(txt) = &record.data else {
            continue;
        };
        for data in txt.txt_data.iter() {
            let Ok(value) = std::str::from_utf8(data) else {
                continue;
            };
            if let Ok(tape) = value.trim().parse() {
                return Some(tape);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hickory_resolver::proto::op::Query;
    use hickory_resolver::proto::rr::rdata::TXT;
    use hickory_resolver::proto::rr::{Name, Record, RecordType};

    use super::*;

    // ip literals and dotless names never reach the resolver
    #[test]
    fn domain_filter() {
        assert!(looks_like_domain("example.com"));
        assert!(looks_like_domain("a.b.example.com"));
        assert!(!looks_like_domain("localhost"));
        assert!(!looks_like_domain("127.0.0.1"));
        assert!(!looks_like_domain("::1"));
    }

    // the first parseable TXT value wins; junk values are skipped
    #[test]
    fn record_parsing() {
        let tape = Address::new_unique();
        let name = Name::from_str("_tape.example.com.").expect("name parses");
        let query = Query::query(name.clone(), RecordType::TXT);
        let records = [
            Record::from_rdata(name.clone(), 300, RData::TXT(TXT::new(vec!["junk".into()]))),
            Record::from_rdata(name, 300, RData::TXT(TXT::new(vec![tape.to_string()]))),
        ];
        let lookup = Lookup::new_with_max_ttl(query, records);

        assert_eq!(tape_in_records(&lookup), Some(tape));
    }
}
