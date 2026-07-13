//! Pluggable admission control for the S3 write path
//!
//! The gateway admits and settles writes; what admission means is the
//! operator's interpretation (a prepaid balance, a flat plan, a quota). An
//! embedder injects an implementation through run_with_context; the stock
//! binary runs AdmitAll, which changes nothing.

use async_trait::async_trait;
use tape_crypto::address::Address;

pub use crate::http::handlers::s3::authz::WriteOp;

/// One write presented to the admission gate
#[derive(Debug)]
pub struct AdmissionRequest {
    /// Process-unique id correlating this reserve with its later settle
    pub ticket: u64,
    /// The resolved owner authority the write acts on behalf of
    pub principal: Address,
    /// The access key id that signed the request
    pub access_key_id: String,
    /// The bucket tape the write targets
    pub bucket: Address,
    /// The object key the write targets
    pub key: String,
    /// The write being admitted
    pub op: WriteOp,
    /// Bytes reserved up front; streamed writes reserve a declared ceiling
    pub estimated_bytes: u64,
}

/// Deny returned by an admission implementation
#[derive(Debug)]
pub struct AdmissionDeny {
    /// Reason surfaced to the client and recorded in the audit log
    pub reason: String,
    /// Retry hint for a transient deny (503); a hard deny (403) carries none
    pub retry_after_seconds: Option<u64>,
}

/// Admission gate at the S3 write chokepoint
///
/// Reserve is called after authentication, policy, the on-chain precondition,
/// and the budget caps have all passed, so implementations only see otherwise
/// admissible traffic. Every admitted write settles exactly once: commit on
/// success at actual cost, refund on failure.
///
/// Settlement is at-most-once. A crash between reserve and settle orphans the
/// ticket, so implementations must expire unsettled tickets on their own TTL,
/// the same way the gateway sweeps its own ledger reservations. A denied
/// reserve must hold nothing; no settle call follows it.
#[async_trait]
pub trait Admission: Send + Sync {
    /// Admit or deny one write; runs on the request path, so implementations
    /// may do I/O but own their latency budget
    async fn reserve(&self, request: AdmissionRequest) -> Result<(), AdmissionDeny>;

    /// Settle an admitted write at its actual size, which may be below the
    /// estimate; called synchronously on the settle path, must not block
    fn commit(&self, ticket: u64, actual_bytes: u64);

    /// Release an admitted write without settling it, after a failed or no-op
    /// write; called synchronously on the settle path, must not block
    fn refund(&self, ticket: u64);
}

/// The default gate: every write is admitted, matching a gateway without one
pub struct AdmitAll;

#[async_trait]
impl Admission for AdmitAll {
    async fn reserve(&self, _request: AdmissionRequest) -> Result<(), AdmissionDeny> {
        Ok(())
    }

    fn commit(&self, _ticket: u64, _actual_bytes: u64) {}

    fn refund(&self, _ticket: u64) {}
}
