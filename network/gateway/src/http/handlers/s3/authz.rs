//! S3 write authorization.

use rpc::Rpc;
use store::Store;
use tape_crypto::address::Address;
use tape_node::config::gateway::WriteDefault;
use tape_protocol::Api;
use tape_store::ops::{
    AuditOps, AuthStateOps, CredentialOps, PolicyDecision, PolicyOps, ReserveOutcome, ReserveRequest,
};
use tape_store::types::{
    AuditDecision, AuditEntry, AuditOp, Credential, CredentialCaps, LedgerReservationKey,
    PolicyAction,
};
use tape_store::TapeStore;

use super::accounting;
use super::clock::now_unix;
use super::error::S3Error;
use super::sigv4;
use crate::admission::{AdmissionDeny, AdmissionRequest};
use crate::http::state::AppState;

/// Flat per-op SOL fee estimate (lamports) reserved before a cost-bearing write.
const ESTIMATED_LAMPORTS_PER_OP: u64 = 5_000;

pub fn peppered_secret_hmac(pepper: &str, secret: &str) -> Result<[u8; 32], S3Error> {
    sigv4::hmac_sha256(pepper.as_bytes(), secret.as_bytes())
}

/// The authenticated principal a request carries into the write path.
#[derive(Clone, Debug)]
pub enum Auth {
    /// No SigV4 credentials were presented (anonymous request). Reads are public;
    /// writes are denied.
    Anonymous,
    /// Credentials were presented and verified against an active credential
    Verified(Principal),
}

impl Auth {
    /// Build a verified Auth for a successfully-authenticated access key
    pub fn verified(access_key_id: String) -> Self {
        Self::Verified(Principal { access_key_id })
    }

    /// The verified access key id, when the request was signed
    pub fn access_key(&self) -> Option<&str> {
        match self {
            Auth::Verified(principal) => Some(&principal.access_key_id),
            Auth::Anonymous => None,
        }
    }
}

/// The verified identity behind a signed request.
#[derive(Clone, Debug)]
pub struct Principal {
    /// The access key id whose secret signed the request
    pub access_key_id: String,
}

/// The kind of write being authorized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOp {
    /// `PutObject` — write one named track
    Put,
    /// `DeleteObject` — delete the track backing an object
    Delete,
    /// `CreateMultipartUpload` — mint an upload id (no on-chain cost yet)
    CreateMultipart,
    /// `UploadPart` — buffer one part (no on-chain cost yet)
    UploadPart,
    /// `CompleteMultipartUpload` — assemble buffered parts and write the object
    CompleteMultipart,
    /// `AbortMultipartUpload` — discard a buffered upload (no on-chain cost)
    Abort,
}

impl WriteOp {
    /// Map to the audit-log op so a logged decision maps one-to-one onto the
    /// chokepoint.
    fn audit_op(self) -> AuditOp {
        match self {
            WriteOp::Put => AuditOp::Put,
            WriteOp::Delete => AuditOp::Delete,
            WriteOp::CreateMultipart => AuditOp::CreateMultipart,
            WriteOp::UploadPart => AuditOp::UploadPart,
            WriteOp::CompleteMultipart => AuditOp::CompleteMultipart,
            WriteOp::Abort => AuditOp::Abort,
        }
    }

    /// Map to the policy action this op is matched against. The multipart
    /// lifecycle ops collapse to the multipart action.
    fn policy_action(self) -> PolicyAction {
        match self {
            WriteOp::Put => PolicyAction::Put,
            WriteOp::Delete => PolicyAction::Delete,
            WriteOp::CreateMultipart
            | WriteOp::UploadPart
            | WriteOp::CompleteMultipart
            | WriteOp::Abort => PolicyAction::Multipart,
        }
    }

    /// Whether `caps` permit this op
    fn permitted_by(self, caps: &CredentialCaps) -> bool {
        match self {
            WriteOp::Put => caps.can_put,
            WriteOp::Delete => caps.can_delete,
            WriteOp::CreateMultipart
            | WriteOp::UploadPart
            | WriteOp::CompleteMultipart
            | WriteOp::Abort => caps.can_multipart,
        }
    }

    /// Whether this op performs a cost-bearing on-chain write.
    fn is_cost_bearing(self) -> bool {
        matches!(
            self,
            WriteOp::Put | WriteOp::Delete | WriteOp::CompleteMultipart
        )
    }

    /// The budget estimate this op reserves up front.
    fn reserve_request(self, size: u64) -> ReserveRequest {
        match self {
            WriteOp::Put | WriteOp::CompleteMultipart => ReserveRequest {
                writes: 1,
                bytes: size,
                sol: ESTIMATED_LAMPORTS_PER_OP,
                is_onchain: true,
                meters_capacity: true,
            },
            WriteOp::Delete => ReserveRequest {
                writes: 0,
                bytes: 0,
                sol: ESTIMATED_LAMPORTS_PER_OP,
                is_onchain: true,
                meters_capacity: false,
            },
            WriteOp::CreateMultipart | WriteOp::UploadPart | WriteOp::Abort => ReserveRequest {
                writes: 0,
                bytes: 0,
                sol: 0,
                is_onchain: false,
                meters_capacity: false,
            },
        }
    }
}

/// A budget reservation granted by authorize_write.
#[must_use = "a WritePermit must be committed on success or refunded on failure"]
#[derive(Debug)]
pub struct WritePermit {
    /// The access key id the write is billed to
    access_key_id: String,
    /// The resolved owner authority the write acts on behalf of
    /// (`Address::default()` for the bootstrap credential, which carries none).
    owner: Address,
    /// The write being performed
    op: WriteOp,
    /// Bytes reserved up front (the pre-write estimate)
    reserved: u64,
    /// The durable ledger reservation to reconcile, when the op reserved budget
    reservation: Option<LedgerReservationKey>,
    /// The admission ticket to settle, when a gate admitted the write
    ticket: Option<u64>,
}

impl WritePermit {
    /// The resolved owner authority this write acts on behalf of.
    pub fn owner(&self) -> Address {
        self.owner
    }

    /// Reconcile the reservation to the `actual` bytes written and commit the cost
    /// against the owner's accounting ledger (a no-op when nothing was reserved).
    pub fn commit<Db, Cluster, Blockchain>(
        self,
        state: &AppState<Db, Cluster, Blockchain>,
        actual: u64,
    ) where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        if let Some(key) = &self.reservation {
            accounting::commit_budget(state, key, actual, now_unix());
        }
        if let Some(ticket) = self.ticket {
            state.admission.commit(ticket, actual);
        }
        tracing::trace!(
            access_key_id = %self.access_key_id,
            owner = %self.owner,
            op = ?self.op,
            reserved = self.reserved,
            actual,
            "s3 write permit committed",
        );
    }

    /// Release the reservation after a failed (or no-op) write so it never bills
    /// the principal (a no-op when nothing was reserved).
    pub fn refund<Db, Cluster, Blockchain>(
        self,
        state: &AppState<Db, Cluster, Blockchain>,
    ) where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        if let Some(key) = &self.reservation {
            accounting::refund_budget(state, key);
        }
        if let Some(ticket) = self.ticket {
            state.admission.refund(ticket);
        }
        tracing::trace!(
            access_key_id = %self.access_key_id,
            owner = %self.owner,
            op = ?self.op,
            reserved = self.reserved,
            "s3 write permit refunded",
        );
    }
}

/// The internal outcome of the decision flow, before it is audited and turned
/// into a WritePermit or an S3Error.
struct Decision {
    /// Whether the write is admitted
    allowed: bool,
    /// The resolved owner authority (`Address::default()` when unknown — anonymous,
    /// unresolved credential, or the bootstrap credential).
    owner: Address,
    /// The reason code recorded in the audit log and (on deny) surfaced to the
    /// client.
    reason: String,
}

impl Decision {
    fn deny(owner: Address, reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            owner,
            reason: reason.into(),
        }
    }
}


/// The three durable reads the synchronous decision core (decide) makes,
trait AuthzReads {
    /// Whether the global write kill switch is engaged
    fn is_write_killed(&self) -> Result<bool, String>;

    /// The credential record for `access_key_id`, if any
    fn get_credential(&self, access_key_id: &str) -> Result<Option<Credential>, String>;

    /// Evaluate the policy ruleset for `(owner, bucket, action)` with the
    /// configured default applied when no rule matches.
    fn evaluate_policy(
        &self,
        owner: &Address,
        bucket: &Address,
        action: PolicyAction,
        default_allow: bool,
    ) -> Result<PolicyDecision, String>;
}

impl<S: Store> AuthzReads for TapeStore<S> {
    fn is_write_killed(&self) -> Result<bool, String> {
        AuthStateOps::is_write_killed(self).map_err(|error| error.to_string())
    }

    fn get_credential(&self, access_key_id: &str) -> Result<Option<Credential>, String> {
        CredentialOps::get_credential(self, access_key_id).map_err(|error| error.to_string())
    }

    fn evaluate_policy(
        &self,
        owner: &Address,
        bucket: &Address,
        action: PolicyAction,
        default_allow: bool,
    ) -> Result<PolicyDecision, String> {
        PolicyOps::evaluate_policy(self, owner, bucket, action, default_allow)
            .map_err(|error| error.to_string())
    }
}

/// Run the ordered, fail-closed decision flow.
fn decide<R: AuthzReads>(
    reads: &R,
    bootstrap_id: Option<&str>,
    default_allow: bool,
    auth: &Auth,
    bucket: Address,
    op: WriteOp,
    now: i64,
) -> Decision {
    // 1. Global kill switch — a single durable flip pauses every write. Any
    //    store error reading it is fail-closed (deny).
    match reads.is_write_killed() {
        Ok(true) => return Decision::deny(Address::default(), "global write kill switch is engaged"),
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%error, "s3 write authz: kill-switch state unavailable");
            return Decision::deny(
                Address::default(),
                "write kill-switch state is unavailable".to_string(),
            );
        }
    }

    // 2. The request must carry a SigV4-verified principal.
    let access_key_id = match auth {
        Auth::Verified(principal) => principal.access_key_id.as_str(),
        Auth::Anonymous => {
            return Decision::deny(
                Address::default(),
                "anonymous access to this S3 write operation is not allowed",
            );
        }
    };

    // 3. Resolve the credential record.
    let owner = match reads.get_credential(access_key_id) {
        Ok(Some(credential)) => {
            if !credential.is_usable(now) {
                return Decision::deny(
                    credential.principal,
                    "credential is revoked or expired",
                );
            }
            if !op.permitted_by(&credential.caps) {
                return Decision::deny(
                    credential.principal,
                    format!("credential is not permitted to perform {op:?}"),
                );
            }
            if !credential.allows_bucket(&bucket) {
                return Decision::deny(
                    credential.principal,
                    "credential scope does not include this bucket",
                );
            }
            credential.principal
        }
        Ok(None) => match bootstrap_id {
            Some(bootstrap) if bootstrap == access_key_id => Address::default(),
            Some(_) | None => {
                return Decision::deny(
                    Address::default(),
                    "no active credential for this access key id",
                );
            }
        },
        Err(error) => {
            tracing::warn!(%error, "s3 write authz: credential store unavailable");
            return Decision::deny(
                Address::default(),
                "credential store is unavailable".to_string(),
            );
        }
    };

    // 4. Policy engine: (principal, bucket, op) → Allow | Deny. Default-deny with
    //    deny-precedence; the `gateway.s3.write.default` config is the fallback
    //    when no rule matches.
    match reads.evaluate_policy(&owner, &bucket, op.policy_action(), default_allow) {
        Ok(decision) => Decision {
            allowed: decision.is_allowed,
            owner,
            reason: decision.reason,
        },
        Err(error) => {
            tracing::warn!(%error, "s3 write authz: policy engine unavailable");
            Decision::deny(owner, "policy engine is unavailable".to_string())
        }
    }
}

/// Apply a ledger reserve outcome to the running decision.
fn apply_reserve_outcome(
    outcome: ReserveOutcome,
    owner: Address,
) -> (Option<LedgerReservationKey>, Option<Decision>, Option<u64>) {
    match outcome {
        ReserveOutcome::Granted(key) => (Some(key), None, None),
        ReserveOutcome::SlowDown { retry_after_secs: retry_after_seconds } => (
            None,
            Some(Decision::deny(
                owner,
                format!("over budget; retry after {retry_after_seconds}s"),
            )),
            Some(retry_after_seconds),
        ),
        ReserveOutcome::Ceiling(reason) => (None, Some(Decision::deny(owner, reason)), None),
    }
}

/// Apply an admission deny to the running decision; a transient deny carries
/// its retry-after into the throttle
fn apply_admission_deny(deny: AdmissionDeny, owner: Address) -> (Decision, Option<u64>) {
    let retry_after = deny.retry_after_seconds;
    (Decision::deny(owner, deny.reason), retry_after)
}

/// Map a denied decision to its S3 error: an over-budget throttle becomes a
/// `SlowDown` (503) carrying the retry-after; every other denial is an
/// `AccessDenied` (403).
fn deny_error(reason: String, throttle: Option<u64>) -> S3Error {
    match throttle {
        Some(retry_after_seconds) => S3Error::SlowDown { retry_after_seconds },
        None => S3Error::AccessDenied(reason),
    }
}

/// The configured bootstrap S3 access key id (the dev/bootstrap credential), if
/// any.
fn bootstrap_access_key_id<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
) -> Option<&str>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    state
        .context
        .config
        .gateway
        .s3
        .access_key_id
        .as_deref()
        .filter(|id| !id.is_empty())
}

/// The single write-authorization chokepoint.
pub async fn authorize_write<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket: Address,
    key: &str,
    op: WriteOp,
    size: u64,
) -> Result<WritePermit, S3Error>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let now = now_unix();
    let bootstrap_id = bootstrap_access_key_id(state);
    let access_key_id = match auth {
        Auth::Verified(principal) => principal.access_key_id.as_str(),
        Auth::Anonymous => "",
    };
    let default_allow = matches!(
        state.context.config.gateway.s3.write.default,
        WriteDefault::Allow
    );
    let mut decision = decide(
        state.context.store.as_ref(),
        bootstrap_id,
        default_allow,
        auth,
        bucket,
        op,
        now,
    );

    if decision.allowed && op.is_cost_bearing() {
        if let Err(reason) = accounting::check_onchain_precondition(state, bucket, size).await {
            decision = Decision::deny(decision.owner, reason);
        }
    }

    let mut reservation: Option<LedgerReservationKey> = None;
    let mut throttle: Option<u64> = None;
    if decision.allowed && op.is_cost_bearing() {
        match accounting::reserve_budget(state, decision.owner, op.reserve_request(size), now) {
            Ok(outcome) => {
                let (key, deny, retry_after) = apply_reserve_outcome(outcome, decision.owner);
                reservation = key;
                throttle = retry_after;
                if let Some(deny) = deny {
                    decision = deny;
                }
            }
            Err(reason) => decision = Decision::deny(decision.owner, reason),
        }
    }

    // The admission gate runs last so the caps above stay an independent abuse
    // backstop even over a broken implementation.
    let mut ticket: Option<u64> = None;
    if decision.allowed {
        let candidate = state.accounting.next_audit_sequence();
        let request = AdmissionRequest {
            ticket: candidate,
            principal: decision.owner,
            access_key_id: access_key_id.to_string(),
            bucket,
            key: key.to_string(),
            op,
            estimated_bytes: size,
        };
        match state.admission.reserve(request).await {
            Ok(()) => ticket = Some(candidate),
            Err(deny) => {
                // A deny here must release the caps reservation granted above
                if let Some(reservation_key) = reservation.take() {
                    accounting::refund_budget(state, &reservation_key);
                }
                let (denied, retry_after) = apply_admission_deny(deny, decision.owner);
                decision = denied;
                throttle = retry_after;
            }
        }
    }

    // Audit the final decision exactly once.
    let entry = AuditEntry {
        timestamp: now,
        principal: decision.owner,
        bucket,
        op: op.audit_op(),
        decision: if decision.allowed {
            AuditDecision::Allow
        } else {
            AuditDecision::Deny
        },
        reason: decision.reason.clone(),
    };
    let audit_result = state
        .context
        .store
        .append_audit(&entry, state.accounting.next_audit_sequence());

    if !decision.allowed {
        if let Err(error) = audit_result {
            tracing::error!(%error, "s3 write-authz: failed to record deny decision");
        }
        return Err(deny_error(decision.reason, throttle));
    }

    if let Err(error) = audit_result {
        tracing::error!(%error, "s3 write-authz: failed to record allow decision; denying fail-closed");
        if let Some(reservation_key) = &reservation {
            accounting::refund_budget(state, reservation_key);
        }
        if let Some(ticket) = ticket {
            state.admission.refund(ticket);
        }
        return Err(S3Error::AccessDenied(
            "write authorization audit log is unavailable".to_string(),
        ));
    }

    Ok(WritePermit {
        access_key_id: access_key_id.to_string(),
        owner: decision.owner,
        op,
        reserved: size,
        reservation,
        ticket,
    })
}

/// Authorize an authenticated principal to inspect a bucket's in-flight
/// multipart state (ListParts / ListMultipartUploads)
///
/// In-flight upload ids and part listings are not public the way object reads
/// are: leaking them lets a stranger enumerate or interfere with another
/// tenant's uploads. They therefore require a usable, multipart-capable
/// credential scoped to the bucket; the configured bootstrap credential is
/// always allowed. This is a read check, so it neither reserves budget nor
/// consults the write kill switch.
pub fn authorize_multipart_read<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket: Address,
) -> Result<(), S3Error>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let access_key_id = match auth {
        Auth::Verified(principal) => principal.access_key_id.as_str(),
        Auth::Anonymous => {
            return Err(S3Error::AccessDenied(
                "this multipart operation requires authentication".to_string(),
            ));
        }
    };

    let store = state.context.store.as_ref();
    match CredentialOps::get_credential(store, access_key_id) {
        Ok(Some(credential)) => {
            if !credential.is_usable(now_unix()) {
                return Err(S3Error::AccessDenied(
                    "credential is revoked or expired".to_string(),
                ));
            }
            if !credential.caps.can_multipart {
                return Err(S3Error::AccessDenied(
                    "credential is not permitted to perform multipart operations".to_string(),
                ));
            }
            if !credential.allows_bucket(&bucket) {
                return Err(S3Error::AccessDenied(
                    "credential scope does not include this bucket".to_string(),
                ));
            }
            Ok(())
        }
        Ok(None) => match bootstrap_access_key_id(state) {
            Some(bootstrap) if bootstrap == access_key_id => Ok(()),
            Some(_) | None => Err(S3Error::AccessDenied(
                "no active credential for this access key id".to_string(),
            )),
        },
        Err(error) => {
            tracing::warn!(%error, "s3 multipart read authz: credential store unavailable");
            Err(S3Error::Internal("credential store is unavailable".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_store::types::{
        CredentialScope, CredentialStatus, PolicyEffect, PolicyRule, PolicyRuleKey,
    };

    /// A test double for the decision core's durable reads. Each field is the
    /// canned result of one read, so a test can deny a single stage (or inject a
    /// store error to prove fail-closed) while leaving the rest permissive.
    struct FakeReads {
        killed: Result<bool, String>,
        credential: Result<Option<Credential>, String>,
        policy: Result<PolicyDecision, String>,
    }

    impl Default for FakeReads {
        fn default() -> Self {
            // Permissive baseline: not killed, no credential record (bootstrap
            // path), policy allows. Tests override exactly one field.
            Self {
                killed: Ok(false),
                credential: Ok(None),
                policy: Ok(PolicyDecision {
                    is_allowed: true,
                    reason: "default-allow".to_string(),
                }),
            }
        }
    }

    impl AuthzReads for FakeReads {
        fn is_write_killed(&self) -> Result<bool, String> {
            self.killed.clone()
        }

        fn get_credential(&self, _access_key_id: &str) -> Result<Option<Credential>, String> {
            self.credential.clone()
        }

        fn evaluate_policy(
            &self,
            _owner: &Address,
            _bucket: &Address,
            _action: PolicyAction,
            _default_allow: bool,
        ) -> Result<PolicyDecision, String> {
            self.policy.clone()
        }
    }

    fn active_credential(
        principal: Address,
        caps: CredentialCaps,
        scope: CredentialScope,
    ) -> Credential {
        Credential {
            secret_hmac: [0u8; 32],
            principal,
            scope,
            caps,
            status: CredentialStatus::Active,
            not_after: None,
            grade: None,
        }
    }

    /// A verified principal carrying `id`
    fn verified(id: &str) -> Auth {
        Auth::verified(id.to_string())
    }

    // each write op maps to its audit and policy action
    #[test]
    fn action_mappings() {
        assert_eq!(WriteOp::Put.audit_op(), AuditOp::Put);
        assert_eq!(WriteOp::Delete.audit_op(), AuditOp::Delete);
        assert_eq!(WriteOp::CreateMultipart.audit_op(), AuditOp::CreateMultipart);
        assert_eq!(WriteOp::UploadPart.audit_op(), AuditOp::UploadPart);
        assert_eq!(
            WriteOp::CompleteMultipart.audit_op(),
            AuditOp::CompleteMultipart
        );

        assert_eq!(WriteOp::Put.policy_action(), PolicyAction::Put);
        assert_eq!(WriteOp::Delete.policy_action(), PolicyAction::Delete);
        for op in [
            WriteOp::CreateMultipart,
            WriteOp::UploadPart,
            WriteOp::CompleteMultipart,
        ] {
            assert_eq!(op.policy_action(), PolicyAction::Multipart);
        }
    }

    // caps permit exactly their corresponding ops
    #[test]
    fn capability_gating() {
        let only_put = CredentialCaps {
            can_put: true,
            can_delete: false,
            can_multipart: false,
        };
        assert!(WriteOp::Put.permitted_by(&only_put));
        assert!(!WriteOp::Delete.permitted_by(&only_put));
        assert!(!WriteOp::UploadPart.permitted_by(&only_put));

        let all = CredentialCaps::all();
        for op in [
            WriteOp::Put,
            WriteOp::Delete,
            WriteOp::CreateMultipart,
            WriteOp::UploadPart,
            WriteOp::CompleteMultipart,
        ] {
            assert!(op.permitted_by(&all));
        }
        let none = CredentialCaps::none();
        for op in [WriteOp::Put, WriteOp::Delete, WriteOp::CompleteMultipart] {
            assert!(!op.permitted_by(&none));
        }
    }

    // the peppered secret HMAC is deterministic and keyed by both inputs
    #[test]
    fn peppered_hmac() {
        let a = peppered_secret_hmac("pepper", "secret").expect("hmac");
        let b = peppered_secret_hmac("pepper", "secret").expect("hmac");
        assert_eq!(a, b, "same pepper+secret yields the same digest");
        assert_ne!(
            a,
            peppered_secret_hmac("other-pepper", "secret").expect("hmac"),
            "pepper is keyed into the digest"
        );
        assert_ne!(
            a,
            peppered_secret_hmac("pepper", "other-secret").expect("hmac"),
            "secret is hashed into the digest"
        );
    }

    // a verified auth carries its access key id
    #[test]
    fn auth_verified() {
        match Auth::verified("AKIDEXAMPLE".to_string()) {
            Auth::Verified(principal) => assert_eq!(principal.access_key_id, "AKIDEXAMPLE"),
            Auth::Anonymous => panic!("expected verified"),
        }
    }

    // each op reserves the right budget estimate
    #[test]
    fn reserve_estimates() {
        // Object writes reserve one write, their byte size, a per-op SOL estimate,
        // and meter capacity.
        for op in [WriteOp::Put, WriteOp::CompleteMultipart] {
            assert!(op.is_cost_bearing());
            let request = op.reserve_request(4096);
            assert_eq!(request.writes, 1);
            assert_eq!(request.bytes, 4096);
            assert_eq!(request.sol, ESTIMATED_LAMPORTS_PER_OP);
            assert!(request.is_onchain);
            assert!(request.meters_capacity);
        }

        // Delete reserves only the SOL fee (it frees space; not a "put").
        assert!(WriteOp::Delete.is_cost_bearing());
        let delete = WriteOp::Delete.reserve_request(0);
        assert_eq!(delete.writes, 0);
        assert_eq!(delete.bytes, 0);
        assert_eq!(delete.sol, ESTIMATED_LAMPORTS_PER_OP);
        assert!(delete.is_onchain);
        assert!(!delete.meters_capacity);

        // Minting an upload id / buffering a part is not cost-bearing and reserves
        // nothing.
        for op in [WriteOp::CreateMultipart, WriteOp::UploadPart] {
            assert!(!op.is_cost_bearing());
            let request = op.reserve_request(8192);
            assert_eq!(request.writes, 0);
            assert_eq!(request.bytes, 0);
            assert_eq!(request.sol, 0);
            assert!(!request.is_onchain);
            assert!(!request.meters_capacity);
        }
    }

    // a permit without a reservation needs no ledger to commit or refund
    #[test]
    fn unreserved_permit() {
        // A non-cost-bearing permit carries no reservation, so commit/refund are
        // pure traces and never need the ledger. (The cost-bearing reserve →
        // commit/refund path is covered by the tape-store ledger tests.)
        let permit = WritePermit {
            access_key_id: "AKIDEXAMPLE".to_string(),
            owner: Address::default(),
            op: WriteOp::CreateMultipart,
            reserved: 0,
            reservation: None,
            ticket: None,
        };
        assert!(permit.reservation.is_none());
        assert_eq!(permit.op, WriteOp::CreateMultipart);
        drop(permit);
    }

    // --- decision-flow ordering (steps 1–4), against the FakeReads double -----

    // an anonymous write is denied
    #[test]
    fn anonymous_denied() {
        let decision = decide(
            &FakeReads::default(),
            None,
            true,
            &Auth::Anonymous,
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("anonymous"));
    }

    // an engaged kill switch denies every write
    #[test]
    fn kill_switch() {
        let reads = FakeReads {
            killed: Ok(true),
            ..FakeReads::default()
        };
        let decision = decide(
            &reads,
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("kill switch"));
    }

    // an unknown access key with no bootstrap is denied
    #[test]
    fn unknown_key() {
        // credential = None and no bootstrap id configured -> deny.
        let decision = decide(
            &FakeReads::default(),
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("no active credential"));
    }

    // the bootstrap key resolves to the default owner and runs policy
    #[test]
    fn bootstrap_key() {
        // No store record, but the id matches the configured bootstrap key, so the
        // request is the bootstrap credential (default owner) and policy decides.
        let decision = decide(
            &FakeReads::default(),
            Some("BOOTSTRAP"),
            true,
            &verified("BOOTSTRAP"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(decision.allowed);
        assert_eq!(decision.owner, Address::default());
    }

    // a revoked or expired credential is denied
    #[test]
    fn unusable_credential() {
        let principal = Address::new_unique();
        let mut revoked =
            active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned);
        revoked.status = CredentialStatus::Revoked;
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(revoked)),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("revoked or expired"));
        assert_eq!(decision.owner, principal);

        // An expired (not_after in the past) credential denies the same way.
        let expired = Credential {
            not_after: Some(500),
            ..active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned)
        };
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(expired)),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("revoked or expired"));
    }

    // a credential lacking the op's cap is denied
    #[test]
    fn missing_capability() {
        let principal = Address::new_unique();
        let no_caps =
            active_credential(principal, CredentialCaps::none(), CredentialScope::AnyOwned);
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(no_caps)),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("not permitted"));
    }

    // a write outside the credential's bucket scope is denied
    #[test]
    fn unscoped_bucket() {
        let principal = Address::new_unique();
        let allowed_bucket = Address::new_unique();
        let scoped = active_credential(
            principal,
            CredentialCaps::all(),
            CredentialScope::Buckets(vec![allowed_bucket]),
        );
        let other_bucket = Address::new_unique();
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(scoped)),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            other_bucket,
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("scope does not include"));
    }

    // a valid credential is still denied by a deny policy
    #[test]
    fn deny_policy() {
        // The headline ordering case: a fully valid credential is still denied when
        // policy denies, and the policy reason is surfaced (not the credential's).
        let principal = Address::new_unique();
        let credential = active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned);
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(credential)),
                policy: Ok(PolicyDecision {
                    is_allowed: false,
                    reason: "explicit deny rule".to_string(),
                }),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert_eq!(decision.reason, "explicit deny rule");
        assert_eq!(decision.owner, principal);
    }

    // a valid credential with an allow policy is admitted
    #[test]
    fn allow_policy() {
        let principal = Address::new_unique();
        let credential = active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned);
        let decision = decide(
            &FakeReads {
                credential: Ok(Some(credential)),
                policy: Ok(PolicyDecision {
                    is_allowed: true,
                    reason: "allow rule".to_string(),
                }),
                ..FakeReads::default()
            },
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(decision.allowed);
        assert_eq!(decision.reason, "allow rule");
        assert_eq!(decision.owner, principal);
    }

    // --- fail-closed: a store error in any read denies, never allows -----------

    // a kill-switch read error denies fail-closed
    #[test]
    fn switch_error() {
        let reads = FakeReads {
            killed: Err("rocksdb unavailable".to_string()),
            ..FakeReads::default()
        };
        let decision = decide(
            &reads,
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("kill-switch state is unavailable"));
    }

    // a credential read error denies fail-closed
    #[test]
    fn credential_error() {
        let reads = FakeReads {
            credential: Err("rocksdb unavailable".to_string()),
            ..FakeReads::default()
        };
        let decision = decide(
            &reads,
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("credential store is unavailable"));
    }

    // a policy read error denies fail-closed
    #[test]
    fn policy_error() {
        let principal = Address::new_unique();
        let credential = active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned);
        let reads = FakeReads {
            credential: Ok(Some(credential)),
            policy: Err("rocksdb unavailable".to_string()),
            ..FakeReads::default()
        };
        let decision = decide(
            &reads,
            None,
            true,
            &verified("AKID"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("policy engine is unavailable"));
        assert_eq!(decision.owner, principal);
    }

    // --- decision core against the real TapeStore (the production AuthzReads) ---

    fn memory_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // against the real store, the default governs when no rule matches
    #[test]
    fn real_default() {
        let store = memory_store();
        let bucket = Address::new_unique();

        let denied = decide(
            &store,
            Some("BOOT"),
            false,
            &verified("BOOT"),
            bucket,
            WriteOp::Put,
            1_000,
        );
        assert!(!denied.allowed);
        assert_eq!(denied.reason, "default-deny");

        let allowed = decide(
            &store,
            Some("BOOT"),
            true,
            &verified("BOOT"),
            bucket,
            WriteOp::Put,
            1_000,
        );
        assert!(allowed.allowed);
        assert_eq!(allowed.reason, "default-allow");
    }

    // against the real store, the kill switch denies
    #[test]
    fn real_switch() {
        let store = memory_store();
        store.set_kill_switch(true).expect("test setup");
        let decision = decide(
            &store,
            Some("BOOT"),
            true,
            &verified("BOOT"),
            Address::new_unique(),
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert!(decision.reason.contains("kill switch"));
    }

    // against the real store, a deny rule wins over default-allow
    #[test]
    fn real_deny() {
        let store = memory_store();
        let principal = Address::new_unique();
        let bucket = Address::new_unique();
        store
            .put_credential(
                "AKID",
                &active_credential(principal, CredentialCaps::all(), CredentialScope::AnyOwned),
            )
            .expect("test setup");
        // Deny-precedence: a deny rule on this subject wins even with default-allow.
        store
            .put_policy_rule(
                PolicyRuleKey::new(1, 1),
                &PolicyRule {
                    principal: Some(principal),
                    bucket: Some(bucket),
                    action: PolicyAction::Put,
                    effect: PolicyEffect::Deny,
                    reason: "blocked by rule".to_string(),
                },
            )
            .expect("test setup");

        let decision = decide(
            &store,
            None,
            true,
            &verified("AKID"),
            bucket,
            WriteOp::Put,
            1_000,
        );
        assert!(!decision.allowed);
        assert_eq!(decision.reason, "blocked by rule");
        assert_eq!(decision.owner, principal);
    }

    // --- ledger reserve-outcome mapping (step 5) and the deny→S3Error mapping ---

    // a granted reserve carries the reservation forward
    #[test]
    fn reserve_grant() {
        let owner = Address::new_unique();
        let key = LedgerReservationKey::new(1_000, owner, 0);
        let (reservation, deny, throttle) =
            apply_reserve_outcome(ReserveOutcome::Granted(key), owner);
        assert!(reservation.is_some());
        assert!(deny.is_none());
        assert!(throttle.is_none());
    }

    // an over-budget reserve slows down with a retry-after
    #[test]
    fn reserve_slowdown() {
        let owner = Address::new_unique();
        let (reservation, deny, throttle) =
            apply_reserve_outcome(ReserveOutcome::SlowDown { retry_after_secs: 42 }, owner);
        assert!(reservation.is_none());
        let deny = deny.expect("a slow-down flips the decision to deny");
        assert!(!deny.allowed);
        assert!(deny.reason.contains("over budget"));
        assert_eq!(throttle, Some(42));
        // ... and over-budget maps to SlowDown (503), not AccessDenied.
        assert!(matches!(
            deny_error(deny.reason, throttle),
            S3Error::SlowDown { retry_after_seconds: 42 }
        ));
    }

    // a ceiling reserve denies without a throttle
    #[test]
    fn reserve_ceiling() {
        let owner = Address::new_unique();
        let (reservation, deny, throttle) =
            apply_reserve_outcome(ReserveOutcome::Ceiling("exceeds ceiling".to_string()), owner);
        assert!(reservation.is_none());
        let deny = deny.expect("a ceiling flips the decision to deny");
        assert!(!deny.allowed);
        assert_eq!(deny.reason, "exceeds ceiling");
        assert!(throttle.is_none());
        // A hard ceiling (no throttle) maps to AccessDenied (403).
        assert!(matches!(
            deny_error(deny.reason, throttle),
            S3Error::AccessDenied(_)
        ));
    }

    // a hard admission deny maps to AccessDenied without a throttle
    #[test]
    fn admission_hard_deny() {
        let owner = Address::new_unique();
        let deny = AdmissionDeny {
            reason: "balance exhausted".to_string(),
            retry_after_seconds: None,
        };

        let (decision, throttle) = apply_admission_deny(deny, owner);

        assert!(!decision.allowed);
        assert_eq!(decision.reason, "balance exhausted");
        assert_eq!(decision.owner, owner);
        assert!(throttle.is_none());
        assert!(matches!(
            deny_error(decision.reason, throttle),
            S3Error::AccessDenied(_)
        ));
    }

    // a transient admission deny maps to SlowDown with its retry-after
    #[test]
    fn admission_transient_deny() {
        let owner = Address::new_unique();
        let deny = AdmissionDeny {
            reason: "settlement backlog".to_string(),
            retry_after_seconds: Some(9),
        };

        let (decision, throttle) = apply_admission_deny(deny, owner);

        assert!(!decision.allowed);
        assert_eq!(throttle, Some(9));
        assert!(matches!(
            deny_error(decision.reason, throttle),
            S3Error::SlowDown { retry_after_seconds: 9 }
        ));
    }

    // a throttle maps to SlowDown, otherwise AccessDenied
    #[test]
    fn deny_mapping() {
        assert!(matches!(
            deny_error("x".to_string(), Some(7)),
            S3Error::SlowDown { retry_after_seconds: 7 }
        ));
        assert!(matches!(
            deny_error("x".to_string(), None),
            S3Error::AccessDenied(_)
        ));
    }
}
