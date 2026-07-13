//! S3 write-authorization admin control plane.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Json, Path, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use serde::{Deserialize, Serialize};
use serde_json::json;

use rpc::Rpc;
use store::Store;
use tape_crypto::address::Address;
use tape_node::context::NodeContext;
use tape_protocol::Api;
use tape_store::ops::{AuditOps, AuthStateOps, CredentialOps, LedgerOps, PolicyOps};
use tape_store::types::{
    AuditDecision, AuditEntry, AuditOp, BudgetLimits, Credential, CredentialCaps, CredentialScope,
    CredentialStatus, LedgerEntry, PolicyAction, PolicyEffect, PolicyRule, PolicyRuleKey,
};
use tape_store::TapeStore;

use super::accounting::{with_ledger_lock, Accounting};
use super::authz::peppered_secret_hmac;
use super::clock::now_unix;
use super::sigv4::constant_time_eq;

/// Shared state for the admin control-plane router.
pub struct AdminState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    /// Node context shared by every admin handler: the backing store plus gateway config
    pub context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    /// Accounting state shared with the S3 data plane: its ledger lock serializes
    /// control-plane mutations against live reserve/commit, and its sequence
    /// counter keeps audit keys unique.
    pub accounting: Arc<Accounting>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Clone for AdminState<Db, Cluster, Blockchain> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            accounting: self.accounting.clone(),
        }
    }
}

/// Build the admin control-plane router, gated by the operator-token middleware
pub fn admin_router<Db, Cluster, Blockchain>(
    state: AdminState<Db, Cluster, Blockchain>,
) -> Router
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    Router::new()
        .route(
            "/credentials",
            post(create_credential::<Db, Cluster, Blockchain>)
                .get(list_credentials::<Db, Cluster, Blockchain>),
        )
        .route(
            "/credentials/{access_key_id}",
            delete(revoke_credential::<Db, Cluster, Blockchain>),
        )
        .route(
            "/credentials/{access_key_id}/grade",
            put(set_credential_grade::<Db, Cluster, Blockchain>),
        )
        .route(
            "/policy/rules",
            post(create_policy_rule::<Db, Cluster, Blockchain>)
                .get(list_policy_rules::<Db, Cluster, Blockchain>),
        )
        .route(
            "/policy/rules/{priority}/{id}",
            delete(delete_policy_rule::<Db, Cluster, Blockchain>),
        )
        .route(
            "/kill-switch",
            get(get_kill_switch::<Db, Cluster, Blockchain>)
                .post(set_kill_switch::<Db, Cluster, Blockchain>),
        )
        .route(
            "/budgets",
            get(get_budgets::<Db, Cluster, Blockchain>)
                .put(set_budgets::<Db, Cluster, Blockchain>),
        )
        .route(
            "/ledger/{principal}",
            get(get_ledger::<Db, Cluster, Blockchain>),
        )
        .route(
            "/ledger/{principal}/budget",
            put(set_principal_budget::<Db, Cluster, Blockchain>)
                .delete(clear_principal_budget::<Db, Cluster, Blockchain>),
        )
        .with_state(state.clone())
        .layer(from_fn_with_state(
            state,
            operator_auth::<Db, Cluster, Blockchain>,
        ))
}

/// Operator-token gate. Requires `Authorization: Bearer <operator_token>` to
/// match `gateway.s3.write.admin.operator_token` in constant time. Fail-closed:
/// when no token is configured, every request is rejected (the listener should
/// not have been started, but this is the belt-and-braces second gate)
async fn operator_auth<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    request: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let configured = state
        .context
        .config
        .gateway
        .s3
        .write
        .admin
        .operator_token
        .as_deref()
        .filter(|token| !token.is_empty());

    let Some(configured) = configured else {
        return AdminError::new(
            StatusCode::FORBIDDEN,
            "admin control plane is not configured (no operator token)",
        )
        .into_response();
    };

    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);

    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), configured.as_bytes()) => {
            next.run(request).await
        }
        Some(_) | None => {
            AdminError::new(StatusCode::FORBIDDEN, "invalid or missing operator token")
                .into_response()
        }
    }
}

// Credentials

/// `POST /credentials` — issue or rotate a credential
async fn create_credential<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Json(request): Json<CreateCredentialRequest>,
) -> Result<Json<CredentialSummary>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let pepper = state
        .context
        .config
        .gateway
        .s3
        .write
        .pepper
        .as_deref()
        .filter(|pepper| !pepper.is_empty())
        .ok_or_else(|| {
            AdminError::new(
                StatusCode::PRECONDITION_FAILED,
                "server pepper is not configured (gateway.s3.write.pepper); cannot issue credentials",
            )
        })?;

    if request.access_key_id.is_empty() {
        return Err(AdminError::bad_request("access_key_id must not be empty"));
    }
    if request.secret_access_key.is_empty() {
        return Err(AdminError::bad_request("secret_access_key must not be empty"));
    }
    let principal = parse_address(&request.principal, "principal")?;
    let scope = request.scope.try_into_scope()?;
    let caps = CredentialCaps {
        can_put: request.caps.can_put,
        can_delete: request.caps.can_delete,
        can_multipart: request.caps.can_multipart,
    };
    if let Some(grade) = request.grade.as_deref() {
        require_known_grade(&state, grade)?;
    }
    let credential = Credential {
        secret_hmac: peppered_secret_hmac(pepper, &request.secret_access_key)
            .map_err(|_| AdminError::internal("credential secret hashing failed"))?,
        principal,
        scope,
        caps,
        status: CredentialStatus::Active,
        not_after: request.not_after,
        grade: request.grade,
    };

    let store = state.context.store.as_ref();
    store
        .put_credential(&request.access_key_id, &credential)
        .map_err(|error| AdminError::internal(format!("credential store: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        principal,
        format!("issue_credential access_key_id={}", request.access_key_id),
    )?;

    Ok(Json(summarize_credential(&request.access_key_id, &credential)))
}

/// `DELETE /credentials/{access_key_id}` — revoke a credential (durable, instant)
async fn revoke_credential<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path(access_key_id): Path<String>,
) -> Result<Json<RevokeResponse>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let store = state.context.store.as_ref();
    let was_present = store
        .revoke_credential(&access_key_id)
        .map_err(|error| AdminError::internal(format!("credential store: {error}")))?;
    if !was_present {
        return Err(AdminError::not_found("no such credential"));
    }
    audit_admin(
        store,
        &state.accounting,
        Address::default(),
        format!("revoke_credential access_key_id={access_key_id}"),
    )?;
    Ok(Json(RevokeResponse {
        access_key_id,
        is_revoked: true,
    }))
}

/// `PUT /credentials/{access_key_id}/grade` — assign or clear the metering
/// grade a credential reads under. Applies on the next read; no restart.
async fn set_credential_grade<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path(access_key_id): Path<String>,
    Json(request): Json<SetGradeRequest>,
) -> Result<Json<CredentialSummary>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if let Some(grade) = request.grade.as_deref() {
        require_known_grade(&state, grade)?;
    }
    let store = state.context.store.as_ref();
    let mut credential = store
        .get_credential(&access_key_id)
        .map_err(|error| AdminError::internal(format!("credential store: {error}")))?
        .ok_or_else(|| AdminError::not_found("no such credential"))?;
    credential.grade = request.grade;
    store
        .put_credential(&access_key_id, &credential)
        .map_err(|error| AdminError::internal(format!("credential store: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        credential.principal,
        format!(
            "set_credential_grade access_key_id={access_key_id} grade={}",
            credential.grade.as_deref().unwrap_or("default")
        ),
    )?;
    Ok(Json(summarize_credential(&access_key_id, &credential)))
}

/// Reject a grade name that is not defined under `gateway.metering.grades`
fn require_known_grade<Db, Cluster, Blockchain>(
    state: &AdminState<Db, Cluster, Blockchain>,
    grade: &str,
) -> Result<(), AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if state
        .context
        .config
        .gateway
        .metering
        .grades
        .contains_key(grade)
    {
        Ok(())
    } else {
        Err(AdminError::bad_request(format!(
            "unknown metering grade `{grade}`"
        )))
    }
}

/// `GET /credentials` — list credentials (never the secret HMAC)
async fn list_credentials<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
) -> Result<Json<Vec<CredentialSummary>>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let credentials = state
        .context
        .store
        .list_credentials()
        .map_err(|error| AdminError::internal(format!("credential store: {error}")))?;
    let mut summaries: Vec<CredentialSummary> = Vec::new();
    for (id, credential) in &credentials {
        summaries.push(summarize_credential(id, credential));
    }
    Ok(Json(summaries))
}

// Policy

/// `POST /policy/rules` — add or overwrite a policy rule; bumps the policy version
async fn create_policy_rule<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Json(request): Json<CreatePolicyRuleRequest>,
) -> Result<Json<PolicyRuleView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let principal = parse_optional_address(request.principal.as_deref(), "principal")?;
    let bucket = parse_optional_address(request.bucket.as_deref(), "bucket")?;
    let rule = PolicyRule {
        principal,
        bucket,
        action: request.action.into(),
        effect: request.effect.into(),
        reason: request.reason,
    };
    let key = PolicyRuleKey::new(request.priority, request.id);

    let store = state.context.store.as_ref();
    store
        .put_policy_rule(key, &rule)
        .map_err(|error| AdminError::internal(format!("policy store: {error}")))?;
    let version = with_ledger_lock(&state.accounting, || store.bump_policy_version())
        .map_err(|error| AdminError::internal(format!("policy store: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        principal.unwrap_or_default(),
        format!(
            "put_policy_rule priority={} id={} effect={:?} version={version}",
            request.priority, request.id, rule.effect
        ),
    )?;

    Ok(Json(view_policy_rule(key, &rule)))
}

/// `DELETE /policy/rules/{priority}/{id}` — remove a rule; bumps the policy version
async fn delete_policy_rule<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path((priority, id)): Path<(u32, u64)>,
) -> Result<Json<DeleteRuleResponse>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let key = PolicyRuleKey::new(priority, id);
    let store = state.context.store.as_ref();
    let was_present = store
        .delete_policy_rule(&key)
        .map_err(|error| AdminError::internal(format!("policy store: {error}")))?;
    if !was_present {
        return Err(AdminError::not_found("no such policy rule"));
    }
    let version = with_ledger_lock(&state.accounting, || store.bump_policy_version())
        .map_err(|error| AdminError::internal(format!("policy store: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        Address::default(),
        format!("delete_policy_rule priority={priority} id={id} version={version}"),
    )?;
    Ok(Json(DeleteRuleResponse {
        priority,
        id,
        is_deleted: true,
    }))
}

/// `GET /policy/rules` — list the ruleset in priority order
async fn list_policy_rules<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
) -> Result<Json<Vec<PolicyRuleView>>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let rules = state
        .context
        .store
        .list_policy_rules()
        .map_err(|error| AdminError::internal(format!("policy store: {error}")))?;
    let mut views: Vec<PolicyRuleView> = Vec::new();
    for (key, rule) in &rules {
        views.push(view_policy_rule(*key, rule));
    }
    Ok(Json(views))
}

// Kill switch

/// `GET /kill-switch` — report whether all writes are paused
async fn get_kill_switch<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
) -> Result<Json<KillSwitchView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let is_kill_switch_engaged = state
        .context
        .store
        .is_write_killed()
        .map_err(|error| AdminError::internal(format!("auth state: {error}")))?;
    Ok(Json(KillSwitchView {
        is_kill_switch_engaged,
    }))
}

/// `POST /kill-switch` — engage or release the global write kill switch
async fn set_kill_switch<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Json(request): Json<KillSwitchView>,
) -> Result<Json<KillSwitchView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let store = state.context.store.as_ref();
    with_ledger_lock(&state.accounting, || {
        store.set_kill_switch(request.is_kill_switch_engaged)
    })
    .map_err(|error| AdminError::internal(format!("auth state: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        Address::default(),
        format!("kill_switch engaged={}", request.is_kill_switch_engaged),
    )?;
    Ok(Json(KillSwitchView {
        is_kill_switch_engaged: request.is_kill_switch_engaged,
    }))
}

// Budgets

/// `GET /budgets` — the persisted default-budget override, or the YAML defaults
/// when no override is set
async fn get_budgets<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
) -> Result<Json<BudgetView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let auth_state = state
        .context
        .store
        .get_auth_state()
        .map_err(|error| AdminError::internal(format!("auth state: {error}")))?;
    let view = match auth_state.default_budget {
        Some(budget) => BudgetView::from(budget),
        None => {
            let defaults = &state.context.config.gateway.s3.write.budgets;
            BudgetView {
                sol_per_day: defaults.sol_per_day,
                bytes_per_day: defaults.bytes_per_day,
                puts_per_hour: defaults.puts_per_hour,
                max_concurrent_multipart: defaults.max_concurrent_multipart,
            }
        }
    };
    Ok(Json(view))
}

/// `PUT /budgets` — persist a default-budget override (enforced by the accounting
/// ledger.
async fn set_budgets<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Json(request): Json<BudgetView>,
) -> Result<Json<BudgetView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let limits = BudgetLimits {
        sol_per_day: request.sol_per_day,
        bytes_per_day: request.bytes_per_day,
        puts_per_hour: request.puts_per_hour,
        max_concurrent_multipart: request.max_concurrent_multipart,
    };
    let store = state.context.store.as_ref();
    with_ledger_lock(&state.accounting, || store.set_default_budget(limits))
        .map_err(|error| AdminError::internal(format!("auth state: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        Address::default(),
        format!(
            "set_default_budget sol_per_day={} bytes_per_day={} puts_per_hour={} max_concurrent_multipart={}",
            request.sol_per_day, request.bytes_per_day, request.puts_per_hour, request.max_concurrent_multipart
        ),
    )?;
    Ok(Json(request))
}

// Per-principal accounting ledger

/// `GET /ledger/{principal}` — a principal's accounting usage and budget override
async fn get_ledger<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path(principal): Path<String>,
) -> Result<Json<LedgerView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let principal = parse_address(&principal, "principal")?;
    let entry = state
        .context
        .store
        .get_ledger(&principal)
        .map_err(|error| AdminError::internal(format!("ledger store: {error}")))?;
    Ok(Json(LedgerView::from_entry(&principal, &entry)))
}

/// `PUT /ledger/{principal}/budget` — set a per-principal budget override (takes
/// precedence over the default for that principal)
async fn set_principal_budget<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path(principal): Path<String>,
    Json(request): Json<BudgetView>,
) -> Result<Json<BudgetView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let principal = parse_address(&principal, "principal")?;
    let limits = BudgetLimits {
        sol_per_day: request.sol_per_day,
        bytes_per_day: request.bytes_per_day,
        puts_per_hour: request.puts_per_hour,
        max_concurrent_multipart: request.max_concurrent_multipart,
    };
    let store = state.context.store.as_ref();
    with_ledger_lock(&state.accounting, || {
        store.set_principal_budget(&principal, Some(limits))
    })
    .map_err(|error| AdminError::internal(format!("ledger store: {error}")))?;
    audit_admin(
        store,
        &state.accounting,
        principal,
        format!(
            "set_principal_budget sol_per_day={} bytes_per_day={} puts_per_hour={} max_concurrent_multipart={}",
            request.sol_per_day, request.bytes_per_day, request.puts_per_hour, request.max_concurrent_multipart
        ),
    )?;
    Ok(Json(request))
}

/// `DELETE /ledger/{principal}/budget` — clear a per-principal budget override
/// (the principal falls back to the default budget)
async fn clear_principal_budget<Db, Cluster, Blockchain>(
    State(state): State<AdminState<Db, Cluster, Blockchain>>,
    Path(principal): Path<String>,
) -> Result<Json<LedgerView>, AdminError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let principal = parse_address(&principal, "principal")?;
    let store = state.context.store.as_ref();
    with_ledger_lock(&state.accounting, || store.set_principal_budget(&principal, None))
        .map_err(|error| AdminError::internal(format!("ledger store: {error}")))?;
    audit_admin(store, &state.accounting, principal, "clear_principal_budget".to_string())?;
    let entry = store
        .get_ledger(&principal)
        .map_err(|error| AdminError::internal(format!("ledger store: {error}")))?;
    Ok(Json(LedgerView::from_entry(&principal, &entry)))
}

// Request / response bodies
#[derive(Deserialize)]
struct CreateCredentialRequest {
    /// Access key id clients embed in their credential scope
    access_key_id: String,
    /// Secret the client signs with; only its peppered HMAC is stored
    secret_access_key: String,
    /// Owner authority pubkey this credential acts on behalf of
    principal: String,
    /// Buckets this credential may write to; omitted allows any owned bucket
    #[serde(default)]
    scope: ScopeSpec,
    /// Write operations this credential may perform
    caps: CapsSpec,
    /// Optional expiry as a unix timestamp; omitted never expires
    #[serde(default)]
    not_after: Option<i64>,
    /// Metering grade this key reads under; omitted uses the operator default
    #[serde(default)]
    grade: Option<String>,
}

#[derive(Deserialize)]
struct SetGradeRequest {
    /// Grade to assign; null clears the assignment back to the default
    grade: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum ScopeSpec {
    /// Any bucket whose on-chain authority is this credential's principal
    #[default]
    AnyOwned,
    /// An explicit allow-list of bucket tape addresses (base58)
    Buckets { buckets: Vec<String> },
}

impl ScopeSpec {
    fn try_into_scope(self) -> Result<CredentialScope, AdminError> {
        match self {
            ScopeSpec::AnyOwned => Ok(CredentialScope::AnyOwned),
            ScopeSpec::Buckets { buckets } => {
                let mut parsed: Vec<Address> = Vec::new();
                for bucket in &buckets {
                    parsed.push(parse_address(bucket, "scope bucket")?);
                }
                Ok(CredentialScope::Buckets(parsed))
            }
        }
    }
}

#[derive(Deserialize)]
struct CapsSpec {
    #[serde(default)]
    can_put: bool,
    #[serde(default)]
    can_delete: bool,
    #[serde(default)]
    can_multipart: bool,
}

#[derive(Serialize)]
struct CredentialSummary {
    access_key_id: String,
    principal: String,
    status: String,
    caps: CapsView,
    scope: ScopeView,
    not_after: Option<i64>,
    grade: Option<String>,
}

#[derive(Serialize)]
struct CapsView {
    can_put: bool,
    can_delete: bool,
    can_multipart: bool,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum ScopeView {
    AnyOwned,
    Buckets { buckets: Vec<String> },
}

#[derive(Serialize)]
struct RevokeResponse {
    access_key_id: String,
    is_revoked: bool,
}

#[derive(Deserialize)]
struct CreatePolicyRuleRequest {
    priority: u32,
    id: u64,
    #[serde(default)]
    principal: Option<String>,
    #[serde(default)]
    bucket: Option<String>,
    action: PolicyActionSpec,
    effect: PolicyEffectSpec,
    reason: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum PolicyActionSpec {
    Any,
    Put,
    Delete,
    Multipart,
}

impl From<PolicyActionSpec> for PolicyAction {
    fn from(spec: PolicyActionSpec) -> Self {
        match spec {
            PolicyActionSpec::Any => PolicyAction::Any,
            PolicyActionSpec::Put => PolicyAction::Put,
            PolicyActionSpec::Delete => PolicyAction::Delete,
            PolicyActionSpec::Multipart => PolicyAction::Multipart,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum PolicyEffectSpec {
    Allow,
    Deny,
}

impl From<PolicyEffectSpec> for PolicyEffect {
    fn from(spec: PolicyEffectSpec) -> Self {
        match spec {
            PolicyEffectSpec::Allow => PolicyEffect::Allow,
            PolicyEffectSpec::Deny => PolicyEffect::Deny,
        }
    }
}

#[derive(Serialize)]
struct PolicyRuleView {
    priority: u32,
    id: u64,
    principal: Option<String>,
    bucket: Option<String>,
    action: String,
    effect: String,
    reason: String,
}

#[derive(Serialize)]
struct DeleteRuleResponse {
    priority: u32,
    id: u64,
    is_deleted: bool,
}

#[derive(Deserialize, Serialize)]
struct KillSwitchView {
    is_kill_switch_engaged: bool,
}

#[derive(Deserialize, Serialize)]
struct BudgetView {
    sol_per_day: u64,
    bytes_per_day: u64,
    puts_per_hour: u32,
    max_concurrent_multipart: u32,
}

impl From<BudgetLimits> for BudgetView {
    fn from(limits: BudgetLimits) -> Self {
        Self {
            sol_per_day: limits.sol_per_day,
            bytes_per_day: limits.bytes_per_day,
            puts_per_hour: limits.puts_per_hour,
            max_concurrent_multipart: limits.max_concurrent_multipart,
        }
    }
}

/// A principal's accounting ledger: outstanding reservations, windowed committed
/// usage, lifetime meters, and any per-principal budget override
#[derive(Serialize)]
struct LedgerView {
    principal: String,
    /// Per-principal budget override, or `null` when the principal uses the
    /// default budget
    budget_override: Option<BudgetView>,
    writes_reserved: u32,
    bytes_reserved: u64,
    sol_reserved: u64,
    writes_committed_hour: u32,
    bytes_committed_day: u64,
    sol_committed_day: u64,
    writes_total: u64,
    bytes_total: u64,
    onchain_ops_total: u64,
    sol_spent_total: u64,
    capacity_consumed_total: u64,
}

impl LedgerView {
    fn from_entry(principal: &Address, entry: &LedgerEntry) -> Self {
        Self {
            principal: principal.to_string(),
            budget_override: entry.budget_override.map(BudgetView::from),
            writes_reserved: entry.writes_reserved,
            bytes_reserved: entry.bytes_reserved,
            sol_reserved: entry.sol_reserved,
            writes_committed_hour: entry.writes_committed,
            bytes_committed_day: entry.bytes_committed,
            sol_committed_day: entry.sol_committed,
            writes_total: entry.writes_total,
            bytes_total: entry.bytes_total,
            onchain_ops_total: entry.onchain_ops_total,
            sol_spent_total: entry.sol_spent_total,
            capacity_consumed_total: entry.capacity_consumed_total,
        }
    }
}

// Helpers

/// Build a CredentialSummary from a stored credential, omitting the secret
/// HMAC
fn summarize_credential(access_key_id: &str, credential: &Credential) -> CredentialSummary {
    let scope = match &credential.scope {
        CredentialScope::AnyOwned => ScopeView::AnyOwned,
        CredentialScope::Buckets(buckets) => {
            let mut names: Vec<String> = Vec::new();
            for bucket in buckets {
                names.push(bucket.to_string());
            }
            ScopeView::Buckets { buckets: names }
        }
    };
    CredentialSummary {
        access_key_id: access_key_id.to_string(),
        principal: credential.principal.to_string(),
        status: match credential.status {
            CredentialStatus::Active => "active",
            CredentialStatus::Revoked => "revoked",
        }
        .to_string(),
        caps: CapsView {
            can_put: credential.caps.can_put,
            can_delete: credential.caps.can_delete,
            can_multipart: credential.caps.can_multipart,
        },
        scope,
        not_after: credential.not_after,
        grade: credential.grade.clone(),
    }
}

/// Build a PolicyRuleView from a stored rule and its key
fn view_policy_rule(key: PolicyRuleKey, rule: &PolicyRule) -> PolicyRuleView {
    PolicyRuleView {
        priority: key.priority,
        id: key.id,
        principal: rule.principal.map(|address| address.to_string()),
        bucket: rule.bucket.map(|address| address.to_string()),
        action: match rule.action {
            PolicyAction::Any => "any",
            PolicyAction::Put => "put",
            PolicyAction::Delete => "delete",
            PolicyAction::Multipart => "multipart",
        }
        .to_string(),
        effect: match rule.effect {
            PolicyEffect::Allow => "allow",
            PolicyEffect::Deny => "deny",
        }
        .to_string(),
        reason: rule.reason.clone(),
    }
}

/// Append an admin-mutation entry to the audit log. The mutation has already
/// been applied durably; a failure to record it surfaces as a 500 so the
/// operator notices the missing audit trail
fn audit_admin<S: Store>(
    store: &TapeStore<S>,
    accounting: &Accounting,
    principal: Address,
    reason: String,
) -> Result<(), AdminError> {
    let entry = AuditEntry {
        timestamp: now_unix(),
        principal,
        bucket: Address::default(),
        op: AuditOp::Admin,
        decision: AuditDecision::Allow,
        reason,
    };
    store
        .append_audit(&entry, accounting.next_audit_sequence())
        .map_err(|error| AdminError::internal(format!("audit log unavailable: {error}")))
}

/// Parse a required base58 Address field, mapping a parse failure to a 400
fn parse_address(value: &str, field: &str) -> Result<Address, AdminError> {
    value
        .parse()
        .map_err(|_| AdminError::bad_request(format!("invalid {field} address")))
}

/// Parse an optional base58 Address field
fn parse_optional_address(value: Option<&str>, field: &str) -> Result<Option<Address>, AdminError> {
    match value {
        Some(value) => Ok(Some(parse_address(value, field)?)),
        None => Ok(None),
    }
}


/// A JSON-rendered admin error: `{ "error": "<message>" }` with an HTTP status
#[derive(Debug)]
struct AdminError {
    status: StatusCode,
    message: String,
}

impl AdminError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            tracing::error!(status = %self.status, "s3 admin error: {}", self.message);
        }
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // an unset scope defaults to AnyOwned
    #[test]
    fn scope_default() {
        assert!(matches!(
            ScopeSpec::default().try_into_scope().expect("test setup"),
            CredentialScope::AnyOwned
        ));
    }

    // an explicit bucket scope parses and rejects bad addresses
    #[test]
    fn scope_buckets() {
        let spec = ScopeSpec::Buckets {
            buckets: vec!["11111111111111111111111111111111".to_string()],
        };
        match spec.try_into_scope().expect("test setup") {
            CredentialScope::Buckets(buckets) => assert_eq!(buckets.len(), 1),
            CredentialScope::AnyOwned => panic!("expected explicit buckets"),
        }

        let bad = ScopeSpec::Buckets {
            buckets: vec!["not-an-address!".to_string()],
        };
        assert!(bad.try_into_scope().is_err());
    }
}
