//! Accounting glue for the write chokepoint.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_node::context::NodeContext;
use tape_protocol::Api;
use tape_store::ops::{AuthStateOps, LedgerOps, ReserveOutcome, ReserveRequest};
use tape_store::types::{BudgetLimits, LedgerReservationKey};
use tape_store::TapeStore;

use super::clock::now_unix;
use crate::http::state::AppState;


/// How long a fetched on-chain tape precondition stays cached.
const TAPE_PRECONDITION_TTL: Duration = Duration::from_secs(10);

/// Age at which an uncommitted reservation is presumed orphaned.
pub const RESERVATION_TTL_SECS: i64 = 300;

/// How often the reservation TTL sweep runs.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// In-process accounting state shared (behind an `Arc`) across the S3 listeners.
#[derive(Default)]
pub struct Accounting {
    /// Serializes the ledger read-modify-write (see module docs)
    ledger_lock: Mutex<()>,
    /// Short-TTL cache of the on-chain write precondition, keyed by bucket tape
    tape_cache: Mutex<HashMap<Address, CachedTape>>,
    /// Process-monotonic source of audit-log sequence numbers, keeping every
    /// audit key unique so concurrent appends never clobber each other.
    audit_sequence: AtomicU64,
}

impl Accounting {
    /// Create empty in-process accounting state
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the audit-sequence counter so it resumes above every existing entry.
    pub fn seed_audit_sequence(&self, next: u64) {
        self.audit_sequence.store(next, Ordering::Relaxed);
    }

    /// Allocate the next unique audit-log sequence number.
    pub fn next_audit_sequence(&self) -> u64 {
        self.audit_sequence.fetch_add(1, Ordering::Relaxed)
    }
}

/// A cached on-chain tape precondition snapshot
#[derive(Clone, Copy)]
struct CachedTape {
    expires_at: Instant,
    state: TapeState,
}

/// The on-chain fields the write precondition checks
#[derive(Clone, Copy)]
struct TapeState {
    delegate: Address,
    expiry_epoch: EpochNumber,
    capacity_bytes: u64,
    used_bytes: u64,
}

/// Lock the ledger RMW mutex.
fn lock_ledger(accounting: &Accounting) -> std::sync::MutexGuard<'_, ()> {
    accounting
        .ledger_lock
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Map a fail-closed backend error to its sanitized deny reason.
fn unavailable<Error: std::fmt::Display>(reason: &'static str) -> impl FnOnce(Error) -> String {
    move |error| {
        tracing::warn!(%error, "s3 accounting: {reason}");
        reason.to_string()
    }
}

/// The effective default budget.
fn default_budget<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
) -> Result<BudgetLimits, String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let configured = &state.context.config.gateway.s3.write.budgets;
    let yaml = BudgetLimits {
        sol_per_day: configured.sol_per_day,
        bytes_per_day: configured.bytes_per_day,
        puts_per_hour: configured.puts_per_hour,
        max_concurrent_multipart: configured.max_concurrent_multipart,
    };
    let auth_state = state
        .context
        .store
        .get_auth_state()
        .map_err(unavailable("auth state unavailable"))?;
    Ok(auth_state.default_budget.unwrap_or(yaml))
}

/// Run `critical` while holding the ledger RMW lock.
pub fn with_ledger_lock<T>(accounting: &Accounting, critical: impl FnOnce() -> T) -> T {
    let _guard = lock_ledger(accounting);
    critical()
}

/// The effective `max_concurrent_multipart` budget for `principal`.
pub fn concurrent_multipart_limit<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    principal: &Address,
) -> Result<u32, String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let default = default_budget(state)?;
    let entry = state
        .context
        .store
        .get_ledger(principal)
        .map_err(unavailable("ledger unavailable"))?;
    Ok(entry
        .budget_override
        .unwrap_or(default)
        .max_concurrent_multipart)
}

/// Reserve a budget estimate for one write, atomically under the ledger lock.
pub fn reserve_budget<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    principal: Address,
    request: ReserveRequest,
    now: i64,
) -> Result<ReserveOutcome, String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let budget = default_budget(state)?;
    let _guard = lock_ledger(state.accounting.as_ref());
    state
        .context
        .store
        .reserve(&principal, request, budget, now)
        .map_err(unavailable("ledger unavailable"))
}

/// Reconcile a reservation to its actual cost and commit it.
pub fn commit_budget<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    key: &LedgerReservationKey,
    actual_bytes: u64,
    now: i64,
) where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let _guard = lock_ledger(state.accounting.as_ref());
    match state.context.store.commit(key, actual_bytes, now) {
        Ok(true) => {}
        Ok(false) => tracing::warn!(
            "s3 accounting: reservation already reclaimed at commit; write went unbilled \
             (raise gateway.s3.write.budgets sweep ttl_secs above write latency)"
        ),
        Err(error) => {
            tracing::warn!(%error, "s3 accounting: failed to commit reservation (will be swept)")
        }
    }
}

/// Release a reservation without billing, under the ledger lock.
pub fn refund_budget<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    key: &LedgerReservationKey,
) where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let _guard = lock_ledger(state.accounting.as_ref());
    if let Err(error) = state.context.store.refund(key) {
        tracing::warn!(%error, "s3 accounting: failed to refund reservation (will be swept)");
    }
}

/// Reclaim orphaned reservations older than the TTL.
pub fn sweep_reservations<S: Store>(
    accounting: &Accounting,
    store: &TapeStore<S>,
    now: i64,
    ttl_secs: i64,
) -> usize {
    let _guard = lock_ledger(accounting);
    match store.sweep_reservations(now, ttl_secs) {
        Ok(count) => count,
        Err(error) => {
            tracing::error!(%error, "s3 accounting: reservation sweep failed");
            0
        }
    }
}

/// Background loop that runs the reservation TTL sweep.
pub async fn reservation_sweep_loop<S: Store>(
    accounting: Arc<Accounting>,
    store: Arc<TapeStore<S>>,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            // Safe: `cancelled` is cancellation-safe and holds no state; losing this
            // branch just means another branch ran first and the loop re-selects.
            _ = cancel.cancelled() => break,
            // Safe: `Interval::tick` is cancellation-safe (the next tick is recomputed
            // on the following poll); the synchronous sweep runs only after the tick
            // resolves, so a dropped branch never leaves a partial sweep.
            _ = ticker.tick() => {
                let reclaimed = sweep_reservations(&accounting, &store, now_unix(), RESERVATION_TTL_SECS);
                if reclaimed > 0 {
                    tracing::warn!(reclaimed, "s3 accounting: reclaimed orphaned write reservations");
                }
            }
        }
    }
}

/// Verify the on-chain write precondition for `tape`: our delegate is set, the
/// tape is not expired, and it has at least `size_bytes` remaining capacity.
pub async fn check_onchain_precondition<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    size_bytes: u64,
) -> Result<(), String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    // The value the tape's `delegate` must equal for our signer to be an operator.
    let our_delegate = match state.write_ctx.as_ref() {
        Some(write_ctx) => write_ctx.delegate_address(),
        None => return Err("gateway has no delegate signing key configured".to_string()),
    };
    let current_epoch = state.context.state().epoch();

    // Fast path: a fresh cached snapshot avoids the RPC entirely.
    if let Some(cached) = cache_get(state.accounting.as_ref(), &tape) {
        return evaluate_precondition(&cached, our_delegate, current_epoch, size_bytes, &tape);
    }

    // Cache miss: fetch on-chain. The await is deliberately outside any lock.
    let tape_account = fetch_tape_state(&state.context, &tape).await?;
    cache_put(state.accounting.as_ref(), tape, tape_account);
    evaluate_precondition(&tape_account, our_delegate, current_epoch, size_bytes, &tape)
}

/// Fetch the on-chain tape precondition fields, mapping any RPC failure to a
/// fail-closed deny reason
async fn fetch_tape_state<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    tape: &Address,
) -> Result<TapeState, String>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let account = context
        .rpc
        .get_tape_by_address(tape)
        .await
        .map_err(unavailable("on-chain tape state unavailable"))?;
    Ok(TapeState {
        delegate: account.delegate,
        expiry_epoch: account.expiry_epoch,
        capacity_bytes: account.capacity.to_bytes(),
        used_bytes: account.used.to_bytes(),
    })
}

/// Evaluate the precondition against a (cached or freshly fetched) tape snapshot
fn evaluate_precondition(
    tape_state: &TapeState,
    our_delegate: Address,
    current_epoch: EpochNumber,
    size_bytes: u64,
    tape: &Address,
) -> Result<(), String> {
    if tape_state.delegate != our_delegate {
        return Err(format!(
            "bucket tape {tape} has not delegated writes to this gateway"
        ));
    }
    if tape_state.expiry_epoch <= current_epoch {
        return Err(format!("bucket tape {tape} has expired"));
    }
    let remaining = tape_state.capacity_bytes.saturating_sub(tape_state.used_bytes);
    if remaining < size_bytes {
        // No S3 code maps cleanly to "insufficient storage" (507).
        return Err(format!(
            "bucket tape {tape} has insufficient remaining capacity ({remaining} < {size_bytes} bytes)"
        ));
    }
    Ok(())
}

/// Read a fresh (unexpired) cached tape snapshot, if any
fn cache_get(accounting: &Accounting, tape: &Address) -> Option<TapeState> {
    let cache = accounting
        .tape_cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    cache
        .get(tape)
        .filter(|entry| entry.expires_at > Instant::now())
        .map(|entry| entry.state)
}

/// Cache a freshly fetched tape snapshot with the precondition TTL
fn cache_put(accounting: &Accounting, tape: Address, state: TapeState) {
    let mut cache = accounting
        .tape_cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    cache.insert(
        tape,
        CachedTape {
            expires_at: Instant::now() + TAPE_PRECONDITION_TTL,
            state,
        },
    );
}
