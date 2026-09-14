//! Process orchestration: build state, expose HTTP observability immediately,
//! run bootstrap to completion, then spawn live-tail tasks under a shared
//! shutdown token.
//!
//! Bootstrap is still required before the cache is considered healthy, but
//! `/v1/health` and `/v1/stats` are reachable while it catches up.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use rpc::skip::{TRUSTED_SKIP_RUN, slots_to_fetch, trusted_skip};
use serde_json::{Value, json};
use solana_client::rpc_config::RpcBlockConfig;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding};
use tape_api::program::tapedrive::{epoch_pda, SYSTEM_ADDRESS};
use tape_api::state::{Epoch, System};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cache::{CacheStore, Policy};
use crate::config::Config;
use crate::filter::filter_block;
use crate::server::{AppState, CacheStats, CachedBlock};
use crate::upstream::{RateLimitWait, Upstream, UpstreamError};

/// A rooted skip: permanent, stored as a tombstone
const SKIPPED_SLOT_CODE: i64 = -32007;
/// No block at this commitment yet, which is also how a skip reads until it roots
const BLOCK_NOT_AVAILABLE_CODE: i64 = -32004;
/// Skipped, or missing from long-term storage; only the confirmed listing tells which
const LONG_TERM_STORAGE_SLOT_SKIPPED_CODE: i64 = -32009;

const BOOTSTRAP_CONCURRENCY: usize = 16;
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const LIVE_TAIL_POLL: Duration = Duration::from_millis(400);
const LIVE_TAIL_CONCURRENCY: usize = 4;
/// Slots listed per getBlocks round trip while filling
const FILL_WINDOW_SLOTS: u64 = 4096;
const MAX_BLOCK_FETCH_ATTEMPTS: u32 = 5;
const RETRY_BASE: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(30);

/// Blocks come from upstream as base64 bytes
///
/// Smaller than the json tree, decoded once by the filter, and what the
/// fleet's block decoder reads.
const BLOCK_FETCH_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: Some(UiTransactionEncoding::Base64),
    transaction_details: Some(TransactionDetails::Full),
    rewards: Some(false),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    }),
    max_supported_transaction_version: Some(1),
};

/// Status of a single upstream `getBlock` call.
enum BlockOutcome {
    Present(UiConfirmedBlock),
    Skipped,
    /// No block, and no word yet on whether the slot was skipped; carries the upstream error
    NotAvailable(Value),
    /// Gone from this upstream's ledger for good
    Pruned(String),
    Retriable(String),
}

/// Why a slot could not be filled
#[derive(Debug, Error)]
pub enum FillError {
    /// No block, and the confirmed chain does not list the slot as skipped;
    /// carries the upstream error to answer with
    #[error("block not available: {0}")]
    NotAvailable(Value),

    /// Transport, decode, or upstream failure
    #[error("{0}")]
    Failed(String),
}

pub async fn run_application(config: Config) -> Result<()> {
    let state = build_state(&config)?;
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    info!(
        listen = %config.listen,
        upstream = %config.upstream,
        "rpc-cache accepting observability connections"
    );

    let server_state = state.clone();
    let server_cancel = cancel.clone();
    let mut http_task = tokio::spawn(async move {
        let router = crate::server::router(server_state)
            .into_make_service_with_connect_info::<SocketAddr>();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_cancel.cancelled().await })
            .await
            .context("axum serve")
    });

    let bootstrap_state = state.clone();
    let bootstrap_cancel = cancel.clone();
    let mut bootstrap_task = tokio::spawn(async move {
        bootstrap(
            bootstrap_state.clone(),
            config.bootstrap_lookback_epochs,
            bootstrap_cancel,
        )
        .await?;
        bootstrap_state
            .stats
            .bootstrap_done
            .store(true, Ordering::Relaxed);
        Ok(())
    });

    tokio::select! {
        _ = wait_shutdown_signal() => {
            info!("shutdown signal received");
            cancel.cancel();
            return Ok(());
        }
        r = &mut http_task => {
            warn!(result = ?join_result(r), "http server exited before bootstrap completed");
            cancel.cancel();
            return Ok(());
        }
        r = &mut bootstrap_task => {
            join_result(r).context("rpc-cache bootstrap")?;
        }
    }

    info!(
        listen = %config.listen,
        upstream = %config.upstream,
        epoch_start_slot = state.stats.epoch_start_slot.load(Ordering::Relaxed),
        bootstrap_target_slot = state.stats.bootstrap_target_slot.load(Ordering::Relaxed),
        "rpc-cache accepting warmed connections"
    );

    let tail_state = state.clone();
    let tail_cancel = cancel.clone();
    let mut tail_task = tokio::spawn(async move { live_tail(tail_state, tail_cancel).await });

    let signal_cancel = cancel.clone();
    tokio::select! {
        _ = wait_shutdown_signal() => {
            info!("shutdown signal received");
            signal_cancel.cancel();
        }
        r = &mut http_task => {
            let r = join_result(r);
            warn!(result = ?r, "http server exited before shutdown");
            signal_cancel.cancel();
        }
        r = &mut tail_task => {
            let r = join_result(r);
            warn!(result = ?r, "live tail exited before shutdown");
            signal_cancel.cancel();
        }
    }

    Ok(())
}

fn build_state(config: &Config) -> Result<Arc<AppState>> {
    let policy = Policy::new(config.ttls.clone());
    let cache = CacheStore::new(config.max_entries);
    let upstream = Upstream::new(
        config.upstream.clone(),
        config.min_429_delay,
        config.upstream_headers.clone(),
        config.upstream_max_in_flight,
    );
    let slot_store = build_slot_store(config.slot_store_max_bytes);
    let program_ids = parse_program_ids(&config.filter_program_ids)?;
    let stats = CacheStats::new();

    Ok(Arc::new(AppState {
        policy,
        cache,
        upstream,
        log_submits: config.log_submits,
        api_key: config.api_key.clone(),
        slot_store,
        program_ids,
        stats,
    }))
}

fn build_slot_store(max_bytes: u64) -> MokaCache<u64, CachedBlock> {
    MokaCache::builder()
        .weigher(|_key: &u64, value: &CachedBlock| match value {
            CachedBlock::Present { base64, json } => (base64.len() + json.len()).min(u32::MAX as usize) as u32,
            // Tombstone — small fixed weight. Big enough that they
            // can't dominate the cache, small enough that they
            // don't crowd out real blocks.
            CachedBlock::Skipped => 64,
        })
        .max_capacity(max_bytes)
        .build()
}

fn parse_program_ids(raw: &[String]) -> Result<Vec<Address>> {
    raw.iter()
        .map(|s| {
            Address::from_str(s).map_err(|e| anyhow!("invalid program id {s:?}: {e}"))
        })
        .collect()
}

async fn bootstrap(
    state: Arc<AppState>,
    lookback_epochs: u64,
    cancel: CancellationToken,
) -> Result<()> {
    let system = fetch_system_account(&state.upstream)
        .await
        .context("fetching system account during bootstrap")?;
    let epoch = fetch_epoch_account(&state.upstream, system.current_epoch)
        .await
        .context("fetching current epoch account during bootstrap")?;
    let epoch_start = epoch.start_slot.0;
    state.stats.epoch_start_slot.store(epoch_start, Ordering::Relaxed);

    // Fill from `lookback_epochs` before the current epoch so a reader that
    // bootstraps from an older snapshot finds its blocks here. Falls back to
    // the current epoch's start when the older epoch account is gone.
    let fill_start = bootstrap_floor(&state, system.current_epoch, lookback_epochs, epoch_start)
        .await;
    state.stats.bootstrap_fill_start_slot.store(fill_start, Ordering::Relaxed);

    let live = fetch_live_slot(&state.upstream)
        .await
        .context("fetching live slot during bootstrap")?;
    state.stats.bootstrap_target_slot.store(live, Ordering::Relaxed);
    state.stats.last_observed_live_slot.store(live, Ordering::Relaxed);

    info!(
        epoch_start_slot = epoch_start,
        fill_start,
        target_slot = live,
        slots_to_fetch = live.saturating_sub(fill_start) + 1,
        "rpc-cache bootstrap starting"
    );

    if epoch_start == 0 {
        warn!(
            current_epoch = system.current_epoch.0,
            epoch_start_slot = epoch_start,
            live_slot = live,
            "current epoch has zero start slot; refusing historical bootstrap"
        );
        return Err(anyhow!(
            "current epoch {} has start_slot 0; rerun genesis init with epoch0 replay boundary support",
            system.current_epoch.0
        ));
    }

    if fill_start > live {
        warn!(
            fill_start,
            live_slot = live,
            "bootstrap floor is ahead of live slot; skipping bootstrap fill"
        );
        return Ok(());
    }

    let covered = fill_range(&state, fill_start, live, BOOTSTRAP_CONCURRENCY, &cancel).await;
    state.stats.newest_cached_slot.store(covered, Ordering::Relaxed);

    info!(
        slots_fetched = state.stats.slots_fetched.load(Ordering::Relaxed),
        slots_skipped = state.stats.slots_skipped.load(Ordering::Relaxed),
        newest_cached_slot = covered,
        "rpc-cache bootstrap complete"
    );
    Ok(())
}

/// Choose the lowest slot to fill at bootstrap. Walk back `lookback_epochs`
/// from the current epoch and use that epoch's start slot, so the cache retains
/// older blocks a lagging reader may still ask for. Falls back to the current
/// epoch's start (`current_start`) when the lookback is zero, would reach epoch
/// 0, or the older epoch account can't be read.
async fn bootstrap_floor(
    state: &AppState,
    current_epoch: EpochNumber,
    lookback_epochs: u64,
    current_start: u64,
) -> u64 {
    if lookback_epochs == 0 {
        return current_start;
    }

    let target = EpochNumber(current_epoch.0.saturating_sub(lookback_epochs));
    if target.0 == 0 {
        return current_start;
    }

    match fetch_epoch_account(&state.upstream, target).await {
        Ok(epoch) if epoch.start_slot.0 > 0 && epoch.start_slot.0 < current_start => {
            info!(
                lookback_epoch = target.0,
                floor = epoch.start_slot.0,
                "rpc-cache: widened bootstrap floor to retain older epochs"
            );
            epoch.start_slot.0
        }
        Ok(_) => current_start,
        Err(error) => {
            warn!(
                lookback_epoch = target.0,
                %error,
                "rpc-cache: lookback epoch account unavailable; filling from current epoch start"
            );
            current_start
        }
    }
}

async fn live_tail(state: Arc<AppState>, cancel: CancellationToken) -> Result<()> {
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(LIVE_TAIL_POLL) => {}
        }

        let live = match fetch_live_slot(&state.upstream).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "live tail: getSlot failed, will retry");
                continue;
            }
        };
        state.stats.last_observed_live_slot.store(live, Ordering::Relaxed);

        let newest = state.stats.newest_cached_slot.load(Ordering::Relaxed);
        if live <= newest {
            continue;
        }

        let covered = fill_range(&state, newest + 1, live, LIVE_TAIL_CONCURRENCY, &cancel).await;
        state.stats.newest_cached_slot.fetch_max(covered, Ordering::Relaxed);
    }
}

/// Fill `from..=to` window by window and return the last slot covered
///
/// A failed listing is retried until it succeeds or shutdown. A window with
/// nothing listed ends the fill at the live edge, where the backend is behind
/// the tip it reported, and is passed over below it.
async fn fill_range(
    state: &Arc<AppState>,
    from: u64,
    to: u64,
    concurrency: usize,
    cancel: &CancellationToken,
) -> u64 {
    let started = Instant::now();
    let mut last_progress_log = Instant::now();
    let mut next = from;
    let mut failures: u32 = 0;

    while next <= to && !cancel.is_cancelled() {
        let window_end = to.min(next.saturating_add(FILL_WINDOW_SLOTS - 1));
        match fill_window(state, next, window_end, concurrency, cancel).await {
            Ok(Some(last)) => {
                next = last + 1;
                failures = 0;
            }
            Ok(None) if window_end < to => next = window_end + 1,
            Ok(None) => break,
            Err(error) => {
                warn!(from = next, %error, "fill listing failed; retrying");
                tokio::time::sleep(retry_delay(failures)).await;
                failures += 1;
            }
        }

        if last_progress_log.elapsed() >= PROGRESS_LOG_INTERVAL {
            last_progress_log = Instant::now();
            log_fill_progress(next, next - from, to - from + 1, started);
        }
    }
    next.saturating_sub(1)
}

/// Fill `from..=to` from one getBlocks listing
///
/// Produced slots and absent runs too long to trust are fetched; short absent
/// runs are stored as skips. Returns the last listed slot, which the next
/// window starts after, or None when nothing is listed.
async fn fill_window(
    state: &Arc<AppState>,
    from: u64,
    to: u64,
    concurrency: usize,
    cancel: &CancellationToken,
) -> Result<Option<u64>, String> {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    let produced = fetch_produced(&state.upstream, from, to, RateLimitWait::Unbounded).await?;
    let Some(&last) = produced.last() else {
        return Ok(None);
    };

    let fetch = slots_to_fetch(from, &produced);
    for slot in skipped_slots(from, last, &fetch) {
        // Never overwrite a block a fill already cached.
        if !state.slot_store.contains_key(&slot) {
            state.slot_store.insert(slot, CachedBlock::Skipped).await;
            state.stats.slots_skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fetch_slots(state, &fetch, concurrency, cancel).await;
    Ok(Some(last))
}

/// The slots in `from..=last` left out of the sorted `fetch`, which the listing settled as skipped
fn skipped_slots(from: u64, last: u64, fetch: &[u64]) -> Vec<u64> {
    let mut skipped = Vec::new();
    let mut fetch = fetch.iter().peekable();
    for slot in from..=last {
        if fetch.next_if_eq(&&slot).is_none() {
            skipped.push(slot);
        }
    }
    skipped
}

/// Fetch the given slots with bounded concurrency, passing over any a fill already cached
async fn fetch_slots(
    state: &Arc<AppState>,
    slots: &[u64],
    concurrency: usize,
    cancel: &CancellationToken,
) {
    let mut set: JoinSet<()> = JoinSet::new();
    for &slot in slots {
        if cancel.is_cancelled() {
            break;
        }
        if state.slot_store.contains_key(&slot) {
            continue;
        }
        while set.len() >= concurrency {
            let _ = set.join_next().await;
        }

        let task_state = state.clone();
        let task_cancel = cancel.clone();
        set.spawn(async move {
            if task_cancel.is_cancelled() {
                return;
            }
            fetch_and_store(&task_state, slot).await;
        });
    }
    while set.join_next().await.is_some() {}
}

fn log_fill_progress(slot: u64, done: u64, total: u64, started: Instant) {
    let elapsed = started.elapsed().as_secs_f64();
    let slots_per_sec = if elapsed > 0.0 { done as f64 / elapsed } else { 0.0 };
    let remaining = total.saturating_sub(done);
    let eta_secs = if slots_per_sec > 0.0 {
        (remaining as f64 / slots_per_sec) as u64
    } else {
        0
    };

    info!(
        slot,
        done,
        total,
        percent = (done as f64 * 1000.0 / total as f64).round() / 10.0,
        slots_per_sec = (slots_per_sec * 10.0).round() / 10.0,
        eta_secs,
        "fill progress"
    );
}

async fn fetch_and_store(state: &AppState, slot: u64) {
    match fetch_slot_block(state, slot, RateLimitWait::Unbounded).await {
        Ok(cached) => {
            state.slot_store.insert(slot, cached).await;
            state.stats.newest_cached_slot.fetch_max(slot, Ordering::Relaxed);
        }
        Err(FillError::NotAvailable(error)) => {
            debug!(slot, %error, "block not available yet; leaving slot uncached");
        }
        Err(FillError::Failed(msg)) => {
            warn!(slot, error = %msg, "block fetch failed; leaving slot uncached");
        }
    }
}

/// Fetch one slot's block
///
/// A not-available answer becomes a skip when the confirmed chain lists a
/// later slot within reach, the same check a node makes.
pub async fn fetch_slot_block(
    state: &AppState,
    slot: u64,
    wait: RateLimitWait,
) -> Result<CachedBlock, FillError> {
    let mut attempt: u32 = 0;
    loop {
        state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
        match fetch_block(&state.upstream, slot, wait).await {
            BlockOutcome::Present(block) => {
                let cached = build_present_block(state, slot, block).map_err(FillError::Failed)?;
                state.stats.slots_fetched.fetch_add(1, Ordering::Relaxed);
                return Ok(cached);
            }
            BlockOutcome::Skipped => {
                state.stats.slots_skipped.fetch_add(1, Ordering::Relaxed);
                return Ok(CachedBlock::Skipped);
            }
            BlockOutcome::NotAvailable(error) => {
                return resolve_not_available(state, slot, error, wait).await;
            }
            // Permanently gone from this upstream but not a skip. Retrying the
            // same endpoint won't help.
            BlockOutcome::Pruned(msg) => return Err(FillError::Failed(msg)),
            BlockOutcome::Retriable(msg) if attempt < MAX_BLOCK_FETCH_ATTEMPTS => {
                let delay = retry_delay(attempt);
                debug!(slot, attempt, delay_ms = delay.as_millis() as u64, error = %msg, "retrying block fetch");
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            BlockOutcome::Retriable(msg) => return Err(FillError::Failed(msg)),
        }
    }
}

/// Settle a not-available answer: a skip when getBlocks lists a later slot within reach
async fn resolve_not_available(
    state: &AppState,
    slot: u64,
    error: Value,
    wait: RateLimitWait,
) -> Result<CachedBlock, FillError> {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    let produced = fetch_produced(&state.upstream, slot, slot + TRUSTED_SKIP_RUN, wait)
        .await
        .map_err(FillError::Failed)?;

    if !produced.first().is_some_and(|first| trusted_skip(slot, *first)) {
        return Err(FillError::NotAvailable(error));
    }
    state.stats.slots_skipped.fetch_add(1, Ordering::Relaxed);
    Ok(CachedBlock::Skipped)
}

fn build_present_block(
    state: &AppState,
    slot: u64,
    block: UiConfirmedBlock,
) -> Result<CachedBlock, String> {
    let mut filtered = filter_block(block, &state.program_ids);
    let base64 = serialize_block(&filtered.block, slot)?;
    // The same block, its kept transactions swapped for their json
    // rendering, is the json body; nothing else differs between the two.
    let kept = filtered.block.transactions.iter_mut().flatten();
    for (tx, json_transaction) in kept.zip(filtered.json_transactions) {
        tx.transaction = json_transaction;
    }
    let json = serialize_block(&filtered.block, slot)?;
    Ok(CachedBlock::Present { base64, json })
}

/// One encoding of a filtered block as the `result` body it is served as
fn serialize_block(block: &UiConfirmedBlock, slot: u64) -> Result<Bytes, String> {
    match serde_json::to_vec(block) {
        Ok(serialized) => Ok(Bytes::from(serialized)),
        Err(e) => Err(format!("serialize filtered block at slot {slot}: {e}")),
    }
}

fn retry_delay(attempt: u32) -> Duration {
    let shift = attempt.min(10);
    let scaled = RETRY_BASE.saturating_mul(1u32 << shift);
    scaled.min(RETRY_CAP)
}

async fn fetch_block(upstream: &Upstream, slot: u64, wait: RateLimitWait) -> BlockOutcome {
    let config_json = match serde_json::to_value(BLOCK_FETCH_CONFIG) {
        Ok(v) => v,
        Err(e) => return BlockOutcome::Retriable(format!("config encode: {e}")),
    };
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, config_json],
    });

    match upstream.forward(&body, wait).await {
        Ok(env) => match (env.result, env.error) {
            (Some(result), _) => match serde_json::from_value::<UiConfirmedBlock>(result) {
                Ok(block) => BlockOutcome::Present(block),
                Err(e) => BlockOutcome::Retriable(format!("decode block: {e}")),
            },
            (None, Some(err)) => classify_block_error(err),
            (None, None) => BlockOutcome::Retriable("empty envelope".into()),
        },
        Err(e) => upstream_to_outcome(&e),
    }
}

fn classify_block_error(error: Value) -> BlockOutcome {
    let code = error.get("code").and_then(Value::as_i64);
    match code {
        Some(SKIPPED_SLOT_CODE) => return BlockOutcome::Skipped,
        // Settled by code ahead of the message checks, since -32009's text says skipped.
        Some(BLOCK_NOT_AVAILABLE_CODE | LONG_TERM_STORAGE_SLOT_SKIPPED_CODE) => {
            return BlockOutcome::NotAvailable(error);
        }
        Some(_) | None => {}
    }

    let msg = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    let described = format!("code={} message={}", code.unwrap_or(0), msg);
    let lower = msg.to_lowercase();
    if lower.contains("cleaned up") || lower.contains("does not exist") {
        return BlockOutcome::Pruned(described);
    }
    // A genuine skip reported only by message (non-standard or missing code).
    if lower.contains("skipped") {
        return BlockOutcome::Skipped;
    }
    BlockOutcome::Retriable(described)
}

fn upstream_to_outcome(e: &UpstreamError) -> BlockOutcome {
    BlockOutcome::Retriable(e.to_string())
}

/// The slots getBlocks lists as produced on the confirmed chain in `start..=end`
async fn fetch_produced(
    upstream: &Upstream,
    start: u64,
    end: u64,
    wait: RateLimitWait,
) -> Result<Vec<u64>, String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlocks",
        "params": [start, end, {"commitment": "confirmed"}],
    });
    let envelope = upstream
        .forward(&body, wait)
        .await
        .map_err(|error| format!("getBlocks upstream: {error}"))?;
    match (envelope.result, envelope.error) {
        (Some(result), _) => {
            serde_json::from_value(result).map_err(|error| format!("decode getBlocks: {error}"))
        }
        (None, Some(error)) => Err(format!("getBlocks error: {error}")),
        (None, None) => Err("getBlocks returned an empty envelope".into()),
    }
}

async fn fetch_live_slot(upstream: &Upstream) -> Result<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "confirmed"}],
    });
    let env = upstream
        .forward(&body, RateLimitWait::Unbounded)
        .await
        .map_err(|e| anyhow!("getSlot upstream: {e}"))?;
    let result = env.result.ok_or_else(|| anyhow!("getSlot returned no result"))?;
    result
        .as_u64()
        .ok_or_else(|| anyhow!("getSlot result not a number: {result}"))
}

async fn fetch_system_account(upstream: &Upstream) -> Result<System> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            SYSTEM_ADDRESS.to_string(),
            {"encoding": "base64", "commitment": "confirmed"},
        ],
    });
    let data = fetch_account_data(upstream, &body, "system", &SYSTEM_ADDRESS.to_string()).await?;
    let system = System::unpack_with_discriminator(&data)
        .map_err(|e| anyhow!("system account unpack: {e}"))?;
    Ok(*system)
}

async fn fetch_epoch_account(upstream: &Upstream, epoch: EpochNumber) -> Result<Epoch> {
    let (epoch_address, _) = epoch_pda(epoch);
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            epoch_address.to_string(),
            {"encoding": "base64", "commitment": "confirmed"},
        ],
    });
    let data = fetch_account_data(upstream, &body, "epoch", &epoch_address.to_string()).await?;
    let epoch_account = Epoch::unpack_with_discriminator(&data)
        .map_err(|e| anyhow!("epoch account unpack: {e}"))?;
    Ok(*epoch_account)
}

async fn fetch_account_data(
    upstream: &Upstream,
    body: &Value,
    label: &str,
    address: &str,
) -> Result<Vec<u8>> {
    let env = upstream
        .forward(body, RateLimitWait::Unbounded)
        .await
        .map_err(|e| anyhow!("getAccountInfo upstream: {e}"))?;
    let result = env
        .result
        .ok_or_else(|| anyhow!("getAccountInfo returned no result"))?;
    let value = result
        .get("value")
        .ok_or_else(|| anyhow!("getAccountInfo result has no `value`"))?;
    if value.is_null() {
        return Err(anyhow!("{label} account does not exist at {address}"));
    }
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{label} account data missing or not base64"))?;
    let decoded = base64::decode(data)
        .map_err(|e| anyhow!("{label} account base64 decode: {e}"))?;
    Ok(decoded)
}

async fn wait_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "failed to install SIGTERM handler; falling back to ctrl-c only");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

fn join_result<T>(result: std::result::Result<Result<T>, tokio::task::JoinError>) -> Result<T> {
    match result {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow!("task join: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use crate::mock_upstream::{MockUpstream, Reply};

    use super::*;

    const SLOT: u64 = 100;

    fn state(upstream: &str) -> Arc<AppState> {
        let yaml = format!("listen: \"127.0.0.1:0\"\nupstream: \"{upstream}\"\napi_key: \"key\"\n");
        let config: Config = serde_yaml::from_str(&yaml).expect("test config");
        build_state(&config).expect("test state")
    }

    /// An upstream answering every getBlock with this error and every getBlocks with this listing
    async fn upstream(block_code: i64, message: &'static str, listed: Value) -> MockUpstream {
        MockUpstream::start(move |_, body| match body["method"].as_str() {
            Some("getBlock") => Reply::error(block_code, message),
            Some("getBlocks") => Reply::result(listed.clone()),
            Some(_) | None => Reply::status("400 Bad Request"),
        })
        .await
    }

    // retry delays grow, then cap
    #[test]
    fn retry_growth() {
        let d0 = retry_delay(0);
        let d1 = retry_delay(1);
        let d10 = retry_delay(10);
        assert!(d1 > d0);
        assert!(d10 <= RETRY_CAP);
    }

    // -32007 is a skip
    #[test]
    fn skip_code() {
        let error = json!({"code": SKIPPED_SLOT_CODE, "message": "anything"});

        assert!(matches!(classify_block_error(error), BlockOutcome::Skipped));
    }

    // -32004 and -32009 need the listing, whatever the message says
    #[test]
    fn not_available_codes() {
        let not_available = json!({
            "code": BLOCK_NOT_AVAILABLE_CODE,
            "message": "Block not available for slot 42",
        });
        let long_term_storage = json!({
            "code": LONG_TERM_STORAGE_SLOT_SKIPPED_CODE,
            "message": "Slot 42 was skipped, or missing in long-term storage",
        });

        assert!(matches!(classify_block_error(not_available), BlockOutcome::NotAvailable(_)));
        assert!(matches!(classify_block_error(long_term_storage), BlockOutcome::NotAvailable(_)));
    }

    // a nonstandard code whose message says skipped is a skip
    #[test]
    fn skip_message() {
        let error = json!({"code": -1, "message": "Slot 42 was Skipped or missing"});

        assert!(matches!(classify_block_error(error), BlockOutcome::Skipped));
    }

    // a cleaned-up block is pruned
    #[test]
    fn pruned() {
        let error = json!({
            "code": -32001,
            "message": "Block 2170 cleaned up, does not exist on node. First available block: 4756",
        });

        assert!(matches!(classify_block_error(error), BlockOutcome::Pruned(_)));
    }

    // any other error is retried
    #[test]
    fn retriable() {
        let error = json!({"code": -32099, "message": "transient hiccup"});

        assert!(matches!(classify_block_error(error), BlockOutcome::Retriable(_)));
    }

    // a not-available slot with a later slot listed within reach is a skip
    #[tokio::test]
    async fn resolved_skip() {
        let mock = upstream(BLOCK_NOT_AVAILABLE_CODE, "Block not available for slot 100", json!([SLOT + 2])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(matches!(filled, Ok(CachedBlock::Skipped)));
    }

    // with nothing listed past it, the slot answers with the upstream error
    #[tokio::test]
    async fn not_available() {
        let mock = upstream(BLOCK_NOT_AVAILABLE_CODE, "Block not available for slot 100", json!([])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(matches!(
            &filled,
            Err(FillError::NotAvailable(error)) if error["code"] == BLOCK_NOT_AVAILABLE_CODE
        ));
    }

    // -32009 on a slot the listing shows as produced is not a skip
    #[tokio::test]
    async fn long_term_storage() {
        let message = "Slot 100 was skipped, or missing in long-term storage";
        let mock = upstream(LONG_TERM_STORAGE_SLOT_SKIPPED_CODE, message, json!([SLOT, SLOT + 1])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(matches!(filled, Err(FillError::NotAvailable(_))));
    }

    // a window fetches listed slots and long runs, stores short runs as skips, and stops at the last listed slot
    #[tokio::test]
    async fn window_plan() {
        let mock = upstream(SKIPPED_SLOT_CODE, "Slot was skipped", json!([10, 12, 30])).await;
        let state = state(&mock.url);
        state.slot_store.insert(13, CachedBlock::Skipped).await;

        let last = fill_window(&state, 10, 40, LIVE_TAIL_CONCURRENCY, &CancellationToken::new()).await;

        let mut fetched: Vec<u64> = Vec::new();
        for body in mock.bodies() {
            if body["method"] == "getBlock" {
                fetched.extend(body["params"][0].as_u64());
            }
        }
        fetched.sort_unstable();
        let mut expected = vec![10, 12];
        expected.extend(14..=30);
        assert_eq!(last, Ok(Some(30)));
        assert_eq!(fetched, expected);
        assert!(state.slot_store.contains_key(&11));
        assert!(!state.slot_store.contains_key(&31));
    }
}
