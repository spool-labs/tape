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
use tape_rpc::skip::{TRUSTED_SKIP_RUN, slots_to_fetch, trusted_skip};
use serde_json::{Value, json};
use solana_client::rpc_config::RpcBlockConfig;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding};
use tape_api::program::tapedrive::{epoch_pda, SYSTEM_ADDRESS};
use tape_api::state::{Epoch, System};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cache::{CacheStore, Policy};
use crate::config::Config;
use crate::filter::{block_fingerprint, filter_block};
use crate::public::Public;
use crate::server::{AppState, CacheStats};
use crate::store::block::CachedBlock;
use crate::store::slot_store::SlotStore;
use crate::upstream::{RateLimitWait, Upstream, UpstreamError};

/// A rooted skip: permanent, stored as a tombstone
const SKIPPED_SLOT_CODE: i64 = -32007;
/// No block at this commitment yet, which is also how a skip reads until it roots
const BLOCK_NOT_AVAILABLE_CODE: i64 = -32004;
/// Skipped, or missing from long-term storage; only the confirmed listing tells which
const LONG_TERM_STORAGE_SLOT_SKIPPED_CODE: i64 = -32009;

const BOOTSTRAP_CONCURRENCY: usize = 16;
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const LIVE_TAIL_POLL: Duration = Duration::from_millis(100);
const LIVE_TAIL_CONCURRENCY: usize = 4;
const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);
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
    NotAvailable(String),
    Pruned(String),
    Retriable(String),
}

pub async fn run_application(config: Config) -> Result<()> {
    let upstream = build_upstream(&config);
    let program_ids = parse_program_ids(&config.filter_program_ids)?;
    let slot_store = open_slot_store(&config, &upstream, &program_ids).await?;
    let state = build_state(&config, upstream, program_ids, slot_store);
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

    // A public instance serves no blocks, so it has nothing to fill
    if state.public.is_some() {
        state.stats.bootstrap_done.store(true, Ordering::Relaxed);
        info!(
            listen = %config.listen,
            upstream = %config.upstream,
            "rpc-cache accepting public connections"
        );
        tokio::select! {
            _ = wait_shutdown_signal() => {
                info!("shutdown signal received");
                cancel.cancel();
            }
            r = &mut http_task => {
                let r = join_result(r);
                warn!(result = ?r, "http server exited before shutdown");
                cancel.cancel();
            }
        }
        return Ok(());
    }

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

    let prune_state = state.clone();
    let prune_cancel = cancel.clone();
    let lookback_epochs = config.bootstrap_lookback_epochs;
    let mut prune_task =
        tokio::spawn(async move { prune_loop(prune_state, lookback_epochs, prune_cancel).await });

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
        r = &mut prune_task => {
            let r = join_result(r);
            warn!(result = ?r, "prune loop exited before shutdown");
            signal_cancel.cancel();
        }
    }

    Ok(())
}

fn build_state(
    config: &Config,
    upstream: Upstream,
    program_ids: Vec<Address>,
    slot_store: SlotStore,
) -> Arc<AppState> {
    Arc::new(AppState {
        policy: Policy::new(config.ttls.clone()),
        cache: CacheStore::new(config.max_entries),
        upstream,
        log_submits: config.log_submits,
        api_key: config.api_key.clone(),
        public: config.public.as_ref().map(Public::new),
        slot_store,
        program_ids,
        stats: CacheStats::new(),
    })
}

fn build_upstream(config: &Config) -> Upstream {
    Upstream::new(
        config.upstream.clone(),
        config.min_429_delay,
        config.upstream_headers.clone(),
        config.upstream_max_in_flight,
    )
}

/// Open the block file for the chain upstream serves. A public instance
/// serves no blocks, so its store stays empty in memory.
async fn open_slot_store(config: &Config, upstream: &Upstream, program_ids: &[Address]) -> Result<SlotStore> {
    if config.public.is_some() {
        return SlotStore::try_in_memory(config.slot_store_max_bytes).context("opening the in-memory slot store");
    }

    let genesis_hash = fetch_genesis_hash(upstream)
        .await
        .context("fetching the genesis hash for the slot store")?;
    let fingerprint = block_fingerprint(&genesis_hash, program_ids);
    SlotStore::try_open(&config.slot_store_path, config.slot_store_max_bytes, fingerprint)
        .await
        .with_context(|| format!("opening slot store {}", config.slot_store_path.display()))
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
        .await
        .unwrap_or(epoch_start);
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

/// Choose the lowest slot the cache keeps. Walk back `lookback_epochs` from
/// the current epoch and use that epoch's start slot, so the cache retains
/// older blocks a lagging reader may still ask for. Falls back to the current
/// epoch's start (`current_start`) when the lookback is zero or would reach
/// epoch 0. None when the older epoch account can't be read, so the bootstrap
/// fills from `current_start` and the prune deletes nothing.
async fn bootstrap_floor(
    state: &AppState,
    current_epoch: EpochNumber,
    lookback_epochs: u64,
    current_start: u64,
) -> Option<u64> {
    if lookback_epochs == 0 {
        return Some(current_start);
    }

    let target = EpochNumber(current_epoch.0.saturating_sub(lookback_epochs));
    if target.0 == 0 {
        return Some(current_start);
    }

    match fetch_epoch_account(&state.upstream, target).await {
        Ok(epoch) if epoch.start_slot.0 > 0 && epoch.start_slot.0 < current_start => {
            info!(
                lookback_epoch = target.0,
                floor = epoch.start_slot.0,
                "rpc-cache: widened bootstrap floor to retain older epochs"
            );
            Some(epoch.start_slot.0)
        }
        Ok(_) => Some(current_start),
        Err(error) => {
            warn!(
                lookback_epoch = target.0,
                %error,
                "rpc-cache: lookback epoch account unavailable"
            );
            None
        }
    }
}

/// Delete the stored blocks below the lookback floor at once, then every `PRUNE_INTERVAL`
async fn prune_loop(state: Arc<AppState>, lookback_epochs: u64, cancel: CancellationToken) -> Result<()> {
    let mut interval = tokio::time::interval(PRUNE_INTERVAL);
    loop {
        tokio::select! {
            // Safe: neither future holds partial state
            _ = cancel.cancelled() => return Ok(()),
            _ = interval.tick() => {}
        }
        tokio::select! {
            // Safe: a pass dropped between batches keeps the ones it committed
            _ = cancel.cancelled() => return Ok(()),
            _ = prune_below_floor(&state, lookback_epochs) => {}
        }
    }
}

/// One prune pass. An unknown floor deletes nothing.
async fn prune_below_floor(state: &AppState, lookback_epochs: u64) {
    let floor = match lookback_floor(state, lookback_epochs).await {
        Ok(floor) => floor,
        Err(error) => {
            warn!(%error, "lookback floor unavailable; prune skipped");
            return;
        }
    };
    // bootstrap_floor has logged why the floor is unknown
    let Some(floor) = floor else {
        return;
    };

    match state.slot_store.prune_below(floor).await {
        Ok(pruned) => info!(floor, pruned, "pruned stored blocks below the lookback floor"),
        Err(error) => warn!(floor, %error, "slot store prune failed"),
    }
}

/// Read the current epoch and give its lookback floor
async fn lookback_floor(state: &AppState, lookback_epochs: u64) -> Result<Option<u64>> {
    let system = fetch_system_account(&state.upstream).await?;
    let epoch = fetch_epoch_account(&state.upstream, system.current_epoch).await?;
    Ok(bootstrap_floor(state, system.current_epoch, lookback_epochs, epoch.start_slot.0).await)
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
            state.stats.record_tail(newest);
            continue;
        }

        let covered = fill_range(&state, newest + 1, live, LIVE_TAIL_CONCURRENCY, &cancel).await;
        state.stats.newest_cached_slot.fetch_max(covered, Ordering::Relaxed);
        state.stats.record_tail(covered);
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

    // A restart keeps the stored blocks, so only the rest are filled
    let stored = state
        .slot_store
        .stored_in(from, last)
        .await
        .map_err(|error| format!("slot store listing: {error}"))?;
    let fetch = slots_to_fetch(from, &produced);

    let mut skipped = Vec::new();
    for slot in skipped_slots(from, last, &fetch) {
        if !stored.contains(&slot) {
            skipped.push(slot);
        }
    }
    let skipped_count = skipped.len() as u64;
    state
        .slot_store
        .insert_skipped(skipped)
        .await
        .map_err(|error| format!("slot store skips: {error}"))?;
    state.stats.slots_skipped.fetch_add(skipped_count, Ordering::Relaxed);

    let mut missing = Vec::new();
    for slot in fetch {
        if !stored.contains(&slot) {
            missing.push(slot);
        }
    }
    fetch_slots(state, &missing, concurrency, cancel).await;
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
        // A request may have filled the slot since the window was listed
        if state.slot_store.is_in_memory(slot) {
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
    let cached = match fetch_slot_block(state, slot, RateLimitWait::Unbounded).await {
        Ok(cached) => cached,
        Err(msg) => {
            warn!(slot, error = %msg, "block fetch failed; leaving slot uncached");
            return;
        }
    };
    if let Err(error) = state.slot_store.insert(slot, cached).await {
        warn!(slot, %error, "slot store write failed; the block stays in memory only");
    }
    state.stats.newest_cached_slot.fetch_max(slot, Ordering::Relaxed);
}

/// Fetch one slot's block
///
/// A first not-available answer is checked against the confirmed listing, and
/// a short absent run settles as a skip. Anything else is retried like a late
/// block, so every node waiting on the slot shares one fill window.
pub async fn fetch_slot_block(
    state: &AppState,
    slot: u64,
    wait: RateLimitWait,
) -> Result<CachedBlock, String> {
    let mut attempt: u32 = 0;
    loop {
        state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
        let mut outcome = fetch_block(&state.upstream, slot, wait).await;
        if attempt == 0
            && matches!(outcome, BlockOutcome::NotAvailable(_))
            && is_confirmed_skip(state, slot, wait).await?
        {
            outcome = BlockOutcome::Skipped;
        }

        let msg = match outcome {
            BlockOutcome::Present(block) => {
                let cached = build_present_block(state, slot, block)?;
                state.stats.slots_fetched.fetch_add(1, Ordering::Relaxed);
                return Ok(cached);
            }
            BlockOutcome::Skipped => {
                state.stats.slots_skipped.fetch_add(1, Ordering::Relaxed);
                return Ok(CachedBlock::Skipped);
            }
            BlockOutcome::NotAvailable(msg) | BlockOutcome::Retriable(msg) => msg,
            // Permanently gone from this upstream but not a skip. Retrying the
            // same endpoint won't help.
            BlockOutcome::Pruned(msg) => return Err(msg),
        };

        if attempt >= MAX_BLOCK_FETCH_ATTEMPTS {
            return Err(msg);
        }
        let delay = retry_delay(attempt);
        debug!(slot, attempt, delay_ms = delay.as_millis() as u64, error = %msg, "retrying block fetch");
        tokio::time::sleep(delay).await;
        attempt += 1;
    }
}

/// Whether getBlocks lists a later slot within reach, which settles the slot as skipped
async fn is_confirmed_skip(state: &AppState, slot: u64, wait: RateLimitWait) -> Result<bool, String> {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    let produced = fetch_produced(&state.upstream, slot, slot + TRUSTED_SKIP_RUN, wait).await?;
    Ok(produced.first().is_some_and(|first| trusted_skip(slot, *first)))
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
            (None, Some(err)) => classify_block_error(&err),
            (None, None) => BlockOutcome::Retriable("empty envelope".into()),
        },
        Err(e) => upstream_to_outcome(&e),
    }
}

fn classify_block_error(error: &Value) -> BlockOutcome {
    let code = error.get("code").and_then(Value::as_i64);
    let msg = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    let described = format!("code={} message={}", code.unwrap_or(0), msg);

    match code {
        Some(SKIPPED_SLOT_CODE) => return BlockOutcome::Skipped,
        // Settled by code ahead of the message checks, since -32009's text says skipped.
        Some(BLOCK_NOT_AVAILABLE_CODE | LONG_TERM_STORAGE_SLOT_SKIPPED_CODE) => {
            return BlockOutcome::NotAvailable(described);
        }
        Some(_) | None => {}
    }

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

async fn fetch_genesis_hash(upstream: &Upstream) -> Result<String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getGenesisHash",
        "params": [],
    });
    let env = upstream
        .forward(&body, RateLimitWait::Unbounded)
        .await
        .map_err(|e| anyhow!("getGenesisHash upstream: {e}"))?;
    let result = env.result.ok_or_else(|| anyhow!("getGenesisHash returned no result"))?;
    result
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("getGenesisHash result not a string: {result}"))
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
    const MAX_BYTES: u64 = 1 << 20;

    fn state(upstream: &str) -> Arc<AppState> {
        let slot_store = SlotStore::try_in_memory(MAX_BYTES).expect("slot store");
        state_with(upstream, slot_store)
    }

    fn state_with(upstream: &str, slot_store: SlotStore) -> Arc<AppState> {
        let yaml = format!("listen: \"127.0.0.1:0\"\nupstream: \"{upstream}\"\napi_key: \"key\"\n");
        let config: Config = serde_yaml::from_str(&yaml).expect("test config");
        let program_ids = parse_program_ids(&config.filter_program_ids).expect("program ids");
        build_state(&config, build_upstream(&config), program_ids, slot_store)
    }

    /// Whether the store holds the slot, in memory or the file
    async fn is_stored(state: &AppState, slot: u64) -> bool {
        state.slot_store.get(slot).await.expect("slot store read").is_some()
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

    /// How many requests of this method the mock received
    fn count(mock: &MockUpstream, method: &str) -> usize {
        let mut matched = 0;
        for body in mock.bodies() {
            if body["method"] == method {
                matched += 1;
            }
        }
        matched
    }

    // a lookback epoch that can't be read gives no floor, so nothing is pruned
    #[tokio::test]
    async fn unknown_floor() {
        let mock = MockUpstream::start(|_, _| Reply::status("400 Bad Request")).await;
        let state = state(&mock.url);

        assert_eq!(bootstrap_floor(&state, EpochNumber(10), 1, 500).await, None);
        assert_eq!(bootstrap_floor(&state, EpochNumber(10), 0, 500).await, Some(500));
        assert_eq!(bootstrap_floor(&state, EpochNumber(1), 1, 500).await, Some(500));
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

        assert!(matches!(classify_block_error(&error), BlockOutcome::Skipped));
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

        assert!(matches!(classify_block_error(&not_available), BlockOutcome::NotAvailable(_)));
        assert!(matches!(classify_block_error(&long_term_storage), BlockOutcome::NotAvailable(_)));
    }

    // a nonstandard code whose message says skipped is a skip
    #[test]
    fn skip_message() {
        let error = json!({"code": -1, "message": "Slot 42 was Skipped or missing"});

        assert!(matches!(classify_block_error(&error), BlockOutcome::Skipped));
    }

    // a cleaned-up block is pruned
    #[test]
    fn pruned() {
        let error = json!({
            "code": -32001,
            "message": "Block 2170 cleaned up, does not exist on node. First available block: 4756",
        });

        assert!(matches!(classify_block_error(&error), BlockOutcome::Pruned(_)));
    }

    // any other error is retried
    #[test]
    fn retriable() {
        let error = json!({"code": -32099, "message": "transient hiccup"});

        assert!(matches!(classify_block_error(&error), BlockOutcome::Retriable(_)));
    }

    // a not-available slot with a later slot listed within reach is a skip
    #[tokio::test]
    async fn resolved_skip() {
        let mock = upstream(BLOCK_NOT_AVAILABLE_CODE, "Block not available for slot 100", json!([SLOT + 2])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(matches!(filled, Ok(CachedBlock::Skipped)));
        assert_eq!(count(&mock, "getBlock"), 1);
    }

    // an unsettled slot is checked against the listing once, then retried like a late block
    #[tokio::test]
    async fn unsettled_retries() {
        let mock = upstream(BLOCK_NOT_AVAILABLE_CODE, "Block not available for slot 100", json!([])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(matches!(&filled, Err(msg) if msg.contains("code=-32004")));
        assert_eq!(count(&mock, "getBlocks"), 1);
        assert_eq!(count(&mock, "getBlock"), MAX_BLOCK_FETCH_ATTEMPTS as usize + 1);
    }

    // -32009 on a slot the listing shows as produced is not a skip
    #[tokio::test]
    async fn long_term_storage() {
        let message = "Slot 100 was skipped, or missing in long-term storage";
        let mock = upstream(LONG_TERM_STORAGE_SLOT_SKIPPED_CODE, message, json!([SLOT, SLOT + 1])).await;
        let state = state(&mock.url);

        let filled = fetch_slot_block(&state, SLOT, RateLimitWait::Unbounded).await;

        assert!(filled.is_err());
    }

    // a window fetches listed slots and long runs, stores short runs as skips, and stops at the last listed slot
    #[tokio::test]
    async fn window_plan() {
        let mock = upstream(SKIPPED_SLOT_CODE, "Slot was skipped", json!([10, 12, 30])).await;
        let state = state(&mock.url);
        state.slot_store.insert(13, CachedBlock::Skipped).await.expect("store skip");

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
        assert!(is_stored(&state, 11).await);
        assert!(!is_stored(&state, 31).await);
    }

    // a window filled before a restart fetches no block after it
    #[tokio::test]
    async fn restart_fill() {
        let mock = upstream(SKIPPED_SLOT_CODE, "Slot was skipped", json!([10, 12, 30])).await;
        let directory = std::env::temp_dir().join(format!("rpc-cache-restart-fill-{}", std::process::id()));
        std::fs::remove_dir_all(&directory).ok();
        std::fs::create_dir_all(&directory).expect("temp directory");
        let path = directory.join("blocks.db");
        let cancel = CancellationToken::new();
        let store = SlotStore::try_open(&path, MAX_BYTES, "test".to_string()).await.expect("open");
        let first = state_with(&mock.url, store);
        fill_window(&first, 10, 40, LIVE_TAIL_CONCURRENCY, &cancel).await.expect("first fill");
        let fetched = count(&mock, "getBlock");
        drop(first);

        let reopened = SlotStore::try_open(&path, MAX_BYTES, "test".to_string()).await.expect("reopen");
        let second = state_with(&mock.url, reopened);
        let last = fill_window(&second, 10, 40, LIVE_TAIL_CONCURRENCY, &cancel).await;

        assert_eq!(last, Ok(Some(30)));
        assert_eq!(count(&mock, "getBlock"), fetched);
        assert!(is_stored(&second, 11).await);
        std::fs::remove_dir_all(&directory).ok();
    }

    // the live tail fills the new slots, then reports the last one it holds
    #[tokio::test]
    async fn tail_slot() {
        let mock = MockUpstream::start(|_, body| match body["method"].as_str() {
            Some("getSlot") => Reply::result(json!(SLOT + 2)),
            Some("getBlocks") => Reply::result(json!([SLOT + 1, SLOT + 2])),
            Some("getBlock") => Reply::error(SKIPPED_SLOT_CODE, "Slot was skipped"),
            Some(_) | None => Reply::status("400 Bad Request"),
        })
        .await;
        let state = state(&mock.url);
        state.stats.newest_cached_slot.store(SLOT, Ordering::Relaxed);
        assert_eq!(state.stats.fresh_tail_slot(), None);

        let cancel = CancellationToken::new();
        let tail = tokio::spawn(live_tail(state.clone(), cancel.clone()));
        let reported = tokio::time::timeout(Duration::from_secs(5), async {
            while state.stats.fresh_tail_slot() != Some(SLOT + 2) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        cancel.cancel();
        tail.await.expect("join tail").expect("tail");

        assert!(reported.is_ok());
        assert!(is_stored(&state, SLOT + 1).await);
        assert!(is_stored(&state, SLOT + 2).await);
    }

    // a confirmed getSlot goes upstream until the tail has a reading, then is answered from it
    #[tokio::test]
    async fn tip_from_tail() {
        let mock = MockUpstream::start(|_, body| match body["method"].as_str() {
            Some("getSlot") => Reply::result(json!(SLOT + 9)),
            Some(_) | None => Reply::status("400 Bad Request"),
        })
        .await;
        let state = state(&mock.url);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}/?api=key", listener.local_addr().expect("local address"));
        let router = crate::server::router(state.clone())
            .into_make_service_with_connect_info::<SocketAddr>();
        let server = tokio::spawn(async move { axum::serve(listener, router).await });

        let client = reqwest::Client::new();
        let mut tips = Vec::new();
        for commitment in ["confirmed", "confirmed", "finalized"] {
            let ask = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSlot",
                "params": [{"commitment": commitment}],
            });
            let response = client.post(&url).json(&ask).send().await.expect("send");
            let reply: Value = response.json().await.expect("reply");
            tips.push(reply["result"].as_u64());
            state.stats.record_tail(SLOT);
        }
        server.abort();

        assert_eq!(tips, [Some(SLOT + 9), Some(SLOT), Some(SLOT + 9)]);
        assert_eq!(count(&mock, "getSlot"), 2);
    }
}
