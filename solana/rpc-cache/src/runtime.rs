//! Process orchestration: build state, run bootstrap to completion,
//! then spawn the HTTP server and live-tail tasks under a shared
//! shutdown token.
//!
//! Mirrors the pattern used by `network/node` — bootstrap is *not* a
//! supervised service. It runs in `run_application` before the HTTP
//! listener binds, so the cache simply doesn't accept connections
//! until the slot store is filled. There is no in-between "503 while
//! warming" state to think about.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use rpc::BLOCK_FETCH_CONFIG;
use serde_json::{Value, json};
use solana_transaction_status::UiConfirmedBlock;
use tape_api::program::tapedrive::EPOCH_ADDRESS;
use tape_api::state::Epoch;
use tape_crypto::address::Address;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cache::{CacheStore, Policy};
use crate::config::Config;
use crate::filter::filter_block;
use crate::server::{AppState, CacheStats, CachedBlock};
use crate::upstream::{Upstream, UpstreamError};

/// Solana JSON-RPC error code returned for `getBlock` on a slot that
/// was either skipped or has aged out of long-term storage. Both are
/// permanent — cache as a tombstone and stop retrying.
const SKIPPED_SLOT_CODE: i64 = -32007;
const SKIPPED_OR_LTS_CODE: i64 = -32009;

const BOOTSTRAP_CONCURRENCY: usize = 16;
const LIVE_TAIL_POLL: Duration = Duration::from_millis(400);
const LIVE_TAIL_CONCURRENCY: usize = 4;
const MAX_BLOCK_FETCH_ATTEMPTS: u32 = 5;
const RETRY_BASE: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(30);

/// Status of a single upstream `getBlock` call.
enum BlockOutcome {
    Present(UiConfirmedBlock),
    Skipped,
    Retriable(String),
}

pub async fn run_application(config: Config) -> Result<()> {
    let state = build_state(&config)?;
    let cancel = CancellationToken::new();

    bootstrap(state.clone(), cancel.clone()).await?;
    state.stats.bootstrap_done.store(true, Ordering::Relaxed);

    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    info!(
        listen = %config.listen,
        upstream = %config.upstream,
        epoch_start_slot = state.stats.epoch_start_slot.load(Ordering::Relaxed),
        bootstrap_target_slot = state.stats.bootstrap_target_slot.load(Ordering::Relaxed),
        "rpc-cache accepting connections"
    );

    let server_state = state.clone();
    let server_cancel = cancel.clone();
    let http_task = tokio::spawn(async move {
        let router = crate::server::router(server_state)
            .into_make_service_with_connect_info::<SocketAddr>();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_cancel.cancelled().await })
            .await
            .context("axum serve")
    });

    let tail_state = state.clone();
    let tail_cancel = cancel.clone();
    let tail_task = tokio::spawn(async move { live_tail(tail_state, tail_cancel).await });

    let signal_cancel = cancel.clone();
    tokio::select! {
        _ = wait_shutdown_signal() => {
            info!("shutdown signal received");
            signal_cancel.cancel();
        }
        r = flatten(http_task) => {
            warn!(result = ?r, "http server exited before shutdown");
            signal_cancel.cancel();
        }
        r = flatten(tail_task) => {
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
            CachedBlock::Present(b) => b.len().min(u32::MAX as usize) as u32,
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

async fn bootstrap(state: Arc<AppState>, cancel: CancellationToken) -> Result<()> {
    let epoch = fetch_epoch_account(&state.upstream)
        .await
        .context("fetching epoch account during bootstrap")?;
    let epoch_start = epoch.start_slot.0;
    state.stats.epoch_start_slot.store(epoch_start, Ordering::Relaxed);

    let live = fetch_live_slot(&state.upstream)
        .await
        .context("fetching live slot during bootstrap")?;
    state.stats.bootstrap_target_slot.store(live, Ordering::Relaxed);
    state.stats.last_observed_live_slot.store(live, Ordering::Relaxed);

    info!(
        epoch_start_slot = epoch_start,
        target_slot = live,
        slots_to_fetch = live.saturating_sub(epoch_start) + 1,
        "rpc-cache bootstrap starting"
    );

    if epoch_start > live {
        warn!(
            epoch_start_slot = epoch_start,
            live_slot = live,
            "epoch start is ahead of live slot; skipping bootstrap fill"
        );
        return Ok(());
    }

    fetch_range(&state, epoch_start..=live, BOOTSTRAP_CONCURRENCY, &cancel).await;
    state.stats.newest_cached_slot.store(live, Ordering::Relaxed);

    info!(
        slots_fetched = state.stats.slots_fetched.load(Ordering::Relaxed),
        slots_skipped = state.stats.slots_skipped.load(Ordering::Relaxed),
        newest_cached_slot = live,
        "rpc-cache bootstrap complete"
    );
    Ok(())
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

        let from = newest + 1;
        fetch_range(&state, from..=live, LIVE_TAIL_CONCURRENCY, &cancel).await;
        state.stats.newest_cached_slot.store(live, Ordering::Relaxed);
    }
}

async fn fetch_range(
    state: &Arc<AppState>,
    range: std::ops::RangeInclusive<u64>,
    concurrency: usize,
    cancel: &CancellationToken,
) {
    let mut set: JoinSet<()> = JoinSet::new();
    for slot in range {
        if cancel.is_cancelled() {
            break;
        }
        while set.len() >= concurrency {
            let _ = set.join_next().await;
        }
        let st = state.clone();
        let ct = cancel.clone();
        set.spawn(async move {
            if ct.is_cancelled() {
                return;
            }
            fetch_and_store(&st, slot).await;
        });
    }
    while set.join_next().await.is_some() {}
}

async fn fetch_and_store(state: &AppState, slot: u64) {
    let mut attempt: u32 = 0;
    loop {
        state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
        match fetch_block(&state.upstream, slot).await {
            BlockOutcome::Present(block) => {
                store_present(state, slot, block).await;
                return;
            }
            BlockOutcome::Skipped => {
                store_skipped(state, slot).await;
                return;
            }
            BlockOutcome::Retriable(msg) if attempt < MAX_BLOCK_FETCH_ATTEMPTS => {
                let delay = retry_delay(attempt);
                debug!(slot, attempt, delay_ms = delay.as_millis() as u64, error = %msg, "retrying block fetch");
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            BlockOutcome::Retriable(msg) => {
                warn!(slot, attempt, error = %msg, "giving up on block; storing as skipped");
                store_skipped(state, slot).await;
                return;
            }
        }
    }
}

async fn store_present(state: &AppState, slot: u64, block: UiConfirmedBlock) {
    let filtered = filter_block(block, &state.program_ids);
    let serialized = match serde_json::to_vec(&filtered) {
        Ok(v) => Bytes::from(v),
        Err(e) => {
            warn!(slot, error = %e, "failed to serialize filtered block; storing as skipped");
            store_skipped(state, slot).await;
            return;
        }
    };
    state
        .slot_store
        .insert(slot, CachedBlock::Present(serialized))
        .await;
    state.stats.slots_fetched.fetch_add(1, Ordering::Relaxed);
}

async fn store_skipped(state: &AppState, slot: u64) {
    state
        .slot_store
        .insert(slot, CachedBlock::Skipped)
        .await;
    state.stats.slots_skipped.fetch_add(1, Ordering::Relaxed);
}

fn retry_delay(attempt: u32) -> Duration {
    let shift = attempt.min(10);
    let scaled = RETRY_BASE.saturating_mul(1u32 << shift);
    scaled.min(RETRY_CAP)
}

async fn fetch_block(upstream: &Upstream, slot: u64) -> BlockOutcome {
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

    match upstream.forward(&body).await {
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

fn classify_block_error(err: &Value) -> BlockOutcome {
    let code = err.get("code").and_then(Value::as_i64);
    if matches!(code, Some(SKIPPED_SLOT_CODE) | Some(SKIPPED_OR_LTS_CODE)) {
        return BlockOutcome::Skipped;
    }
    let msg = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    if msg.to_lowercase().contains("skipped") {
        return BlockOutcome::Skipped;
    }
    BlockOutcome::Retriable(format!(
        "code={} message={}",
        code.unwrap_or(0),
        msg
    ))
}

fn upstream_to_outcome(e: &UpstreamError) -> BlockOutcome {
    BlockOutcome::Retriable(e.to_string())
}

async fn fetch_live_slot(upstream: &Upstream) -> Result<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "finalized"}],
    });
    let env = upstream
        .forward(&body)
        .await
        .map_err(|e| anyhow!("getSlot upstream: {e}"))?;
    let result = env.result.ok_or_else(|| anyhow!("getSlot returned no result"))?;
    result
        .as_u64()
        .ok_or_else(|| anyhow!("getSlot result not a number: {result}"))
}

async fn fetch_epoch_account(upstream: &Upstream) -> Result<Epoch> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            EPOCH_ADDRESS.to_string(),
            {"encoding": "base64", "commitment": "finalized"},
        ],
    });
    let env = upstream
        .forward(&body)
        .await
        .map_err(|e| anyhow!("getAccountInfo upstream: {e}"))?;
    let result = env
        .result
        .ok_or_else(|| anyhow!("getAccountInfo returned no result"))?;
    let value = result
        .get("value")
        .ok_or_else(|| anyhow!("getAccountInfo result has no `value`"))?;
    if value.is_null() {
        return Err(anyhow!("epoch account does not exist at {EPOCH_ADDRESS}"));
    }
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("epoch account data missing or not base64"))?;
    let decoded = base64::decode(data)
        .map_err(|e| anyhow!("epoch account base64 decode: {e}"))?;
    let epoch = Epoch::unpack_with_discriminator(&decoded)
        .map_err(|e| anyhow!("epoch account unpack: {e}"))?;
    Ok(*epoch)
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

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow!("task join: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_grows_then_caps() {
        let d0 = retry_delay(0);
        let d1 = retry_delay(1);
        let d10 = retry_delay(10);
        assert!(d1 > d0);
        assert!(d10 <= RETRY_CAP);
    }

    #[test]
    fn classify_block_error_skipped_by_code() {
        let err = json!({"code": SKIPPED_SLOT_CODE, "message": "anything"});
        assert!(matches!(classify_block_error(&err), BlockOutcome::Skipped));

        let err = json!({"code": SKIPPED_OR_LTS_CODE, "message": "anything"});
        assert!(matches!(classify_block_error(&err), BlockOutcome::Skipped));
    }

    #[test]
    fn classify_block_error_skipped_by_message_text() {
        let err = json!({"code": -1, "message": "Slot 42 was Skipped or missing"});
        assert!(matches!(classify_block_error(&err), BlockOutcome::Skipped));
    }

    #[test]
    fn classify_block_error_retriable_otherwise() {
        let err = json!({"code": -32099, "message": "transient hiccup"});
        assert!(matches!(
            classify_block_error(&err),
            BlockOutcome::Retriable(_)
        ));
    }
}
