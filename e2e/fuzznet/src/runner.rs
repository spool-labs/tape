use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use std::sync::atomic::Ordering::Relaxed;
use tape_core::erasure::SPOOL_COUNT;
use tape_core::types::coin::TAPE;
use tape_crypto::{hash, Hash};
use tape_e2e_simnet::SimnetHarness;
use tape_store::types::SpoolStatus;
use tokio::time::interval;
use tracing::Level;

use crate::log_histogram::LogHistogram;
use crate::stats::{EpochStats, FuzzPhase, FuzzStats, UploadRecord};

#[derive(Debug, Clone, Copy)]
pub struct FuzzConfig {
    pub node_count: usize,
    pub target_epochs: u64,
    pub min_alive: usize,
    pub poll_interval: Duration,
    pub upload_interval: Duration,
    pub download_interval: Duration,
    pub churn_interval: Duration,
    pub blob_size_min: usize,
    pub blob_size_max: usize,
    pub epoch_timeout: Duration,
}

pub async fn run_fuzz(
    harness: &mut SimnetHarness,
    seed: u64,
    stats: &Arc<Mutex<FuzzStats>>,
    histogram: &LogHistogram,
    aborted: &Arc<AtomicBool>,
    config: FuzzConfig,
) -> Result<FuzzStats> {
    let mut rng = StdRng::seed_from_u64(seed);
    {
        let mut state = stats.lock().expect("stats lock poisoned");
        state.phase = FuzzPhase::Bootstrap;
    }

    let health_timeout = Duration::from_secs(30);
    harness
        .bootstrap_nodes(tape_core::types::BasisPoints(100), 1_000, health_timeout)
        .await
        .context("bootstrap_nodes")?;

    // Create a separate user keypair for uploads/downloads (not a node)
    let user_keypair = Keypair::new();
    harness
        .chain()
        .airdrop(&user_keypair.pubkey(), 50_000_000_000)
        .context("airdrop user")?;

    // Transfer TAPE tokens from admin (treasury) to user keypair
    {
        let admin = harness.admin();
        let ixs = tape_api::helpers::build_authority_with_tokens_ix(
            admin.pubkey(),
            user_keypair.pubkey(),
            TAPE(1_000_000 * TAPE::SCALE),
        );
        harness
            .chain()
            .send_instructions_and_advance(admin, ixs, 1)
            .await
            .context("fund user with TAPE")?;
    }

    {
        let mut state = stats.lock().expect("stats lock poisoned");
        state.phase = FuzzPhase::Warmup;
    }

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_active(&(0..config.node_count).collect::<Vec<_>>(), Duration::from_secs(60))
            .await
            .context("wait_nodes_active")?;
    }

    for _ in 0..2 {
        let current = {
            let scenario = harness.scenario();
            scenario.current_epoch_number().await.context("warmup epoch")?
        };
        {
            let scenario = harness.scenario();
            scenario
                .wait_epoch_change(current, config.epoch_timeout)
                .await
                .context("warmup wait_epoch_change")?;
            scenario
                .wait_phase("Active", config.epoch_timeout)
                .await
                .context("warmup wait_phase")?;
        }
    }

    let mut alive: HashSet<usize> = (0..config.node_count).collect();
    let mut stopped: HashSet<usize> = HashSet::new();
    let mut recover_streak = 0usize;
    let mut prev_sync_snapshot: u64 = 0;
    let mut prev_repair_snapshot: u64 = 0;

    let start_epoch = {
        let scenario = harness.scenario();
        scenario
            .current_epoch_number()
            .await
            .context("start epoch")?
    };
    let target_epoch = start_epoch + config.target_epochs;
    let mut last_epoch = start_epoch;
    let mut upload_counter = 0u64;

    // Per-epoch counters (reset on each epoch transition)
    let mut epoch_uploads = 0usize;
    let mut epoch_uploaded_bytes = 0u64;
    let mut epoch_churn_stopped = 0usize;
    let mut epoch_churn_started = 0usize;
    let mut epoch_wall_start = Instant::now();
    let mut warnings: Vec<String> = Vec::new();

    {
        let mut state = stats.lock().expect("stats lock poisoned");
        state.phase = FuzzPhase::Fuzzing {
            current_epoch: start_epoch,
            target_epoch,
        };
    }

    let mut poll_tick = interval(config.poll_interval);
    let mut upload_tick = interval(config.upload_interval);
    let mut download_tick = interval(config.download_interval);
    let mut churn_tick = interval(config.churn_interval);

    loop {
        if aborted.load(Ordering::Acquire) {
            break;
        }

        tokio::select! {
            biased;

            _ = poll_tick.tick() => {
                let current_epoch = {
                    let scenario = harness.scenario();
                    scenario.current_epoch_number().await.context("poll epoch")?
                };

                if current_epoch > last_epoch {
                    snapshot_epoch(
                        harness,
                        stats,
                        histogram,
                        &config,
                        &alive,
                        current_epoch,
                        epoch_wall_start.elapsed(),
                        epoch_uploads,
                        epoch_uploaded_bytes,
                        epoch_churn_stopped,
                        epoch_churn_started,
                        &mut warnings,
                        &mut prev_sync_snapshot,
                        &mut prev_repair_snapshot,
                        &mut recover_streak,
                        target_epoch,
                    )?;

                    epoch_uploads = 0;
                    epoch_uploaded_bytes = 0;
                    epoch_churn_stopped = 0;
                    epoch_churn_started = 0;
                    epoch_wall_start = Instant::now();
                    last_epoch = current_epoch;
                }

                if current_epoch >= target_epoch {
                    break;
                }

                {
                    let mut state = stats.lock().expect("stats lock poisoned");
                    state.phase = FuzzPhase::Fuzzing {
                        current_epoch,
                        target_epoch,
                    };
                }
            }

            _ = upload_tick.tick() => {
                let current_epoch = {
                    let scenario = harness.scenario();
                    scenario.current_epoch_number().await.unwrap_or(last_epoch)
                };

                let size = rng.gen_range(config.blob_size_min..=config.blob_size_max);
                let mut data = vec![0u8; size];
                rng.fill_bytes(&mut data);

                let tape_lifetime = rng.gen_range(4u64..=20);
                upload_counter += 1;
                let key_source = format!("{seed}-{upload_counter}");
                let key: Hash = hash::hash(key_source.as_bytes());

                let upload = {
                    let scenario = harness.scenario();
                    scenario.upload(&user_keypair, key, &data, tape_lifetime).await
                };
                match upload {
                    Ok((tape_key, _track)) => {
                        let track_address = tape_key.track_address(&key);
                        epoch_uploads += 1;
                        epoch_uploaded_bytes += data.len() as u64;
                        let mut state = stats.lock().expect("stats lock poisoned");
                        state.upload_registry.push(UploadRecord {
                            key,
                            data,
                            track_address,
                            epoch: current_epoch,
                            expiry_epoch: current_epoch + tape_lifetime,
                        });
                    }
                    Err(error) => {
                        warnings.push(format!("upload failed: {error}"));
                    }
                }
            }

            _ = download_tick.tick() => {
                let current_epoch = {
                    let scenario = harness.scenario();
                    scenario.current_epoch_number().await.unwrap_or(last_epoch)
                };

                let candidates: Vec<usize> = {
                    let state = stats.lock().expect("stats lock poisoned");
                    state
                        .upload_registry
                        .iter()
                        .enumerate()
                        .filter(|(_, r)| r.expiry_epoch > current_epoch)
                        .map(|(i, _)| i)
                        .collect()
                };

                if let Some(&idx) = candidates.choose(&mut rng) {
                    let (track_address, expected_data) = {
                        let state = stats.lock().expect("stats lock poisoned");
                        let r = &state.upload_registry[idx];
                        (r.track_address, r.data.clone())
                    };
                    let result = {
                        let scenario = harness.scenario();
                        scenario.download(&user_keypair, &track_address).await
                    };
                    let ok = match result {
                        Ok(downloaded) => downloaded == expected_data,
                        Err(_) => false,
                    };
                    let mut state = stats.lock().expect("stats lock poisoned");
                    state.download_results.push((track_address, ok));
                }
            }

            _ = churn_tick.tick() => {
                let bft = {
                    let scenario = harness.scenario();
                    scenario.bft_targets()
                };

                let max_stoppable = alive
                    .len()
                    .saturating_sub(config.min_alive.max(bft.min_for_advance));

                if max_stoppable > 0 {
                    let prior_stopped: Vec<usize> = stopped.iter().copied().collect();

                    let mut stop_choices: Vec<usize> = alive.iter().copied().collect();
                    stop_choices.shuffle(&mut rng);
                    let churn_count = rng.gen_range(1..=max_stoppable.min(3));
                    let stop_picks: Vec<usize> =
                        stop_choices.into_iter().take(churn_count).collect();

                    if !stop_picks.is_empty() {
                        harness.stop_nodes(&stop_picks).await.context("stop_nodes")?;
                        for node in &stop_picks {
                            alive.remove(node);
                            stopped.insert(*node);
                            epoch_churn_stopped += 1;
                        }
                    }

                    if !prior_stopped.is_empty() {
                        let mut start_choices = prior_stopped;
                        start_choices.shuffle(&mut rng);
                        let start_count = start_choices.len().min(3);
                        let start_picks: Vec<usize> =
                            start_choices.into_iter().take(start_count).collect();

                        if !start_picks.is_empty() {
                            harness
                                .start_nodes(&start_picks)
                                .await
                                .context("start_nodes")?;
                            for node in &start_picks {
                                stopped.remove(node);
                                alive.insert(*node);
                                epoch_churn_started += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    harness.stop_all().await.context("stop_all")?;

    let final_pass = {
        let state = stats.lock().expect("stats lock poisoned");
        !aborted.load(Ordering::Acquire) && state.download_results.iter().all(|(_, pass)| *pass)
    };
    {
        let mut state = stats.lock().expect("stats lock poisoned");
        state.phase = FuzzPhase::Done {
            passed: final_pass,
        };
        return Ok(state.clone());
    }
}

#[allow(clippy::too_many_arguments)]
fn snapshot_epoch(
    harness: &SimnetHarness,
    stats: &Arc<Mutex<FuzzStats>>,
    histogram: &LogHistogram,
    config: &FuzzConfig,
    alive: &HashSet<usize>,
    current_epoch: u64,
    wall_duration: Duration,
    epoch_uploads: usize,
    epoch_uploaded_bytes: u64,
    epoch_churn_stopped: usize,
    epoch_churn_started: usize,
    warnings: &mut Vec<String>,
    prev_sync_snapshot: &mut u64,
    prev_repair_snapshot: &mut u64,
    recover_streak: &mut usize,
    target_epoch: u64,
) -> Result<()> {
    let alive_indices: Vec<usize> = alive.iter().copied().collect();
    let all_nodes: Vec<usize> = (0..config.node_count).collect();

    let sync_snapshot = sync_bytes_snapshot(harness, &all_nodes);
    let sync_bytes = sync_snapshot.saturating_sub(*prev_sync_snapshot);
    *prev_sync_snapshot = sync_snapshot;

    let repair_snapshot = repair_bytes_snapshot(harness, &all_nodes);
    let repair_bytes = repair_snapshot.saturating_sub(*prev_repair_snapshot);
    *prev_repair_snapshot = repair_snapshot;

    let (active, sync, recover, locked, total) = if !alive_indices.is_empty() {
        spool_status_snapshot(harness, &alive_indices).context("spool_status_snapshot")?
    } else {
        warnings.push("epoch had no alive nodes".to_string());
        (0, 0, 0, 0, 0)
    };

    if total > 0 && total != SPOOL_COUNT as usize {
        warnings.push(format!("spool coverage mismatch: {total}/{SPOOL_COUNT}"));
    }

    if recover > 0 {
        *recover_streak += 1;
    } else {
        *recover_streak = 0;
    }
    if *recover_streak > 5 {
        warnings.push(format!(
            "stuck recoveries for {} consecutive epochs",
            *recover_streak
        ));
    }

    let net_size = network_store_size(harness, &alive_indices);

    let logs = histogram.snapshot_and_reset();
    let error_count: u64 = logs
        .iter()
        .filter(|((level, _), _)| *level == Level::ERROR)
        .map(|(_, count)| *count)
        .sum();
    if error_count > 0 {
        let mut top_errors: Vec<_> = logs
            .iter()
            .filter(|((level, _), _)| *level == Level::ERROR)
            .map(|((_, target), count)| (target.clone(), *count))
            .collect();
        top_errors.sort_by(|a, b| b.1.cmp(&a.1));
        let top = top_errors
            .iter()
            .take(2)
            .map(|(target, count)| format!("{target}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        warnings.push(format!("error logs ({error_count}): {top}"));
    }

    let mut state = stats.lock().expect("stats lock poisoned");

    if !state.epochs.is_empty() {
        let mean = state
            .epochs
            .iter()
            .map(|entry| entry.wall_duration.as_secs_f64())
            .sum::<f64>()
            / state.epochs.len() as f64;
        if mean > 0.0 && wall_duration.as_secs_f64() > mean * 3.0 {
            warnings.push(format!(
                "epoch duration spike: {:.1}s > 3x mean {:.1}s",
                wall_duration.as_secs_f64(),
                mean
            ));
        }
    }

    state.epochs.push(EpochStats {
        epoch: current_epoch,
        wall_duration,
        uploads: epoch_uploads,
        uploaded_bytes: epoch_uploaded_bytes,
        network_size_bytes: net_size,
        alive_count: alive.len(),
        churn_stopped: epoch_churn_stopped,
        churn_started: epoch_churn_started,
        spools_active: active,
        spools_sync: sync,
        spools_recover: recover,
        spools_locked: locked,
        committee_count: alive.len(),
        sync_bytes,
        repair_bytes,
        log_counts: logs,
        warnings: std::mem::take(warnings),
    });

    state.phase = FuzzPhase::Fuzzing {
        current_epoch,
        target_epoch,
    };

    Ok(())
}

fn sync_bytes_snapshot(harness: &SimnetHarness, nodes: &[usize]) -> u64 {
    let mut total = 0u64;
    for &i in nodes {
        if let Some(node) = harness.node(i) {
            total += node.context().stats.sync_bytes_received.load(Relaxed);
        }
    }
    total
}

fn repair_bytes_snapshot(harness: &SimnetHarness, nodes: &[usize]) -> u64 {
    let mut total = 0u64;
    for &i in nodes {
        if let Some(node) = harness.node(i) {
            total += node.context().stats.repair_bytes_received.load(Relaxed);
        }
    }
    total
}

fn network_store_size(harness: &SimnetHarness, nodes: &[usize]) -> u64 {
    let mut total = 0u64;
    for &i in nodes {
        if let Some(node) = harness.node(i) {
            let size = node.context().store.inner().inner().total_size_bytes();
            total += size as u64;
        }
    }
    total
}

fn spool_status_snapshot(
    harness: &SimnetHarness,
    nodes: &[usize],
) -> Result<(usize, usize, usize, usize, usize)> {
    let mut active = 0usize;
    let mut syncing = 0usize;
    let mut recover = 0usize;
    let mut locked = 0usize;
    let mut total = 0usize;

    let scenario = harness.scenario();
    for &index in nodes {
        let statuses = scenario
            .node_spool_statuses(index)
            .with_context(|| format!("node {index} spool statuses"))?;
        for (_spool, status) in statuses {
            total += 1;
            match status {
                SpoolStatus::Active => active += 1,
                SpoolStatus::ActiveSync => syncing += 1,
                SpoolStatus::ActiveRecover => recover += 1,
                SpoolStatus::LockedToMove => locked += 1,
                _ => {}
            }
        }
    }

    Ok((active, syncing, recover, locked, total))
}
