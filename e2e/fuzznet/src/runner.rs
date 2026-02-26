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
use tape_core::erasure::SPOOL_COUNT;
use tape_core::types::coin::TAPE;
use tape_crypto::{hash, Hash};
use tape_e2e_simnet::SimnetHarness;
use tape_store::types::SpoolStatus;
use tracing::Level;

use crate::log_histogram::LogHistogram;
use crate::stats::{EpochStats, FuzzPhase, FuzzStats, UploadRecord};

#[derive(Debug, Clone, Copy)]
pub struct FuzzConfig {
    pub node_count: usize,
    pub target_epochs: u64,
    pub churn_enabled: bool,
    pub churn_prob: f64,
    pub uploads_min: usize,
    pub uploads_max: usize,
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
        {
            let scenario = harness.scenario();
            scenario
                .self_advance_epoch(config.epoch_timeout)
                .await
                .context("warmup self_advance_epoch")?;
        }
    }

    let mut alive: HashSet<usize> = (0..config.node_count).collect();
    let mut stopped: HashSet<usize> = HashSet::new();
    let mut recover_streak = 0usize;

    let start_epoch = {
        let scenario = harness.scenario();
        scenario
            .current_epoch_number()
            .await
            .context("start epoch")?
    };
    let target_epoch = start_epoch + config.target_epochs;
    let mut iteration = 0u64;

    loop {
        if aborted.load(Ordering::Acquire) {
            break;
        }

        let next_epoch = {
            let scenario = harness.scenario();
            scenario
                .current_epoch_number()
                .await
                .context("current_epoch_number")?
        };

        if next_epoch >= target_epoch {
            break;
        }

        iteration += 1;

        {
            let mut state = stats.lock().expect("stats lock poisoned");
            state.phase = FuzzPhase::Fuzzing {
                iteration,
                current_epoch: next_epoch,
            };
        }

        let epoch_started = Instant::now();
        let mut churn_stopped = 0usize;
        let mut churn_started = 0usize;
        let mut uploads_this_epoch = 0usize;
        let mut warnings: Vec<String> = Vec::new();
        let upload_count = rng.gen_range(config.uploads_min..=config.uploads_max);


        for upload_idx in 0..upload_count {
            if aborted.load(Ordering::Acquire) {
                break;
            }

            let size = rng.gen_range(config.blob_size_min..=config.blob_size_max);
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);

            let key_source = format!("{seed}-{next_epoch}-{upload_idx}");
            let key: Hash = hash::hash(key_source.as_bytes());

            let upload = {
                let scenario = harness.scenario();
                let tape_lifetime = 10;
                scenario.upload(&user_keypair, key, &data, tape_lifetime).await
            };

            match upload {
                Ok((tape_key, _track)) => {
                    let track_address = tape_key.track_address(&key);
                    uploads_this_epoch += 1;
                    let mut state = stats.lock().expect("stats lock poisoned");
                    state.upload_registry.push(UploadRecord {
                        key,
                        data,
                        track_address,
                        epoch: next_epoch,
                    });
                }
                Err(error) => {
                    warnings.push(format!("upload failed: {error}"));
                }
            }
        }

        if config.churn_enabled {
            let bft = {
                let scenario = harness.scenario();
                scenario.bft_targets()
            };

            let max_stoppable = alive.len().saturating_sub(bft.min_for_advance);
            if max_stoppable > 0 && rng.gen_bool(config.churn_prob) {
                let mut stop_choices: Vec<usize> = alive.iter().copied().collect();
                stop_choices.shuffle(&mut rng);
                let churn_count = rng.gen_range(1..=max_stoppable);
                let stop_picks = stop_choices.into_iter().take(churn_count).collect::<Vec<_>>();

            if !stop_picks.is_empty() {
                harness.stop_nodes(&stop_picks).await.context("stop_nodes")?;
                for node in &stop_picks {
                    alive.remove(node);
                    stopped.insert(*node);
                    churn_stopped += 1;
                    }
                }

                let mut start_choices: Vec<usize> = stopped.iter().copied().collect();
                start_choices.shuffle(&mut rng);
                let start_count = start_choices.len().min(churn_count);
                let start_picks = start_choices.into_iter().take(start_count).collect::<Vec<_>>();

                if !start_picks.is_empty() {
                    harness.start_nodes(&start_picks).await.context("start_nodes")?;
                    for node in &start_picks {
                        stopped.remove(node);
                        alive.insert(*node);
                        churn_started += 1;
                    }
                }
            }
        }

        let alive_indices: Vec<usize> = alive.iter().copied().collect();
        if alive_indices.is_empty() {
            warnings.push("epoch had no alive nodes".to_string());
            let epoch = {
                let scenario = harness.scenario();
                scenario.current_epoch_number().await.context("read epoch")?
            };
            let logs = histogram.snapshot_and_reset();
            {
                let mut state = stats.lock().expect("stats lock poisoned");
                state.epochs.push(EpochStats {
                    epoch,
                    wall_duration: epoch_started.elapsed(),
                    uploads: uploads_this_epoch,
                    churn_stopped,
                    churn_started,
                    spools_active: 0,
                    spools_sync: 0,
                    spools_recover: 0,
                    spools_locked: 0,
                    log_counts: logs,
                    warnings,
                });
            }
            break;
        }

        {
            let scenario = harness.scenario();
            scenario
                .self_advance_epoch(config.epoch_timeout)
                .await
                .context("self_advance_epoch")?;
            scenario
                .wait_nodes_active(&alive_indices, config.epoch_timeout)
                .await
                .context("wait_nodes_active")?;
        }

        let (active, sync, recover, locked, total) =
            spool_status_snapshot(harness, &alive_indices).context("spool_status_snapshot")?;
        if total != SPOOL_COUNT as usize {
            warnings.push(format!("spool coverage mismatch: {total}/{SPOOL_COUNT}"));
        }

        if recover > 0 {
            recover_streak += 1;
        } else {
            recover_streak = 0;
        }
        if recover_streak > 5 {
            warnings.push(format!("stuck recoveries for {recover_streak} consecutive epochs"));
        }

        let wall_duration = epoch_started.elapsed();
        let epoch = {
            let scenario = harness.scenario();
            scenario.current_epoch_number().await.context("read next epoch")?
        };

        {
            let state = stats.lock().expect("stats lock poisoned");
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
        }

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

        {
            let mut state = stats.lock().expect("stats lock poisoned");
            state.epochs.push(EpochStats {
                epoch,
                wall_duration,
                uploads: uploads_this_epoch,
                churn_stopped,
                churn_started,
                spools_active: active,
                spools_sync: sync,
                spools_recover: recover,
                spools_locked: locked,
                log_counts: logs,
                warnings,
            });
        }
    }

    let records = {
        let state = stats.lock().expect("stats lock poisoned");
        state.upload_registry.clone()
    };

    {
        let mut state = stats.lock().expect("stats lock poisoned");
        state.phase = FuzzPhase::Verifying {
            checked: 0,
            total: records.len(),
        };
    }

    if records.is_empty() {
        harness.stop_all().await.context("stop_all")?;
        let mut state = stats.lock().expect("stats lock poisoned");
        let final_pass = !aborted.load(Ordering::Acquire);
        state.phase = FuzzPhase::Done { passed: final_pass };
        return Ok(state.clone());
    }

    for (index, record) in records.iter().enumerate() {
        if aborted.load(Ordering::Acquire) {
            break;
        }

        let result = {
            let scenario = harness.scenario();
            scenario.download(&user_keypair, &record.track_address).await
        };

        let ok = match result {
            Ok(downloaded) => downloaded == record.data,
            Err(_) => false,
        };

        {
            let mut state = stats.lock().expect("stats lock poisoned");
            state.download_results.push((record.track_address, ok));
            state.phase = FuzzPhase::Verifying {
                checked: index + 1,
                total: records.len(),
            };
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
