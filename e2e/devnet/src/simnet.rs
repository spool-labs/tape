use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use peer_tls::{apply_pinned_tls, install_default_provider};
use rand::RngCore;
use reqwest::Client;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::signature::Keypair;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_protocol::api::NODE_HEALTH_PATH;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::app::Command;
use crate::log_layer::LogHistogram;
use crate::poller::{PollerHandle, PollerUpdate, SnapshotHandle};
use crate::stake_fuzzer::StakeFuzzer;
use crate::verify::verify_spool_integrity;

const SLOT_BUMP: u64 = 1;
const UPLOAD_EPOCHS: u64 = 100;
const UPLOAD_RETRY_BASE_MS: u64 = 500;
const UPLOAD_RETRY_MAX_MS: u64 = 60_000;
const UPLOAD_STALL_THRESHOLD_SECS: u64 = 20;
const HTTP_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const HTTP_HEALTH_TIMEOUT: Duration = Duration::from_millis(750);
const INITIAL_NODES: usize = GROUP_SIZE;
const INITIAL_STAKE_TAPE: u64 = 1_000;
const DEFAULT_TARGET_GROUPS: u64 = 5;

enum UploadResult {
    AttemptStarted {
        upload_id: Address,
    },
    Success {
        upload_id: Address,
        expiry_epoch: u64,
    },
    Retrying {
        upload_id: Address,
        error: String,
        next_retry_in_ms: u64,
    },
    Failed {
        upload_id: Address,
    },
}

pub fn run(
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<Command>,
    snapshot: SnapshotHandle,
    histogram: LogHistogram,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("build tokio runtime");

    rt.block_on(async_run(cmd_rx, snapshot, histogram));
}

async fn async_run(
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<Command>,
    snapshot: SnapshotHandle,
    histogram: LogHistogram,
) {
    tracing::info!("starting devnet harness");
    let harness = match init_harness().await {
        Ok(harness) => harness,
        Err(e) => {
            tracing::error!("init failed: {e:#}");
            while let Some(cmd) = cmd_rx.recv().await {
                if matches!(cmd, Command::Quit) {
                    break;
                }
            }
            return;
        }
    };

    let rpc = harness.chain().rpc().clone();
    let poller = PollerHandle::spawn(rpc.clone(), snapshot, histogram);
    publish_running_nodes(&harness, &poller);

    let (upload_tx, upload_rx) = mpsc::unbounded_channel();
    let mut state = SimnetState {
        harness,
        poller,
        stake_fuzzer: StakeFuzzer::new(),
        stake_fuzz_enabled: false,
        prev_epoch: 0,
        upload_pending: 0,
        upload_completed: Vec::new(),
        upload_failed: 0,
        upload_retries: 0,
        upload_running: HashMap::new(),
        upload_last_retry_error: None,
        upload_next_retry_in_ms: None,
        upload_next_retry_deadlines: HashMap::new(),
        upload_retry_in_progress: false,
        upload_tx,
        upload_rx,
        health_clients: HashMap::new(),
    };

    tracing::info!("ready");

    let mut epoch_interval = tokio::time::interval(Duration::from_secs(2));
    let mut status_interval = tokio::time::interval(Duration::from_millis(250));
    let mut health_interval = tokio::time::interval(HTTP_HEALTH_POLL_INTERVAL);
    health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::AddNode) => {
                        let id = state.harness.nodes().len();
                        tracing::info!("adding node {id}");
                        if let Err(e) = state.add_node().await {
                            tracing::error!("add_node failed: {e:#}");
                        } else {
                            tracing::info!("node {id} added");
                        }
                    }
                    Some(Command::RemoveNode) => {
                        tracing::info!("removing node");
                        if let Err(e) = state.remove_node().await {
                            tracing::error!("remove failed: {e:#}");
                        } else {
                            tracing::info!("node removed");
                        }
                    }
                    Some(Command::UploadBlob) => {
                        let tape_key = TapeKey::generate();
                        let upload_id = tape_key.address();
                        state.upload_pending += 1;
                        state.upload_running.insert(upload_id, Instant::now());
                        let rpc = state.harness.chain().rpc().clone();
                        let admin_bytes = state.harness.admin().to_bytes();
                        let tx = state.upload_tx.clone();
                        tokio::spawn(async move {
                            tracing::info!("uploading blob");
                            let admin = Keypair::try_from(admin_bytes.as_ref()).unwrap();
                            match upload_random_blob(rpc, &admin, tape_key, tx.clone()).await {
                                Ok(expiry) => {
                                    tracing::info!("uploaded, expires epoch {expiry}");
                                    let _ = tx.send(UploadResult::Success {
                                        upload_id,
                                        expiry_epoch: expiry,
                                    });
                                }
                                Err(e) => {
                                    tracing::error!("upload failed: {e:#}");
                                    let _ = tx.send(UploadResult::Failed { upload_id });
                                }
                            }
                        });
                    }
                    Some(Command::ToggleStakeFuzz) => {
                        state.stake_fuzz_enabled = !state.stake_fuzz_enabled;
                        if state.stake_fuzz_enabled {
                            state.prev_epoch = state.current_epoch().await;
                        }
                        tracing::info!("stake fuzz {}", if state.stake_fuzz_enabled { "on" } else { "off" });
                        state.poller.send(PollerUpdate::StakeFuzzStatus {
                            enabled: state.stake_fuzz_enabled,
                            succeeded: state.stake_fuzzer.tx_succeeded,
                            failed: state.stake_fuzzer.tx_failed,
                        });
                    }
                    Some(Command::Quit) | None => {
                        tracing::info!("shutting down");
                        if let Err(error) = state.harness.stop_all().await {
                            tracing::warn!("node stop failed during shutdown: {error:#}");
                        }
                        break;
                    }
                }
            }
            result = state.upload_rx.recv() => {
                match result {
                    Some(UploadResult::AttemptStarted { upload_id }) => {
                        state.upload_running.insert(upload_id, Instant::now());
                        state.upload_next_retry_deadlines.remove(&upload_id);
                    }
                    Some(UploadResult::Success {
                        upload_id,
                        expiry_epoch,
                    }) => {
                        state.upload_pending -= 1;
                        state.upload_completed.push(expiry_epoch);
                        state.upload_running.remove(&upload_id);
                        state.upload_next_retry_deadlines.remove(&upload_id);
                    }
                    Some(UploadResult::Retrying {
                        upload_id,
                        error,
                        next_retry_in_ms,
                    }) => {
                        state.upload_retries += 1;
                        state.upload_last_retry_error = Some(error);
                        state.upload_running.remove(&upload_id);
                        let deadline = Instant::now() + Duration::from_millis(next_retry_in_ms);
                        state
                            .upload_next_retry_deadlines
                            .insert(upload_id, deadline);
                    }
                    Some(UploadResult::Failed { upload_id }) => {
                        state.upload_pending -= 1;
                        state.upload_failed += 1;
                        state.upload_running.remove(&upload_id);
                        state.upload_next_retry_deadlines.remove(&upload_id);
                    }
                    None => {}
                }
                let epoch = state.prev_epoch;
                let certified = state.upload_completed.iter().filter(|e| **e > epoch).count() as u64;
                let expired = state.upload_completed.iter().filter(|e| **e <= epoch).count() as u64;
                state.refresh_upload_retry_countdown();
                state.poller.send(PollerUpdate::UploadStatus {
                    pending: state.upload_pending,
                    certified,
                    expired,
                    failed: state.upload_failed,
                    retries: state.upload_retries,
                    running: state.running_upload_count(),
                    waiting_retry: state.waiting_retry_count(),
                    stalled: state.stalled_upload_count(),
                    last_retry_error: state.upload_last_retry_error.clone(),
                    next_retry_in_ms: state.upload_next_retry_in_ms,
                    retry_in_progress: state.upload_retry_in_progress,
                });
            }
            _ = epoch_interval.tick() => {
                let rpc = state.harness.chain().rpc().clone();
                let epoch = state.current_epoch().await;

                // Upload status
                let certified = state.upload_completed.iter().filter(|e| **e > epoch).count() as u64;
                let expired = state.upload_completed.iter().filter(|e| **e <= epoch).count() as u64;
                state.refresh_upload_retry_countdown();
                state.poller.send(PollerUpdate::UploadStatus {
                    pending: state.upload_pending,
                    certified,
                    expired,
                    failed: state.upload_failed,
                    retries: state.upload_retries,
                    running: state.running_upload_count(),
                    waiting_retry: state.waiting_retry_count(),
                    stalled: state.stalled_upload_count(),
                    last_retry_error: state.upload_last_retry_error.clone(),
                    next_retry_in_ms: state.upload_next_retry_in_ms,
                    retry_in_progress: state.upload_retry_in_progress,
                });

                // Spool integrity check on epoch change
                if epoch != state.prev_epoch && epoch > 0 {
                    verify_spool_integrity(state.harness.nodes());
                }

                // Stake fuzzing
                if state.stake_fuzz_enabled && epoch != state.prev_epoch {
                    let authorities: Vec<_> = state
                        .harness
                        .nodes()
                        .iter()
                        .filter(|node| node.is_running())
                        .map(|node| node.authority())
                        .collect();
                    state
                        .stake_fuzzer
                        .step_epoch(&rpc, state.harness.admin(), &authorities)
                        .await;
                    state.poller.send(PollerUpdate::StakeFuzzStatus {
                        enabled: state.stake_fuzz_enabled,
                        succeeded: state.stake_fuzzer.tx_succeeded,
                        failed: state.stake_fuzzer.tx_failed,
                    });
                }
                state.prev_epoch = epoch;
            }
            _ = status_interval.tick() => {
                let epoch = state.prev_epoch;
                let certified = state.upload_completed.iter().filter(|e| **e > epoch).count() as u64;
                let expired = state.upload_completed.iter().filter(|e| **e <= epoch).count() as u64;
                state.refresh_upload_retry_countdown();
                state.poller.send(PollerUpdate::UploadStatus {
                    pending: state.upload_pending,
                    certified,
                    expired,
                    failed: state.upload_failed,
                    retries: state.upload_retries,
                    running: state.running_upload_count(),
                    waiting_retry: state.waiting_retry_count(),
                    stalled: state.stalled_upload_count(),
                    last_retry_error: state.upload_last_retry_error.clone(),
                    next_retry_in_ms: state.upload_next_retry_in_ms,
                    retry_in_progress: state.upload_retry_in_progress,
                });
            }
            _ = health_interval.tick() => {
                state.poll_http_health().await;
            }
        }
    }
}

async fn init_harness() -> Result<SimnetHarness> {
    let mut harness = SimnetBuilder::new()
        .node_count(INITIAL_NODES)
        .runtime_mode(NodeRuntimeMode::Full)
        .slot_advance_per_tx(SLOT_BUMP)
        .build()
        .context("build harness")?;

    let initial_nodes: Vec<usize> = (0..INITIAL_NODES).collect();
    {
        let scenario = harness.scenario();
        scenario.init_system().await.context("init system")?;
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .context("register initial nodes")?;
        scenario
            .stake_all(INITIAL_STAKE_TAPE)
            .await
            .context("stake initial nodes")?;
        scenario
            .set_spool_groups_many(&initial_nodes, DEFAULT_TARGET_GROUPS)
            .await
            .context("set initial spool group preferences")?;
        scenario
            .set_committee_size_many(&initial_nodes, INITIAL_NODES as u64)
            .await
            .context("set initial committee size preferences")?;
        scenario.start_network().await.context("start network")?;
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .context("start initial runtimes")?;

    Ok(harness)
}

fn publish_running_nodes(harness: &SimnetHarness, poller: &PollerHandle) {
    for node in harness.nodes() {
        if !node.is_running() {
            continue;
        }
        let Some(runtime_status) = node.runtime_status() else {
            continue;
        };
        poller.send(PollerUpdate::AddNode(
            node.id(),
            node.context(),
            runtime_status,
        ));
    }
}

struct SimnetState {
    harness: SimnetHarness,
    poller: PollerHandle,
    stake_fuzzer: StakeFuzzer,
    stake_fuzz_enabled: bool,
    prev_epoch: u64,
    upload_pending: u64,
    upload_completed: Vec<u64>,
    upload_failed: u64,
    upload_retries: u64,
    upload_running: HashMap<Address, Instant>,
    upload_last_retry_error: Option<String>,
    upload_next_retry_in_ms: Option<u64>,
    upload_next_retry_deadlines: HashMap<Address, Instant>,
    upload_retry_in_progress: bool,
    upload_tx: mpsc::UnboundedSender<UploadResult>,
    upload_rx: mpsc::UnboundedReceiver<UploadResult>,
    health_clients: HashMap<usize, Client>,
}

impl SimnetState {
    async fn add_node(&mut self) -> Result<()> {
        let id = self.harness.add_node().context("add node to harness")?;

        {
            let scenario = self.harness.scenario();
            scenario
                .register_many(&[id], BasisPoints(100))
                .await
                .with_context(|| format!("register node {id}"))?;
            scenario
                .stake_many(&[id], INITIAL_STAKE_TAPE)
                .await
                .with_context(|| format!("stake node {id}"))?;
            scenario
                .set_spool_groups(id, DEFAULT_TARGET_GROUPS)
                .await
                .with_context(|| format!("set spool group preference for node {id}"))?;

            let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
            scenario
                .set_committee_size_many(&all, self.harness.nodes().len() as u64)
                .await
                .context("set committee size preferences")?;
        }

        self.harness
            .start_nodes_with_retry(&[id], 3, Duration::from_millis(200))
            .await
            .with_context(|| format!("start node {id}"))?;

        let node = self
            .harness
            .node(id)
            .with_context(|| format!("node {id} missing after start"))?;
        let runtime_status = node
            .runtime_status()
            .expect("runtime status available after start");
        self.poller
            .send(PollerUpdate::AddNode(id, node.context(), runtime_status));
        Ok(())
    }

    async fn remove_node(&mut self) -> Result<()> {
        let Some(id) = self
            .harness
            .nodes()
            .iter()
            .rev()
            .find(|node| node.is_running())
            .map(|node| node.id())
        else {
            return Ok(());
        };

        self.harness
            .stop_nodes(&[id])
            .await
            .with_context(|| format!("stop node {id}"))?;
        self.poller.send(PollerUpdate::RemoveNode(id));
        self.health_clients.remove(&id);
        Ok(())
    }

    async fn poll_http_health(&mut self) {
        // Snapshot enough about each running node to build clients without
        // holding a borrow on `self.harness` during the cache-mutating call.
        let snapshots: Vec<(usize, String, NetworkTlsPubkey)> = self
            .harness
            .nodes()
            .iter()
            .filter(|node| node.is_running())
            .map(|node| {
                (
                    node.id(),
                    format!("{}{}", node.base_url(), NODE_HEALTH_PATH),
                    node.tls_pubkey(),
                )
            })
            .collect();

        let mut probes = JoinSet::new();
        for (id, url, tls_pubkey) in snapshots {
            let client = self.health_client_for_key(id, tls_pubkey);
            let Some(client) = client else {
                self.poller
                    .send(PollerUpdate::NodeHttpStatus { id, healthy: false });
                continue;
            };
            probes.spawn(async move {
                let healthy = match client.get(url).send().await {
                    Ok(response) => response.status().is_success(),
                    Err(_) => false,
                };
                (id, healthy)
            });
        }

        while let Some(result) = probes.join_next().await {
            match result {
                Ok((id, healthy)) => {
                    self.poller
                        .send(PollerUpdate::NodeHttpStatus { id, healthy });
                }
                Err(error) => {
                    tracing::warn!("node health probe join failed: {error}");
                }
            }
        }
    }

    fn health_client_for_key(
        &mut self,
        id: usize,
        tls_pubkey: NetworkTlsPubkey,
    ) -> Option<Client> {
        if let Some(client) = self.health_clients.get(&id) {
            return Some(client.clone());
        }

        install_default_provider();
        let builder = Client::builder().timeout(HTTP_HEALTH_TIMEOUT);
        let builder = match apply_pinned_tls(builder, tls_pubkey) {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!(node = id, error = %err, "failed to configure health TLS pin");
                return None;
            }
        };
        let client = match builder.build() {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(node = id, error = %err, "failed to build health client");
                return None;
            }
        };

        self.health_clients.insert(id, client.clone());
        Some(client)
    }

    async fn current_epoch(&self) -> u64 {
        self.harness
            .scenario()
            .current_epoch_number()
            .await
            .unwrap_or(0)
    }

    fn refresh_upload_retry_countdown(&mut self) {
        let now = Instant::now();
        self.upload_retry_in_progress = false;
        let mut next_retry_ms = None;

        self.upload_next_retry_deadlines.iter().for_each(|(_, deadline)| {
            if *deadline <= now {
                self.upload_retry_in_progress = true;
            } else {
                let remaining_ms = deadline
                    .duration_since(now)
                    .as_millis()
                    .max(1) as u64;
                next_retry_ms = Some(next_retry_ms.map_or(remaining_ms, |current: u64| {
                    current.min(remaining_ms)
                }));
            }
        });

        self.upload_next_retry_in_ms = next_retry_ms;
    }

    fn running_upload_count(&self) -> u64 {
        self.upload_running.len() as u64
    }

    fn waiting_retry_count(&self) -> u64 {
        self.upload_next_retry_deadlines.len() as u64
    }

    fn stalled_upload_count(&self) -> u64 {
        let threshold = Duration::from_secs(UPLOAD_STALL_THRESHOLD_SECS);
        let now = Instant::now();
        self.upload_running
            .values()
            .filter(|started_at| now.duration_since(**started_at) >= threshold)
            .count() as u64
    }

}

async fn upload_random_blob(
    rpc: LiteSvmRpc,
    admin: &Keypair,
    tape_key: TapeKey,
    tx: mpsc::UnboundedSender<UploadResult>,
) -> Result<u64> {
    let (key, data) = {
        let mut rng = rand::thread_rng();
        let size = (rng.next_u32() as usize % (1024 * 1024 - 1024)) + 1024; // 1KB..1MB
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        let key = hash(&data[..32.min(data.len())]);
        (key, data)
    };

    let payer = CryptoKeypair::from_solana_keypair(admin)
        .expect("convert devnet uploader to crypto keypair");
    let sdk = Tapedrive::new(rpc, payer);
    let upload_id = tape_key.address();
    let capacity = StorageUnits::from_bytes(data.len() as u64);
    let reserve_capacity = capacity + StorageUnits::mb(1); // 1 MB headroom

    let mut attempt = 0u32;
    let expiry_epoch = loop {
        let _ = tx.send(UploadResult::AttemptStarted { upload_id });
        match sdk.reserve(&tape_key, reserve_capacity, UPLOAD_EPOCHS).await {
            Ok(tape) => break tape.expiry_epoch.as_u64(),
            Err(error) if !is_retriable_upload_error(&error) => {
                return Err(anyhow::Error::from(error))
                    .context("reserve failed with non-retriable error");
            }
            Err(error) => {
                let delay = upload_retry_delay(attempt);
                let _ = tx.send(UploadResult::Retrying {
                    upload_id,
                    error: format!("{error:#}"),
                    next_retry_in_ms: delay.as_millis() as u64,
                });
                tracing::warn!(attempt = attempt + 1, delay_ms = delay.as_millis(), error = %error, "reserve failed, retrying");
                attempt = attempt.saturating_add(1);
                sleep(delay).await;
            }
        }
    };

    let _ = tx.send(UploadResult::AttemptStarted { upload_id });
    sdk.write_track(&tape_key, key, &data)
        .await
        .map_err(anyhow::Error::from)
        .context("write track")?;
    Ok(expiry_epoch)
}

fn is_retriable_upload_error(error: &TapedriveError) -> bool {
    !matches!(
        error,
        TapedriveError::CommitmentMismatch
            | TapedriveError::InvalidArgument(_)
            | TapedriveError::InsufficientCapacity { .. }
    )
}

fn upload_retry_delay(attempt: u32) -> Duration {
    let delay_ms = (UPLOAD_RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(12))).min(UPLOAD_RETRY_MAX_MS);
    Duration::from_millis(delay_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_tape_succeeds_after_chain_init() {
        let handle = std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let mut harness = init_harness().await.expect("init harness");
                    let payer = CryptoKeypair::from_solana_keypair(harness.admin())
                        .expect("convert admin keypair");
                    let sdk = Tapedrive::new(harness.chain().rpc().clone(), payer);
                    let tape_key = TapeKey::generate();

                    let tape = sdk
                        .reserve(&tape_key, StorageUnits::mb(2), UPLOAD_EPOCHS)
                        .await
                        .expect("reserve tape");

                    assert_eq!(tape.authority, tape_key.pubkey().into());
                    assert_eq!(tape.capacity, StorageUnits::mb(2));
                    assert_eq!(tape.expiry_epoch.as_u64(), tape.active_epoch.as_u64() + UPLOAD_EPOCHS);
                    harness.stop_all().await.expect("stop harness");
                });
            })
            .expect("spawn reserve test thread");

        handle.join().expect("reserve test thread joins");
    }

    #[test]
    fn add_late_node() {
        let handle = std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let harness = init_harness().await.expect("init harness");
                    let (upload_tx, upload_rx) = mpsc::unbounded_channel();
                    let mut state = SimnetState {
                        harness,
                        poller: PollerHandle::spawn_noop(),
                        stake_fuzzer: StakeFuzzer::new(),
                        stake_fuzz_enabled: false,
                        prev_epoch: 0,
                        upload_pending: 0,
                        upload_completed: Vec::new(),
                        upload_failed: 0,
                        upload_retries: 0,
                        upload_running: HashMap::new(),
                        upload_last_retry_error: None,
                        upload_next_retry_in_ms: None,
                        upload_next_retry_deadlines: HashMap::new(),
                        upload_retry_in_progress: false,
                        upload_tx,
                        upload_rx,
                        health_clients: HashMap::new(),
                    };

                    state
                        .add_node()
                        .await
                        .unwrap_or_else(|e| panic!("add_node failed: {e:#}"));
                    state.harness.stop_all().await.expect("stop harness");
                });
            })
            .unwrap();
        handle.join().unwrap();
    }
}
