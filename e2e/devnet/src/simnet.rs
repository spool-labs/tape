use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::RngCore;
use reqwest::Client;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::errors::{is_account_state_pending_error, ProgramError, TapeError};
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_advance_pool_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix, build_join_network_ix, build_register_node_ix,
    build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::types::coin::TAPE;
use tape_core::types::StorageUnits;
use tape_core::types::network::NetworkAddress;
use tape_core::types::BasisPoints;
use tape_e2e_simnet::tls::pick_bind;
use tape_e2e_simnet::{ChainFixture, NodeRuntimeMode, TestNode};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_protocol::api::NODE_HEALTH_PATH;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::app::Command;
use crate::log_layer::LogHistogram;
use crate::poller::{PollerHandle, PollerUpdate, SnapshotHandle};
use crate::stake_fuzzer::StakeFuzzer;
use crate::verify::verify_spool_integrity;

const SLOT_BUMP: u64 = 1;
const CU_HIGH: u32 = 1_400_000;
const CU_MED: u32 = 400_000;
const UPLOAD_EPOCHS: u64 = 100;
const UPLOAD_MAX_RETRIES: u32 = 8;
const UPLOAD_RETRY_BASE_MS: u64 = 500;
const UPLOAD_RETRY_MAX_MS: u64 = 30_000;
const UPLOAD_STALL_THRESHOLD_SECS: u64 = 20;
const HTTP_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const HTTP_HEALTH_TIMEOUT: Duration = Duration::from_millis(750);

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

fn is_already_advanced(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        if let Some(ProgramError::Tape(TapeError::AlreadyAdvanced)) =
            ProgramError::from_error_string(&cause.to_string())
        {
            return true;
        }
    }
    false
}

fn is_join_done(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        if let Some(ProgramError::Tape(TapeError::UnexpectedState)) =
            ProgramError::from_error_string(&cause.to_string())
        {
            return true;
        }
    }
    false
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
    tracing::info!("creating chain");
    let chain = ChainFixture::new();

    tracing::info!("loading programs");
    let admin = match init_chain(&chain).await {
        Ok(admin) => admin,
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

    tracing::info!("starting block producer");
    let _block_producer = chain.rpc().start_block_producer(Duration::from_secs(1));

    let rpc = chain.rpc().clone();
    let poller = PollerHandle::spawn(rpc.clone(), snapshot, histogram);

    let (upload_tx, upload_rx) = mpsc::unbounded_channel();
    let mut state = SimnetState {
        chain,
        admin,
        nodes: Vec::new(),
        next_id: 0,
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
                        let id = state.next_id;
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
                        let rpc = state.chain.rpc().clone();
                        let admin_bytes = state.admin.to_bytes();
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
                            let rpc = state.chain.rpc().clone();
                            state.prev_epoch = RpcClient::from_rpc(rpc)
                                .get_epoch()
                                .await
                                .map(|e| e.id.as_u64())
                                .unwrap_or(0);
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
                        let mut stops = JoinSet::new();
                        for mut node in std::mem::take(&mut state.nodes) {
                            stops.spawn(async move {
                                let id = node.id();
                                let result = node.stop().await;
                                (id, result)
                            });
                        }

                        while let Some(result) = stops.join_next().await {
                            match result {
                                Ok((id, Err(error))) => {
                                    tracing::warn!(id, "node stop failed during shutdown: {error:#}");
                                }
                                Ok((_id, Ok(()))) => {}
                                Err(error) => {
                                    tracing::warn!("node stop task join failed during shutdown: {error}");
                                }
                            }
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
                let rpc = state.chain.rpc().clone();
                let epoch = RpcClient::from_rpc(rpc.clone())
                    .get_epoch()
                    .await
                    .map(|e| e.id.as_u64())
                    .unwrap_or(0);

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
                    verify_spool_integrity(&state.nodes);
                }

                // Stake fuzzing
                if state.stake_fuzz_enabled && epoch != state.prev_epoch {
                    let authorities: Vec<_> = state.nodes.iter().map(|n| n.authority()).collect();
                    state.stake_fuzzer.step_epoch(&rpc, &state.admin, &authorities).await;
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

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    ChainFixture::workspace_root_from_manifest(&manifest_dir)
}

async fn init_chain(chain: &ChainFixture) -> Result<Keypair> {
    let workspace = workspace_root()?;
    chain
        .load_default_programs(&workspace)
        .context("load_default_programs")?;

    let admin = Keypair::new();
    chain
        .airdrop(&admin.pubkey(), 50_000_000_000)
        .context("airdrop init admin")?;

    let admin_pub = admin.pubkey();

    chain
        .send_instructions_and_advance(
            &admin,
            vec![build_initialize_mint_ix(admin_pub.into(), admin_pub.into())],
            SLOT_BUMP,
        )
        .await
        .context("initialize_mint")?;

    chain
        .send_instructions_and_advance(
            &admin,
            vec![build_create_system_ix(admin_pub.into(), admin_pub.into())],
            SLOT_BUMP,
        )
        .await
        .context("create_system")?;

    for _ in 0..10 {
        let result = chain
            .send_instructions_and_advance(
                &admin,
                vec![build_expand_system_ix(admin_pub.into(), admin_pub.into())],
                SLOT_BUMP,
            )
            .await;

        match result {
            Ok(_) => {}
            Err(e) => {
                let es = format!("{e:?}");
                if es.contains("AccountAlreadyInitialized")
                    || es.contains("already initialized")
                    || is_account_state_pending_error(&es)
                {
                    break;
                }
                return Err(e).context("expand_system");
            }
        }
    }

    chain
        .send_instructions_and_advance(
            &admin,
            vec![build_initialize_ix(admin_pub.into(), admin_pub.into())],
            SLOT_BUMP,
        )
        .await
        .context("initialize archive/epoch")?;

    Ok(admin)
}

struct SimnetState {
    chain: ChainFixture,
    admin: Keypair,
    nodes: Vec<TestNode>,
    next_id: usize,
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
        let id = self.next_id;
        self.next_id += 1;

        let bind = pick_bind(id as u64).context("pick_bind")?;
        let port = bind.port();
        let rpc = self.chain.rpc().clone();

        let mut node = TestNode::new(
            id,
            rpc,
            NodeRuntimeMode::Full,
            bind,
            port,
            Duration::from_secs(5),
        )
        .with_context(|| format!("TestNode::new({id})"))?;

        self.chain
            .airdrop(&node.authority(), 10_000_000_000)
            .with_context(|| format!("airdrop node {id}"))?;

        // Register node
        let name = {
            let s = format!("sim-node-{id}");
            to_name(s)
        };
        let network_address: NetworkAddress = node.network_address();
        let network_tls = node.tls_pubkey();
        let bls_pubkey = node
            .bls_keypair()
            .public_key()
            .map_err(|e| anyhow::anyhow!("bls public_key: {e:?}"))?;
        let bls_pop = node
            .bls_keypair()
            .proof_of_possession()
            .map_err(|e| anyhow::anyhow!("bls pop: {e:?}"))?;

        let ix = build_register_node_ix(
            node.authority().into(),
            node.authority().into(),
            name,
            BasisPoints(0),
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        self.chain
            .send_instructions_and_advance(node.keypair(), vec![ix], SLOT_BUMP)
            .await
            .with_context(|| format!("register_node {id}"))?;

        // Stake
        let authority = Address::from(node.authority());
        let (node_address, _) = node_pda(authority);
        let amount = TAPE::parse("100").map_err(|_| anyhow::anyhow!("parse stake amount"))?;

        let mut stake_ixs =
            vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];
        stake_ixs.extend(build_authority_with_tokens_ix(
            self.admin.pubkey().into(),
            authority,
            amount,
        )?);
        stake_ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey().into(),
            authority,
            node_address,
            amount,
        ));

        self.chain
            .send_instructions_with_signers_and_advance(
                &self.admin,
                stake_ixs,
                &[node.keypair()],
                SLOT_BUMP,
            )
            .await
            .with_context(|| format!("stake node {id}"))?;

        // Advance pool (tolerate AlreadyAdvanced)
        let adv_ix = build_advance_pool_ix(
            self.admin.pubkey().into(),
            authority,
            node_address,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        if let Err(e) = self.chain
            .send_instructions_and_advance(&self.admin, vec![cu_ix, adv_ix], SLOT_BUMP)
            .await
        {
            if !is_already_advanced(&e) {
                return Err(e).with_context(|| format!("advance pool {id}"));
            }
        }

        // Join network non-fatal if stake is pending activation
        let join_ix =
            build_join_network_ix(self.admin.pubkey().into(), authority, node_address);
        let cu_ix2 = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        if let Err(e) = self.chain
            .send_instructions_with_signers_and_advance(
                &self.admin,
                vec![cu_ix2, join_ix],
                &[node.keypair()],
                SLOT_BUMP,
            )
            .await
        {
            if !is_join_done(&e) {
                tracing::warn!(id, "join_network deferred (stake pending): {e:#}");
            }
        }

        // Start runtime
        node.start().await.with_context(|| format!("start node {id}"))?;

        let ctx = node.context();
        let runtime_status = node
            .runtime_status()
            .expect("runtime status available after start");
        self.poller
            .send(PollerUpdate::AddNode(id, ctx, runtime_status));
        self.nodes.push(node);

        Ok(())
    }

    async fn add_node_no_start(&mut self) -> Result<()> {
        let id = self.next_id;
        self.next_id += 1;

        let bind = pick_bind(id as u64).context("pick_bind")?;
        let port = bind.port();
        let rpc = self.chain.rpc().clone();

        let node = TestNode::new(
            id,
            rpc,
            NodeRuntimeMode::Full,
            bind,
            port,
            Duration::from_secs(5),
        )
        .with_context(|| format!("TestNode::new({id})"))?;

        self.chain
            .airdrop(&node.authority(), 10_000_000_000)
            .with_context(|| format!("airdrop node {id}"))?;

        let name = {
            let s = format!("sim-node-{id}");
            to_name(s)
        };
        let network_address: NetworkAddress = node.network_address();
        let network_tls = node.tls_pubkey();
        let bls_pubkey = node
            .bls_keypair()
            .public_key()
            .map_err(|e| anyhow::anyhow!("bls public_key: {e:?}"))?;
        let bls_pop = node
            .bls_keypair()
            .proof_of_possession()
            .map_err(|e| anyhow::anyhow!("bls pop: {e:?}"))?;

        let ix = build_register_node_ix(
            node.authority().into(),
            node.authority().into(),
            name,
            BasisPoints(0),
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        self.chain
            .send_instructions_and_advance(node.keypair(), vec![ix], SLOT_BUMP)
            .await
            .with_context(|| format!("register_node {id}"))?;

        let authority = Address::from(node.authority());
        let (node_address, _) = node_pda(authority);
        let amount = TAPE::parse("100").map_err(|_| anyhow::anyhow!("parse stake amount"))?;

        let mut stake_ixs =
            vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];
        stake_ixs.extend(build_authority_with_tokens_ix(
            self.admin.pubkey().into(),
            authority,
            amount,
        )?);
        stake_ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey().into(),
            authority,
            node_address,
            amount,
        ));

        self.chain
            .send_instructions_with_signers_and_advance(
                &self.admin,
                stake_ixs,
                &[node.keypair()],
                SLOT_BUMP,
            )
            .await
            .with_context(|| format!("stake node {id}"))?;

        let adv_ix = build_advance_pool_ix(
            self.admin.pubkey().into(),
            authority,
            node_address,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        if let Err(e) = self.chain
            .send_instructions_and_advance(&self.admin, vec![cu_ix, adv_ix], SLOT_BUMP)
            .await
        {
            if !is_already_advanced(&e) {
                return Err(e).with_context(|| format!("advance pool {id}"));
            }
        }

        let join_ix =
            build_join_network_ix(self.admin.pubkey().into(), authority, node_address);
        let cu_ix2 = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        if let Err(e) = self.chain
            .send_instructions_with_signers_and_advance(
                &self.admin,
                vec![cu_ix2, join_ix],
                &[node.keypair()],
                SLOT_BUMP,
            )
            .await
        {
            if !is_join_done(&e) {
                tracing::warn!(id, "join_network deferred (stake pending): {e:#}");
            }
        }

        self.nodes.push(node);
        Ok(())
    }

    async fn remove_node(&mut self) -> Result<()> {
        if let Some(mut node) = self.nodes.pop() {
            let id = node.id();
            self.poller.send(PollerUpdate::RemoveNode(id));
            self.health_clients.remove(&id);
            node.stop()
                .await
                .with_context(|| format!("stop node {id}"))?;
        }
        Ok(())
    }

    async fn poll_http_health(&mut self) {
        // Snapshot enough about each running node to build clients without
        // holding a borrow on `self.nodes` during the cache-mutating call.
        let snapshots: Vec<(usize, String, tape_crypto::address::Address)> = self
            .nodes
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
        tls_pubkey: tape_crypto::address::Address,
    ) -> Option<Client> {
        if let Some(client) = self.health_clients.get(&id) {
            return Some(client.clone());
        }

        peer_tls::install_default_provider();
        let builder = Client::builder().timeout(HTTP_HEALTH_TIMEOUT);
        let builder = match peer_tls::apply_pinned_tls(builder, tls_pubkey) {
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
    let mut expiry_epoch = None;

    for attempt in 0..=UPLOAD_MAX_RETRIES {
        let _ = tx.send(UploadResult::AttemptStarted { upload_id });
        match sdk.reserve(&tape_key, reserve_capacity, UPLOAD_EPOCHS).await {
            Ok(tape) => {
                expiry_epoch = Some(tape.expiry_epoch.as_u64());
                break;
            }
            Err(error) if !is_retriable_upload_error(&error) => {
                return Err(anyhow::Error::from(error))
                    .context("reserve failed with non-retriable error");
            }
            Err(error) if attempt == UPLOAD_MAX_RETRIES => {
                return Err(anyhow::Error::from(error))
                    .context(format!("reserve exhausted after {} attempts", attempt + 1));
            }
            Err(error) => {
                let delay = upload_retry_delay(attempt);
                let _ = tx.send(UploadResult::Retrying {
                    upload_id,
                    error: format!("{error:#}"),
                    next_retry_in_ms: delay.as_millis() as u64,
                });
                tracing::warn!(attempt = attempt + 1, delay_ms = delay.as_millis(), error = %error, "reserve failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }

    let _ = tx.send(UploadResult::AttemptStarted { upload_id });
    sdk.write_track(&tape_key, key, &data)
        .await
        .map_err(anyhow::Error::from)
        .context("write track")?;
    Ok(expiry_epoch.expect("reserve succeeds before write"))
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
                    let chain = ChainFixture::new();
                    let admin = init_chain(&chain).await.expect("init_chain");
                    let payer = CryptoKeypair::from_solana_keypair(&admin)
                        .expect("convert admin keypair");
                    let sdk = Tapedrive::new(chain.rpc().clone(), payer);
                    let tape_key = TapeKey::generate();

                    let tape = sdk
                        .reserve(&tape_key, StorageUnits::mb(2), UPLOAD_EPOCHS)
                        .await
                        .expect("reserve tape");

                    assert_eq!(tape.authority, tape_key.pubkey().into());
                    assert_eq!(tape.capacity, StorageUnits::mb(2));
                    assert_eq!(tape.expiry_epoch.as_u64(), tape.active_epoch.as_u64() + UPLOAD_EPOCHS);
                });
            })
            .expect("spawn reserve test thread");

        handle.join().expect("reserve test thread joins");
    }

    #[test]
    fn add_20_nodes() {
        let handle = std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let chain = ChainFixture::new();
                    let admin = init_chain(&chain).await.expect("init_chain");

                    let _bp = chain.rpc().start_block_producer(Duration::from_secs(1));

                    let (upload_tx, upload_rx) = mpsc::unbounded_channel();
                    let mut state = SimnetState {
                        chain,
                        admin,
                        nodes: Vec::new(),
                        next_id: 0,
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

                    for i in 0..25 {
                        eprintln!("adding node {i}...");
                        state.add_node_no_start().await
                            .unwrap_or_else(|e| panic!("add_node {i} failed: {e:#}"));
                        eprintln!("node {i} ok");
                    }
                });
            })
            .unwrap();
        handle.join().unwrap();
    }
}
