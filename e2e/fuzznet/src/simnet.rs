use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use rand::RngCore;
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
    build_reserve_snapshot_tape_ix, build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_core::types::coin::TAPE;
use tape_core::types::network::NetworkAddress;
use tape_core::types::BasisPoints;
use tape_e2e_simnet::tls::{init_tls, pick_bind};
use tape_e2e_simnet::{ChainFixture, NodeRuntimeMode, TestNode};
use tape_sdk::Tapedrive;

use crate::app::Command;
use crate::log_layer::LogHistogram;
use crate::poller::{PollerHandle, PollerUpdate};
use crate::stake_fuzzer::StakeFuzzer;

const SLOT_BUMP: u64 = 1;
const CU_HIGH: u32 = 1_400_000;
const CU_MED: u32 = 400_000;

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
    snapshot: crate::poller::SnapshotHandle,
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
    snapshot: crate::poller::SnapshotHandle,
    histogram: LogHistogram,
) {
    tracing::info!("initializing tls");
    init_tls();

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

    let mut state = SimnetState {
        chain,
        admin,
        nodes: Vec::new(),
        next_id: 0,
        poller,
        stake_fuzzer: StakeFuzzer::new(),
        stake_fuzz_enabled: false,
        prev_epoch: 0,
    };

    tracing::info!("ready");

    let mut epoch_interval = tokio::time::interval(Duration::from_secs(2));

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
                        let rpc = state.chain.rpc().clone();
                        let admin_bytes = state.admin.to_bytes();
                        tokio::spawn(async move {
                            tracing::info!("uploading blob");
                            let admin = Keypair::try_from(admin_bytes.as_ref()).unwrap();
                            match upload_random_blob(rpc, &admin).await {
                                Ok(size) => tracing::info!("uploaded {size} bytes"),
                                Err(e) => tracing::error!("upload failed: {e:#}"),
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
                        for node in &mut state.nodes {
                            let _ = node.stop().await;
                        }
                        break;
                    }
                }
            }
            _ = epoch_interval.tick() => {
                if !state.stake_fuzz_enabled {
                    continue;
                }
                let rpc = state.chain.rpc().clone();
                let epoch = RpcClient::from_rpc(rpc.clone())
                    .get_epoch()
                    .await
                    .map(|e| e.id.as_u64())
                    .unwrap_or(0);
                if epoch != state.prev_epoch {
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
            vec![build_initialize_mint_ix(admin_pub, admin_pub)],
            SLOT_BUMP,
        )
        .await
        .context("initialize_mint")?;

    chain
        .send_instructions_and_advance(
            &admin,
            vec![build_create_system_ix(admin_pub, admin_pub)],
            SLOT_BUMP,
        )
        .await
        .context("create_system")?;

    for _ in 0..10 {
        let result = chain
            .send_instructions_and_advance(
                &admin,
                vec![build_expand_system_ix(admin_pub, admin_pub)],
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
            vec![build_initialize_ix(admin_pub, admin_pub)],
            SLOT_BUMP,
        )
        .await
        .context("initialize archive/epoch")?;

    chain
        .send_instructions_and_advance(
            &admin,
            vec![build_reserve_snapshot_tape_ix(admin_pub)],
            SLOT_BUMP,
        )
        .await
        .context("reserve snapshot tape")?;

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
            tape_api::utils::to_name(s)
        };
        let network_address: NetworkAddress = node.network_address();
        let network_tls = node.authority();
        let bls_pubkey = node
            .bls_keypair()
            .public_key()
            .map_err(|e| anyhow::anyhow!("bls public_key: {e:?}"))?;
        let bls_pop = node
            .bls_keypair()
            .proof_of_possession()
            .map_err(|e| anyhow::anyhow!("bls pop: {e:?}"))?;

        let ix = build_register_node_ix(
            node.authority(),
            node.authority(),
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
        let (node_address, _) = node_pda(node.authority());
        let amount = TAPE::parse("100").map_err(|_| anyhow::anyhow!("parse stake amount"))?;

        let mut stake_ixs =
            vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];
        stake_ixs.extend(build_authority_with_tokens_ix(
            self.admin.pubkey(),
            node.authority(),
            amount,
        ));
        stake_ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey(),
            node.authority(),
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
            self.admin.pubkey(),
            node.authority(),
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

        // Join network — non-fatal if stake is pending activation
        let join_ix =
            build_join_network_ix(self.admin.pubkey(), node.authority(), node_address);
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
        self.poller.send(PollerUpdate::AddNode(id, ctx));
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
            tape_api::utils::to_name(s)
        };
        let network_address: NetworkAddress = node.network_address();
        let network_tls = node.authority();
        let bls_pubkey = node
            .bls_keypair()
            .public_key()
            .map_err(|e| anyhow::anyhow!("bls public_key: {e:?}"))?;
        let bls_pop = node
            .bls_keypair()
            .proof_of_possession()
            .map_err(|e| anyhow::anyhow!("bls pop: {e:?}"))?;

        let ix = build_register_node_ix(
            node.authority(),
            node.authority(),
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

        let (node_address, _) = node_pda(node.authority());
        let amount = TAPE::parse("100").map_err(|_| anyhow::anyhow!("parse stake amount"))?;

        let mut stake_ixs =
            vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];
        stake_ixs.extend(build_authority_with_tokens_ix(
            self.admin.pubkey(),
            node.authority(),
            amount,
        ));
        stake_ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey(),
            node.authority(),
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
            self.admin.pubkey(),
            node.authority(),
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
            build_join_network_ix(self.admin.pubkey(), node.authority(), node_address);
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
            node.stop()
                .await
                .with_context(|| format!("stop node {id}"))?;
        }
        Ok(())
    }

}

async fn upload_random_blob(rpc: LiteSvmRpc, admin: &Keypair) -> Result<usize> {
    let (key, data) = {
        let mut rng = rand::thread_rng();
        let size = (rng.next_u32() as usize % (1024 * 1024 - 1024)) + 1024; // 1KB..1MB
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        let key = tape_crypto::hash::hash(&data[..32.min(data.len())]);
        (key, data)
    };

    let size = data.len();
    let sdk = Tapedrive::new(RpcClient::from_rpc(rpc), admin);
    sdk.write(key, &data, 4)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    init_tls();
                    let chain = ChainFixture::new();
                    let admin = init_chain(&chain).await.expect("init_chain");

                    let _bp = chain.rpc().start_block_producer(Duration::from_secs(1));

                    let mut state = SimnetState {
                        chain,
                        admin,
                        nodes: Vec::new(),
                        next_id: 0,
                        poller: PollerHandle::spawn_noop(),
                        stake_fuzzer: StakeFuzzer::new(),
                        stake_fuzz_enabled: false,
                        prev_epoch: 0,
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
