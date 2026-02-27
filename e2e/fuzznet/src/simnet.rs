use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
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

use crate::app::{Command, PollSnapshot};
use crate::log_layer::LogHistogram;
use crate::poller::{PollerHandle, PollerUpdate};

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
    snapshot: Arc<ArcSwap<PollSnapshot>>,
    histogram: LogHistogram,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    rt.block_on(async_run(cmd_rx, snapshot, histogram));
}

fn set_status(snapshot: &Arc<ArcSwap<PollSnapshot>>, status: &str) {
    let mut snap = (**snapshot.load()).clone();
    snap.status = status.to_owned();
    snapshot.store(Arc::new(snap));
}

async fn async_run(
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<Command>,
    snapshot: Arc<ArcSwap<PollSnapshot>>,
    histogram: LogHistogram,
) {
    set_status(&snapshot, "initializing tls");
    init_tls();

    set_status(&snapshot, "creating chain");
    let chain = ChainFixture::new();

    set_status(&snapshot, "loading programs");
    let admin = match init_chain(&chain).await {
        Ok(admin) => admin,
        Err(e) => {
            set_status(&snapshot, &format!("INIT FAILED: {e:#}"));
            while let Some(cmd) = cmd_rx.recv().await {
                if matches!(cmd, Command::Quit) {
                    break;
                }
            }
            return;
        }
    };

    set_status(&snapshot, "starting block producer");
    let _block_producer = chain.rpc().start_block_producer(Duration::from_secs(1));

    let rpc = chain.rpc().clone();
    let poller = PollerHandle::spawn(rpc.clone(), Arc::clone(&snapshot), histogram);

    let mut state = SimnetState {
        chain,
        admin,
        nodes: Vec::new(),
        next_id: 0,
        poller,
    };

    set_status(&snapshot, "ready");

    loop {
        match cmd_rx.recv().await {
            Some(Command::AddNode) => {
                let id = state.next_id;
                set_status(&snapshot, &format!("adding node {id}"));
                if let Err(e) = state.add_node().await {
                    set_status(&snapshot, &format!("add_node failed: {e:#}"));
                } else {
                    set_status(&snapshot, "ready");
                }
            }
            Some(Command::RemoveNode) => {
                set_status(&snapshot, "removing node");
                if let Err(e) = state.remove_node().await {
                    set_status(&snapshot, &format!("remove failed: {e:#}"));
                } else {
                    set_status(&snapshot, "ready");
                }
            }
            Some(Command::Quit) | None => {
                set_status(&snapshot, "shutting down");
                for node in &mut state.nodes {
                    let _ = node.stop().await;
                }
                break;
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
