use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use solana_sdk::signature::Signature;
use tape_api::consts::NAME_LENGTH;
use tape_api::instruction::{
    build_advance_epoch_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix, build_join_network_ix, build_register_node_ix,
    build_reserve_snapshot_tape_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::types::network::NetworkAddress;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_store::ops::MetaOps;

use crate::simnet::SimnetHarness;

#[derive(Debug, Clone)]
pub struct JoinResult {
    pub node_id: usize,
    pub authority: solana_sdk::pubkey::Pubkey,
    pub result: Result<Signature, String>,
}

pub struct SimnetScenario<'a> {
    pub(crate) harness: &'a SimnetHarness,
}

impl<'a> SimnetScenario<'a> {
    pub fn new(harness: &'a SimnetHarness) -> Self {
        Self { harness }
    }

    pub fn workspace_root(&self) -> Result<PathBuf> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        crate::ChainFixture::workspace_root_from_manifest(&manifest_dir)
    }

    pub async fn init_system(&self, admin_node_index: usize) -> Result<()> {
        let workspace = self.workspace_root()?;
        self.harness
            .chain()
            .load_default_programs(&workspace)
            .context("load_default_programs")?;

        let admin = self
            .harness
            .node(admin_node_index)
            .context("admin node missing")?;

        self.harness
            .chain()
            .airdrop(&admin.authority(), 50_000_000_000)
            .context("airdrop admin")?;

        let slot_bump = self.harness.config().slot_advance_per_tx;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin.keypair(),
                vec![build_initialize_mint_ix(admin.authority(), admin.authority())],
                slot_bump,
            )
            .await
            .context("initialize_mint")?;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin.keypair(),
                vec![build_create_system_ix(admin.authority(), admin.authority())],
                slot_bump,
            )
            .await
            .context("create_system")?;

        for _ in 0..10 {
            let result = self
                .harness
                .chain()
                .send_instructions_and_advance(
                    admin.keypair(),
                    vec![build_expand_system_ix(admin.authority(), admin.authority())],
                    slot_bump,
                )
                .await;

            match result {
                Ok(_) => {}
                Err(e) => {
                    let es = format!("{e:?}");
                    if es.contains("AccountAlreadyInitialized")
                        || es.contains("already initialized")
                        || es.contains("uninitialized account")
                    {
                        break;
                    }
                    return Err(e).context("expand_system");
                }
            }
        }

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin.keypair(),
                vec![build_initialize_ix(admin.authority(), admin.authority())],
                slot_bump,
            )
            .await
            .context("initialize archive/epoch")?;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin.keypair(),
                vec![build_reserve_snapshot_tape_ix(admin.authority())],
                slot_bump,
            )
            .await
            .context("reserve snapshot tape")?;

        Ok(())
    }

    pub async fn register_nodes(&self, commission: BasisPoints) -> Result<Vec<Signature>> {
        let mut sigs = Vec::with_capacity(self.harness.nodes().len());
        let slot_bump = self.harness.config().slot_advance_per_tx;

        for node in self.harness.nodes() {
            self.harness
                .chain()
                .airdrop(&node.authority(), 10_000_000_000)
                .with_context(|| format!("airdrop node {}", node.id()))?;

            let name = node_name(node.id());
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
                commission,
                network_address,
                network_tls,
                bls_pubkey,
                bls_pop,
            );

            let sig = self
                .harness
                .chain()
                .send_instructions_and_advance(node.keypair(), vec![ix], slot_bump)
                .await
                .with_context(|| format!("register_node {}", node.id()))?;

            sigs.push(sig);
        }

        Ok(sigs)
    }

    pub async fn join_network(&self) -> Vec<JoinResult> {
        let mut out = Vec::with_capacity(self.harness.nodes().len());
        let slot_bump = self.harness.config().slot_advance_per_tx;

        for node in self.harness.nodes() {
            let (node_address, _) = node_pda(node.authority());
            let ix = build_join_network_ix(node.authority(), node.authority(), node_address);

            let result = self
                .harness
                .chain()
                .send_instructions_and_advance(node.keypair(), vec![ix], slot_bump)
                .await
                .map_err(|e| e.to_string());

            out.push(JoinResult {
                node_id: node.id(),
                authority: node.authority(),
                result,
            });
        }

        out
    }

    pub async fn advance_epoch(&self, authority_node_index: usize) -> Result<Signature> {
        let authority = self
            .harness
            .node(authority_node_index)
            .context("authority node missing")?;

        let ix = build_advance_epoch_ix(authority.authority(), authority.authority());

        self.harness
            .chain()
            .send_instructions_and_advance(
                authority.keypair(),
                vec![ix],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .context("advance_epoch")
    }

    pub async fn wait_for_all_nodes_epoch(
        &self,
        expected: Option<EpochNumber>,
        timeout: Duration,
    ) -> Result<()> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.wait_for_nodes_epoch(&all, expected, timeout).await
    }

    pub async fn wait_for_nodes_epoch(
        &self,
        indices: &[usize],
        expected: Option<EpochNumber>,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut last_seen: Vec<Option<EpochNumber>> = Vec::new();

        loop {
            let mut ready = true;
            last_seen.clear();
            for &i in indices {
                let node = self
                    .harness
                    .node(i)
                    .with_context(|| format!("node {i} missing"))?;
                let got = node
                    .context()
                    .store
                    .get_current_epoch()
                    .with_context(|| format!("node {i} get_current_epoch"))?;
                last_seen.push(got);

                match expected {
                    Some(exp) => {
                        if got != Some(exp) {
                            ready = false;
                            break;
                        }
                    }
                    None => {
                        if got.is_none() {
                            ready = false;
                            break;
                        }
                    }
                }
            }

            if ready {
                return Ok(());
            }

            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timeout waiting for all nodes to converge epoch (expected={expected:?}, last_seen={last_seen:?})"
                );
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn node_name(id: usize) -> [u8; NAME_LENGTH] {
    let name = format!("sim-node-{id}");
    to_name(name)
}
