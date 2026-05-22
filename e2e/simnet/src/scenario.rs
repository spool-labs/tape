use std::path::PathBuf;

use anyhow::{Context, Result};
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use tape_api::consts::NAME_LENGTH;
use tape_api::instruction::{
    build_create_archive_ix, build_create_committee_ix,
    build_create_epoch_ix, build_create_peer_set_ix, build_create_system_ix,
    build_initialize_mint_ix, build_register_node_ix, build_start_network_ix,
};
use tape_api::program::MIN_COMMITTEE_SIZE;
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_core::types::network::NetworkAddress;
use tape_crypto::address::Address;

use crate::chain::ChainFixture;
use crate::simnet::SimnetHarness;

pub struct SimnetScenario<'a> {
    pub(crate) harness: &'a SimnetHarness,
}

impl<'a> SimnetScenario<'a> {
    pub fn new(harness: &'a SimnetHarness) -> Self {
        Self { harness }
    }

    pub fn workspace_root(&self) -> Result<PathBuf> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        ChainFixture::workspace_root_from_manifest(&manifest_dir)
    }

    pub async fn init_system(&self) -> Result<()> {
        let workspace = self.workspace_root()?;
        self.harness
            .chain()
            .load_default_programs(&workspace)
            .context("load_default_programs")?;

        let admin = self.harness.admin();
        let admin_pub = admin.pubkey();

        self.harness
            .chain()
            .airdrop(&admin_pub, 50_000_000_000)
            .context("airdrop admin")?;

        let slot_bump = self.harness.config().slot_advance_per_tx;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin,
                vec![build_initialize_mint_ix(admin_pub.into(), admin_pub.into())],
                slot_bump,
            )
            .await
            .context("initialize_mint")?;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin,
                vec![
                    build_create_system_ix(admin_pub.into(), admin_pub.into()),
                    build_create_peer_set_ix(admin_pub.into()),
                ],
                slot_bump,
            )
            .await
            .context("create_system/peer_set")?;

        self.harness
            .chain()
            .send_instructions_and_advance(
                admin,
                vec![build_create_archive_ix(admin_pub.into(), admin_pub.into())],
                slot_bump,
            )
            .await
            .context("initialize archive/epoch")?;

        let genesis_epoch = EpochNumber(1);
        self.harness
            .chain()
            .send_instructions_and_advance(
                admin,
                vec![
                    build_create_epoch_ix(admin_pub.into(), genesis_epoch),
                    build_create_committee_ix(admin_pub.into(), genesis_epoch),
                ],
                slot_bump,
            )
            .await
            .context("create genesis epoch/committee")?;

        Ok(())
    }

    pub async fn start_network(&self) -> Result<Signature> {
        let slot_bump = self.harness.config().slot_advance_per_tx;
        let admin = self.harness.admin();
        let admin_pub = admin.pubkey();
        let genesis_nodes = self
            .harness
            .nodes()
            .iter()
            .take(MIN_COMMITTEE_SIZE)
            .map(|node| {
                let (node_address, _) = node_pda(Address::from(node.authority()));
                node_address
            })
            .collect::<Vec<_>>();

        let committee_size = self.harness.nodes().len().max(MIN_COMMITTEE_SIZE) as u64;
        let ix = build_start_network_ix(
            admin_pub.into(),
            committee_size,
            1,
            &genesis_nodes,
        );

        self.harness
            .chain()
            .send_instructions_and_advance(admin, vec![ix], slot_bump)
            .await
            .context("start_network")
    }

    pub async fn register_node(
        &self,
        node_index: usize,
        commission: BasisPoints,
    ) -> Result<Signature> {
        let slot_bump = self.harness.config().slot_advance_per_tx;
        let node = self
            .harness
            .node(node_index)
            .with_context(|| format!("node {node_index} missing"))?;

        self.harness
            .chain()
            .airdrop(&node.authority(), 10_000_000_000)
            .with_context(|| format!("airdrop node {}", node.id()))?;

        let name = node_name(node.id());
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
            commission,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        self.harness
            .chain()
            .send_instructions_and_advance(node.keypair(), vec![ix], slot_bump)
            .await
            .with_context(|| format!("register_node {}", node.id()))
    }

    pub async fn register_many(
        &self,
        node_indices: &[usize],
        commission: BasisPoints,
    ) -> Result<Vec<Signature>> {
        let mut sigs = Vec::with_capacity(node_indices.len());
        for &i in node_indices {
            sigs.push(
                self.register_node(i, commission)
                    .await
                    .with_context(|| format!("register node {i}"))?,
            );
        }
        Ok(sigs)
    }

    pub async fn register_nodes(&self, commission: BasisPoints) -> Result<Vec<Signature>> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.register_many(&all, commission).await
    }
}

fn node_name(id: usize) -> [u8; NAME_LENGTH] {
    let name = format!("sim-node-{id}");
    to_name(name)
}
