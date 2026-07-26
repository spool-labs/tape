use std::path::PathBuf;

use anyhow::{Context, Result};
use solana_instruction::Instruction;
use solana_signature::Signature;
use solana_signer::Signer;
use tape_api::consts::NAME_LENGTH;
use tape_api::instruction::{
    build_create_archive_ix, build_create_committee_ix, build_create_epoch_ix,
    build_create_peer_set_ix, build_create_system_ix, build_initialize_mint_ix,
    build_register_node_ix, build_set_name_ix, build_set_network_address_ix,
    build_set_network_tls_ix, build_stage_genesis_node_ix, build_start_network_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_api::genesis::GenesisConfig;
use tape_core::system::NodePreferences;
use tape_api::utils::to_name;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_core::types::network::NetworkAddress;
use tape_crypto::address::Address;

use crate::chain::ChainFixture;
use crate::gateway::TestGateway;
use crate::simnet::SimnetHarness;

pub struct SimnetScenario<'a> {
    pub harness: &'a SimnetHarness,
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
                    build_create_system_ix(admin_pub.into(), admin_pub.into(), &GenesisConfig::simnet()),
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
                vec![build_create_archive_ix(admin_pub.into(), admin_pub.into(), &GenesisConfig::simnet())],
                slot_bump,
            )
            .await
            .context("initialize archive/epoch")?;

        let bootstrap_epoch = EpochNumber(0);
        let genesis_epoch = EpochNumber(1);
        let candidate_epoch = EpochNumber(2);
        self.harness
            .chain()
            .send_instructions_and_advance(
                admin,
                vec![
                    build_create_epoch_ix(admin_pub.into(), bootstrap_epoch),
                    build_create_committee_ix(admin_pub.into(), bootstrap_epoch),
                    build_create_epoch_ix(admin_pub.into(), genesis_epoch),
                    build_create_committee_ix(admin_pub.into(), genesis_epoch),
                    build_create_epoch_ix(admin_pub.into(), candidate_epoch),
                    build_create_committee_ix(admin_pub.into(), candidate_epoch),
                ],
                slot_bump,
            )
            .await
            .context("create bootstrap epochs/committees")?;

        Ok(())
    }

    pub async fn start_network(&self) -> Result<Signature> {
        let slot_bump = self.harness.config().slot_advance_per_tx;
        let admin = self.harness.admin();
        let admin_pub = admin.pubkey();
        for (index, node) in self.harness.nodes().iter().take(GROUP_SIZE).enumerate() {
            let authority = Address::from(node.authority());
            let (node_address, _) = node_pda(authority);
            let ix = build_stage_genesis_node_ix(
                admin_pub.into(),
                authority,
                node_address,
            );
            self.harness
                .chain()
                .send_instructions_with_signers_and_advance(
                    admin,
                    vec![ix],
                    &[node.keypair()],
                    slot_bump,
                )
                .await
                .with_context(|| format!("stage genesis node {index}"))?;
        }

        let ix = build_start_network_ix(
            admin_pub.into(),
            admin_pub.into(),
            &GenesisConfig::simnet(),
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
            NodePreferences::from(&GenesisConfig::simnet()),
        );

        self.harness
            .chain()
            .send_instructions_and_advance(node.keypair(), vec![ix], slot_bump)
            .await
            .with_context(|| format!("register_node {}", node.id()))
    }

    pub async fn register_gateway(
        &self,
        gateway: &TestGateway,
        commission: BasisPoints,
    ) -> Result<Signature> {
        let slot_bump = self.harness.config().slot_advance_per_tx;

        self.harness
            .chain()
            .airdrop(&gateway.authority(), 10_000_000_000)
            .with_context(|| format!("airdrop gateway {}", gateway.id()))?;

        let name = gateway_name(gateway.id());
        let bls_pubkey = gateway
            .bls_keypair()
            .public_key()
            .map_err(|e| anyhow::anyhow!("gateway bls public_key: {e:?}"))?;
        let bls_pop = gateway
            .bls_keypair()
            .proof_of_possession()
            .map_err(|e| anyhow::anyhow!("gateway bls pop: {e:?}"))?;

        let ix = build_register_node_ix(
            gateway.authority().into(),
            gateway.authority().into(),
            name,
            commission,
            gateway.network_address(),
            gateway.tls_pubkey(),
            bls_pubkey,
            bls_pop,
            NodePreferences::from(&GenesisConfig::simnet()),
        );

        self.harness
            .chain()
            .send_instructions_and_advance(gateway.keypair(), vec![ix], slot_bump)
            .await
            .with_context(|| format!("register_gateway {}", gateway.id()))
    }

    /// Submit one gateway-signed instruction against its own node account.
    async fn send_gateway_ix(
        &self,
        gateway: &TestGateway,
        build: impl FnOnce(Address, Address) -> Instruction,
        what: &str,
    ) -> Result<Signature> {
        let authority: Address = gateway.authority().into();
        let (node_address, _) = node_pda(authority);
        self.harness
            .chain()
            .send_instructions_and_advance(
                gateway.keypair(),
                vec![build(authority, node_address)],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("{what} gateway {}", gateway.id()))
    }

    /// Change a registered gateway's on-chain network address.
    pub async fn set_gateway_network_address(
        &self,
        gateway: &TestGateway,
        network_address: NetworkAddress,
    ) -> Result<Signature> {
        self.send_gateway_ix(
            gateway,
            |authority, node| build_set_network_address_ix(authority, authority, node, network_address),
            "set_network_address",
        )
        .await
    }

    /// Change a registered gateway's on-chain display name.
    pub async fn set_gateway_name(&self, gateway: &TestGateway, name: &str) -> Result<Signature> {
        self.send_gateway_ix(
            gateway,
            |authority, node| build_set_name_ix(authority, authority, node, name),
            "set_name",
        )
        .await
    }

    /// Change a registered gateway's on-chain TLS pubkey.
    pub async fn set_gateway_network_tls(
        &self,
        gateway: &TestGateway,
        network_tls: tape_core::types::tls::NetworkTlsPubkey,
    ) -> Result<Signature> {
        self.send_gateway_ix(
            gateway,
            |authority, node| build_set_network_tls_ix(authority, authority, node, network_tls),
            "set_network_tls",
        )
        .await
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

fn gateway_name(id: usize) -> [u8; NAME_LENGTH] {
    let name = format!("sim-gateway-{id}");
    to_name(name)
}
