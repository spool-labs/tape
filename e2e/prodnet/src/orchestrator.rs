use std::time::Duration;

use anyhow::{Context, Result};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tracing::info;

use crate::chain::ChainManager;
use crate::config::ProdnetConfig;
use crate::process::ProcessSupervisor;

pub struct Orchestrator {
    config: ProdnetConfig,
    chain: ChainManager,
    processes: ProcessSupervisor,
}

impl Orchestrator {
    pub fn new(config: ProdnetConfig) -> Result<Self> {
        let admin = Keypair::new();
        info!(admin = %admin.pubkey(), "generated ephemeral admin keypair");

        let chain = ChainManager::new(&config.rpc_url, admin).context("create chain manager")?;

        let processes = ProcessSupervisor::new(
            config.node_binary.clone(),
            config.data_dir.clone(),
            config.rpc_url.clone(),
            config.base_port,
        );

        Ok(Self {
            config,
            chain,
            processes,
        })
    }

    pub async fn init(&self) -> Result<()> {
        self.chain.init_chain().await
    }

    pub async fn add_node(&mut self) -> Result<usize> {
        let id = self.processes.prepare_node().context("prepare node")?;
        let authority_pubkey = self.processes.node(id).authority.pubkey();
        info!(id, authority = %authority_pubkey, "prepared node");

        // Fund so node can pay for self-registration tx
        self.chain
            .airdrop(&authority_pubkey, self.config.sol_airdrop)
            .await
            .context("airdrop SOL for node")?;
        info!(id, "airdropped SOL");

        // Spawn process — node will call ensure_registered() during bootstrap
        self.processes
            .spawn_node(id)
            .context("spawn node process")?;
        info!(id, "spawned process");

        // Everything after spawn must clean up on failure
        if let Err(setup_err) = self.complete_node_setup(id).await {
            info!(id, error = %setup_err, "node setup failed, stopping process");
            if let Err(stop_err) = self.processes.stop_node(id).await {
                return Err(setup_err.context(format!("cleanup also failed: {stop_err:#}")));
            }
            return Err(setup_err);
        }

        Ok(id)
    }

    async fn complete_node_setup(&mut self, id: usize) -> Result<()> {
        // Wait for full bootstrap: registration, context build, HTTP bind
        self.processes
            .wait_healthy(id, Duration::from_secs(60))
            .await
            .context("wait for node health")?;

        let authority_pubkey = self.processes.node(id).authority.pubkey();
        let authority_keypair = clone_keypair(&self.processes.node(id).authority);

        // Stake TAPE with the node's pool (requires node authority as cosigner)
        self.chain
            .stake_node(&authority_keypair, self.config.stake_amount)
            .await
            .context("stake node")?;
        info!(id, "staked TAPE");

        // Advance pool to activate stake
        self.chain
            .advance_pool(authority_pubkey)
            .await
            .context("advance pool")?;
        info!(id, "pool advanced");

        // Join next epoch committee (idempotent — tolerates UnexpectedState)
        self.chain
            .join_network(&authority_keypair)
            .await
            .context("join network")?;
        info!(id, "joined network");

        Ok(())
    }

    pub async fn add_nodes(&mut self, count: usize) -> Result<()> {
        for i in 0..count {
            self.add_node()
                .await
                .with_context(|| format!("add node {i}"))?;
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        info!("shutting down all nodes");
        self.processes.shutdown_all().await
    }
}

fn clone_keypair(keypair: &Keypair) -> Keypair {
    Keypair::try_from(keypair.to_bytes().as_ref()).expect("keypair round-trip")
}
