use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::join_all;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::EpochNumber;
use tape_sdk::keys::helpers::load_solana_keypair;
use tracing::info;

use crate::chain::ChainManager;
use crate::config::TestnetConfig;
use crate::observer::NodeRef;
use crate::process::{ProcessSupervisor, RemoveNodeError, write_solana_keypair};

pub struct Orchestrator {
    config: TestnetConfig,
    chain: ChainManager,
    processes: ProcessSupervisor,
}

struct NodeSetupContext {
    id: usize,
    authority_pubkey: Pubkey,
    authority_keypair: Keypair,
}

impl Orchestrator {
    pub fn new(config: TestnetConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .with_context(|| format!("create testnet data dir: {}", config.data_dir.display()))?;

        let admin_path = config.data_dir.join("admin.json");
        let (admin, created) = load_or_create_admin_keypair(&admin_path)?;
        if created {
            info!(admin = %admin.pubkey(), path = %admin_path.display(), "created testnet admin keypair");
        } else {
            info!(admin = %admin.pubkey(), path = %admin_path.display(), "loaded testnet admin keypair");
        }

        let chain = ChainManager::new(&config.rpc_url, admin).context("create chain manager")?;

        let mut processes = ProcessSupervisor::new(
            config.node_binary.clone(),
            config.data_dir.clone(),
            config.rpc_url.clone(),
            config.base_port,
        );
        let existing_nodes = processes
            .load_existing_nodes()
            .context("load existing testnet nodes")?;
        if existing_nodes > 0 {
            info!(count = existing_nodes, "loaded existing testnet node identities");
        }

        Ok(Self {
            config,
            chain,
            processes,
        })
    }

    pub async fn init(&self) -> Result<()> {
        self.chain
            .ensure_chain_initialized(self.config.sol_airdrop)
            .await
    }

    pub async fn add_node(&mut self) -> Result<usize> {
        let setup = self.prepare_node_setup().context("prepare node")?;
        let id = setup.id;

        // Fund so node can pay for self-registration tx
        self.chain
            .airdrop(&setup.authority_pubkey, self.config.sol_airdrop)
            .await
            .context("airdrop SOL for node")?;
        info!(id, "airdropped SOL");

        self.processes
            .spawn_node(id)
            .context("spawn node process")?;
        info!(id, "spawned process");

        // Everything after spawn must clean up on failure
        if let Err(setup_err) = self.complete_node_setup(setup).await {
            info!(id, error = %setup_err, "node setup failed, stopping process");
            if let Err(stop_err) = self.processes.stop_node(id).await {
                return Err(setup_err.context(format!("cleanup also failed: {stop_err:#}")));
            }
            return Err(setup_err);
        }

        self.maybe_start_genesis_network().await?;

        Ok(id)
    }

    fn prepare_node_setup(&mut self) -> Result<NodeSetupContext> {
        match self.processes.first_stopped_node_id() {
            Some(id) => self.build_node_setup(id, true),
            None => self.create_new_node_setup(),
        }
    }

    fn create_new_node_setup(&mut self) -> Result<NodeSetupContext> {
        let id = self.processes.prepare_node().context("prepare node")?;
        self.build_node_setup(id, false)
    }

    fn build_node_setup(&self, id: usize, reused: bool) -> Result<NodeSetupContext> {
        if reused {
            info!(id, "reusing existing node identity");
        }
        let authority_pubkey = self.processes.node(id).authority.pubkey();
        info!(id, authority = %authority_pubkey, "prepared node");

        Ok(NodeSetupContext {
            id,
            authority_pubkey,
            authority_keypair: clone_keypair(&self.processes.node(id).authority),
        })
    }

    async fn complete_node_setup(&self, setup: NodeSetupContext) -> Result<()> {
        let live = self.chain.current_epoch().await? != EpochNumber(0);
        Self::complete_node_setup_inner(
            &self.processes,
            &self.chain,
            self.config.stake_amount,
            live,
            setup,
        )
        .await
    }

    async fn complete_node_setup_inner(
        processes: &ProcessSupervisor,
        chain: &ChainManager,
        stake_amount: u64,
        live: bool,
        setup: NodeSetupContext,
    ) -> Result<()> {
        let id = setup.id;
        // Wait for full bootstrap: registration, context build, HTTP bind
        processes
            .wait_healthy(id, Duration::from_secs(60))
            .await
            .context("wait for node health")?;

        // Stake TAPE with the node's pool (requires node authority as cosigner)
        chain
            .ensure_node_staked(&setup.authority_keypair, stake_amount)
            .await
            .context("stake node")?;
        info!(id, "stake ensured");

        if !live {
            info!(id, "genesis node staged");
            return Ok(());
        }

        // Advance pool to activate stake
        chain
            .advance_pool(setup.authority_pubkey)
            .await
            .context("advance pool")?;
        info!(id, "pool advanced");

        chain
            .join_committee(&setup.authority_keypair)
            .await
            .context("join committee")?;
        info!(id, "joined committee");

        Ok(())
    }

    pub async fn add_nodes(&mut self, count: usize) -> Result<()> {
        let current = self.processes.running_node_count();
        if current >= count {
            self.maybe_start_genesis_network().await?;
            return Ok(());
        }

        let to_add = count - current;
        let mut setups = Vec::with_capacity(to_add);
        let mut reusable_ids = self.processes.stopped_node_ids().into_iter();

        for index in 0..to_add {
            let setup = if let Some(id) = reusable_ids.next() {
                self.build_node_setup(id, true)
            } else {
                self.create_new_node_setup()
            }
            .with_context(|| format!("prepare node {}", current + index))?;
            setups.push(setup);
        }

        let airdrop_results = join_all(setups.iter().map(|setup| async {
            self.chain
                .airdrop(&setup.authority_pubkey, self.config.sol_airdrop)
                .await
                .with_context(|| format!("airdrop SOL for node {}", setup.id))
        }))
        .await;

        for result in airdrop_results {
            result?;
        }

        for setup in &setups {
            self.processes
                .spawn_node(setup.id)
                .with_context(|| format!("spawn node {}", setup.id))?;
            info!(id = setup.id, "spawned process");
        }

        let chain = &self.chain;
        let processes = &self.processes;
        let stake_amount = self.config.stake_amount;
        let live = chain.current_epoch().await? != EpochNumber(0);
        let setup_results = join_all(setups.into_iter().map(|setup| async move {
            let id = setup.id;
            let result = Self::complete_node_setup_inner(processes, chain, stake_amount, live, setup)
                .await
                .with_context(|| format!("complete node setup {id}"));
            (id, result)
        }))
        .await;

        let mut first_error = None;
        for (id, result) in setup_results {
            if let Err(error) = result {
                info!(id, error = %error, "node setup failed, stopping process");
                let cleanup = self.processes.stop_node(id).await;
                let error = match cleanup {
                    Ok(_) => error,
                    Err(stop_err) => error.context(format!("cleanup also failed: {stop_err:#}")),
                };
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        self.maybe_start_genesis_network().await?;

        Ok(())
    }

    async fn maybe_start_genesis_network(&self) -> Result<()> {
        if self.chain.current_epoch().await? != EpochNumber(0) {
            return Ok(());
        }

        let running = self.processes.running_node_ids();
        if running.len() < GROUP_SIZE {
            info!(
                running = running.len(),
                required = GROUP_SIZE,
                "genesis network not ready to start"
            );
            return Ok(());
        }

        let genesis_ids = running.into_iter().take(GROUP_SIZE).collect::<Vec<_>>();
        let genesis_authorities = genesis_ids
            .iter()
            .map(|id| clone_keypair(&self.processes.node(*id).authority))
            .collect::<Vec<_>>();

        self.chain
            .start_network(&genesis_authorities, self.config.spool_groups)
            .await
            .context("start genesis network")?;

        for id in genesis_ids {
            let keypair = clone_keypair(&self.processes.node(id).authority);
            self.chain
                .join_committee(&keypair)
                .await
                .with_context(|| format!("join genesis node {id} into next committee"))?;
        }

        Ok(())
    }

    pub fn node_refs(&self) -> Vec<NodeRef> {
        self.processes.node_refs()
    }

    pub async fn remove_node(&mut self, id: usize) -> Result<(), RemoveNodeError> {
        self.processes.remove_node(id).await
    }

    pub async fn remove_last_node(&mut self) -> Result<Option<usize>, RemoveNodeError> {
        let Some(id) = self.processes.last_running_node_id() else {
            return Ok(None);
        };
        self.processes.remove_node(id).await?;
        Ok(Some(id))
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        info!("shutting down all nodes");
        self.processes.shutdown_all().await
    }
}

fn clone_keypair(keypair: &Keypair) -> Keypair {
    Keypair::try_from(keypair.to_bytes().as_ref()).expect("keypair round-trip")
}

fn load_or_create_admin_keypair(path: &std::path::Path) -> Result<(Keypair, bool)> {
    if path.exists() {
        return Ok((
            load_solana_keypair(path)
                .with_context(|| format!("load testnet admin keypair: {}", path.display()))?,
            false,
        ));
    }

    let keypair = Keypair::new();
    write_solana_keypair(path, &keypair)?;
    Ok((keypair, true))
}
