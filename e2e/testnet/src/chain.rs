use std::time::Duration;

use anyhow::{Context, Result};
use rpc::RpcError;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use solana_client::nonblocking::rpc_client::RpcClient as SolRpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::errors::{ProgramError, TapeError};
use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_advance_pool_ix, build_create_archive_ix, build_create_committee_ix,
    build_create_epoch_ix, build_create_peer_set_ix, build_create_system_ix,
    build_initialize_mint_ix, build_join_committee_ix, build_stake_with_pool_ix,
    build_start_network_ix,
};
use tape_api::program::tapedrive::{
    node_pda, DEFAULT_BURN_FEE_BPS, DEFAULT_SUBSIDY_DECAY_BPS,
};
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::coin::TAPE;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tracing::info;

const CU_HIGH: u32 = 1_400_000;
const CU_MED: u32 = 400_000;
const BOOTSTRAP_EPOCHS: [EpochNumber; 3] = [EpochNumber(0), EpochNumber(1), EpochNumber(2)];

pub struct ChainManager {
    rpc: RpcClient<SolanaRpc>,
    sol_rpc: SolRpcClient,
    admin: Keypair,
    admin_signer: CryptoKeypair,
}

impl ChainManager {
    pub fn new(rpc_url: &str, admin: Keypair) -> Result<Self> {
        let rpc = RpcClient::new(RpcConfig {
            endpoints: vec![rpc_url.to_string()],
            ..Default::default()
        })
        .context("create tapedrive rpc client")?;

        let sol_rpc = SolRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );
        let admin_signer = CryptoKeypair::from_solana_keypair(&admin)
            .context("convert admin keypair")?;

        Ok(Self {
            rpc,
            sol_rpc,
            admin,
            admin_signer,
        })
    }

    pub fn rpc(&self) -> &RpcClient<SolanaRpc> {
        &self.rpc
    }

    pub fn admin_pubkey(&self) -> Pubkey {
        self.admin.pubkey()
    }

    pub async fn current_epoch(&self) -> Result<EpochNumber> {
        Ok(self.rpc.get_system().await.context("get_system")?.current_epoch)
    }

    pub async fn airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<()> {
        let sig = self
            .sol_rpc
            .request_airdrop(pubkey, lamports)
            .await
            .context("request_airdrop")?;

        for _ in 0..60 {
            let confirmed = self
                .sol_rpc
                .confirm_transaction(&sig)
                .await
                .unwrap_or(false);
            if confirmed {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("airdrop confirmation timeout for {pubkey}");
    }

    /// Ensure chain state exists. Safe to call repeatedly against the same ledger.
    ///
    /// Programs must already be loaded via solana-test-validator --bpf-program flags.
    pub async fn ensure_chain_initialized(&self, admin_airdrop: u64) -> Result<()> {
        let admin_pub = self.admin.pubkey();
        let admin_address = Address::from(admin_pub);

        info!("airdropping SOL to admin");
        self.airdrop(&admin_pub, admin_airdrop)
            .await
            .context("airdrop admin")?;

        info!("ensuring mint exists");
        self.send_idempotent(
            "initialize_mint",
            vec![build_initialize_mint_ix(admin_address, admin_address)],
        )
        .await?;

        self.send_idempotent(
            "create_system",
            vec![build_create_system_ix(admin_address, admin_address)],
        )
        .await?;

        self.send_idempotent(
            "create_peer_set",
            vec![build_create_peer_set_ix(admin_address)],
        )
        .await?;

        self.send_idempotent(
            "create_archive",
            vec![build_create_archive_ix(admin_address, admin_address)],
        )
        .await?;

        for epoch in BOOTSTRAP_EPOCHS {
            self.send_idempotent(
                &format!("create_epoch({})", epoch.0),
                vec![build_create_epoch_ix(admin_address, epoch)],
            )
            .await?;

            self.send_idempotent(
                &format!("create_committee({})", epoch.0),
                vec![build_create_committee_ix(admin_address, epoch)],
            )
            .await?;
        }

        info!("chain initialization complete");
        Ok(())
    }

    async fn send_idempotent(&self, label: &str, ixs: Vec<Instruction>) -> Result<()> {
        match self.rpc.send_instructions(&self.admin_signer, ixs).await {
            Ok(_) => {
                info!(%label, "chain setup step complete");
                Ok(())
            }
            Err(e) if is_already_initialized(&e) => {
                info!(%label, "chain setup step already complete");
                Ok(())
            }
            Err(e) => Err(e).with_context(|| label.to_string()),
        }
    }

    pub async fn stake_node(&self, authority_keypair: &Keypair, amount_tape: u64) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let authority_address = Address::from(authority);
        let (node_address, _) = node_pda(authority_address);
        let amount = TAPE::parse(&amount_tape.to_string())
            .map_err(|_| anyhow::anyhow!("invalid stake amount: {amount_tape}"))?;
        let authority_signer = CryptoKeypair::from_solana_keypair(authority_keypair)
            .context("convert authority keypair")?;

        let mut ixs = vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];

        let payer_is_authority = self.admin.pubkey() == authority;
        if !payer_is_authority {
            ixs.extend(build_authority_with_tokens_ix(
                self.admin.pubkey().into(),
                authority_address,
                amount,
            )?);
        }
        ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey().into(),
            authority_address,
            node_address,
            amount,
        ));
        if !payer_is_authority {
            ixs.push(build_close_ata_ix(
                authority_address,
                self.admin.pubkey().into(),
            )?);
        }

        self.rpc
            .send_instructions_with_signers(&self.admin_signer, ixs, &[&authority_signer])
            .await
            .context("stake_node")?;

        Ok(())
    }

    pub async fn ensure_node_staked(
        &self,
        authority_keypair: &Keypair,
        target_amount_tape: u64,
    ) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let authority_address = Address::from(authority);
        let current_amount = match self.rpc.get_stake(&authority_address).await {
            Ok(stake) => stake.inner.amount.as_u64(),
            Err(RpcError::AccountNotFound(_)) => 0,
            Err(e) => return Err(e).context("get_stake"),
        };

        if current_amount >= target_amount_tape {
            info!(
                authority = %authority,
                current = current_amount,
                target = target_amount_tape,
                "stake already satisfied",
            );
            return Ok(());
        }

        let top_up_amount = target_amount_tape - current_amount;
        info!(
            authority = %authority,
            current = current_amount,
            target = target_amount_tape,
            top_up = top_up_amount,
            "topping up node stake",
        );

        self.stake_node(authority_keypair, top_up_amount).await
    }

    pub async fn advance_pool(&self, authority: Pubkey) -> Result<()> {
        let authority_address = Address::from(authority);
        let (node_address, _) = node_pda(authority_address);
        let current_epoch = self.rpc.get_system().await?.current_epoch;
        let ix = build_advance_pool_ix(
            self.admin.pubkey().into(),
            node_address,
            current_epoch,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        let result = self
            .rpc
            .send_instructions(&self.admin_signer, vec![cu_ix, ix])
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if is_already_advanced(&e) {
                    Ok(())
                } else {
                    Err(e).context("advance_pool")
                }
            }
        }
    }

    pub async fn start_network(
        &self,
        genesis_authorities: &[Pubkey],
        spool_groups: u64,
    ) -> Result<()> {
        if self.current_epoch().await? != EpochNumber(0) {
            info!("network already started");
            return Ok(());
        }

        if genesis_authorities.len() != GROUP_SIZE {
            anyhow::bail!(
                "start_network requires exactly {GROUP_SIZE} genesis nodes, got {}",
                genesis_authorities.len()
            );
        }
        if spool_groups == 0 {
            anyhow::bail!("spool_groups must be > 0");
        }

        let genesis_nodes = genesis_authorities
            .iter()
            .map(|authority| node_pda(Address::from(*authority)).0)
            .collect::<Vec<_>>();

        let ix = build_start_network_ix(
            self.admin.pubkey().into(),
            self.admin.pubkey().into(),
            GROUP_SIZE as u64,
            spool_groups,
            TAPE(0),
            DEFAULT_BURN_FEE_BPS,
            DEFAULT_SUBSIDY_DECAY_BPS,
            &genesis_nodes,
        );

        let result = self.rpc.send_instructions(&self.admin_signer, vec![ix]).await;
        match result {
            Ok(_) => {
                info!(spool_groups, "network started");
                Ok(())
            }
            Err(e) => {
                if is_bad_epoch_state(&e) {
                    info!("start_network skipped (network already live)");
                    Ok(())
                } else {
                    Err(e).context("start_network")
                }
            }
        }
    }

    pub async fn join_committee(&self, authority_keypair: &Keypair) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let authority_address = Address::from(authority);
        let (node_address, _) = node_pda(authority_address);
        let current_epoch = self.current_epoch().await?;
        let ix = build_join_committee_ix(
            self.admin.pubkey().into(),
            authority_address,
            node_address,
            current_epoch,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);
        let authority_signer = CryptoKeypair::from_solana_keypair(authority_keypair)
            .context("convert authority keypair")?;

        let result = self
            .rpc
            .send_instructions_with_signers(
                &self.admin_signer,
                vec![cu_ix, ix],
                &[&authority_signer],
            )
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if is_join_done(&e) {
                    Ok(())
                } else {
                    Err(e).context("join_committee")
                }
            }
        }
    }
}

fn program_error(error: &rpc::RpcError) -> Option<ProgramError> {
    ProgramError::from_error_string(&error.to_string())
}

fn is_already_advanced(error: &rpc::RpcError) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(
            TapeError::AlreadyAdvanced | TapeError::BadEpochState
        ))
    )
}

fn is_join_done(error: &rpc::RpcError) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(
            TapeError::UnexpectedState | TapeError::NodeStale
        ))
    )
}

fn is_bad_epoch_state(error: &rpc::RpcError) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::BadEpochState))
    )
}

fn is_already_initialized(error: &rpc::RpcError) -> bool {
    if matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::UnexpectedState))
    ) {
        return true;
    }

    let message = error.to_string().to_lowercase();
    message.contains("accountalreadyinitialized")
        || message.contains("already initialized")
        || message.contains("already in use")
        || message.contains("requires an uninitialized account")
}
