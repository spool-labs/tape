use std::time::Duration;

use anyhow::{Context, Result};
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use solana_client::nonblocking::rpc_client::RpcClient as SolRpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::errors::{ProgramError, TapeError, is_account_state_pending_error};
use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_advance_pool_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix, build_join_network_ix, build_reserve_snapshot_tape_ix,
    build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_core::types::coin::TAPE;
use tracing::info;

const CU_HIGH: u32 = 1_400_000;
const CU_MED: u32 = 400_000;

pub struct ChainManager {
    rpc: RpcClient<SolanaRpc>,
    sol_rpc: SolRpcClient,
    admin: Keypair,
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

        Ok(Self {
            rpc,
            sol_rpc,
            admin,
        })
    }

    pub fn rpc(&self) -> &RpcClient<SolanaRpc> {
        &self.rpc
    }

    pub fn admin_pubkey(&self) -> Pubkey {
        self.admin.pubkey()
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

    /// Initialize chain state. Mirrors `e2e/simnet/src/scenario.rs:43-127`.
    ///
    /// Programs must already be loaded via solana-test-validator --bpf-program flags.
    pub async fn init_chain(&self) -> Result<()> {
        let admin_pub = self.admin.pubkey();

        info!("airdropping SOL to admin");
        self.airdrop(&admin_pub, 50_000_000_000)
            .await
            .context("airdrop admin")?;

        info!("initializing mint");
        self.rpc
            .send_instructions(
                &self.admin,
                vec![build_initialize_mint_ix(admin_pub, admin_pub)],
            )
            .await
            .context("initialize_mint")?;

        info!("creating system account");
        self.rpc
            .send_instructions(
                &self.admin,
                vec![build_create_system_ix(admin_pub, admin_pub)],
            )
            .await
            .context("create_system")?;

        info!("expanding system account");
        for i in 0..10 {
            let result = self
                .rpc
                .send_instructions(
                    &self.admin,
                    vec![build_expand_system_ix(admin_pub, admin_pub)],
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
                        info!(iterations = i + 1, "system expansion complete");
                        break;
                    }
                    return Err(e).context("expand_system");
                }
            }
        }

        info!("initializing archive/epoch");
        self.rpc
            .send_instructions(
                &self.admin,
                vec![build_initialize_ix(admin_pub, admin_pub)],
            )
            .await
            .context("initialize archive/epoch")?;

        info!("reserving snapshot tape");
        self.rpc
            .send_instructions(
                &self.admin,
                vec![build_reserve_snapshot_tape_ix(admin_pub)],
            )
            .await
            .context("reserve snapshot tape")?;

        info!("chain initialization complete");
        Ok(())
    }

    pub async fn stake_node(&self, authority_keypair: &Keypair, amount_tape: u64) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let (node_address, _) = node_pda(authority);
        let amount = TAPE::parse(&amount_tape.to_string())
            .map_err(|_| anyhow::anyhow!("invalid stake amount: {amount_tape}"))?;

        let mut ixs = vec![ComputeBudgetInstruction::set_compute_unit_limit(CU_HIGH)];

        let payer_is_authority = self.admin.pubkey() == authority;
        if !payer_is_authority {
            ixs.extend(build_authority_with_tokens_ix(
                self.admin.pubkey(),
                authority,
                amount,
            ));
        }
        ixs.push(build_stake_with_pool_ix(
            self.admin.pubkey(),
            authority,
            node_address,
            amount,
        ));
        if !payer_is_authority {
            ixs.push(build_close_ata_ix(authority, self.admin.pubkey()));
        }

        self.rpc
            .send_instructions_with_signers(&self.admin, ixs, &[authority_keypair])
            .await
            .context("stake_node")?;

        Ok(())
    }

    pub async fn advance_pool(&self, authority: Pubkey) -> Result<()> {
        let (node_address, _) = node_pda(authority);
        let ix = build_advance_pool_ix(self.admin.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        let result = self
            .rpc
            .send_instructions(&self.admin, vec![cu_ix, ix])
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

    pub async fn join_network(&self, authority_keypair: &Keypair) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let (node_address, _) = node_pda(authority);
        let ix = build_join_network_ix(self.admin.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_MED);

        let result = self
            .rpc
            .send_instructions_with_signers(&self.admin, vec![cu_ix, ix], &[authority_keypair])
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if is_join_done(&e) {
                    Ok(())
                } else {
                    Err(e).context("join_network")
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
        Some(ProgramError::Tape(TapeError::AlreadyAdvanced))
    )
}

fn is_join_done(error: &rpc::RpcError) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::UnexpectedState))
    )
}
