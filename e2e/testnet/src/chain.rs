use std::time::Duration;

use anyhow::{Context, Result};
use rpc_client::RpcClient;
use rpc::RpcError;
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
    build_advance_pool_ix, build_create_system_ix, build_expand_system_ix, build_create_archive_ix,
    build_initialize_mint_ix, build_join_network_ix,
    build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_core::types::coin::TAPE;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tracing::info;

const CU_HIGH: u32 = 1_400_000;
const CU_MED: u32 = 400_000;

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

        info!("airdropping SOL to admin");
        self.airdrop(&admin_pub, admin_airdrop)
            .await
            .context("airdrop admin")?;

        info!("ensuring mint exists");
        let mint_result = self
            .rpc
            .send_instructions(
                &self.admin_signer,
                vec![build_initialize_mint_ix(admin_pub.into(), admin_pub.into())],
            )
            .await;
        match mint_result {
            Ok(_) => info!("mint initialized"),
            Err(e) if is_already_initialized(&e) => info!("mint already initialized"),
            Err(e) => return Err(e).context("initialize_mint"),
        }

        match self.rpc.get_system().await {
            Ok(_) => info!("system account already exists"),
            Err(RpcError::AccountNotFound(_)) => {
                info!("creating system account");
                let result = self
                    .rpc
                    .send_instructions(
                        &self.admin_signer,
                        vec![build_create_system_ix(admin_pub.into(), admin_pub.into())],
                    )
                    .await;
                match result {
                    Ok(_) => info!("system account created"),
                    Err(e) if is_already_initialized(&e) => info!("system account already created"),
                    Err(e) => return Err(e).context("create_system"),
                }
            }
            Err(RpcError::Deserialization(_)) => {
                info!("system account exists but is not fully expanded yet");
            }
            Err(e) => return Err(e).context("get_system"),
        }

        info!("expanding system account");
        for i in 0..10 {
            let result = self
                .rpc
                .send_instructions(
                    &self.admin_signer,
                    vec![build_expand_system_ix(admin_pub.into(), admin_pub.into())],
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

        let epoch_exists = self.rpc.get_epoch().await.is_ok();
        let archive_exists = self.rpc.get_archive().await.is_ok();
        if epoch_exists && archive_exists {
            info!("archive/epoch already initialized");
        } else {
            info!("initializing archive/epoch");
            let result = self
                .rpc
                .send_instructions(
                    &self.admin_signer,
                    vec![build_create_archive_ix(admin_pub.into(), admin_pub.into())],
                )
                .await;
            match result {
                Ok(_) => info!("archive/epoch initialized"),
                Err(e) if is_already_initialized(&e) => info!("archive/epoch already initialized"),
                Err(e) => return Err(e).context("initialize archive/epoch"),
            }
        }

        info!("chain initialization complete");
        Ok(())
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

    pub async fn join_network(&self, authority_keypair: &Keypair) -> Result<()> {
        let authority = authority_keypair.pubkey();
        let authority_address = Address::from(authority);
        let (node_address, _) = node_pda(authority_address);
        let ix = build_join_network_ix(
            self.admin.pubkey().into(),
            authority_address,
            node_address,
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

fn is_already_initialized(error: &rpc::RpcError) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("accountalreadyinitialized")
        || message.contains("already initialized")
        || message.contains("already in use")
}
