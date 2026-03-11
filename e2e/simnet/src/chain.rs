use std::path::{Path, PathBuf};
use std::fs;
use std::str::FromStr;

use anyhow::{Context, Result};
use rpc::Rpc;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use tape_api::program::{exchange, staking, tapedrive, token};
use tracing::trace;

/// Shared chain fixture backed by LiteSVM.
#[derive(Clone)]
pub struct ChainFixture {
    rpc: LiteSvmRpc,
}

impl ChainFixture {
    pub fn new() -> Self {
        Self {
            rpc: LiteSvmRpc::new(),
        }
    }

    pub fn rpc(&self) -> &LiteSvmRpc {
        &self.rpc
    }

    pub fn workspace_root_from_manifest(manifest_dir: &Path) -> Result<PathBuf> {
        let mut cur = manifest_dir;

        loop {
            let cargo_toml = cur.join("Cargo.toml");
            if cargo_toml.exists() {
                let contents = fs::read_to_string(&cargo_toml)
                    .with_context(|| format!("read {}", cargo_toml.display()))?;
                if contents.contains("[workspace]") {
                    return Ok(cur.to_path_buf());
                }
            }

            cur = cur
                .parent()
                .with_context(|| format!("failed to derive workspace root from {}", manifest_dir.display()))?;
        }
    }

    pub fn deploy_path(workspace_root: &Path, name: &str) -> PathBuf {
        workspace_root.join("target/deploy").join(format!("{name}.so"))
    }

    pub fn external_program_path(workspace_root: &Path, name: &str) -> PathBuf {
        workspace_root.join("test/elfs").join(format!("{name}.so"))
    }

    /// Loads tapedrive programs expected by node flows.
    pub fn load_default_programs(&self, workspace_root: &Path) -> Result<()> {
        self.rpc
            .add_program_from_file(
                tapedrive::ID,
                Self::deploy_path(workspace_root, "tapedrive"),
            )
            .context("load tapedrive program")?;

        self.rpc
            .add_program_from_file(
                token::ID,
                Self::deploy_path(workspace_root, "token"),
            )
            .context("load token program")?;

        self.rpc
            .add_program_from_file(
                exchange::ID,
                Self::deploy_path(workspace_root, "exchange"),
            )
            .context("load exchange program")?;

        self.rpc
            .add_program_from_file(
                staking::ID,
                Self::deploy_path(workspace_root, "staking"),
            )
            .context("load staking program")?;

        let mpl_id = Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
            .context("parse mpl token metadata program id")?;
        self.rpc
            .add_program_from_file(
                mpl_id,
                Self::external_program_path(workspace_root, "mpl_token_metadata"),
            )
            .context("load mpl token metadata program")?;

        Ok(())
    }

    pub fn airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<()> {
        self.rpc.airdrop(pubkey, lamports).context("airdrop")
    }

    pub fn seed_account(
        &self,
        address: &Pubkey,
        owner: &Pubkey,
        data: &[u8],
    ) -> Result<()> {
        self.rpc
            .set_account_data(address, owner, data)
            .map_err(|e| anyhow::anyhow!(e))
            .context("seed_account")
    }

    pub async fn current_slot(&self) -> Result<u64> {
        self.rpc.get_slot().await.context("get_slot")
    }

    pub async fn advance_slots(&self, delta: u64) -> Result<u64> {
        let current = self.current_slot().await?;
        let target = current.saturating_add(delta);
        trace!(
            from_slot = current,
            to_slot = target,
            delta = delta,
            "advancing litesvm slot cursor"
        );
        self.rpc.warp_to_slot(target).context("warp_to_slot")?;
        Ok(target)
    }

    pub async fn send_instructions_and_advance(
        &self,
        payer: &Keypair,
        instructions: Vec<Instruction>,
        slot_advance_per_tx: u64,
    ) -> Result<Signature> {
        let payer_pubkey = payer.pubkey();
        let instruction_count = instructions.len();
        trace!(
            payer = %payer_pubkey,
            instruction_count,
            "submitting instructions to litesvm"
        );

        let client = RpcClient::from_rpc(self.rpc.clone());
        let sig = client
            .send_instructions(payer, instructions)
            .await
            .context("send_instructions")?;
        trace!(
            signature = %sig,
            payer = %payer_pubkey,
            "submitted instruction batch"
        );

        if slot_advance_per_tx > 0 {
            let tip_slot = self.advance_slots(slot_advance_per_tx).await?;
            trace!(
                signature = %sig,
                new_slot = tip_slot,
                "advanced litesvm slots after submit"
            );
        } else {
            let slot = self.current_slot().await?;
            trace!(signature = %sig, slot = slot, "slot advancement disabled for this tx");
        }

        Ok(sig)
    }

    pub async fn send_instructions_with_signers_and_advance(
        &self,
        payer: &Keypair,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
        slot_advance_per_tx: u64,
    ) -> Result<Signature> {
        let payer_pubkey = payer.pubkey();
        let instruction_count = instructions.len();
        let signer_count = signers.len();
        let signer_pubkeys: Vec<String> = signers.iter().map(|s| s.pubkey().to_string()).collect();
        trace!(
            payer = %payer_pubkey,
            instruction_count,
            signer_count,
            signers = ?signer_pubkeys,
            "submitting signed instructions to litesvm"
        );
        let client = RpcClient::from_rpc(self.rpc.clone());
        let sig = client
            .send_instructions_with_signers(payer, instructions, signers)
            .await
            .context("send_instructions_with_signers")?;
        trace!(
            signature = %sig,
            payer = %payer_pubkey,
            "submitted signed instruction batch"
        );

        if slot_advance_per_tx > 0 {
            let tip_slot = self.advance_slots(slot_advance_per_tx).await?;
            trace!(
                signature = %sig,
                new_slot = tip_slot,
                "advanced litesvm slots after submit"
            );
        } else {
            let slot = self.current_slot().await?;
            trace!(signature = %sig, slot = slot, "slot advancement disabled for this tx");
        }

        Ok(sig)
    }
}

impl Default for ChainFixture {
    fn default() -> Self {
        Self::new()
    }
}
