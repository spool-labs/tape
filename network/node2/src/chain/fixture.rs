use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use rpc::Rpc;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use tape_api::program::{exchange, staking, tapedrive, token};

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

            cur = cur.parent().with_context(|| {
                format!(
                    "failed to derive workspace root from {}",
                    manifest_dir.display()
                )
            })?;
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
            .add_program_from_file(token::ID, Self::deploy_path(workspace_root, "token"))
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
            .set_account_data(*address, *owner, data)
            .map_err(anyhow::Error::from)
            .context("seed_account")
    }

    pub async fn current_slot(&self) -> Result<u64> {
        self.rpc.get_slot().await.context("get_slot")
    }
}
