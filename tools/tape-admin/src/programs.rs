use std::path::{Path, PathBuf};
use std::process::Command;

use solana_sdk::pubkey::Pubkey;

use crate::context::Context;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum Program {
    All,
    Tapedrive,
    Token,
    Staking,
    Exchange,
}

impl Program {
    pub fn names(self) -> &'static [&'static str] {
        match self {
            Program::All => &["tapedrive", "token", "staking", "exchange"],
            Program::Tapedrive => &["tapedrive"],
            Program::Token => &["token"],
            Program::Staking => &["staking"],
            Program::Exchange => &["exchange"],
        }
    }
}

/// Deploy one or more tapedrive-authored Solana programs by shelling out to
/// `solana program deploy`. Returns the program id for each deployed program.
///
/// Requires `solana` CLI on PATH and a pre-built `<name>.so` plus matching
/// `<name>-keypair.json` in `deploy_dir` (produced by `cargo build-sbf`).
pub fn deploy(ctx: &Context, which: Program, deploy_dir: &Path) -> Result<Vec<(String, Pubkey)>> {
    let mut deployed = Vec::new();

    for name in which.names() {
        let so = deploy_dir.join(format!("{name}.so"));
        let program_keypair = deploy_dir.join(format!("{name}-keypair.json"));

        if !so.exists() {
            return Err(Error::Invalid(format!(
                "missing program artifact {}; run `cargo build-sbf`",
                so.display()
            )));
        }
        if !program_keypair.exists() {
            return Err(Error::Invalid(format!(
                "missing program keypair {}; run `cargo build-sbf`",
                program_keypair.display()
            )));
        }

        let pk = run_solana_deploy(&ctx.rpc_url, &ctx.payer_path, &program_keypair, &so, name)?;
        deployed.push((name.to_string(), pk));
    }

    Ok(deployed)
}

fn run_solana_deploy(
    rpc_url: &str,
    payer_keypair: &Path,
    program_keypair: &Path,
    so: &Path,
    name: &str,
) -> Result<Pubkey> {
    let payer_str = utf8_path(payer_keypair)?;
    let program_id_str = utf8_path(program_keypair)?;

    let output = Command::new("solana")
        .arg("program")
        .arg("deploy")
        .args(["--url", rpc_url])
        .args(["--keypair", payer_str])
        .args(["--program-id", program_id_str])
        .arg(so)
        .output()
        .map_err(|e| Error::Subprocess(format!("spawn solana: {e}")))?;

    if !output.status.success() {
        return Err(Error::Subprocess(format!(
            "solana program deploy {name} exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let pubkey_str = stdout
        .lines()
        .find_map(|line| line.strip_prefix("Program Id:").map(str::trim))
        .ok_or_else(|| {
            Error::Subprocess(format!("no Program Id in solana output for {name}:\n{stdout}"))
        })?;
    pubkey_str
        .parse::<Pubkey>()
        .map_err(|e| Error::Subprocess(format!("parse program id {pubkey_str}: {e}")))
}

fn utf8_path(p: &Path) -> Result<&str> {
    p.to_str()
        .ok_or_else(|| Error::Invalid(format!("non-utf8 path: {}", p.display())))
}

/// Default location of the `cargo build-sbf` output relative to the workspace
/// root.
pub fn default_deploy_dir() -> PathBuf {
    PathBuf::from("target/deploy")
}
