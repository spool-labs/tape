//! Shared global args for every tape CLI tool. Flatten this into your
//! clap `Cli` struct so all tools speak the same flag language.

use std::path::PathBuf;

use clap::Args;

use crate::output::OutputFormat;

#[derive(Debug, Args)]
pub struct GlobalArgs {
    /// Solana RPC endpoint. Accepts `l`/`d`/`t`/`m` shortcuts (like the
    /// solana CLI) or a full URL.
    #[arg(short = 'u', long = "url", global = true, env = "TAPE_RPC_URL")]
    pub rpc_url: Option<String>,

    /// Payer keypair. Defaults to `~/.config/solana/id.json` to match the
    /// solana CLI.
    #[arg(short = 'k', long = "keypair", global = true, env = "TAPE_KEYPAIR")]
    pub keypair: Option<PathBuf>,

    /// Config file path. Defaults to `~/.tape/cli-config.yaml`.
    #[arg(short = 'c', long = "config", global = true, env = "TAPE_CONFIG")]
    pub config: Option<PathBuf>,

    /// Output format.
    #[arg(short = 'o', long = "output", global = true, default_value = "text")]
    pub output: OutputFormat,

    /// Verbose logs (bumps tracing filter to debug if unset).
    #[arg(short = 'v', long = "verbose", global = true)]
    pub verbose: bool,
}
