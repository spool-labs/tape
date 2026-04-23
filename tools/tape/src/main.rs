use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tape_cli::commands::{
    balance, create, delete, expand, extend, info, list, read, use_cmd, write,
};
use tape_cli::{Context, OutputFormat, emit};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "tape",
    about = "Tapedrive user CLI — read, write, manage cassettes",
    version,
)]
struct Cli {
    /// Solana RPC endpoint. Accepts `l`/`d`/`t`/`m` shortcuts (like the
    /// solana CLI) or a full URL.
    #[arg(short = 'u', long = "url", global = true, env = "TAPE_RPC_URL")]
    rpc_url: Option<String>,

    /// Payer keypair. Defaults to `~/.config/solana/id.json` to match the
    /// solana CLI.
    #[arg(short = 'k', long = "keypair", global = true, env = "TAPE_KEYPAIR")]
    keypair: Option<PathBuf>,

    /// Config file path. Defaults to `~/.tape/cli-config.yaml`.
    #[arg(short = 'c', long = "config", global = true, env = "TAPE_CONFIG")]
    config: Option<PathBuf>,

    /// Output format.
    #[arg(short = 'o', long = "output", global = true, default_value = "text")]
    output: OutputFormat,

    /// Verbose logs (sets RUST_LOG=debug if unset).
    #[arg(short = 'v', long = "verbose", global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the wallet's SOL + TAPE balance.
    Balance,

    /// Create a new cassette (TapeKey) and save it to disk.
    Create {
        /// Where to save the cassette. Defaults to
        /// `~/.tape/cassettes/<pubkey>.json`.
        #[arg(long = "out")]
        out: Option<PathBuf>,

        /// Don't set the new cassette as the active one.
        #[arg(long)]
        no_use: bool,

        /// Overwrite if the target path already exists.
        #[arg(long)]
        force: bool,
    },

    /// Select a cassette to use for subsequent commands.
    Use {
        /// Path to a cassette keypair file.
        cassette: PathBuf,
    },

    /// Write data to a cassette. Provide a file path or `-m <message>`.
    Write {
        /// File to upload. Streamed; large files never land in memory.
        file: Option<PathBuf>,

        /// Inline message to upload instead of a file.
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Target cassette. Falls back to the `use`'d default.
        #[arg(long = "cassette")]
        cassette: Option<PathBuf>,

        /// Epochs of storage to reserve on first write (auto-reserve).
        #[arg(long, default_value_t = 4)]
        epochs: u64,
    },

    /// Read data back by track address.
    Read {
        /// Track address (base58) produced by a previous `write`.
        track_address: String,

        /// Write output to this file. Default: stdout.
        #[arg(long = "out")]
        out: Option<PathBuf>,
    },

    /// List every cassette owned by the current wallet.
    List,

    /// Show on-chain state of a cassette.
    Info {
        /// Target cassette. Falls back to the `use`'d default.
        #[arg(long = "cassette")]
        cassette: Option<PathBuf>,
    },

    /// Add more epochs to a cassette's reservation.
    Extend {
        /// Number of epochs to add.
        epochs: u64,
        /// Target cassette. Falls back to the `use`'d default.
        #[arg(long = "cassette")]
        cassette: Option<PathBuf>,
    },

    /// Grow a cassette's capacity. Size takes `k`/`m`/`g` suffixes or raw bytes.
    Expand {
        /// Extra capacity (e.g. `100m`, `2g`, or a byte count).
        size: String,
        /// Target cassette. Falls back to the `use`'d default.
        #[arg(long = "cassette")]
        cassette: Option<PathBuf>,
    },

    /// Delete a single track from a cassette, freeing its capacity.
    /// (Destroying the whole cassette is not supported.)
    Delete {
        /// Track address (base58).
        track_address: String,
        /// Target cassette. Falls back to the `use`'d default.
        #[arg(long = "cassette")]
        cassette: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let default_filter = if cli.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .try_init();

    let mut ctx = match Context::load(cli.rpc_url, cli.keypair, cli.config, cli.output) {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("context init failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = dispatch(&mut ctx, cli.command).await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(ctx: &mut Context, command: Command) -> anyhow::Result<()> {
    let format = ctx.output;
    match command {
        Command::Balance => {
            let out = balance::run(ctx).await?;
            emit(&out, format)?;
        }
        Command::Create { out, no_use, force } => {
            let result = create::run(ctx, out, !no_use, force)?;
            emit(&result, format)?;
        }
        Command::Use { cassette } => {
            let result = use_cmd::run(ctx, &cassette)?;
            emit(&result, format)?;
        }
        Command::Write {
            file,
            message,
            cassette,
            epochs,
        } => {
            let args = write::Args {
                file: file.as_deref(),
                message: message.as_deref(),
                cassette: cassette.as_deref(),
                epochs,
            };
            let result = write::run(ctx, args).await?;
            emit(&result, format)?;
        }
        Command::Read { track_address, out } => {
            let result = read::run(ctx, &track_address, out.as_deref()).await?;
            emit(&result, format)?;
        }
        Command::List => {
            let result = list::run(ctx).await?;
            emit(&result, format)?;
        }
        Command::Info { cassette } => {
            let result = info::run(ctx, cassette.as_deref()).await?;
            emit(&result, format)?;
        }
        Command::Extend { epochs, cassette } => {
            let result = extend::run(ctx, cassette.as_deref(), epochs).await?;
            emit(&result, format)?;
        }
        Command::Expand { size, cassette } => {
            let result = expand::run(ctx, cassette.as_deref(), &size).await?;
            emit(&result, format)?;
        }
        Command::Delete {
            track_address,
            cassette,
        } => {
            let result = delete::run(ctx, cassette.as_deref(), &track_address).await?;
            emit(&result, format)?;
        }
    }
    Ok(())
}
