use std::path::PathBuf;
use std::process::ExitCode;

use self::Command::*;
use clap::{Parser, Subcommand};
use tape_cli::commands::{
    balance::run as balance_run,
    create::run as create_run,
    delete::run as delete_run,
    extend::run as extend_run,
    info::run as info_run,
    list::run as list_run,
    read::{ReadMode, run as read_run},
    resize::run as resize_run,
    use_cmd::run as use_run,
    write::{Args as WriteArgs, run as write_run},
};
use tape_cli::{Context, OutputFormat, emit};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "tape",
    about = "Tapedrive user CLI — read, write, manage tapes",
    version,
)]
struct Cli {
    /// Solana RPC endpoint. Accepts `l`/`d`/`t`/`m` shortcuts (like the
    /// solana CLI) or a full URL.
    #[arg(short = 'u', long = "url", global = true, env = "TAPE_RPC_URL", help_heading = "Global Options")]
    rpc_url: Option<String>,

    /// Payer keypair. Defaults to `~/.config/solana/id.json` to match the
    /// solana CLI.
    #[arg(short = 'k', long = "keypair", global = true, env = "TAPE_KEYPAIR", help_heading = "Global Options")]
    keypair: Option<PathBuf>,

    /// Config file path. Defaults to `~/.tape/cli-config.yaml`.
    #[arg(short = 'c', long = "config", global = true, env = "TAPE_CONFIG", help_heading = "Global Options")]
    config: Option<PathBuf>,

    /// Output format.
    #[arg(short = 'o', long = "output", global = true, default_value = "text", help_heading = "Global Options")]
    output: OutputFormat,

    /// Verbose logs (sets RUST_LOG=debug if unset).
    #[arg(short = 'v', long = "verbose", global = true, help_heading = "Global Options")]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the wallet's SOL + TAPE balance.
    Balance,

    /// Reserve a new tape and save its keypair locally.
    #[command(long_about = "Reserve a new tape on-chain and save the tape keypair locally.\n\nThis command spends TAPE immediately. It does not overwrite the active tape unless --use is passed.\n\nExamples:\n  tape create --capacity 100m --epochs 4 --use\n  tape create --capacity 2g --epochs 12 --out ./project.tape.json")]
    Create {
        /// Reserved storage capacity. Accepts bytes or k/m/g suffixes.
        #[arg(long)]
        capacity: String,

        /// Number of epochs to reserve.
        #[arg(long)]
        epochs: u64,

        /// Where to save the tape keypair. Defaults to
        /// `~/.tape/cassettes/<tape-address>.json`.
        #[arg(long = "out")]
        out: Option<PathBuf>,

        /// Set the new tape as the active tape for later commands.
        #[arg(long = "use")]
        use_tape: bool,

        /// Replace an existing local keypair file at --out.
        #[arg(long = "overwrite-key")]
        overwrite_key: bool,
    },

    /// Select a tape keypair to use for subsequent commands.
    Use {
        /// Path to a tape keypair file.
        tape: PathBuf,
    },

    /// Write data to a tape. Provide a file path, --message, or --stdin.
    #[command(long_about = "Write one object to an existing reserved tape.\n\nInputs up to 825 bytes are raw on-chain writes and return after the Solana write finalizes. Larger inputs up to 64 MiB are written as one certified blob track. Files over 64 MiB are written as streams and return a stream-manifest address.\n\nExamples:\n  tape write ./photo.jpg\n  tape write --message \"hello\"\n  cat archive.tar | tape write --stdin")]
    Write {
        /// File to upload. Files over 64 MiB are streamed.
        file: Option<PathBuf>,

        /// Inline message to upload instead of a file.
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Read bytes from stdin instead of a file.
        #[arg(long)]
        stdin: bool,

        /// Target tape keypair. Falls back to the active tape set by `tape use`.
        #[arg(long = "tape")]
        tape: Option<PathBuf>,
    },

    /// Read data back by track or stream-manifest address.
    #[command(long_about = "Read data by an address returned from tape write.\n\nBy default, auto mode detects stream manifests and direct tracks. Use --mode track for a raw/blob track address, or --mode stream for a stream-manifest address.\n\nExamples:\n  tape read <ADDRESS> --out ./download.bin\n  tape read <ADDRESS> --mode track > message.txt")]
    Read {
        /// Address produced by a previous `write`.
        address: String,

        /// Write output to this file. Default: stdout.
        #[arg(long = "out")]
        out: Option<PathBuf>,

        /// Replace --out if it already exists.
        #[arg(long)]
        overwrite: bool,

        /// Read mode. auto detects stream manifests and direct tracks.
        #[arg(long, default_value = "auto")]
        mode: ReadMode,
    },

    /// List every tape owned by the current wallet.
    List,

    /// Show on-chain state of a tape.
    Info {
        /// Target tape keypair. Falls back to the active tape set by `tape use`.
        #[arg(long = "tape")]
        tape: Option<PathBuf>,
    },

    /// Add more epochs to a tape's reservation.
    #[command(long_about = "Extend the lifetime of an existing tape by buying additional epochs.\n\nExample:\n  tape extend --epochs 4")]
    Extend {
        /// Number of epochs to add.
        #[arg(long)]
        epochs: u64,

        /// Target tape keypair. Falls back to the active tape set by `tape use`.
        #[arg(long = "tape")]
        tape: Option<PathBuf>,
    },

    /// Grow a tape's capacity.
    #[command(long_about = "Increase an existing tape's reserved capacity.\n\nUse --add to buy a capacity delta, or --to to grow to a target total capacity.\n\nExamples:\n  tape resize --add 100m\n  tape resize --to 2g")]
    Resize {
        /// Add this much capacity. Accepts bytes or k/m/g suffixes.
        #[arg(long)]
        add: Option<String>,

        /// Grow to this total capacity. Accepts bytes or k/m/g suffixes.
        #[arg(long)]
        to: Option<String>,

        /// Target tape keypair. Falls back to the active tape set by `tape use`.
        #[arg(long = "tape")]
        tape: Option<PathBuf>,
    },

    /// Delete a single track from a tape, freeing its capacity.
    /// (Destroying the whole tape is not supported.)
    Delete {
        /// Track address (base58).
        track_address: String,

        /// Target tape keypair. Falls back to the active tape set by `tape use`.
        #[arg(long = "tape")]
        tape: Option<PathBuf>,
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
        Balance => {
            let out = balance_run(ctx).await?;
            emit(&out, format)?;
        }
        Create {
            capacity,
            epochs,
            out,
            use_tape,
            overwrite_key,
        } => {
            let result = create_run(ctx, out, &capacity, epochs, use_tape, overwrite_key).await?;
            emit(&result, format)?;
        }
        Use { tape } => {
            let result = use_run(ctx, &tape)?;
            emit(&result, format)?;
        }
        Write {
            file,
            message,
            stdin,
            tape,
        } => {
            let args = WriteArgs {
                file: file.as_deref(),
                message: message.as_deref(),
                stdin,
                tape: tape.as_deref(),
            };
            let result = write_run(ctx, args).await?;
            emit(&result, format)?;
        }
        Read {
            address,
            out,
            overwrite,
            mode,
        } => {
            let result = read_run(ctx, &address, out.as_deref(), overwrite, mode).await?;
            emit(&result, format)?;
        }
        List => {
            let result = list_run(ctx).await?;
            emit(&result, format)?;
        }
        Info { tape } => {
            let result = info_run(ctx, tape.as_deref()).await?;
            emit(&result, format)?;
        }
        Extend { epochs, tape } => {
            let result = extend_run(ctx, tape.as_deref(), epochs).await?;
            emit(&result, format)?;
        }
        Resize { add, to, tape } => {
            let result = resize_run(ctx, tape.as_deref(), add.as_deref(), to.as_deref()).await?;
            emit(&result, format)?;
        }
        Delete {
            track_address,
            tape,
        } => {
            let result = delete_run(ctx, tape.as_deref(), &track_address).await?;
            emit(&result, format)?;
        }
    }
    Ok(())
}
