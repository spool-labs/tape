use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use solana_sdk::pubkey::Pubkey;
use tape_admin::{chain, mint, node, programs, status, treasury, Context, Error};
use tape_cli_common::{CliOutput, GlobalArgs, OkMessage, emit};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "tape-admin", about = "Tapedrive on-chain admin CLI", version)]
struct Cli {
    #[command(flatten)]
    globals: GlobalArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Deploy or upgrade tapedrive-authored Solana programs.
    Programs {
        #[command(subcommand)]
        op: ProgramsOp,
    },
    /// TAPE mint setup.
    Mint {
        #[command(subcommand)]
        op: MintOp,
    },
    /// On-chain genesis: System + Epoch + Archive PDAs.
    Chain {
        #[command(subcommand)]
        op: ChainOp,
    },
    /// Fan out SOL + TAPE from the payer to a list of recipients.
    Treasury {
        #[command(subcommand)]
        op: TreasuryOp,
    },
    /// Node lifecycle on-chain operations.
    Node {
        #[command(subcommand)]
        op: NodeOp,
    },
    /// Print a cluster-wide snapshot (epoch, committee sizes, quorum).
    Status,
}

#[derive(Subcommand)]
enum ProgramsOp {
    /// Run `solana program deploy` for one or all programs.
    Deploy {
        #[arg(long, default_value = "all")]
        program: programs::Program,
        /// Directory containing <name>.so and <name>-keypair.json.
        #[arg(long, default_value = "target/deploy")]
        deploy_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum MintOp {
    /// Initialize the TAPE mint PDA. Run once per cluster.
    Init,
}

#[derive(Subcommand)]
enum ChainOp {
    /// Create + expand the System account and initialize the Epoch and
    /// Archive PDAs. Idempotent — safe to re-run; already-initialized
    /// accounts are skipped with a log line. Required before any node
    /// registration or rpc-cache deployment, because both expect the Epoch
    /// account to exist.
    Init,
}

#[derive(Subcommand)]
enum TreasuryOp {
    /// Disburse SOL + TAPE to each pubkey in the given file.
    Fund {
        /// Path to a file with one base58 pubkey per line (# comments allowed).
        #[arg(long)]
        pubkeys_file: PathBuf,
        /// SOL per recipient (fractional OK).
        #[arg(long, default_value_t = 2.0)]
        sol: f64,
        /// TAPE per recipient (fractional OK).
        #[arg(long, default_value_t = 1000.0)]
        tape: f64,
    },
}

#[derive(Subcommand)]
enum NodeOp {
    /// Submit a register_node instruction signed by the node's own identity.
    Register {
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        bls: PathBuf,
        #[arg(long)]
        tls: PathBuf,
        /// Advertised network address as `IP:PORT`.
        #[arg(long)]
        address: String,
        #[arg(long, default_value_t = 0)]
        commission_bp: u16,
        #[arg(long, default_value = "tape-node")]
        name: String,
    },
    /// Submit a join_network instruction for a registered node.
    JoinNetwork {
        #[arg(long)]
        identity: PathBuf,
    },
    /// Update the node's on-chain advertised network address. Useful when a
    /// node is resurrected on a new droplet with a new IP.
    SetAddress {
        #[arg(long)]
        identity: PathBuf,
        /// New network address as `IP:PORT`.
        #[arg(long)]
        address: String,
    },
    /// List every registered node with its on-chain metadata + stake.
    List,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let default_filter = if cli.globals.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .try_init();

    let ctx = match Context::from_global_args(
        cli.globals.rpc_url,
        cli.globals.keypair,
        cli.globals.config,
        cli.globals.output,
    ) {
        Ok(ctx) => ctx,
        Err(err) => {
            eprintln!("context init failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    let result = dispatch(&ctx, cli.command).await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(ctx: &Context, command: Command) -> Result<(), Error> {
    match command {
        Command::Programs { op } => run_programs(ctx, op),
        Command::Mint { op } => run_mint(ctx, op).await,
        Command::Chain { op } => run_chain(ctx, op).await,
        Command::Treasury { op } => run_treasury(ctx, op).await,
        Command::Node { op } => run_node(ctx, op).await,
        Command::Status => {
            let out = status::cluster(ctx).await?;
            emit_output(&out, ctx)
        }
    }
}

fn emit_output<T: CliOutput>(value: &T, ctx: &Context) -> Result<(), Error> {
    emit(value, ctx.output).map_err(|e| Error::Other(e.to_string()))
}

#[derive(serde::Serialize)]
struct ProgramsDeployOutput {
    programs: Vec<ProgramDeployed>,
}

#[derive(serde::Serialize)]
struct ProgramDeployed {
    name: String,
    pubkey: String,
}

impl CliOutput for ProgramsDeployOutput {
    fn print_text(&self) {
        for p in &self.programs {
            println!("{}: {}", p.name, p.pubkey);
        }
    }
}

fn run_programs(ctx: &Context, op: ProgramsOp) -> Result<(), Error> {
    let ProgramsOp::Deploy { program, deploy_dir } = op;
    let deployed: Vec<(String, Pubkey)> = programs::deploy(ctx, program, &deploy_dir)?;
    let out = ProgramsDeployOutput {
        programs: deployed
            .into_iter()
            .map(|(name, pk)| ProgramDeployed {
                name,
                pubkey: pk.to_string(),
            })
            .collect(),
    };
    emit_output(&out, ctx)
}

async fn run_mint(ctx: &Context, op: MintOp) -> Result<(), Error> {
    match op {
        MintOp::Init => {
            mint::init(ctx).await?;
            emit_output(&OkMessage::new("TAPE mint initialized"), ctx)
        }
    }
}

async fn run_chain(ctx: &Context, op: ChainOp) -> Result<(), Error> {
    match op {
        ChainOp::Init => {
            chain::init_all(ctx).await?;
            emit_output(
                &OkMessage::new("chain initialized (system + epoch + archive)"),
                ctx,
            )
        }
    }
}

async fn run_treasury(ctx: &Context, op: TreasuryOp) -> Result<(), Error> {
    match op {
        TreasuryOp::Fund { pubkeys_file, sol, tape } => {
            let recipients = treasury::parse_pubkeys_file(&pubkeys_file)?;
            let lamports = (sol * 1e9) as u64;
            let flux = (tape * 1e6) as u64;
            let count = recipients.len();
            treasury::fund(ctx, &recipients, lamports, flux).await?;
            emit_output(
                &OkMessage::new(format!(
                    "funded {count} recipients (target: {sol} SOL, {tape} TAPE each)"
                )),
                ctx,
            )
        }
    }
}

async fn run_node(ctx: &Context, op: NodeOp) -> Result<(), Error> {
    match op {
        NodeOp::Register {
            identity,
            bls,
            tls,
            address,
            commission_bp,
            name,
        } => {
            node::register(
                ctx,
                node::RegisterParams {
                    name: name.clone(),
                    identity_path: identity,
                    bls_path: bls,
                    tls_path: tls,
                    address: address.clone(),
                    commission_bp,
                },
            )
            .await?;
            emit_output(
                &OkMessage::new(format!("registered {name} at {address}")),
                ctx,
            )
        }
        NodeOp::JoinNetwork { identity } => {
            node::join_network(ctx, &identity).await?;
            emit_output(&OkMessage::new("join_network submitted"), ctx)
        }
        NodeOp::SetAddress { identity, address } => {
            node::set_address(ctx, &identity, &address).await?;
            emit_output(
                &OkMessage::new(format!("network_address updated to {address}")),
                ctx,
            )
        }
        NodeOp::List => {
            let out = status::list_nodes(ctx).await?;
            emit_output(&out, ctx)
        }
    }
}
