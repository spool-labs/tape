use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tape_admin::{mint, node, programs, status, treasury, Context, Error};

#[derive(Parser)]
#[command(name = "tape-admin", about = "Tapedrive on-chain admin CLI")]
struct Cli {
    /// Solana RPC endpoint (devnet, testnet, mainnet-beta, or a custom URL).
    #[arg(long, env = "TAPE_RPC_URL")]
    rpc_url: String,

    /// Path to the payer/treasury Solana keypair JSON.
    #[arg(long, env = "TAPE_PAYER")]
    payer: PathBuf,

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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let cli = Cli::parse();

    let ctx = match Context::new(cli.rpc_url, &cli.payer) {
        Ok(ctx) => ctx,
        Err(err) => {
            eprintln!("context init failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    let result: Result<(), Error> = match cli.command {
        Command::Programs { op } => run_programs(&ctx, op),
        Command::Mint { op } => run_mint(&ctx, op).await,
        Command::Treasury { op } => run_treasury(&ctx, op).await,
        Command::Node { op } => run_node(&ctx, op).await,
        Command::Status => status::cluster(&ctx).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run_programs(ctx: &Context, op: ProgramsOp) -> Result<(), Error> {
    let ProgramsOp::Deploy { program, deploy_dir } = op;
    let deployed = programs::deploy(ctx, program, &deploy_dir)?;
    for (name, pubkey) in deployed {
        println!("{name}: {pubkey}");
    }
    Ok(())
}

async fn run_mint(ctx: &Context, op: MintOp) -> Result<(), Error> {
    match op {
        MintOp::Init => mint::init(ctx).await,
    }
}

async fn run_treasury(ctx: &Context, op: TreasuryOp) -> Result<(), Error> {
    match op {
        TreasuryOp::Fund { pubkeys_file, sol, tape } => {
            let recipients = treasury::parse_pubkeys_file(&pubkeys_file)?;
            let lamports = (sol * 1e9) as u64;
            let flux = (tape * 1e6) as u64;
            treasury::fund(ctx, &recipients, lamports, flux).await
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
                    name,
                    identity_path: identity,
                    bls_path: bls,
                    tls_path: tls,
                    address,
                    commission_bp,
                },
            )
            .await
        }
        NodeOp::JoinNetwork { identity } => node::join_network(ctx, &identity).await,
        NodeOp::SetAddress { identity, address } => {
            node::set_address(ctx, &identity, &address).await
        }
        NodeOp::List => status::list_nodes(ctx).await,
    }
}
