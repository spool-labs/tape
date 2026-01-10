//! Tapedrive CLI - Command-line interface for distributed storage.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod config;
mod output;
mod commands;
mod utils;

use config::{Cluster, ConfigFile, expand_path};
use output::OutputFormat;

/// Tapedrive distributed storage CLI.
#[derive(Parser, Debug)]
#[command(name = "tapedrive")]
#[command(author, version, about = "Tapedrive distributed storage CLI")]
#[command(propagate_version = true)]
pub struct Cli {
    /// Config file path.
    #[arg(short, long, env = "TAPE_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Keypair file path.
    #[arg(short = 'k', long, env = "TAPE_KEYPAIR", global = true)]
    keypair: Option<PathBuf>,

    /// Cluster: l (localnet), m (mainnet), d (devnet), t (testnet), or URL.
    #[arg(short = 'u', long, env = "TAPE_CLUSTER", global = true)]
    cluster: Option<Cluster>,

    /// Storage node URLs (comma-separated).
    #[arg(short = 'n', long, env = "TAPE_NODES", value_delimiter = ',', global = true)]
    nodes: Option<Vec<String>>,

    /// Output format: json, table, plain.
    #[arg(short, long, env = "TAPE_OUTPUT", global = true)]
    output: Option<OutputFormat>,

    /// Verbose output.
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Quiet mode (errors only).
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Dry run (show what would be done).
    #[arg(long, global = true)]
    dry_run: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Configuration management.
    Config {
        #[command(subcommand)]
        command: commands::config::ConfigCommand,
    },

    /// Key management.
    Keys {
        #[command(subcommand)]
        command: commands::keys::KeysCommand,
    },

    /// System administration (privileged).
    Admin {
        #[command(subcommand)]
        command: commands::admin::AdminCommand,
    },

    /// Storage node management.
    Node(commands::node::NodeArgs),

    /// On-chain account queries.
    Account {
        #[command(subcommand)]
        command: commands::account::AccountCommand,
    },

    /// Storage resource (tape) management.
    Tape(commands::tape::TapeArgs),

    /// Track/blob management.
    Track(commands::track::TrackArgs),

    /// Blob upload/download.
    Storage {
        #[command(subcommand)]
        command: commands::storage::StorageCommand,
    },

    /// Staking operations.
    Stake(commands::stake::StakeArgs),

    /// Token exchange.
    Exchange(commands::exchange::ExchangeArgs),

    /// Database utilities.
    #[cfg(feature = "db")]
    Db {
        #[command(subcommand)]
        command: commands::db::DbCommand,
    },

    /// Metrics display.
    Metrics {
        #[command(subcommand)]
        command: commands::metrics::MetricsCommand,
    },

    /// Network diagnostics.
    Network {
        #[command(subcommand)]
        command: commands::network::NetworkCommand,
    },

    /// Local testnet management.
    Testnet(commands::testnet::TestnetArgs),
}

/// Execution context with resolved configuration.
pub struct Context {
    pub config: ConfigFile,
    pub cluster: Cluster,
    pub keypair: Option<PathBuf>,
    pub nodes: Vec<String>,
    pub output: OutputFormat,
    pub verbose: bool,
    pub quiet: bool,
    pub dry_run: bool,
}

impl Context {
    /// Create context from CLI arguments and config file.
    pub fn new(cli: &Cli) -> Result<Self> {
        // Load config file
        let config = match &cli.config {
            Some(path) => ConfigFile::load_from(path)?,
            None => ConfigFile::load()?,
        };

        // Resolve cluster (CLI > env > config > default)
        let cluster = cli
            .cluster
            .clone()
            .or_else(|| config.get_cluster().ok().flatten())
            .unwrap_or_default();

        // Resolve keypair (CLI > config > default)
        let keypair = cli
            .keypair
            .clone()
            .or_else(|| config.default_keypair())
            .or_else(|| Some(expand_path("~/.config/solana/id.json")));

        // Resolve nodes (CLI > config)
        let nodes = cli
            .nodes
            .clone()
            .unwrap_or_else(|| config.nodes.clone());

        // Resolve output format (CLI > config > default)
        let output = cli
            .output
            .or_else(|| {
                config.output.as_ref().and_then(|s| s.parse().ok())
            })
            .unwrap_or_default();

        Ok(Self {
            config,
            cluster,
            keypair,
            nodes,
            output,
            verbose: cli.verbose,
            quiet: cli.quiet,
            dry_run: cli.dry_run,
        })
    }

    /// Get the RPC URL for the configured cluster.
    pub fn rpc_url(&self) -> String {
        self.cluster.rpc_url()
    }

    /// Print a message (respects quiet mode).
    pub fn print(&self, msg: &str) {
        if !self.quiet {
            println!("{}", msg);
        }
    }

    /// Print a verbose message.
    pub fn debug(&self, msg: &str) {
        if self.verbose && !self.quiet {
            eprintln!("[DEBUG] {}", msg);
        }
    }

    /// Print an error message.
    pub fn error(&self, msg: &str) {
        eprintln!("Error: {}", msg);
    }
}

fn print_title() {
    use colored::Colorize;
    eprintln!("{}", format!("⊙⊙ TAPEDRIVE {}", env!("CARGO_PKG_VERSION")).bold());
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing (use RUST_LOG env var, default to info)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    print_title();

    let cli = Cli::parse();
    let ctx = Context::new(&cli)?;

    match cli.command {
        Commands::Config { command } => commands::config::execute(&ctx, command).await,
        Commands::Keys { command } => commands::keys::execute(&ctx, command).await,
        Commands::Admin { command } => commands::admin::execute(&ctx, command).await,
        Commands::Node(args) => commands::node::execute(&ctx, args).await,
        Commands::Account { command } => commands::account::execute(&ctx, command).await,
        Commands::Tape(args) => commands::tape::execute(&ctx, args).await,
        Commands::Track(args) => commands::track::execute(&ctx, args).await,
        Commands::Storage { command } => commands::storage::execute(&ctx, command).await,
        Commands::Stake(args) => commands::stake::execute(&ctx, args).await,
        Commands::Exchange(args) => commands::exchange::execute(&ctx, args).await,
        #[cfg(feature = "db")]
        Commands::Db { command } => commands::db::execute(&ctx, command).await,
        Commands::Metrics { command } => commands::metrics::execute(&ctx, command).await,
        Commands::Network { command } => commands::network::execute(&ctx, command).await,
        Commands::Testnet(args) => commands::testnet::execute(&ctx, args).await,
    }
}
