use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tape_network::{Settings, bootstrap, build, cache, genesis, stats, testnet, upgrade};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "tape-network", about = "Tapedrive testnet provisioning tool")]
struct Cli {
    /// Path to the settings YAML file.
    #[arg(long, default_value = "settings.yaml", env = "TAPE_NETWORK_SETTINGS")]
    settings: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Droplet-level operations: provision, destroy, inspect, ssh.
    Testnet {
        #[command(subcommand)]
        op: TestnetOp,
    },
    /// On-chain setup: deploy programs, init chain.
    Genesis {
        #[command(subcommand)]
        op: GenesisOp,
    },
    /// End-to-end bring-up from keygen to running nodes.
    Bootstrap {
        /// Optional override of the local working directory for per-node
        /// bundles. Defaults to `work/<testbed_id>`.
        #[arg(long)]
        work_dir: Option<PathBuf>,
        /// Skip step 6b (fund wallets). Use when resuming against nodes that
        /// were already funded in a previous run.
        #[arg(long)]
        skip_fund: bool,
    },
    /// Tail the `tape-node` systemd journal on one of the provisioned droplets.
    Logs {
        /// Node index (0-based, sorted by droplet name).
        node_index: usize,
        /// How many recent lines to show.
        #[arg(long, default_value_t = 200)]
        tail: usize,
        /// Follow (stream). Ctrl-C to stop.
        #[arg(long, short)]
        follow: bool,
    },
    /// Compile `tape-node` for linux on an ephemeral DO droplet and pull the
    /// binary back into `target/x86_64-unknown-linux-gnu/release/`.
    BuildLinux {
        /// Keep the builder droplet alive after the build (so subsequent
        /// incremental builds reuse the warm `target/` dir). Default is to
        /// destroy.
        #[arg(long)]
        keep: bool,
        /// Override builder droplet size (default `s-8vcpu-16gb`).
        #[arg(long)]
        size: Option<String>,
    },
    /// Rolling upgrade: for each droplet in turn, stop service → upload new
    /// binary → start service → wait for health.
    Upgrade {
        /// Optional explicit path to the binary. Defaults to the same lookup
        /// `bootstrap` uses (prefers
        /// `target/x86_64-unknown-linux-gnu/release/tape-node`).
        #[arg(long)]
        binary: Option<PathBuf>,
    },
    /// Scrape every node's /v1/stats endpoint and print one row per node.
    Stats {
        /// Per-node HTTP request timeout in milliseconds.
        #[arg(long, default_value_t = 2000)]
        timeout_ms: u64,
        /// Include cumulative transfer counters, latency, and scrape errors.
        #[arg(long, short)]
        verbose: bool,
    },
    /// Manage the RPC cache droplet that sits in front of the fleet.
    Cache {
        #[command(subcommand)]
        op: CacheOp,
    },
}

#[derive(Subcommand)]
enum CacheOp {
    /// Provision (or reinstall) the cache droplet and start the service.
    Deploy {
        /// Override the DigitalOcean droplet size slug. Only takes effect
        /// when provisioning a new cache droplet — an existing droplet is
        /// reused as-is. Examples: `s-1vcpu-2gb`, `s-2vcpu-4gb-amd`,
        /// `s-4vcpu-8gb`.
        #[arg(long)]
        size: Option<String>,
    },
    /// Destroy the cache droplet.
    Destroy,
    /// Print current state of the cache droplet.
    Status,
    /// Tail the cache's systemd journal.
    Logs {
        #[arg(long, default_value_t = 200)]
        tail: usize,
        #[arg(long, short)]
        follow: bool,
    },
}

#[derive(Subcommand)]
enum TestnetOp {
    /// Provision N droplets tagged with this testbed id.
    Deploy {
        /// Override `network.node_count` from settings.
        #[arg(long)]
        count: Option<u32>,
    },
    /// Delete droplets. Without `--node`, destroys every droplet tagged with
    /// this testbed id. With `--node N`, destroys just that one (simulates a
    /// single-node crash — local keys and on-chain state are untouched).
    Destroy {
        #[arg(long)]
        node: Option<usize>,
    },
    /// List droplets in the testbed with their status and public IP.
    Status,
    /// Open an interactive ssh session to the n-th droplet.
    Ssh {
        node_index: usize,
        /// Extra args passed through to ssh (e.g. a remote command).
        #[arg(trailing_var_arg = true)]
        extra: Vec<String>,
    },
}

#[derive(Subcommand)]
enum GenesisOp {
    /// Deploy the four tapedrive-authored Solana programs to the configured
    /// cluster.
    DeployPrograms {
        /// Path to the directory containing `<name>.so` and
        /// `<name>-keypair.json`. Defaults to `target/deploy`.
        #[arg(long)]
        deploy_dir: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let cli = Cli::parse();

    let settings = match Settings::from_file(&cli.settings) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("settings load failed: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    let result: Result<()> = match cli.command {
        Command::Testnet { op } => run_testnet(&settings, op).await,
        Command::Genesis { op } => run_genesis(&settings, op),
        Command::Bootstrap { work_dir, skip_fund } => {
            bootstrap::run(&settings, work_dir, bootstrap::RunOptions { skip_fund }).await
        }
        Command::Logs { node_index, tail, follow } => {
            testnet::logs(&settings, node_index, tail, follow).await
        }
        Command::BuildLinux { keep, size } => build::run(&settings, keep, size).await,
        Command::Upgrade { binary } => upgrade::run(&settings, binary).await,
        Command::Stats {
            timeout_ms,
            verbose,
        } => stats::run(&settings, timeout_ms, verbose).await,
        Command::Cache { op } => run_cache(&settings, op).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run_testnet(settings: &Settings, op: TestnetOp) -> Result<()> {
    match op {
        TestnetOp::Deploy { count } => {
            testnet::deploy(settings, count).await?;
            Ok(())
        }
        TestnetOp::Destroy { node: None } => testnet::destroy(settings).await,
        TestnetOp::Destroy { node: Some(idx) } => testnet::destroy_one(settings, idx).await,
        TestnetOp::Status => testnet::status(settings).await,
        TestnetOp::Ssh { node_index, extra } => testnet::ssh(settings, node_index, extra).await,
    }
}

fn run_genesis(settings: &Settings, op: GenesisOp) -> Result<()> {
    match op {
        GenesisOp::DeployPrograms { deploy_dir } => {
            let deployed = genesis::deploy_programs(settings, deploy_dir)?;
            for (name, pubkey) in deployed {
                println!("{name}: {pubkey}");
            }
            Ok(())
        }
    }
}

async fn run_cache(settings: &Settings, op: CacheOp) -> Result<()> {
    match op {
        CacheOp::Deploy { size } => {
            let st = cache::deploy(settings, size.as_deref()).await?;
            println!("cache deployed");
            if let Some(ip) = st.public_ip {
                println!("  ip:  {ip}");
            }
            if let Some(url) = st.url {
                println!("  url: {url}");
            }
            Ok(())
        }
        CacheOp::Destroy => cache::destroy(settings).await,
        CacheOp::Status => {
            let st = cache::status(settings).await?;
            if !st.present {
                println!("no cache droplet for testbed {}", settings.testbed_id);
            } else {
                println!("cache present");
                if let Some(ip) = st.public_ip {
                    println!("  ip:  {ip}");
                }
                if let Some(url) = st.url {
                    println!("  url: {url}");
                }
            }
            Ok(())
        }
        CacheOp::Logs { tail, follow } => cache::logs(settings, tail, follow).await,
    }
}
