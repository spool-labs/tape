//! Local testnet management commands for multi-node testing.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction;
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::{BasisPoints, NetworkAddress};
use tape_crypto::bls12254::min_sig::PrivKey;

use tape_sdk::create_rpc_client;

use crate::utils::get_keypair;
use crate::Context;

/// Testnet command arguments.
#[derive(Args, Debug)]
pub struct TestnetArgs {
    #[command(subcommand)]
    pub command: TestnetCommand,
}

#[derive(Subcommand, Debug)]
pub enum TestnetCommand {
    /// Spawn a local testnet with N nodes.
    Spawn {
        /// Number of nodes to spawn.
        count: usize,

        /// Base directory for testnet data (default: ~/.tape/testnet).
        #[arg(long)]
        base_dir: Option<PathBuf>,

        /// Base port for nodes (default: 8080).
        #[arg(long, default_value = "8080")]
        base_port: u16,

        /// Initial stake per node in TAPE tokens.
        #[arg(long, default_value = "1000")]
        stake: u64,
    },

    /// Start all spawned testnet nodes.
    Start {
        /// Base directory for testnet data.
        #[arg(long)]
        base_dir: Option<PathBuf>,
    },

    /// Stop all running testnet nodes.
    Stop,

    /// Show testnet status.
    Status {
        /// Base directory for testnet data.
        #[arg(long)]
        base_dir: Option<PathBuf>,
    },

    /// Clean up testnet data.
    Clean {
        /// Base directory for testnet data.
        #[arg(long)]
        base_dir: Option<PathBuf>,
    },
}

pub async fn execute(ctx: &Context, args: TestnetArgs) -> Result<()> {
    match args.command {
        TestnetCommand::Spawn {
            count,
            base_dir,
            base_port,
            stake,
        } => spawn_testnet(ctx, count, base_dir, base_port, stake).await,
        TestnetCommand::Start { base_dir } => start_testnet(ctx, base_dir).await,
        TestnetCommand::Stop => stop_testnet().await,
        TestnetCommand::Status { base_dir } => show_status(ctx, base_dir).await,
        TestnetCommand::Clean { base_dir } => clean_testnet(base_dir).await,
    }
}

fn get_base_dir(base_dir: Option<PathBuf>) -> PathBuf {
    base_dir.unwrap_or_else(|| {
        dirs::home_dir()
            .map(|h| h.join(".tape").join("testnet"))
            .unwrap_or_else(|| PathBuf::from(".tape/testnet"))
    })
}

fn node_dir(base_dir: &PathBuf, index: usize) -> PathBuf {
    base_dir.join(format!("node-{}", index))
}

/// Generate node config content with parameterized paths and ports.
fn generate_node_config(
    name: &str,
    node_dir: &PathBuf,
    port: u16,
) -> String {
    let keys_dir = node_dir.join("keys");
    let data_dir = node_dir.join("data");

    format!(
        r#"# Tapedrive Storage Node Configuration (testnet)
version: 1

name: "{name}"
commission: 500

tls_keypair: {keys_dir}/tls.json
bls_keypair: {keys_dir}/bls.json
node_keypair: {keys_dir}/node.json

bind_address: "0.0.0.0:{port}"
public_host: "127.0.0.1"
public_port: {port}

tls:
  generate_self_signed: true

storage_path: {data_dir}
"#,
        name = name,
        keys_dir = keys_dir.display(),
        data_dir = data_dir.display(),
        port = port,
    )
}

/// Spawn a local testnet with N nodes.
async fn spawn_testnet(
    ctx: &Context,
    count: usize,
    base_dir: Option<PathBuf>,
    base_port: u16,
    stake_amount: u64,
) -> Result<()> {
    if count == 0 {
        anyhow::bail!("Node count must be at least 1");
    }

    let base_dir = get_base_dir(base_dir);
    println!("Spawning {} node testnet in {}", count, base_dir.display());
    println!();

    // Create base directory
    std::fs::create_dir_all(&base_dir)?;

    // Get fee payer for on-chain transactions
    let fee_payer = get_keypair(ctx)?;
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Store node info for summary
    let mut node_infos: Vec<NodeInfo> = Vec::new();

    for i in 0..count {
        let port = base_port + i as u16;
        let name = format!("testnet-node-{}", i);
        let node_path = node_dir(&base_dir, i);

        println!("[Node {}] Setting up {}...", i, name);

        // Create directories
        let keys_dir = node_path.join("keys");
        let data_dir = node_path.join("data");
        std::fs::create_dir_all(&keys_dir)?;
        std::fs::create_dir_all(&data_dir)?;

        // Generate keypairs
        let node_keypair = generate_or_load_keypair(&keys_dir.join("node.json"))?;
        let tls_keypair = generate_or_load_keypair(&keys_dir.join("tls.json"))?;
        let bls_private_key = generate_or_load_bls_keypair(&keys_dir.join("bls.json"))?;

        // Write config
        let config_path = node_path.join("node.yaml");
        let config_content = generate_node_config(&name, &node_path, port);
        std::fs::write(&config_path, &config_content)?;
        println!("  Config: {}", config_path.display());

        // Register node on-chain
        let (node_address, _) = node_pda(node_keypair.pubkey());

        // Check if already registered
        let already_registered = client.get_node(&node_keypair.pubkey()).await.is_ok();

        if !already_registered {
            println!("  Registering on-chain...");

            let bls_pubkey = bls_private_key
                .public_key()
                .map_err(|e| anyhow::anyhow!("Failed to derive BLS public key: {:?}", e))?;
            let bls_pop = bls_private_key
                .proof_of_possession()
                .map_err(|e| anyhow::anyhow!("Failed to generate BLS proof of possession: {:?}", e))?;

            let network_address = NetworkAddress::from(&format!("127.0.0.1:{}", port))
                .map_err(|_| anyhow::anyhow!("Invalid network address"))?;

            let ix = instruction::build_register_node_ix(
                fee_payer.pubkey(),
                node_keypair.pubkey(),
                to_name(&name),
                BasisPoints(500),
                network_address,
                tls_keypair.pubkey(),
                bls_pubkey,
                bls_pop,
            );

            client
                .send_instructions_with_signers(&fee_payer, vec![ix], &[&node_keypair])
                .await
                .map_err(|e| anyhow::anyhow!("Failed to register node: {}", e))?;

            println!("  Registered: {}", node_address);
        } else {
            println!("  Already registered: {}", node_address);
        }

        // Stake tokens to the node using CLI subprocess
        if stake_amount > 0 {
            println!("  Staking {} TAPE...", stake_amount);

            let exe_path = std::env::current_exe()?;
            let output = Command::new(&exe_path)
                .args([
                    "-u", "l",
                    "-k", &ctx.keypair.as_ref().map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_else(|| "~/.config/solana/id.json".to_string()),
                    "stake", "deposit",
                    &node_address.to_string(),
                    &stake_amount.to_string(),
                ])
                .output()
                .with_context(|| "Failed to run stake command")?;

            if output.status.success() {
                println!("  Staked: {} TAPE", stake_amount);
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                println!("  Stake failed: {}", stderr.lines().next().unwrap_or("unknown error"));
            }
        }

        // Join committee
        println!("  Joining committee...");
        let ix = instruction::build_join_network_ix(
            fee_payer.pubkey(),
            node_keypair.pubkey(),
            node_address,
        );

        match client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&node_keypair])
            .await
        {
            Ok(_) => println!("  Joined committee"),
            Err(e) => {
                // May fail if already joined
                println!("  Join skipped: {}", e);
            }
        }

        node_infos.push(NodeInfo {
            index: i,
            name,
            port,
            node_address,
            config_path,
        });

        println!();
    }

    // Advance epoch to activate nodes
    println!("Advancing epoch to activate committee...");
    let ix = instruction::build_advance_epoch_ix(fee_payer.pubkey(), fee_payer.pubkey());
    match client.send_instructions(&fee_payer, vec![ix]).await {
        Ok(_) => println!("Epoch advanced"),
        Err(e) => println!("Epoch advance skipped: {}", e),
    }

    // Summary
    println!();
    println!("=== Testnet Ready ===");
    println!();
    println!("{:<6} {:<20} {:<8} {}", "Index", "Name", "Port", "Node Address");
    println!("{}", "-".repeat(80));
    for info in &node_infos {
        println!(
            "{:<6} {:<20} {:<8} {}",
            info.index, info.name, info.port, info.node_address
        );
    }
    println!();
    println!("To start all nodes:");
    println!("  tape testnet start");
    println!();
    println!("To start individual nodes:");
    for info in &node_infos {
        println!(
            "  tape node start --config {}",
            info.config_path.display()
        );
    }

    Ok(())
}

struct NodeInfo {
    index: usize,
    name: String,
    port: u16,
    node_address: solana_sdk::pubkey::Pubkey,
    config_path: PathBuf,
}

/// Generate or load an Ed25519 keypair.
fn generate_or_load_keypair(path: &PathBuf) -> Result<Keypair> {
    if path.exists() {
        let json = std::fs::read_to_string(path)?;
        let bytes: Vec<u8> = serde_json::from_str(&json)?;
        Keypair::from_bytes(&bytes).map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))
    } else {
        let keypair = Keypair::new();
        let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
        std::fs::write(path, &json)?;
        Ok(keypair)
    }
}

/// Generate or load a BLS keypair.
fn generate_or_load_bls_keypair(path: &PathBuf) -> Result<BlsPrivateKey> {
    if path.exists() {
        let json = std::fs::read_to_string(path)?;
        let bytes: Vec<u8> = serde_json::from_str(&json)?;
        if bytes.len() != 32 {
            anyhow::bail!("Invalid BLS key length");
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(BlsPrivateKey(PrivKey(arr)))
    } else {
        let bls_key = BlsPrivateKey::from_random();
        let bytes: &[u8] = bytemuck::bytes_of(&bls_key);
        let json = serde_json::to_string(&bytes.to_vec())?;
        std::fs::write(path, &json)?;
        Ok(bls_key)
    }
}

/// Start all testnet nodes.
async fn start_testnet(_ctx: &Context, base_dir: Option<PathBuf>) -> Result<()> {
    let base_dir = get_base_dir(base_dir);

    if !base_dir.exists() {
        anyhow::bail!(
            "Testnet not found at {}. Run 'tape testnet spawn' first.",
            base_dir.display()
        );
    }

    // Find all node directories
    let mut nodes: Vec<usize> = Vec::new();
    for entry in std::fs::read_dir(&base_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(name_str) = name.to_str() {
            if name_str.starts_with("node-") {
                if let Ok(idx) = name_str.strip_prefix("node-").unwrap().parse::<usize>() {
                    nodes.push(idx);
                }
            }
        }
    }

    nodes.sort();

    if nodes.is_empty() {
        anyhow::bail!("No nodes found in {}", base_dir.display());
    }

    println!("Starting {} testnet nodes...", nodes.len());
    println!();

    let mut children: Vec<(usize, Child)> = Vec::new();

    for idx in &nodes {
        let node_path = node_dir(&base_dir, *idx);
        let config_path = node_path.join("node.yaml");
        let log_path = node_path.join("node.log");

        if !config_path.exists() {
            println!("[Node {}] Config not found, skipping", idx);
            continue;
        }

        println!("[Node {}] Starting...", idx);

        // Get the path to the tape binary (same as current executable)
        let exe_path = std::env::current_exe()?;

        let log_file = std::fs::File::create(&log_path)?;

        let child = Command::new(&exe_path)
            .args([
                "-u", "l",  // localnet
                "node", "start",
                "--config", config_path.to_str().unwrap(),
            ])
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file))
            .spawn()
            .with_context(|| format!("Failed to start node {}", idx))?;

        println!("  PID: {}", child.id());
        println!("  Log: {}", log_path.display());

        children.push((*idx, child));
    }

    // Write PID file
    let pid_file = base_dir.join("pids.json");
    let pids: Vec<(usize, u32)> = children.iter().map(|(idx, c)| (*idx, c.id())).collect();
    std::fs::write(&pid_file, serde_json::to_string_pretty(&pids)?)?;

    println!();
    println!("Started {} nodes. PIDs saved to {}", children.len(), pid_file.display());
    println!();
    println!("To check status: tape testnet status");
    println!("To stop all:     tape testnet stop");

    Ok(())
}

/// Stop all testnet nodes.
async fn stop_testnet() -> Result<()> {
    // Find and kill all tape node processes
    println!("Stopping testnet nodes...");

    #[cfg(unix)]
    {
        use std::process::Command;

        // Kill all tape node processes
        let output = Command::new("pkill")
            .args(["-f", "tape.*node.*start"])
            .output();

        match output {
            Ok(o) if o.status.success() => {
                println!("Stopped testnet nodes");
            }
            Ok(_) => {
                println!("No testnet nodes running");
            }
            Err(e) => {
                println!("Failed to stop nodes: {}", e);
            }
        }
    }

    #[cfg(not(unix))]
    {
        println!("Manual stop required on this platform");
    }

    Ok(())
}

/// Show testnet status.
async fn show_status(ctx: &Context, base_dir: Option<PathBuf>) -> Result<()> {
    let base_dir = get_base_dir(base_dir);

    if !base_dir.exists() {
        println!("No testnet found at {}", base_dir.display());
        return Ok(());
    }

    // Find all node directories
    let mut nodes: Vec<usize> = Vec::new();
    for entry in std::fs::read_dir(&base_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(name_str) = name.to_str() {
            if name_str.starts_with("node-") {
                if let Ok(idx) = name_str.strip_prefix("node-").unwrap().parse::<usize>() {
                    nodes.push(idx);
                }
            }
        }
    }

    nodes.sort();

    if nodes.is_empty() {
        println!("No nodes configured in {}", base_dir.display());
        return Ok(());
    }

    println!("Testnet Status");
    println!("==============");
    println!("Base directory: {}", base_dir.display());
    println!();

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    println!("{:<6} {:<8} {:<12} {:<12} {}", "Node", "Port", "Process", "Health", "On-Chain");
    println!("{}", "-".repeat(70));

    for idx in &nodes {
        let node_path = node_dir(&base_dir, *idx);
        let config_path = node_path.join("node.yaml");

        // Parse config to get port and authority
        let config_content = std::fs::read_to_string(&config_path).unwrap_or_default();
        let port = extract_port(&config_content).unwrap_or(8080 + *idx as u16);

        // Check if process is running
        let process_status = check_process_running(*idx, &base_dir);

        // Check HTTP health
        let health_status = check_node_health(port).await;

        // Check on-chain status
        let keys_dir = node_path.join("keys");
        let onchain_status = if let Ok(keypair) = generate_or_load_keypair(&keys_dir.join("node.json")) {
            match client.get_node(&keypair.pubkey()).await {
                Ok(_) => "registered".to_string(),
                Err(_) => "not found".to_string(),
            }
        } else {
            "no keypair".to_string()
        };

        println!(
            "{:<6} {:<8} {:<12} {:<12} {}",
            idx, port, process_status, health_status, onchain_status
        );
    }

    Ok(())
}

fn extract_port(config: &str) -> Option<u16> {
    for line in config.lines() {
        if line.trim().starts_with("public_port:") {
            return line.split(':').last()?.trim().parse().ok();
        }
    }
    None
}

fn check_process_running(_idx: usize, _base_dir: &PathBuf) -> String {
    // Simple check - see if any tape node processes are running
    #[cfg(unix)]
    {
        use std::process::Command;
        let output = Command::new("pgrep")
            .args(["-f", "tape.*node.*start"])
            .output();

        match output {
            Ok(o) if o.status.success() => "running".to_string(),
            _ => "stopped".to_string(),
        }
    }

    #[cfg(not(unix))]
    {
        "unknown".to_string()
    }
}

async fn check_node_health(port: u16) -> String {
    let url = format!("http://127.0.0.1:{}/v1/health", port);

    match reqwest::Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => "healthy".to_string(),
        Ok(_) => "unhealthy".to_string(),
        Err(_) => "offline".to_string(),
    }
}

/// Clean up testnet data.
async fn clean_testnet(base_dir: Option<PathBuf>) -> Result<()> {
    let base_dir = get_base_dir(base_dir);

    if !base_dir.exists() {
        println!("No testnet found at {}", base_dir.display());
        return Ok(());
    }

    // Stop any running nodes first
    stop_testnet().await?;

    println!("Removing testnet data at {}...", base_dir.display());
    std::fs::remove_dir_all(&base_dir)?;
    println!("Testnet data cleaned");

    Ok(())
}
