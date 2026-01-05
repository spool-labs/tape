//! Storage node management commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::Signer;

use tape_api::instruction;
use tape_api::program::tapedrive::{node_pda, BLACKLIST_SIZE};
use tape_api::utils::to_name;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{BasisPoints, NetworkAddress, StorageUnits};

use tape_sdk::{
    load_solana_keypair, load_bls_keypair, load_tls_pubkey,
    parse_hash, create_rpc_client, find_member_index,
};

use crate::config::file::{expand_path, default_node_config_path};
use crate::output::format_basis_points;
use crate::Context;

/// Blacklist proof file format.
#[derive(serde::Deserialize)]
struct BlacklistProof {
    /// The track hash.
    hash: String,
    /// Size of the track in storage units.
    size: u64,
    /// Merkle proof hashes (hex-encoded).
    proof: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum NodeCommand {
    /// Initialize node config file.
    Init {
        /// Config file path (default: ~/.tape/node.yaml).
        #[arg(long)]
        config: Option<std::path::PathBuf>,

        /// Overwrite existing config.
        #[arg(long)]
        force: bool,
    },

    /// Start storage node.
    Start {
        /// Config file path (default: ~/.tape/node.yaml).
        #[arg(long)]
        config: Option<std::path::PathBuf>,
    },

    /// Register new node on-chain.
    Register {
        /// Node display name.
        #[arg(long)]
        name: Option<String>,

        /// Commission rate (0-10000 basis points).
        #[arg(long)]
        commission: Option<u64>,

        /// Network address (host:port).
        #[arg(long)]
        address: Option<String>,

        /// BLS keypair path.
        #[arg(long)]
        bls_key: Option<std::path::PathBuf>,

        /// TLS keypair path.
        #[arg(long)]
        tls_key: Option<std::path::PathBuf>,
    },

    /// Request to join committee.
    Join,

    /// Submit epoch sync.
    Sync,

    /// Show node status.
    Status {
        /// Node pubkey (uses config keypair if not specified).
        #[arg(long)]
        node: Option<String>,
    },

    /// Change node authority.
    SetAuthority {
        /// New authority pubkey.
        new_authority: String,
    },

    /// Update node name.
    SetName {
        /// New name.
        name: String,
    },

    /// Update network address.
    SetAddress {
        /// New address (host:port).
        address: String,
    },

    /// Update commission rate.
    SetCommission {
        /// New commission (0-10000 basis points).
        bps: u64,
    },

    /// Update storage capacity.
    SetCapacity {
        /// Capacity in MB.
        mb: u64,
    },

    /// Update storage price.
    SetPrice {
        /// Price in TAPE per MB.
        tape: String,
    },

    /// Claim accumulated commission.
    ClaimCommission,

    /// Add track to blacklist.
    BlacklistAdd {
        /// Track pubkey.
        track: String,
    },

    /// Remove track from blacklist.
    BlacklistRemove {
        /// Blacklist index.
        index: u64,

        /// Merkle proof path.
        #[arg(long)]
        proof: std::path::PathBuf,
    },

    /// Check node health.
    Health {
        /// Node URLs (uses config if not specified).
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,
    },
}

pub async fn execute(ctx: &Context, cmd: NodeCommand) -> Result<()> {
    match cmd {
        NodeCommand::Init { config, force } => init_node_config(config, force).await,
        NodeCommand::Start { config } => {
            let config_path = config.unwrap_or_else(default_node_config_path);
            println!("Starting node with config: {}", config_path.display());
            anyhow::bail!("Not yet implemented - use the node binary directly")
        }
        NodeCommand::Register {
            name,
            commission,
            address,
            bls_key,
            tls_key,
        } => register_node(ctx, name, commission, address, bls_key, tls_key).await,
        NodeCommand::Join => join_committee(ctx).await,
        NodeCommand::Sync => sync_epoch(ctx).await,
        NodeCommand::Status { node } => show_status(ctx, node).await,
        NodeCommand::SetAuthority { new_authority } => set_authority(ctx, &new_authority).await,
        NodeCommand::SetName { name } => set_name(ctx, &name).await,
        NodeCommand::SetAddress { address } => set_address(ctx, &address).await,
        NodeCommand::SetCommission { bps } => set_commission(ctx, bps).await,
        NodeCommand::SetCapacity { mb } => set_capacity(ctx, mb).await,
        NodeCommand::SetPrice { tape } => set_price(ctx, &tape).await,
        NodeCommand::ClaimCommission => claim_commission(ctx).await,
        NodeCommand::BlacklistAdd { track } => blacklist_add(ctx, &track).await,
        NodeCommand::BlacklistRemove { index, proof } => blacklist_remove(ctx, index, proof).await,
        NodeCommand::Health { nodes } => {
            let nodes = nodes.unwrap_or_else(|| ctx.nodes.clone());
            if nodes.is_empty() {
                anyhow::bail!("No nodes specified. Use --nodes or set in config.");
            }
            health_check(&nodes).await
        }
    }
}

/// Default node config content.
fn default_node_config_content() -> &'static str {
    r#"# Tapedrive Storage Node Configuration

# Display name for this node
name: "my-node"

# Keypairs (auto-generated by `tape node init`)
protocol_keypair: ~/.tape/keys/protocol.json
network_keypair: ~/.tape/keys/network.json
bls_keypair: ~/.tape/keys/bls.json
solana_keypair_path: ~/.config/solana/id.json

# Local address to bind the server
bind_address: "0.0.0.0:8080"

# Public address other nodes use to reach this node
public_host: "localhost"
public_port: 8080

# TLS certificate (self-signed for development, provide paths for production)
tls:
  generate_self_signed: true
  # certificate_path: /path/to/cert.pem
  # key_path: /path/to/key.pem

# Directory for slice data storage
storage_path: ~/.tape/data

# Solana RPC endpoint
solana_rpc_url: "https://api.devnet.solana.com"

# Solana pubkey that owns this node account (run `solana address` to get yours)
node_authority: "YOUR_SOLANA_PUBKEY_HERE"

# Performance tuning (optional)
# poll_interval_ms: 400
# sync_concurrency: 4
# sync_batch_size: 1000
"#
}

/// Initialize node config file and generate keypairs.
async fn init_node_config(config: Option<PathBuf>, force: bool) -> Result<()> {
    use solana_sdk::signature::Keypair;
    use tape_core::bls::BlsPrivateKey;

    let config_path = config.unwrap_or_else(default_node_config_path);

    if config_path.exists() && !force {
        println!("Node config already exists at: {}", config_path.display());
        println!("Use --force to overwrite.");
        return Ok(());
    }

    // Create directories
    let tape_dir = dirs::home_dir()
        .map(|h| h.join(".tape"))
        .unwrap_or_else(|| PathBuf::from(".tape"));
    let keys_dir = tape_dir.join("keys");
    std::fs::create_dir_all(&keys_dir)?;

    // Generate keypairs
    println!("Generating keypairs...");

    // Protocol keypair (Ed25519)
    let protocol_path = keys_dir.join("protocol.json");
    if !protocol_path.exists() || force {
        let keypair = Keypair::new();
        let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
        std::fs::write(&protocol_path, &json)?;
        println!("  Created protocol keypair: {}", protocol_path.display());
    } else {
        println!("  Protocol keypair exists: {}", protocol_path.display());
    }

    // Network keypair (Ed25519 for TLS)
    let network_path = keys_dir.join("network.json");
    if !network_path.exists() || force {
        let keypair = Keypair::new();
        let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
        std::fs::write(&network_path, &json)?;
        println!("  Created network keypair: {}", network_path.display());
    } else {
        println!("  Network keypair exists: {}", network_path.display());
    }

    // BLS keypair (32 bytes)
    let bls_path = keys_dir.join("bls.json");
    if !bls_path.exists() || force {
        let bls_key = BlsPrivateKey::from_random();
        let bytes: &[u8] = bytemuck::bytes_of(&bls_key);
        let json = serde_json::to_string(&bytes.to_vec())?;
        std::fs::write(&bls_path, &json)?;
        println!("  Created BLS keypair: {}", bls_path.display());
    } else {
        println!("  BLS keypair exists: {}", bls_path.display());
    }

    // Write config file
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&config_path, default_node_config_content())?;

    println!();
    println!("Created node config at: {}", config_path.display());
    println!();
    println!("Next steps:");
    println!("  1. Edit {} and fill in:", config_path.display());
    println!("     - node_authority (your Solana pubkey)");
    println!("     - public_host (your node's public hostname)");
    println!("     - solana_rpc_url (RPC endpoint for your cluster)");
    println!("  2. Register your node on-chain:");
    println!("     tape node register");
    println!("  3. Start the node:");
    println!("     tape node start");
    println!();
    println!("To use custom keypairs, replace the files in {}", keys_dir.display());

    Ok(())
}

/// Get the keypair from context, returning an error if not configured.
fn get_keypair(ctx: &Context) -> Result<solana_sdk::signature::Keypair> {
    let keypair_path = ctx
        .keypair
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No keypair configured. Use -k or set in config."))?;

    load_solana_keypair(keypair_path).map_err(|e| anyhow::anyhow!("{}", e))
}

async fn register_node(
    ctx: &Context,
    name: Option<String>,
    commission: Option<u64>,
    address: Option<String>,
    bls_key: Option<PathBuf>,
    tls_key: Option<PathBuf>,
) -> Result<()> {
    // Fallback to config values
    let name = name.or_else(|| ctx.config.node.name.clone());
    let commission = commission.or(ctx.config.node.commission);
    let address = address.or_else(|| ctx.config.node.address.clone());
    let bls_key = bls_key.or_else(|| ctx.config.node.bls_key.as_ref().map(|s| expand_path(s)));
    let tls_key = tls_key.or_else(|| ctx.config.node.tls_key.as_ref().map(|s| expand_path(s)));

    // Validate required fields
    let name = name.ok_or_else(|| anyhow::anyhow!("Node name required. Use --name or set in config."))?;
    let commission = commission.ok_or_else(|| {
        anyhow::anyhow!("Commission rate required. Use --commission or set in config.")
    })?;
    let address = address.ok_or_else(|| {
        anyhow::anyhow!("Network address required. Use --address or set in config.")
    })?;
    let bls_key_path = bls_key.ok_or_else(|| {
        anyhow::anyhow!("BLS keypair path required. Use --bls-key or set in config.")
    })?;
    let tls_key_path = tls_key.ok_or_else(|| {
        anyhow::anyhow!("TLS keypair path required. Use --tls-key or set in config.")
    })?;

    // Validate commission rate
    if commission > 10000 {
        anyhow::bail!("Commission rate must be 0-10000 basis points (0-100%)");
    }

    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));
    ctx.debug(&format!("Registering node: {}", name));

    if ctx.dry_run {
        println!("Dry run - would register node:");
        println!("  Name: {}", name);
        println!("  Commission: {} ({})", commission, format_basis_points(commission));
        println!("  Address: {}", address);
        println!("  BLS Key: {}", bls_key_path.display());
        println!("  TLS Key: {}", tls_key_path.display());
        return Ok(());
    }

    // Load keys
    let keypair = get_keypair(ctx)?;
    let bls_private_key = load_bls_keypair(&bls_key_path).map_err(|e| anyhow::anyhow!("{}", e))?;
    let tls_pubkey = load_tls_pubkey(&tls_key_path).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Derive BLS public key and proof of possession
    let bls_pubkey = bls_private_key
        .public_key()
        .map_err(|e| anyhow::anyhow!("Failed to derive BLS public key: {:?}", e))?;
    let bls_pop = bls_private_key
        .proof_of_possession()
        .map_err(|e| anyhow::anyhow!("Failed to generate BLS proof of possession: {:?}", e))?;

    // Parse network address
    let network_address = NetworkAddress::from(&address)
        .map_err(|_| anyhow::anyhow!("Invalid network address format: {}", address))?;

    // Build instruction
    let name_bytes = to_name(&name);
    let ix = instruction::build_register_node_ix(
        keypair.pubkey(),
        name_bytes,
        BasisPoints(commission),
        network_address,
        tls_pubkey,
        bls_pubkey,
        bls_pop,
    );

    // Send transaction
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print("Registering node on-chain...");

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    let (node_address, _) = node_pda(keypair.pubkey());

    println!("Node registered successfully!");
    println!("  Transaction: {}", signature);
    println!("  Node Account: {}", node_address);
    println!("  Authority: {}", keypair.pubkey());

    Ok(())
}

async fn join_committee(ctx: &Context) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if ctx.dry_run {
        println!("Dry run - would request to join committee");
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_join_network_ix(keypair.pubkey(), node_address);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print("Requesting to join committee...");

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Join request submitted!");
    println!("  Transaction: {}", signature);
    println!("  Node: {}", node_address);

    Ok(())
}

async fn sync_epoch(ctx: &Context) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if ctx.dry_run {
        println!("Dry run - would submit epoch sync");
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Get current epoch and system state
    let epoch = client
        .get_epoch()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch epoch: {}", e))?;

    let system = client
        .get_system()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch system: {}", e))?;

    let (node_address, _) = node_pda(keypair.pubkey());

    // Get node to find our member index
    let node_account = client
        .get_node(&keypair.pubkey())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch node account: {}", e))?;

    // Find our member index in the committee using SDK helper
    let member_index = find_member_index(&system.committee, node_account.id)
        .ok_or_else(|| anyhow::anyhow!("Node is not in the current committee"))?;

    // Get our assigned spools
    let spools = system.spools.spools_for_member(member_index);

    let ix = instruction::build_epoch_sync_ix(keypair.pubkey(), node_address, epoch.id, &spools);

    ctx.print(&format!("Submitting epoch sync for epoch {}...", epoch.id));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Epoch sync submitted!");
    println!("  Transaction: {}", signature);
    println!("  Epoch: {}", epoch.id);
    println!("  Spools synced: {}", spools.len());

    Ok(())
}

async fn show_status(ctx: &Context, node: Option<String>) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Determine which node to query
    let authority = if let Some(node_str) = node {
        node_str
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid node pubkey: {}", node_str))?
    } else {
        let keypair = get_keypair(ctx)?;
        keypair.pubkey()
    };

    let (node_address, _) = node_pda(authority);

    ctx.print(&format!("Fetching node status for {}...", authority));

    let node_account = client
        .get_node(&authority)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch node account: {}", e))?;

    // Format network address
    let network_addr = node_account
        .metadata
        .network_address
        .to_socket_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<invalid>".to_string());

    // Get node name
    let name = String::from_utf8_lossy(&node_account.metadata.name)
        .trim_end_matches('\0')
        .to_string();

    println!("Node Status");
    println!("============");
    println!("Account:        {}", node_address);
    println!("Authority:      {}", node_account.authority);
    println!("ID:             {}", node_account.id);
    println!("Name:           {}", name);
    println!("Network:        {}", network_addr);
    println!("BLS Pubkey:     {}", node_account.metadata.bls_pubkey);
    println!();
    println!("Preferences:");
    println!("  Capacity:     {} MB", node_account.preferences.storage_capacity);
    println!("  Price:        {}", node_account.preferences.storage_price);
    println!();
    println!("Staking Pool:");
    println!("  Stake:        {}", node_account.pool.stake);
    println!("  Rewards:      {}", node_account.pool.rewards);
    println!("  Commission:   {} (rate: {})", node_account.pool.commission, format_basis_points(node_account.pool.commission_rate.as_u64()));
    println!();
    println!("Epochs:");
    println!("  Registered:   {}", node_account.registered_epoch);
    println!("  Latest:       {}", node_account.latest_epoch);

    Ok(())
}

async fn set_authority(ctx: &Context, new_authority: &str) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let new_auth_pubkey: solana_sdk::pubkey::Pubkey = new_authority
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid new authority pubkey: {}", new_authority))?;

    if ctx.dry_run {
        println!("Dry run - would set authority to: {}", new_auth_pubkey);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_authority_ix(keypair.pubkey(), node_address, new_auth_pubkey);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Setting node authority to {}...", new_auth_pubkey));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Authority updated!");
    println!("  Transaction: {}", signature);
    println!("  New Authority: {}", new_auth_pubkey);

    Ok(())
}

async fn set_name(ctx: &Context, name: &str) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if ctx.dry_run {
        println!("Dry run - would set name to: {}", name);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_name_ix(keypair.pubkey(), node_address, name);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Setting node name to '{}'...", name));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Name updated!");
    println!("  Transaction: {}", signature);
    println!("  New Name: {}", name);

    Ok(())
}

async fn set_address(ctx: &Context, address: &str) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let network_address = NetworkAddress::from(address)
        .map_err(|_| anyhow::anyhow!("Invalid network address format: {}", address))?;

    if ctx.dry_run {
        println!("Dry run - would set address to: {}", address);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_network_address_ix(keypair.pubkey(), node_address, network_address);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Setting network address to {}...", address));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Address updated!");
    println!("  Transaction: {}", signature);
    println!("  New Address: {}", address);

    Ok(())
}

async fn set_commission(ctx: &Context, bps: u64) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if bps > 10000 {
        anyhow::bail!("Commission rate must be 0-10000 basis points (0-100%)");
    }

    if ctx.dry_run {
        println!(
            "Dry run - would set commission to: {} ({})",
            bps,
            format_basis_points(bps)
        );
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_commission_ix(keypair.pubkey(), node_address, BasisPoints(bps));

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!(
        "Setting commission to {} ({})...",
        bps,
        format_basis_points(bps)
    ));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Commission updated!");
    println!("  Transaction: {}", signature);
    println!("  New Commission: {} ({})", bps, format_basis_points(bps));

    Ok(())
}

async fn set_capacity(ctx: &Context, mb: u64) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if ctx.dry_run {
        println!("Dry run - would set capacity to: {} MB", mb);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_storage_capacity_ix(keypair.pubkey(), node_address, StorageUnits(mb));

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Setting storage capacity to {} MB...", mb));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Capacity updated!");
    println!("  Transaction: {}", signature);
    println!("  New Capacity: {} MB", mb);

    Ok(())
}

async fn set_price(ctx: &Context, tape: &str) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let price: Coin<TAPE> = TAPE::parse(tape)
        .map_err(|_| anyhow::anyhow!("Invalid TAPE amount: {}", tape))?;

    if ctx.dry_run {
        println!("Dry run - would set price to: {} per MB", price);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_set_storage_price_ix(keypair.pubkey(), node_address, price);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Setting storage price to {} per MB...", price));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Price updated!");
    println!("  Transaction: {}", signature);
    println!("  New Price: {} per MB", price);

    Ok(())
}

async fn claim_commission(ctx: &Context) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    if ctx.dry_run {
        println!("Dry run - would claim accumulated commission");
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_claim_commission_ix(keypair.pubkey(), node_address);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print("Claiming accumulated commission...");

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Commission claimed!");
    println!("  Transaction: {}", signature);

    Ok(())
}

async fn blacklist_add(ctx: &Context, track: &str) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let track_pubkey: solana_sdk::pubkey::Pubkey = track
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid track pubkey: {}", track))?;

    if ctx.dry_run {
        println!("Dry run - would add {} to blacklist", track_pubkey);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_add_to_blacklist_ix(keypair.pubkey(), node_address, track_pubkey);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Adding {} to blacklist...", track_pubkey));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Track added to blacklist!");
    println!("  Transaction: {}", signature);
    println!("  Track: {}", track_pubkey);

    Ok(())
}

async fn blacklist_remove(ctx: &Context, index: u64, proof_path: PathBuf) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    // Load and parse proof file
    let proof_json = std::fs::read_to_string(&proof_path)
        .with_context(|| format!("Failed to read proof file: {}", proof_path.display()))?;

    let proof_data: BlacklistProof = serde_json::from_str(&proof_json)
        .with_context(|| format!("Failed to parse proof file: {}", proof_path.display()))?;

    // Parse hash using SDK helper
    let hash = parse_hash(&proof_data.hash, "hash").map_err(|e| anyhow::anyhow!("{}", e))?;

    // Parse proof hashes
    if proof_data.proof.len() != BLACKLIST_SIZE {
        anyhow::bail!(
            "Invalid proof size: expected {} hashes, got {}",
            BLACKLIST_SIZE,
            proof_data.proof.len()
        );
    }

    let mut proof: [tape_crypto::Hash; BLACKLIST_SIZE] = [tape_crypto::Hash::default(); BLACKLIST_SIZE];
    for (i, hash_hex) in proof_data.proof.iter().enumerate() {
        proof[i] = parse_hash(hash_hex, &format!("proof[{}]", i))
            .map_err(|e| anyhow::anyhow!("{}", e))?;
    }

    if ctx.dry_run {
        println!("Dry run - would remove blacklist entry at index {}", index);
        println!("  Hash: {}", proof_data.hash);
        println!("  Size: {} MB", proof_data.size);
        return Ok(());
    }

    let keypair = get_keypair(ctx)?;
    let (node_address, _) = node_pda(keypair.pubkey());

    let ix = instruction::build_remove_from_blacklist_ix(
        keypair.pubkey(),
        node_address,
        index,
        hash,
        StorageUnits(proof_data.size),
        proof,
    );

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    ctx.print(&format!("Removing blacklist entry at index {}...", index));

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

    println!("Blacklist entry removed!");
    println!("  Transaction: {}", signature);
    println!("  Index: {}", index);

    Ok(())
}

async fn health_check(nodes: &[String]) -> Result<()> {
    use tape_sdk::TapeClient;

    let client = TapeClient::builder()
        .node_addresses(nodes.to_vec())
        .build();

    println!("{:<45} {:>10} {:>10}", "Node", "Status", "Latency");
    println!("{}", "-".repeat(67));

    for node in nodes {
        let start = std::time::Instant::now();
        match client.health_check(node).await {
            Ok(true) => {
                let latency = start.elapsed();
                println!(
                    "{:<45} {:>10} {:>7}ms",
                    node,
                    "OK",
                    latency.as_millis()
                );
            }
            Ok(false) => {
                println!("{:<45} {:>10} {:>10}", node, "UNHEALTHY", "-");
            }
            Err(e) => {
                println!("{:<45} {:>10} {:>10}", node, "ERROR", "-");
                eprintln!("  {}", e);
            }
        }
    }

    Ok(())
}
