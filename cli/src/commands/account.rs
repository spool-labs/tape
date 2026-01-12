//! On-chain account query commands.

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

use rpc_solana::{Rpc, RpcConfig, RpcError, SolanaRpc};
use tape_api::program::{
    exchange::exchange_pda,
    tapedrive::{
        node_pda, stake_pda, tape_pda, track_pda,
        ARCHIVE_ADDRESS, EPOCH_ADDRESS, SYSTEM_ADDRESS,
    },
};
use tape_api::state::{
    AccountType, Archive, Epoch, Exchange, History, Node, Stake, System, Tape, Track,
};
use tape_crypto::hash::Hash;
use tape_core::system::EpochPhase;
use tape_core::staking::StakePhase;
use tape_core::tape::TrackPhase;
use tape_core::system::Committee;

use crate::output::{format_basis_points, format_bytes, format_number, print_hex_dump};
use crate::Context;

#[derive(Subcommand, Debug)]
pub enum AccountCommand {
    /// Auto-detect and display any account.
    Show {
        /// Account pubkey.
        pubkey: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show system state (singleton).
    System {
        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show current epoch (singleton).
    Epoch {
        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show archive state (singleton).
    Archive {
        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show node account.
    Node {
        /// Node authority pubkey.
        authority: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show tape account.
    Tape {
        /// Tape authority pubkey.
        authority: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show track account.
    Track {
        /// Track authority pubkey.
        authority: String,

        /// Track key hash.
        hash: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show stake account.
    Stake {
        /// Staker pubkey.
        staker: String,

        /// Node pubkey.
        node: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show exchange account.
    Exchange {
        /// Exchange authority pubkey.
        authority: String,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },

    /// Show current committee.
    Committee {
        /// Epoch number (current if not specified).
        #[arg(long)]
        epoch: Option<u64>,

        /// Display raw hex dump.
        #[arg(long, alias = "hex")]
        raw: bool,
    },
}

pub async fn execute(ctx: &Context, cmd: AccountCommand) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match cmd {
        AccountCommand::Show { pubkey, raw } => show_account(ctx, &pubkey, raw).await,
        AccountCommand::System { raw } => show_system(ctx, raw).await,
        AccountCommand::Epoch { raw } => show_epoch(ctx, raw).await,
        AccountCommand::Archive { raw } => show_archive(ctx, raw).await,
        AccountCommand::Node { authority, raw } => show_node(ctx, &authority, raw).await,
        AccountCommand::Tape { authority, raw } => show_tape(ctx, &authority, raw).await,
        AccountCommand::Track { authority, hash, raw } => show_track(ctx, &authority, &hash, raw).await,
        AccountCommand::Stake { staker, node, raw } => show_stake(ctx, &staker, &node, raw).await,
        AccountCommand::Exchange { authority, raw } => show_exchange(ctx, &authority, raw).await,
        AccountCommand::Committee { epoch, raw } => show_committee(ctx, epoch, raw).await,
    }
}

/// Create an RPC client from context
fn create_rpc(ctx: &Context) -> Result<SolanaRpc> {
    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    SolanaRpc::new(config).map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))
}

/// Parse a pubkey string
fn parse_pubkey(s: &str) -> Result<Pubkey> {
    Pubkey::from_str(s).with_context(|| format!("Invalid pubkey: {}", s))
}

/// Parse a hash from hex string (with or without 0x prefix)
fn parse_hash(s: &str) -> Result<Hash> {
    let hex_str = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(hex_str).with_context(|| format!("Invalid hex: {}", s))?;
    if bytes.len() != 32 {
        anyhow::bail!("Hash must be 32 bytes, got {}", bytes.len());
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes);
    Ok(Hash::from(hash))
}

/// Print raw account data
fn print_raw_account(address: &Pubkey, owner: &Pubkey, data: &[u8]) {
    println!("Account:  {}", address);
    println!("Owner:    {}", owner);
    println!("Size:     {} bytes", data.len());
    println!();
    print_hex_dump(data);
}

/// Format epoch phase as string
fn format_epoch_phase(phase: u64) -> &'static str {
    match EpochPhase::try_from(phase) {
        Ok(EpochPhase::Unknown) => "Unknown",
        Ok(EpochPhase::Syncing) => "Syncing",
        Ok(EpochPhase::Active) => "Active",
        Ok(EpochPhase::NextEpochReady) => "NextEpochReady",
        Err(_) => "Invalid",
    }
}

/// Format stake phase as string
fn format_stake_phase(phase: u64) -> &'static str {
    match StakePhase::try_from(phase) {
        Ok(StakePhase::Active) => "Active",
        Ok(StakePhase::Unlocking) => "Unlocking",
        Ok(StakePhase::Withdrawn) => "Withdrawn",
        Err(_) => "Invalid",
    }
}

/// Format track phase as string
fn format_track_phase(phase: u64) -> &'static str {
    match TrackPhase::try_from(phase) {
        Ok(TrackPhase::Registered) => "Registered",
        Ok(TrackPhase::Certified) => "Certified",
        Ok(TrackPhase::Invalidated) => "Invalidated",
        Err(_) => "Invalid",
    }
}

/// Format node name (strip trailing nulls)
fn format_node_name(name: &[u8; 32]) -> String {
    let end = name.iter().position(|&b| b == 0).unwrap_or(32);
    String::from_utf8_lossy(&name[..end]).to_string()
}

/// Format TAPE amount with decimals (6 decimals)
fn format_tape(amount: u64) -> String {
    let whole = amount / 1_000_000;
    let frac = amount % 1_000_000;
    if frac == 0 {
        format!("{} TAPE", format_number(whole))
    } else {
        format!("{}.{:06} TAPE", format_number(whole), frac)
    }
}

/// Format SOL amount with decimals (9 decimals)
fn format_sol(amount: u64) -> String {
    let whole = amount / 1_000_000_000;
    let frac = amount % 1_000_000_000;
    if frac == 0 {
        format!("{} SOL", format_number(whole))
    } else {
        format!("{}.{:09} SOL", format_number(whole), frac)
    }
}

async fn show_account(ctx: &Context, pubkey: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let address = parse_pubkey(pubkey)?;

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => anyhow::anyhow!("Account not found: {}", address),
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    // Auto-detect account type from discriminator
    if account.data.is_empty() {
        println!("Account:  {}", address);
        println!("Owner:    {}", account.owner);
        println!("(empty account)");
        return Ok(());
    }

    let discriminator = account.data[0];
    let account_type = AccountType::try_from(discriminator).unwrap_or(AccountType::Unknown);

    println!("Account:  {}", address);
    println!("Owner:    {}", account.owner);
    println!("Type:     {:?}", account_type);
    println!();

    match account_type {
        AccountType::System => {
            if let Ok(system) = System::unpack_with_discriminator(&account.data) {
                print_system_account(system);
            } else {
                println!("(failed to deserialize System account)");
            }
        }
        AccountType::Epoch => {
            if let Ok(epoch) = Epoch::unpack_with_discriminator(&account.data) {
                print_epoch_account(epoch);
            } else {
                println!("(failed to deserialize Epoch account)");
            }
        }
        AccountType::Archive => {
            if let Ok(archive) = Archive::unpack_with_discriminator(&account.data) {
                print_archive_account(archive);
            } else {
                println!("(failed to deserialize Archive account)");
            }
        }
        AccountType::Node => {
            if let Ok(node) = Node::unpack_with_discriminator(&account.data) {
                print_node_account(node);
            } else {
                println!("(failed to deserialize Node account)");
            }
        }
        AccountType::Tape => {
            if let Ok(tape) = Tape::unpack_with_discriminator(&account.data) {
                print_tape_account(tape);
            } else {
                println!("(failed to deserialize Tape account)");
            }
        }
        AccountType::Track => {
            if let Ok(track) = Track::unpack_with_discriminator(&account.data) {
                print_track_account(track);
            } else {
                println!("(failed to deserialize Track account)");
            }
        }
        AccountType::Stake => {
            if let Ok(stake) = Stake::unpack_with_discriminator(&account.data) {
                print_stake_account(stake);
            } else {
                println!("(failed to deserialize Stake account)");
            }
        }
        AccountType::Exchange => {
            if let Ok(exchange) = Exchange::unpack_with_discriminator(&account.data) {
                print_exchange_account(exchange);
            } else {
                println!("(failed to deserialize Exchange account)");
            }
        }
        AccountType::History => {
            if let Ok(history) = History::unpack_with_discriminator(&account.data) {
                print_history_account(history);
            } else {
                println!("(failed to deserialize History account)");
            }
        }
        _ => {
            println!("(unknown account type, use --raw to view hex dump)");
        }
    }

    Ok(())
}

async fn show_system(ctx: &Context, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let address = SYSTEM_ADDRESS;

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => anyhow::anyhow!("System account not found"),
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (System singleton)", address);
    println!();

    let system = System::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_system_account(system);
    Ok(())
}

fn print_system_account(system: &System) {
    println!("Version:           {}", system.version);
    println!("Total Nodes:       {}", format_number(system.total_nodes));
    println!();

    println!("Previous Committee ({} members):", system.committee_prev.size());
    print_committee_summary(&system.committee_prev);

    println!("Current Committee ({} members):", system.committee.size());
    print_committee_summary(&system.committee);

    println!("Next Committee ({} members):", system.committee_next.size());
    print_committee_summary(&system.committee_next);
}

fn print_committee_summary<const N: usize>(committee: &Committee<N>) {
    if committee.size() == 0 {
        println!("  (empty)");
        return;
    }

    println!("  Total Stake:     {}", format_tape(committee.total_stake().as_u64()));
    println!("  Threshold Stake: {}", format_tape(committee.threshold_stake().as_u64()));

    // Show first few members
    let max_display = 5;
    for (i, member) in committee.iter().take(max_display).enumerate() {
        println!(
            "  [{}] NodeId: {}, Stake: {}, Weight: {}",
            i,
            member.id,
            format_tape(member.stake.as_u64()),
            member.weight
        );
    }
    if committee.size() > max_display {
        println!("  ... and {} more", committee.size() - max_display);
    }
    println!();
}

async fn show_epoch(ctx: &Context, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let address = EPOCH_ADDRESS;

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => anyhow::anyhow!("Epoch account not found"),
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (Epoch singleton)", address);
    println!();

    let epoch = Epoch::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_epoch_account(epoch);
    Ok(())
}

fn print_epoch_account(epoch: &Epoch) {
    println!("Epoch Number:  {}", epoch.id);
    println!("Phase:         {}", format_epoch_phase(epoch.state.phase));
    if epoch.state.is_syncing() {
        println!("Sync Weight:   {}", epoch.state.weight);
    }
    println!("Last Epoch:    {} (unix timestamp)", epoch.last_epoch);

    // Format as date if available
    if epoch.last_epoch > 0 {
        use std::time::{Duration, UNIX_EPOCH};
        if let Some(datetime) = UNIX_EPOCH.checked_add(Duration::from_secs(epoch.last_epoch as u64)) {
            let duration_since = std::time::SystemTime::now()
                .duration_since(datetime)
                .unwrap_or_default();
            let days = duration_since.as_secs() / 86400;
            let hours = (duration_since.as_secs() % 86400) / 3600;
            println!("               ({} days, {} hours ago)", days, hours);
        }
    }
}

async fn show_archive(ctx: &Context, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let address = ARCHIVE_ADDRESS;

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => anyhow::anyhow!("Archive account not found"),
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (Archive singleton)", address);
    println!();

    let archive = Archive::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_archive_account(archive);
    Ok(())
}

fn print_archive_account(archive: &Archive) {
    println!("Storage Capacity:  {} MB", format_number(archive.storage_capacity.as_u64()));
    println!("Storage Price:     {} per MB", format_tape(archive.storage_price.as_u64()));
    println!("Recent Usage:      {} MB", format_number(archive.recent_usage.as_u64()));
    println!("Rewards Pool:      {}", format_tape(archive.rewards_pool.as_u64()));
    println!("Rewards Paid:      {}", format_tape(archive.rewards_paid.as_u64()));
    println!("Tape Count:        {}", format_number(archive.tape_count));
}

async fn show_node(ctx: &Context, authority: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let authority_pubkey = parse_pubkey(authority)?;
    let (address, _bump) = node_pda(authority_pubkey);

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => {
                anyhow::anyhow!("Node account not found for authority: {}", authority)
            }
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (derived from authority {})", address, authority_pubkey);
    println!();

    let node = Node::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_node_account(node);
    Ok(())
}

fn print_node_account(node: &Node) {
    println!("Node ID:           {}", node.id);
    println!("Authority:         {}", node.authority);
    println!("Name:              {}", format_node_name(&node.metadata.name));

    // Network address
    if let Ok(addr) = node.metadata.network_address.to_socket_addr() {
        println!("Network Address:   {}", addr);
    } else {
        println!("Network Address:   (invalid)");
    }

    println!("Network TLS:       {}", node.metadata.network_tls);
    println!("BLS Pubkey:        {}", node.metadata.bls_pubkey);

    println!();
    println!("Preferences:");
    println!("  Storage Capacity:  {} MB", format_number(node.preferences.storage_capacity.as_u64()));
    println!("  Storage Price:     {} per MB", format_tape(node.preferences.storage_price.as_u64()));

    println!();
    println!("Registration:");
    println!("  Registered Epoch:  {}", node.registered_epoch);
    println!("  Latest Sync:       {}", node.latest_sync_epoch);
    println!("  Latest Advance:    {}", node.latest_advance_epoch);

    println!();
    println!("Staking Pool:");
    println!("  Shares:          {}", format_number(node.pool.shares.as_u64()));
    println!("  Stake:           {}", format_tape(node.pool.stake.as_u64()));
    println!("  Rewards:         {}", format_tape(node.pool.rewards.as_u64()));
    println!("  Commission:      {}", format_tape(node.pool.commission.as_u64()));
    println!("  Commission Rate: {}", format_basis_points(node.pool.commission_rate.as_u64()));
}

async fn show_tape(ctx: &Context, authority: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let authority_pubkey = parse_pubkey(authority)?;
    let (address, _bump) = tape_pda(authority_pubkey);

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => {
                anyhow::anyhow!("Tape account not found for authority: {}", authority)
            }
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (derived from authority {})", address, authority_pubkey);
    println!();

    let tape = Tape::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_tape_account(tape);
    Ok(())
}

fn print_tape_account(tape: &Tape) {
    println!("Tape ID:       {}", tape.id);
    println!("Authority:     {}", tape.authority);
    println!("Capacity:      {} MB", format_number(tape.capacity.as_u64()));
    println!("Used:          {} MB", format_number(tape.used.as_u64()));
    println!("Available:     {} MB", format_number(tape.capacity.as_u64().saturating_sub(tape.used.as_u64())));
    println!("Active Epoch:  {}", tape.active_epoch);
    println!("Expiry Epoch:  {}", tape.expiry_epoch);
    println!("Track Count:   {}", format_number(tape.track_count));
}

async fn show_track(ctx: &Context, authority: &str, hash: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let authority_pubkey = parse_pubkey(authority)?;
    let track_hash = parse_hash(hash)?;
    let (address, _bump) = track_pda(authority_pubkey, track_hash);

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => {
                anyhow::anyhow!("Track account not found for authority: {}, hash: {}", authority, hash)
            }
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (derived from authority {} + hash)", address, authority_pubkey);
    println!();

    let track = Track::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_track_account(track);
    Ok(())
}

fn print_track_account(track: &Track) {
    println!("Track ID:          {}", track.id);
    println!("Tape:              {}", track.tape);
    println!("Key Hash:          0x{}", hex::encode(track.key.as_ref()));
    println!("Size:              {}", format_bytes(track.size.as_u64() * 1024 * 1024)); // StorageUnits is MB

    println!();
    println!("State:");
    println!("  Phase:           {}", format_track_phase(track.data.state.phase));
    println!("  Registered:      epoch {}", track.data.registered_epoch);
    if track.data.state.is_certified() {
        println!("  Certified:       epoch {}", track.data.state.certified_epoch);
    }

    println!();
    println!("Commitment Hash:   0x{}", hex::encode(track.data.commitment_hash.as_ref()));
}

async fn show_stake(ctx: &Context, staker: &str, node: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let staker_pubkey = parse_pubkey(staker)?;
    let node_pubkey = parse_pubkey(node)?;
    let (address, _bump) = stake_pda(staker_pubkey);

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => {
                anyhow::anyhow!("Stake account not found for staker: {}, node: {}", staker, node)
            }
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (derived from staker {} + node {})", address, staker_pubkey, node_pubkey);
    println!();

    let stake = Stake::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_stake_account(stake);
    Ok(())
}

fn print_stake_account(stake: &Stake) {
    println!("Authority:         {}", stake.authority);
    println!("Pool:              {}", stake.pool);

    println!();
    println!("Stake Details:");
    println!("  Amount:          {}", format_tape(stake.inner.amount.as_u64()));
    println!("  Activation Epoch: {}", stake.inner.activation_epoch);
    println!("  Phase:           {}", format_stake_phase(stake.inner.state.phase));

    if stake.inner.state.is_withdrawing() {
        println!("  Unstake Epoch:   {}", stake.inner.state.unstake_epoch);
    }
}

async fn show_exchange(ctx: &Context, authority: &str, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let authority_pubkey = parse_pubkey(authority)?;
    let (address, _bump) = exchange_pda(authority_pubkey);

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => {
                anyhow::anyhow!("Exchange account not found for authority: {}", authority)
            }
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    println!("Account:  {} (derived from authority {})", address, authority_pubkey);
    println!();

    let exchange = Exchange::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    print_exchange_account(exchange);
    Ok(())
}

fn print_exchange_account(exchange: &Exchange) {
    println!("Authority:     {}", exchange.authority);
    println!();
    println!("Balances:");
    println!("  TAPE:        {}", format_tape(exchange.balance_tape.as_u64()));
    println!("  SOL:         {}", format_sol(exchange.balance_sol.as_u64()));
    println!();
    println!("Exchange Rate:");
    println!("  {} TAPE = {} (other units)", exchange.rate.tape, exchange.rate.other);
}

fn print_history_account(history: &History) {
    println!("Node:              {}", history.node);
    println!("Registered Epoch:  {}", history.registered_epoch);
    println!("Latest Epoch:      {}", history.latest_epoch);
    println!();
    println!("Pool History:      (detailed history available in pool)");
}

async fn show_committee(ctx: &Context, epoch: Option<u64>, raw: bool) -> Result<()> {
    let rpc = create_rpc(ctx)?;
    let address = SYSTEM_ADDRESS;

    let account = rpc
        .get_account(&address)
        .await
        .map_err(|e| match e {
            RpcError::AccountNotFound(_) => anyhow::anyhow!("System account not found"),
            _ => anyhow::anyhow!("RPC error: {}", e),
        })?;

    if raw {
        print_raw_account(&address, &account.owner, &account.data);
        return Ok(());
    }

    let system = System::unpack_with_discriminator(&account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    // Get current epoch to determine which committee to show
    let epoch_account = rpc
        .get_account(&EPOCH_ADDRESS)
        .await
        .map_err(|e| anyhow::anyhow!("RPC error: {}", e))?;

    let current_epoch = Epoch::unpack_with_discriminator(&epoch_account.data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

    let current_epoch_num = current_epoch.id.as_u64();

    let (committee, label) = match epoch {
        Some(e) if e == current_epoch_num => (&system.committee, "current"),
        Some(e) if e == current_epoch_num.saturating_sub(1) => (&system.committee_prev, "previous"),
        Some(e) if e == current_epoch_num + 1 => (&system.committee_next, "next"),
        Some(e) => {
            anyhow::bail!(
                "Epoch {} not available. Current epoch is {}. Available: {}, {}, {}",
                e,
                current_epoch_num,
                current_epoch_num.saturating_sub(1),
                current_epoch_num,
                current_epoch_num + 1
            );
        }
        None => (&system.committee, "current"),
    };

    println!("Committee for epoch {} ({}):", epoch.unwrap_or(current_epoch_num), label);
    println!("Account: {}", address);
    println!();

    println!("Members:       {}", committee.size());
    println!("Total Stake:   {}", format_tape(committee.total_stake().as_u64()));
    println!("Min Stake:     {}", format_tape(committee.threshold_stake().as_u64()));
    println!();

    if committee.size() == 0 {
        println!("(no members)");
        return Ok(());
    }

    // Print all members in a table format
    println!(
        "{:<6} {:>10} {:>18} {:>8}",
        "Index", "NodeId", "Stake", "Weight"
    );
    println!("{}", "-".repeat(50));

    for (i, member) in committee.iter().enumerate() {
        println!(
            "{:<6} {:>10} {:>18} {:>8}",
            i,
            member.id,
            format_tape(member.stake.as_u64()),
            member.weight
        );
    }

    Ok(())
}
