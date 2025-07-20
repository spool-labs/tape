use anyhow::{anyhow, Result};
use log::{debug, error};
use std::collections::HashSet;
use solana_transaction_status_client_types::TransactionDetails;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::time::{sleep, Duration};
use tape_client::{get_slot, get_blocks_with_limit, get_block_by_number, get_archive_account};
use tape_client::utils::{process_block, ProcessedBlock};
use reqwest::Client as HttpClient;
use serde_json::json;
use base64::decode;

use super::store::TapeStore;

/// Archive loop that continuously fetches and processes blocks from the Solana network.
pub async fn archive_loop(
    store: &TapeStore,
    client: &RpcClient,
    starting_slot: Option<u64>,
    trusted_peer: Option<String>,
) -> Result<()> {
    // If a trusted peer is provided, sync with it first
    if let Some(peer_url) = trusted_peer.clone() {
        debug!("Using trusted peer: {}", peer_url);
        debug!("Syncing with trusted peer");
        debug!("This may take a while... please be patient");
        sync_with_trusted_peer(store, client, &peer_url).await?;
    }

    let interval = Duration::from_secs(2);

    let mut latest_slot = match starting_slot {
        Some(slot) => slot,
        None => {
            get_slot(client).await?
        }
    };

    debug!("Initial slot tip: {}", latest_slot);

    // Resume from store or start at current tip
    let mut last_processed_slot = starting_slot
        .or_else(|| store.get_health().map(|(slot, _)| slot).ok())
        .unwrap_or(latest_slot);

    let mut iteration_count = 0;

    loop {
        match try_archive_iteration(
            store,
            client,
            &mut latest_slot,
            &mut last_processed_slot,
            &mut iteration_count,
        ).await {
            Ok(()) => debug!("Block processing iteration completed successfully"),
            Err(e) => error!("Block processing iteration failed: {:?}", e),
        }

        print_drift_status(store, latest_slot, last_processed_slot);
        sleep(interval).await;
    }
}

/// Attempts to archive a batch of blocks.
async fn try_archive_iteration(
    store: &TapeStore,
    client: &RpcClient,
    latest_slot: &mut u64,
    last_processed_slot: &mut u64,
    iteration_count: &mut u64,
) -> Result<()> {
    *iteration_count += 1;

    // Refresh the slot tip every 10 iterations
    if *iteration_count % 10 == 0 {
        if let Ok(slot) = get_slot(client).await {
            *latest_slot = slot;
        }
    }

    // Fetch up to 100 new slots starting just above what we've processed
    let start = *last_processed_slot + 1;
    let slots = get_blocks_with_limit(client, start, 100).await?;

    for slot in slots {
        let block = get_block_by_number(client, slot, TransactionDetails::Full).await?;
        let processed = process_block(block, slot)?;

        if !processed.finalized_tapes.is_empty() || !processed.segment_writes.is_empty() {
            archive_block(store, &processed)?;
        }

        *last_processed_slot = slot;
    }

    Ok(())
}

/// Archives the processed block data into the store.
fn archive_block(store: &TapeStore, block: &ProcessedBlock) -> Result<()> {
    for (address, number) in &block.finalized_tapes {
        store.add_tape(*number, address)?;
    }

    for (key, data) in &block.segment_writes {
        store.add_segment(&key.address, key.segment_number, data.clone())?;
        store.add_slot(&key.address, key.segment_number, block.slot)?;
    }

    Ok(())
}

/// Syncs all tapes up to the current archive count from a trusted peer.
async fn sync_with_trusted_peer(
    store: &TapeStore,
    client: &RpcClient,
    trusted_peer_url: &str,
) -> Result<()> {
    // Fetch archive state to know how many tapes exist
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let http = HttpClient::new();

    for tape_number in 1..=total {
        // Skip if we already have this tape
        if store.get_tape_address(tape_number).is_ok() {
            continue;
        }

        let tape_address = fetch_tape_address(&http, trusted_peer_url, tape_number).await?;
        store.add_tape(tape_number, &tape_address)?;

        let segments = fetch_tape_segments(&http, trusted_peer_url, &tape_address).await?;

        for (seg_num, data) in segments {
            store.add_segment(&tape_address, seg_num, data)?;
        }
    }

    Ok(())
}

pub async fn sync_from_block(
    store: &TapeStore,
    client: &RpcClient,
    tape_address: &Pubkey,
    starting_slot: u64,
) -> Result<()> {

    let mut visited: HashSet<u64> = HashSet::new();
    let mut stack: Vec<u64> = Vec::new();

    stack.push(starting_slot);

    while let Some(current_slot) = stack.pop() {
        if !visited.insert(current_slot) {
            continue; // Skip if already visited
        }

        let block = get_block_by_number(client, current_slot, TransactionDetails::Full).await?;
        let processed = process_block(block, current_slot)?;

        if processed.finalized_tapes.is_empty() && 
           processed.segment_writes.is_empty() {
               continue; // Skip empty blocks
        }

        for (address, number) in &processed.finalized_tapes {
            if address != tape_address {
                continue;
            }

            store.add_tape(*number, address)?;
        }

        let mut parents: HashSet<u64> = HashSet::new();

        for (key, data) in &processed.segment_writes {
            if key.address != *tape_address {
                continue;
            }

            store.add_segment(&key.address, key.segment_number, data.clone())?;
            store.add_slot(&key.address, key.segment_number, processed.slot)?;

            if key.prev_slot != 0 {
                if key.prev_slot > processed.slot {
                    return Err(anyhow!("Parent slot must be earlier than current slot"));
                }

                parents.insert(key.prev_slot);
            }
        }

        for parent in parents {
            stack.push(parent);
        }
    }

    Ok(())
}

/// Fetches the Pubkey address for a given tape number from the trusted peer.
async fn fetch_tape_address(
    http: &HttpClient,
    trusted_peer_url: &str,
    tape_number: u64,
) -> Result<Pubkey> {
    let addr_resp = http.post(trusted_peer_url)
        .header("Content-Type", "application/json")
        .body(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "getTapeAddress",
            "params": { "tape_number": tape_number }
        }).to_string())
        .send().await?
        .json::<serde_json::Value>().await?;

    let addr_str = addr_resp["result"]
        .as_str()
        .ok_or_else(|| anyhow!("Invalid getTapeAddress response: {:?}", addr_resp))?;

    addr_str.parse().map_err(|_| anyhow!("Invalid Pubkey: {}", addr_str))
}

/// Fetches all segments for a tape from the trusted peer.
async fn fetch_tape_segments(
    http: &HttpClient,
    trusted_peer_url: &str,
    tape_address: &Pubkey,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let addr_str = tape_address.to_string();
    let seg_resp = http.post(trusted_peer_url)
        .header("Content-Type", "application/json")
        .body(json!({
            "jsonrpc": "2.0", "id": 4,
            "method": "getTape",
            "params": { "tape_address": addr_str }
        }).to_string())
        .send().await?
        .json::<serde_json::Value>().await?;

    let segments = seg_resp["result"].as_array()
        .ok_or_else(|| anyhow!("Invalid getTape response: {:?}", seg_resp))?;

    let mut result = Vec::new();
    for seg in segments {
        let seg_num = seg["segment_number"]
            .as_u64()
            .ok_or_else(|| anyhow!("Invalid segment_number: {:?}", seg))?;
        let data_b64 = seg["data"]
            .as_str()
            .ok_or_else(|| anyhow!("Invalid data field: {:?}", seg))?;
        let data = decode(data_b64)?;

        result.push((seg_num, data));
    }

    Ok(result)
}

/// Prints the current drift status and updates health in the store.
fn print_drift_status(
    store: &TapeStore,
    latest_slot: u64,
    last_processed_slot: u64,
) {
    let drift = latest_slot.saturating_sub(last_processed_slot);

    // Persist updated health (last_processed_slot + drift)
    if let Err(e) = store.update_health(last_processed_slot, drift) {
        eprintln!("ERROR: failed to write health metadata: {:?}", e);
    }

    let health_status = if drift < 50 {
        "Healthy"
    } else if drift < 200 {
        "Slightly behind"
    } else {
        "Falling behind"
    };

    debug!(
        "Drift {} slots behind tip ({}), status: {}",
        drift, latest_slot, health_status
    );
}
