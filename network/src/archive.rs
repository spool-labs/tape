use anyhow::{anyhow, Result};
use log::{debug, error};
use tokio::task::JoinSet;
use std::collections::HashSet;
use std::sync::Arc;
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

pub const ARCHIVE_PROCESS_MAX_CONCURRENCY: usize = 10;

/// Archive loop that continuously fetches and processes blocks from the Solana network.
pub async fn archive_loop(
    store: &TapeStore,
    client: &Arc<RpcClient>,
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
    client: &Arc<RpcClient>,
    latest_slot: &mut u64,
    last_processed_slot: &mut u64,
    iteration_count: &mut u64,
) -> Result<()> {
    *iteration_count += 1;

    // Refresh the slot tip every 10 iterations
    if *iteration_count % 10 == 0 {
        if let Ok(slot) = get_slot(&client).await {
            *latest_slot = slot;
        }
    }

    // Fetch up to 100 new slots starting just above what we've processed
    let start = *last_processed_slot + 1;
    let slots = get_blocks_with_limit(&client, start, 100).await?;
    println!("slots {:?}", slots);

    for slots in slots.chunks(ARCHIVE_PROCESS_MAX_CONCURRENCY) {
        let processed_blocks = get_processed_blocks_by_slots_batch(client, slots).await?;
        let successfully_processed = processed_blocks.len();
        archive_blocks(store, processed_blocks)?;
        *last_processed_slot += successfully_processed as u64;
    }

    Ok(())
}

async fn get_processed_blocks_by_slots_batch(
    client: &Arc<RpcClient>, 
    slots: &[u64],
)-> Result<Vec<ProcessedBlock>> {
    let mut tasks = JoinSet::new();
   
    for s in slots {
        let client = Arc::clone(&client);
        let slot = *s;
        tasks.spawn(async move {
            get_processed_block_by_slot(&client, slot).await
        });
    }

    let processed_blocks = tasks.join_all().await.into_iter()
        .filter_map(|pb| pb.ok())
        .collect::<Vec<ProcessedBlock>>();

    Ok(processed_blocks)
}

async fn get_processed_block_by_slot(
    client: &Arc<RpcClient>, 
    slot: u64
)-> Result<ProcessedBlock> {
    let block = get_block_by_number(client, slot, TransactionDetails::Full).await?;
    let processed = process_block(block, slot)?;
    Ok(processed)
}

#[allow(dead_code)]
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

/// Archives the processed blocks data into the store.
fn archive_blocks(store: &TapeStore, blocks: Vec<ProcessedBlock>) -> Result<()> {
    for block in blocks {

        let ProcessedBlock{
            slot:_,
            finalized_tapes,
            segment_writes
        } = block;
        // 1. Tape insert batch
        let (tape_numbers, tape_addresses): (Vec<_>, Vec<_>) =
            finalized_tapes.into_iter().map(|(addr, num)| (num, addr)).unzip();

        store.add_tapes_batch(&tape_numbers, &tape_addresses)?;

        // 2. Segment and slot insert batches using fold
        let (segment_addresses, segment_numbers, segment_data): (Vec<Pubkey>, Vec<u64>, Vec<Vec<u8>>) =
            segment_writes
                .into_iter()
                .fold((Vec::new(), Vec::new(), Vec::new()), |(mut addrs, mut nums, mut data), (key, val)| {
                    addrs.push(key.address);
                    nums.push(key.segment_number);
                    data.push(val);
                    (addrs, nums, data)
                });

        let slot_values = vec![block.slot; segment_addresses.len()];

        store.add_segments_batch(&segment_addresses, &segment_numbers, segment_data)?;
        store.add_slots_batch(&segment_addresses, &segment_numbers, &slot_values)?;
    }

    Ok(())
}

/// Syncs all tapes up to the current archive count from a trusted peer.
async fn sync_with_trusted_peer(
    store: &TapeStore,
    client: &Arc<RpcClient>,
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
    client: &Arc<RpcClient>,
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
        let ProcessedBlock{
            segment_writes,
            slot,
            finalized_tapes
        } = process_block(block, current_slot)?;

        if finalized_tapes.is_empty() && 
            segment_writes.is_empty() {
               continue; // Skip empty blocks
        }

        let (tape_number_vec,address_vec): (Vec<_>, Vec<_>) = finalized_tapes
        .into_iter()
        .filter_map(|(addr, num)| {
            if addr == *tape_address {
                Some((num, addr))
            } else {
                None
            }
        })
        .unzip();

        store.add_tapes_batch(&tape_number_vec, &address_vec)?;
    

        let mut parents: HashSet<u64> = HashSet::new();

        for (key, _) in &segment_writes {
            if key.address != *tape_address {
                continue;
            }

            if key.prev_slot != 0 {
                if key.prev_slot > slot {
                    return Err(anyhow!("Parent slot must be earlier than current slot"));
                }

                parents.insert(key.prev_slot);
            }
        }

        let (segment_addresses, segment_numbers, segment_data): (Vec<Pubkey>, Vec<u64>, Vec<Vec<u8>>) =
            segment_writes
                .into_iter()
                .filter_map(|s|
                    if s.0.address == *tape_address {
                        Some(s)
                    } else {
                        None
                    }
                )
                .fold((Vec::new(), Vec::new(), Vec::new()), |(mut addrs, mut nums, mut data), (key, val)| {
                    addrs.push(key.address);
                    nums.push(key.segment_number);
                    data.push(val);
                    (addrs, nums, data)
                });

        let slot_values = vec![slot; segment_addresses.len()];

        store.add_segments_batch(&segment_addresses, &segment_numbers, segment_data)?;
        store.add_slots_batch(&segment_addresses, &segment_numbers, &slot_values)?;

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
