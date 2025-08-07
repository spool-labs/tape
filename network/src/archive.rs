use anyhow::{anyhow, Result};
use log::{debug, error};
use tokio::task::JoinSet;
use std::collections::HashSet;
use std::sync::Arc;
use solana_transaction_status_client_types::TransactionDetails;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::time::{sleep, Duration};
use tape_client::{
    get_slot, get_blocks_with_limit, get_block_by_number, get_archive_account,
    get_tape_account, find_tape_account, init_read, process_next_block,
    get_block_account, get_miner_account, get_epoch_account
};
use tape_client::utils::{process_block, ProcessedBlock};
use reqwest::Client as HttpClient;
use serde_json::json;
use base64::decode;
use tape_api::prelude::*;

use crate::metrics::{run_metrics_server, Process};

use super::store::TapeStore;

pub const MAX_CONCURRENCY: usize = 10;

/// Runs the archive loop to continuously fetch and process blocks from the Solana network.
pub async fn archive_loop(
    store: TapeStore,
    client: &Arc<RpcClient>,
    miner_address: Pubkey,
    starting_slot: Option<u64>,
    trusted_peer: Option<String>,
) -> Result<()> {
    // Initialize archive parameters
    run_metrics_server(Process::Archive)?;

    let (mut latest_slot, mut last_processed_slot, mut iteration_count) =
        initialize_archive(&store, client, starting_slot, miner_address).await?;

    // Sync tape addresses
    sync_tape_addresses(&store, client, &trusted_peer).await?;

    // Main processing loop
    run_block_processing_loop(
        &store,
        client,
        miner_address,
        &trusted_peer,
        &mut latest_slot,
        &mut last_processed_slot,
        &mut iteration_count,
    )
    .await
}

/// Initializes archive parameters (slots, iteration count, miner address).
async fn initialize_archive(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    starting_slot: Option<u64>,
    miner_address: Pubkey,
) -> Result<(u64, u64, u64)> {
    debug!("Using provided miner address: {miner_address}");

    let latest_slot = match starting_slot {
        Some(slot) => slot,
        None => get_slot(client).await?,
    };
    debug!("Initial slot tip: {latest_slot}");

    let last_processed_slot = starting_slot
        .or_else(|| store.get_health().map(|(slot, _)| slot).ok())
        .unwrap_or(latest_slot);

    let iteration_count = 0;

    Ok((latest_slot, last_processed_slot, iteration_count))
}

/// Syncs missing tape addresses from either a trusted peer or Solana RPC.
async fn sync_tape_addresses(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    trusted_peer: &Option<String>,
) -> Result<()> {
    debug!("Syncing missing tape addresses");
    debug!("This may take a while... please be patient");

    if let Some(peer_url) = trusted_peer {
        debug!("Using trusted peer: {peer_url}");
        sync_addresses_from_trusted_peer(store, client, peer_url).await?;
    } else {
        debug!("No trusted peer provided, syncing against Solana directly");
        sync_addresses_from_solana(store, client).await?;
    }

    Ok(())
}

/// Processes miner-specific requirements (fetching accounts, computing challenges, syncing segments).
async fn process_miner(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    miner_address: Pubkey,
    trusted_peer: &Option<String>,
) -> Result<()> {
    let block_with_miner = tokio::join!(
        get_block_account(client),
        get_miner_account(client, &miner_address),
        get_epoch_account(client)
    );

    let (block, miner, epoch) = (
        block_with_miner.0.map_err(|e| anyhow!("Failed to get block account: {}", e))?.0,
        block_with_miner.1.map_err(|e| anyhow!("Failed to get miner account: {}", e))?.0,
        block_with_miner.2.map_err(|e| anyhow!("Failed to get epoch account: {}", e))?.0,
    );

    let miner_challenge = compute_challenge(&block.challenge, &miner.challenge);
    let tape_number = compute_recall_tape(&miner_challenge, block.challenge_set);

    debug!("Miner currently needs tape number: {tape_number:?}");

    if let Ok(tape_address) = store.read_tape_address(tape_number) {
        let tape = get_tape_account(client, &tape_address)
            .await
            .map_err(|e| anyhow!("Failed to get tape account: {}", e))?.0;

        if let Ok(segment_count) = store.read_segment_count(&tape_address) {
            // Check if we have the correct number of segments locally
            if segment_count as u64 != tape.total_segments {
                debug!(
                    "Tape {} has {} segments, found {}, syncing...",
                    tape_address, tape.total_segments, segment_count
                );
                debug!("Syncing segments for tape number {tape_number}");

                if let Some(peer_url) = trusted_peer {
                    debug!("Syncing segments from trusted peer: {peer_url}");
                    sync_segments_from_trusted_peer(
                        store, &tape_address, peer_url, &miner_address, epoch.packing_difficulty
                    ).await?;
                } else {
                    debug!("Syncing segments from Solana RPC");
                    sync_segments_from_solana(
                        store, client, &tape_address, &miner_address, epoch.packing_difficulty
                    ).await?;
                    debug!("Segments synced from Solana RPC");
                }
            }
        }
    } else {
        error!("Tape address not found for tape number {tape_number}");
    }

    Ok(())
}

/// Runs the main block processing loop, handling miner requirements and block archiving.
async fn run_block_processing_loop(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    miner_address: Pubkey,
    trusted_peer: &Option<String>,
    latest_slot: &mut u64,
    last_processed_slot: &mut u64,
    iteration_count: &mut u64,
) -> Result<()> {
    let interval = Duration::from_secs(2);

    loop {
        debug!("Starting block processing iteration {iteration_count}: latest_slot={latest_slot}, last_processed_slot={last_processed_slot}");

        // Process miner-specific requirements
        process_miner(store, client, miner_address, trusted_peer).await?;

        // Run a single archive iteration
        match archive_iteration(
            store, client, miner_address, latest_slot, last_processed_slot, iteration_count).await {
            Ok(()) => debug!("Block processing iteration completed successfully"),
            Err(e) => error!("Block processing iteration failed: {e:?}"),
        }

        // Update and report drift status
        update_drift_status(store, *latest_slot, *last_processed_slot);

        // TODO: The fixed interval for the next iteration is nonsensical
        sleep(interval).await;
    }
}

/// Attempts to archive a batch of blocks in a single iteration.
async fn archive_iteration(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    miner_address: Pubkey,
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

    let epoch = get_epoch_account(client)
        .await
        .map_err(|e| anyhow!("Failed to get epoch account: {}", e))?.0;
    let packing_difficulty = epoch.packing_difficulty;

    for slots in slots.chunks(MAX_CONCURRENCY) {
        let processed_blocks = get_processed_blocks_by_slots_batch(client, slots).await?;
        let successfully_processed = processed_blocks.len();
        for block in processed_blocks {
            archive_block(store, &block, &miner_address, packing_difficulty)?;
        }
        *last_processed_slot += successfully_processed as u64;
    }

    Ok(())
}

/// Preprocesses a segment by solving and verifying it, returning the solution bytes.
fn process_segment(miner_address: &Pubkey, segment: &[u8], packing_difficulty: u64) -> Result<Vec<u8>> {
    let miner_address: [u8; 32] = miner_address.to_bytes();
    let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

    let solution = packx::solve(
        &miner_address, 
        &canonical_segment,
        packing_difficulty as u32)
        .ok_or_else(|| anyhow!("Failed to find solution"))?;

    // Technically not required, but let's verify the solution just in case.
    if !packx::verify(
        &miner_address,
        &canonical_segment,
        &solution, 
        packing_difficulty as u32) {
        return Err(anyhow!("Solution verification failed"));
    }

    let segment_bytes = solution.to_bytes();
    Ok(segment_bytes.to_vec())
}

/// Archives the processed block data into the store.
fn archive_block(
    store: &TapeStore, 
    block: &ProcessedBlock,
    miner_address: &Pubkey,
    packing_difficulty: u64
) -> Result<()> {

    for (address, number) in &block.finalized_tapes {
        store.write_tape(*number, address)?;
    }

    for (key, data) in &block.segment_writes {
        let processed_segment = process_segment(miner_address, data, packing_difficulty)?;
        store.write_segment(&key.address, key.segment_number, processed_segment)?;
    }

    Ok(())
}

/// Syncs block data for a specific tape address starting from a given slot.
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
        let ProcessedBlock {
            segment_writes,
            slot,
            finalized_tapes,
        } = process_block(block, current_slot)?;

        if finalized_tapes.is_empty() && segment_writes.is_empty() {
            continue; // Skip empty blocks
        }

        let (tape_number_vec, address_vec): (Vec<_>, Vec<_>) = finalized_tapes
            .into_iter()
            .filter_map(|(addr, num)| {
                if addr == *tape_address {
                    Some((num, addr))
                } else {
                    None
                }
            })
            .unzip();
        store.write_tapes_batch(&tape_number_vec, &address_vec)?;

        let mut parents: HashSet<u64> = HashSet::new();

        for key in segment_writes.keys() {
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

        // Fetch packing difficulty for the current slot
        let epoch = get_epoch_account(client)
            .await
            .map_err(|e| anyhow!("Failed to get epoch account: {}", e))?.0;
        let packing_difficulty = epoch.packing_difficulty;

        for (key, data) in segment_writes {
            if key.address != *tape_address {
                continue;
            }
            let processed_segment = process_segment(&key.address, &data, packing_difficulty)?;
            store.write_segment(&key.address, key.segment_number, processed_segment)?;
        }

        for parent in parents {
            stack.push(parent);
        }
    }

    Ok(())
}

/// Syncs tape addresses up to the current archive count from a trusted peer.
async fn sync_addresses_from_trusted_peer(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    trusted_peer_url: &str,
) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let http = HttpClient::new();

    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.read_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= MAX_CONCURRENCY {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let trusted_peer_url = trusted_peer_url.to_string();
        let http = http.clone();

        tasks.spawn(async move {
            let pubkey = fetch_tape_address(&http, &trusted_peer_url, tape_number).await?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
}

/// Syncs segments for a specific tape address from a trusted peer, preprocessing each segment.
async fn sync_segments_from_trusted_peer(
    store: &TapeStore,
    tape_address: &Pubkey,
    trusted_peer_url: &str,
    miner_address: &Pubkey,
    packing_difficulty: u64,
) -> Result<()> {
    let http = HttpClient::new();
    let segments = fetch_tape_segments(&http, trusted_peer_url, tape_address).await?;

    for (seg_num, data) in segments {
        if store.read_segment_by_address(tape_address, seg_num).is_ok() {
            continue;
        }

        let processed_segment = process_segment(miner_address, &data, packing_difficulty)?;
        store.write_segment(tape_address, seg_num, processed_segment)?;
    }

    Ok(())
}

/// Syncs missing tapes up to the current archive using Solana RPC.
async fn sync_addresses_from_solana(
    store: &TapeStore,
    client: &Arc<RpcClient>,
) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;

    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.read_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= MAX_CONCURRENCY {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let client = client.clone();
        tasks.spawn(async move {
            let (pubkey, _) = find_tape_account(&client, tape_number)
                .await?
                .ok_or(anyhow!("Tape account not found for number {}", tape_number))?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
}

/// Syncs segments for a specific tape address from Solana RPC using the tape's tail slot,
/// preprocessing each segment.
async fn sync_segments_from_solana(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    tape_address: &Pubkey,
    miner_address: &Pubkey,
    packing_difficulty: u64,
) -> Result<()> {
    let (tape, _) = get_tape_account(client, tape_address).await?;
    let mut state = init_read(tape.tail_slot);
    while process_next_block(client, tape_address, &mut state).await? {}

    let mut keys: Vec<u64> = state.segments.keys().cloned().collect();
    keys.sort();

    for seg_num in keys {
        debug!("Syncing segment {seg_num} for tape {tape_address}");
        let processed_segment = process_segment(
            miner_address, &state.segments[&seg_num], packing_difficulty)?;
        store.write_segment(tape_address, seg_num, processed_segment)?;
    }

    Ok(())
}

/// Fetches the Pubkey address for a given tape number from the trusted peer.
async fn fetch_tape_address(
    http: &HttpClient,
    trusted_peer_url: &str,
    tape_number: u64,
) -> Result<Pubkey> {
    let addr_resp = http
        .post(trusted_peer_url)
        .header("Content-Type", "application/json")
        .body(
            json!({
                "jsonrpc": "2.0", "id": 1,
                "method": "getTapeAddress",
                "params": { "tape_number": tape_number }
            })
            .to_string(),
        )
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

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
    let seg_resp = http
        .post(trusted_peer_url)
        .header("Content-Type", "application/json")
        .body(
            json!({
                "jsonrpc": "2.0", "id": 4,
                "method": "getTape",
                "params": { "tape_address": addr_str }
            })
            .to_string(),
        )
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let segments = seg_resp["result"]
        .as_array()
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

async fn get_processed_blocks_by_slots_batch(
    client: &Arc<RpcClient>,
    slots: &[u64],
) -> Result<Vec<ProcessedBlock>> {
    let mut tasks = JoinSet::new();

    for s in slots {
        let client = Arc::clone(client);
        let slot = *s;
        tasks.spawn(async move {
            get_processed_block_by_slot(&client, slot).await
        });
    }

    let processed_blocks = tasks
        .join_all()
        .await
        .into_iter()
        .filter_map(|pb| pb.ok())
        .collect::<Vec<ProcessedBlock>>();

    Ok(processed_blocks)
}

async fn get_processed_block_by_slot(client: &Arc<RpcClient>, slot: u64) -> Result<ProcessedBlock> {
    let block = get_block_by_number(client, slot, TransactionDetails::Full).await?;
    let processed = process_block(block, slot)?;
    Ok(processed)
}

fn update_drift_status(store: &TapeStore, latest_slot: u64, last_processed_slot: u64) {
    // TODO: this function is not working right.

    let drift = latest_slot.saturating_sub(last_processed_slot);

    // Persist updated health (last_processed_slot + drift)
    if let Err(e) = store.update_health(last_processed_slot, drift) {
        eprintln!("ERROR: failed to write health metadata: {e:?}");
    }

    let health_status = if drift < 50 {
        "Healthy"
    } else if drift < 200 {
        "Slightly behind"
    } else {
        "Falling behind"
    };

    debug!(
        "Drift {drift} slots behind tip ({latest_slot}), status: {health_status}"
    );
}

