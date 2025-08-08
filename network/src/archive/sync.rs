use anyhow::{anyhow, Result};
use reqwest::Client as HttpClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_transaction_status_client_types::TransactionDetails;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::collections::HashSet;
use tokio::task::JoinSet;

use tape_client::{
    get_block_by_number, get_archive_account, get_tape_account, find_tape_account, init_read,
    process_next_block, get_epoch_account
};
use tape_client::utils::{process_block, ProcessedBlock};

use crate::store::TapeStore;
use crate::utils::peer;
use super::process::process_segment;
use super::queue::{Tx, SegmentJob};

/// Syncs missing tape addresses from either a trusted peer or Solana RPC.
pub async fn get_tape_addresses(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer: Option<String>,
) -> Result<()> {
    log::debug!("Syncing missing tape addresses");
    log::debug!("This may take a while... please be patient");

    if let Some(peer_url) = trusted_peer {
        log::debug!("Using trusted peer: {}", peer_url);
        sync_addresses_from_trusted_peer(store, client, &peer_url).await?;
    } else {
        log::debug!("No trusted peer provided, syncing against Solana directly");
        sync_addresses_from_solana(store, client).await?;
    }

    Ok(())
}

/// Syncs segments from Solana RPC.
pub async fn sync_segments_from_solana(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    tape_address: &Pubkey,
    tx: &Tx,
) -> anyhow::Result<()> {
    let (tape, _) = get_tape_account(client, tape_address).await?;
    let mut state = init_read(tape.tail_slot);
    while process_next_block(client, tape_address, &mut state).await? {}

    let mut keys: Vec<u64> = state.segments.keys().cloned().collect();
    keys.sort();

    for seg_num in keys {
        if store.get_segment(tape_address, seg_num).is_ok() {
            continue;
        }

        let data = state.segments.remove(&seg_num).ok_or_else(|| anyhow::anyhow!("Segment data missing"))?;
        let job = SegmentJob {
            tape: *tape_address,
            seg_no: seg_num,
            data,
        };
        if tx.send(job).await.is_err() {
            return Err(anyhow::anyhow!("Channel closed"));
        }
    }

    Ok(())
}

///// Syncs segments from a trusted peer.
//pub async fn sync_segments_from_trusted_peer(
//    store: &TapeStore,
//    tape_address: &Pubkey,
//    trusted_peer_url: &str,
//    tx: &Tx,
//) -> anyhow::Result<()> {
//    let http = HttpClient::new();
//    let segments = crate::utils::peer::fetch_tape_segments(&http, trusted_peer_url, tape_address).await?;
//
//    for (seg_num, data) in segments {
//        if store.get_segment(tape_address, seg_num).is_ok() {
//            continue;
//        }
//
//        let job = SegmentJob {
//            tape: *tape_address,
//            seg_no: seg_num,
//            data,
//        };
//        if tx.send(job).await.is_err() {
//            return Err(anyhow::anyhow!("Channel closed"));
//        }
//    }
//
//    Ok(())
//}

/// Syncs tape addresses from a trusted peer.
pub async fn sync_addresses_from_trusted_peer(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer_url: &str,
) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let http = reqwest::Client::new();
    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.get_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= 10 {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let trusted_peer_url = trusted_peer_url.to_string();
        let http = http.clone();
        tasks.spawn(async move {
            let pubkey = peer::fetch_tape_address(&http, &trusted_peer_url, tape_number).await?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    for (pubkey, number) in tape_pubkeys_with_numbers {
        store.put_tape(number, &pubkey)?;
    }


    Ok(())
}

/// Syncs tape addresses from Solana RPC.
pub async fn sync_addresses_from_solana(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>
    ) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.get_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= 10 {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let client = client.clone();
        tasks.spawn(async move {
            let (pubkey, _) = find_tape_account(&client, tape_number)
                .await?
                .ok_or(anyhow::anyhow!("Tape account not found for number {}", tape_number))?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    for (pubkey, number) in tape_pubkeys_with_numbers {
        store.put_tape(number, &pubkey)?;
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

        for (pubkey, number) in finalized_tapes {
            store.put_tape(number, &pubkey)?;
        }

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
            store.put_segment(&key.address, key.segment_number, processed_segment)?;
        }

        for parent in parents {
            stack.push(parent);
        }
    }

    Ok(())
}

