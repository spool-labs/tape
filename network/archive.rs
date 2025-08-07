// sync.rs

use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::task::JoinSet;
use tape_client::{get_archive_account, find_tape_account};
use crate::store::TapeStore;
use crate::utils::peer;

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
        if store.read_tape_address(tape_number).is_ok() {
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

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

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
        if store.read_tape_address(tape_number).is_ok() {
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

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
}



// queue.rs

use tokio::sync::mpsc;
use solana_sdk::pubkey::Pubkey;

pub const QUEUE_CAP: usize = 1_000;

#[derive(Debug)]
pub struct SegmentJob {
    pub tape: Pubkey,
    pub seg_no: u64,
    pub data: Vec<u8>,
}

pub type Tx = mpsc::Sender<SegmentJob>;
pub type Rx = mpsc::Receiver<SegmentJob>;

pub fn channel() -> (Tx, Rx) {
    mpsc::channel::<SegmentJob>(QUEUE_CAP)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel() {
        let (tx, rx) = channel();
        assert_eq!(tx.capacity(), QUEUE_CAP);
        assert_eq!(rx.capacity(), QUEUE_CAP);
    }

    #[test]
    fn test_segment_job() {
        let job = SegmentJob {
            tape: Pubkey::new_unique(),
            seg_no: 1,
            data: vec![1, 2, 3],
        };
        assert_eq!(job.seg_no, 1);
        assert!(!job.data.is_empty());
    }
}



// pack.rs

use anyhow::Result;
use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;

use crate::store::TapeStore;
use super::queue::Rx;
use super::process::process_segment;

/// Spawn task C – CPU-heavy preprocessing.
pub async fn run(mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {
    while let Some(job) = rx.recv().await {
        let store = store.clone();
        let miner = miner.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={} size={}", job.tape, job.seg_no, job.data.len());

            let packing_difficulty = 0;
            let solved = process_segment(&miner, &job.data, packing_difficulty)?;
            store.write_segment(&job.tape, job.seg_no, solved)?;
            Ok(())
        })
        .await??;
    }

    Ok(())
}



// challenge.rs

use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use reqwest::Client as HttpClient;

use tape_client::{
    get_block_account, get_miner_account, get_epoch_account,
    get_tape_account, init_read, process_next_block,
};
use tape_api::prelude::*;

use crate::store::TapeStore;
use super::queue::{Tx, SegmentJob};

/// Spawn task B – periodic miner-challenge sync.
pub async fn run(
    rpc: Arc<RpcClient>,
    store: Arc<TapeStore>,
    miner_address: Pubkey,
    trusted_peer: Option<String>,
    tx: Tx,
) -> anyhow::Result<()> {

    loop {
        // Fetch miner, block, and epoch accounts
        let block_with_miner = tokio::join!(
            get_block_account(&rpc),
            get_miner_account(&rpc, &miner_address),
            get_epoch_account(&rpc)
        );

        let (block, miner, epoch) = (
            block_with_miner.0?.0,
            block_with_miner.1?.0,
            block_with_miner.2?.0,
        );

        let miner_challenge = compute_challenge(&block.challenge, &miner.challenge);
        let tape_number = compute_recall_tape(&miner_challenge, block.challenge_set);

        log::debug!("Miner needs tape number: {}", tape_number);

        // Get tape address (assumed to be synced during initialization)
        if let Ok(tape_address) = store.read_tape_address(tape_number) {
            let tape = get_tape_account(&rpc, &tape_address).await?.0;

            // Check and sync segments
            let segment_count = store.read_segment_count(&tape_address).unwrap_or(0);
            if segment_count as u64 != tape.total_segments {
                log::debug!(
                    "Syncing segments for tape {} ({} of {})",
                    tape_address,
                    segment_count,
                    tape.total_segments
                );
                if let Some(peer_url) = &trusted_peer {
                    sync_segments_from_trusted_peer(
                        &store,
                        &tape_address,
                        peer_url,
                        &miner_address,
                        epoch.packing_difficulty,
                        &tx,
                    )
                    .await?;
                } else {
                    sync_segments_from_solana(
                        &store,
                        &rpc,
                        &tape_address,
                        &miner_address,
                        epoch.packing_difficulty,
                        &tx,
                    )
                    .await?;
                }
            }
        } else {
            log::error!("Tape address not found for tape number {}", tape_number);
        }

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn sync_segments_from_solana(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    tape_address: &Pubkey,
    _miner_address: &Pubkey,
    _packing_difficulty: u64,
    tx: &Tx,
) -> anyhow::Result<()> {
    let (tape, _) = get_tape_account(client, tape_address).await?;
    let mut state = init_read(tape.tail_slot);
    while process_next_block(client, tape_address, &mut state).await? {}

    let mut keys: Vec<u64> = state.segments.keys().cloned().collect();
    keys.sort();

    for seg_num in keys {
        if store.read_segment_by_address(tape_address, seg_num).is_ok() {
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

async fn sync_segments_from_trusted_peer(
    store: &TapeStore,
    tape_address: &Pubkey,
    trusted_peer_url: &str,
    _miner_address: &Pubkey,
    _packing_difficulty: u64,
    tx: &Tx,
) -> anyhow::Result<()> {
    let http = HttpClient::new();
    let segments = crate::utils::peer::fetch_tape_segments(&http, trusted_peer_url, tape_address).await?;

    for (seg_num, data) in segments {
        if store.read_segment_by_address(tape_address, seg_num).is_ok() {
            continue;
        }

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



// mod.rs

pub mod queue;
pub mod live;
pub mod challenge;
pub mod pack;
pub mod process;
pub mod orchestrator;
pub mod sync;

pub use queue::SegmentJob;



// orchestrator.rs

use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::store::TapeStore;
use crate::utils::wait_for_shutdown;
use crate::metrics::{run_metrics_server, Process};
use super::{ queue, live, challenge, pack, sync };

/// Orchestrator for the archive processing tasks.
pub async fn run(
    miner: Pubkey, 
    store: Arc<TapeStore>, 
    rpc: Arc<RpcClient>,
    trusted_peer: Option<String>,
) -> Result<()> {
    let (tx, rx) = queue::channel();

    init(
        &store.clone(), 
        &rpc.clone(), 
        trusted_peer.clone()
    ).await?;

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // A – live updates
    tasks.spawn(live::run(rpc.clone(), tx.clone()));

    // B – miner challenge / tape sync
    tasks.spawn(challenge::run(rpc.clone(), store.clone(), miner, trusted_peer, tx));

    // C – pack segments
    tasks.spawn(pack::run(rx, miner, store));

    wait_for_shutdown(tasks).await
}

pub async fn init(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer: Option<String>,
) ->Result<()> {
    run_metrics_server(Process::Archive)?;

    sync::get_tape_addresses(
        store, client, trusted_peer
    ).await?;

    Ok(())
}



// process.rs

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use tape_api::prelude::*;

pub fn process_segment(miner_address: &Pubkey, segment: &[u8], packing_difficulty: u64) -> Result<Vec<u8>> {
    let miner_address: [u8; 32] = miner_address.to_bytes();
    let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

    let solution = packx::solve(&miner_address, &canonical_segment, packing_difficulty as u32)
        .ok_or_else(|| anyhow!("Failed to find solution"))?;

    if !packx::verify(&miner_address, &canonical_segment, &solution, packing_difficulty as u32) {
        return Err(anyhow!("Solution verification failed"));
    }

    let segment_bytes = solution.to_bytes();
    Ok(segment_bytes.to_vec())
}

fn padded_array<const N: usize>(data: &[u8]) -> [u8; N] {
    let mut result = [0u8; N];
    let len = data.len().min(N);
    result[..len].copy_from_slice(&data[..len]);
    result
}



// live.rs

use anyhow::{anyhow, Result};
use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_transaction_status_client_types::TransactionDetails;
use tape_client::utils::process_block;
use tape_client::{get_block_by_number, get_slot};

use super::queue::{Tx, SegmentJob};

/// Spawn task A – stream live blocks and push raw segments into `tx`.
pub async fn run(rpc: Arc<RpcClient>, tx: Tx) -> Result<()> {
    let mut latest_slot = get_slot(&rpc).await?;
    let mut last_processed_slot = latest_slot;

    loop {

        // Refresh slot tip every 10 iterations
        if last_processed_slot % 10 == 0 {
            latest_slot = get_slot(&rpc).await?;

            if latest_slot <= last_processed_slot {
                log::debug!("No new slots available, waiting...");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        }

        // Fetch up to 100 new slots
        let start = last_processed_slot + 1;
        let slots = tape_client::get_blocks_with_limit(&rpc, start, 100).await?;

        for slot in slots {
            let block = get_block_by_number(&rpc, slot, TransactionDetails::Full).await?;
            let processed = process_block(block, slot)?;

            // Push segments to queue
            for (key, data) in processed.segment_writes {
                let job = SegmentJob {
                    tape: key.address,
                    seg_no: key.segment_number,
                    data,
                };

                // tx.send(job).await?;// back-pressure if queue full

                if tx.send(job).await.is_err() {
                    log::error!("Failed to send segment job for tape {} seg {}", key.address, key.segment_number);
                    return Err(anyhow!("Channel closed"));
                }
            }

            // Store finalized tapes
            for (address, number) in processed.finalized_tapes {
                // Assuming TapeStore is accessible via orchestrator; for now, log
                log::debug!("Finalized tape {} with number {}", address, number);
            }

            last_processed_slot = slot;
        }

        log::debug!("Processed slots up to {}", last_processed_slot);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}



