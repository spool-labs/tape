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
