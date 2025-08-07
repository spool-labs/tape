use anyhow::anyhow;
use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinSet;
use tape_client::{
    get_archive_account, get_block_account, get_miner_account, get_epoch_account,
    get_tape_account, find_tape_account, init_read, process_next_block,
};
use tape_api::prelude::*;

use crate::store::TapeStore;
use super::queue::{Tx, SegmentJob};

/// Spawn task B – periodic miner-challenge sync.
pub async fn run(rpc: Arc<RpcClient>, store: Arc<TapeStore>, miner_address: Pubkey, tx: Tx) -> anyhow::Result<()> {
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

        // Sync tape addresses if needed
        if store.read_tape_address(tape_number).is_err() {
            sync_tape_addresses(&store, &rpc).await?;
        }

        let tape_address = store.read_tape_address(tape_number)?;
        let tape = get_tape_account(&rpc, &tape_address).await?.0;

        // Check and sync segments
        let segment_count = store.read_segment_count(&tape_address).unwrap_or(0);
        if segment_count as u64 != tape.total_segments {
            log::debug!("Syncing segments for tape {} ({} of {})", tape_address, segment_count, tape.total_segments);
            sync_segments_from_solana(&store, &rpc, &tape_address, &miner_address, epoch.packing_difficulty, &tx).await?;
        }

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn sync_tape_addresses(store: &TapeStore, client: &Arc<RpcClient>) -> anyhow::Result<()> {
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
                .ok_or(anyhow!("Tape account not found for number {}", tape_number))?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<anyhow::Result<(Pubkey, u64)>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
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

        let data = state.segments.remove(&seg_num).ok_or_else(|| anyhow!("Segment data missing"))?;
        let job = SegmentJob {
            tape: *tape_address,
            seg_no: seg_num,
            data,
        };
        if tx.send(job).await.is_err() {
            return Err(anyhow!("Channel closed"));
        }
    }

    Ok(())
}
