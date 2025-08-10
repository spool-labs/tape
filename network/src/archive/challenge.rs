use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use tape_api::prelude::*;
use tape_client::{
    get_block_account, get_miner_account, get_epoch_account, get_tape_account
};

use crate::store::*;
use super::queue::Tx;
use super::sync::sync_segments_from_solana;

/// Orchestrator Task B – periodic miner-challenge sync.
pub async fn run(
    rpc: Arc<RpcClient>,
    store: Arc<TapeStore>,
    miner_address: Pubkey,
    _trusted_peer: Option<String>,
    tx: Tx,
) -> anyhow::Result<()> {
    loop {
        // Fetch miner, block, and epoch accounts
        let block_with_miner = tokio::join!(
            get_block_account(&rpc),
            get_miner_account(&rpc, &miner_address),
            get_epoch_account(&rpc)
        );

        let (block, miner, _epoch) = (
            block_with_miner.0?.0,
            block_with_miner.1?.0,
            block_with_miner.2?.0,
        );

        let miner_challenge = compute_challenge(&block.challenge, &miner.challenge);
        let tape_number = compute_recall_tape(&miner_challenge, block.challenge_set);

        log::debug!("Miner needs tape number: {}", tape_number);

        // Get tape address (assumed to be synced during initialization)
        if let Ok(tape_address) = store.get_tape_address(tape_number) {
            let tape = get_tape_account(&rpc, &tape_address).await?.0;

            // Check and sync segments
            let segment_count = store.get_segment_count(&tape_address).unwrap_or(0);

            if segment_count as u64 != tape.total_segments {
                log::debug!(
                    "Syncing segments for tape {} ({} of {})",
                    tape_address,
                    segment_count,
                    tape.total_segments
                );

                //if let Some(peer_url) = &trusted_peer {
                //    sync_segments_from_trusted_peer(&store, &tape_address, peer_url, &tx).await?;
                //} else {
                //    sync_segments_from_solana(&store, &rpc, &tape_address, &tx).await?;
                //}

                // TODO: For now, always sync from Solana, as trusted peer logic is not implemented
                // yet. Need to implement a way to fetch entire sectors from a trusted peer.

                sync_segments_from_solana(&store, &rpc, &tape_address, &tx).await?;

            }
        } else {
            log::error!("Tape address not found for tape number {}", tape_number);
        }

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
