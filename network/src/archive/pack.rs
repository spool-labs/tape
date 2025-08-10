use std::sync::Arc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use tape_api::prelude::*;
use tape_client::get_epoch_account;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::store::TapeStore;
use super::queue::Rx;

/// Orchestrator Task C – CPU-heavy preprocessing (packx)
pub async fn run(rpc: Arc<RpcClient>, mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {

    // Fetch packing difficulty once before the loop
    // TODO: update this once packing difficulty can change dynamically
    let epoch = get_epoch_account(&rpc).await?.0;
    let packing_difficulty = epoch.packing_difficulty;

    while let Some(job) = rx.recv().await {
        let store = store.clone();
        let miner = miner.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={}", job.tape, job.seg_no);

            let solved = pack_segment(&miner, &job.data, packing_difficulty)?;
            store.put_segment(&job.tape, job.seg_no, solved)?;

            Ok(())
        })
        .await??;
    }

    Ok(())
}

pub fn pack_segment(miner_address: &Pubkey, segment: &[u8], packing_difficulty: u64) -> Result<Vec<u8>> {
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
