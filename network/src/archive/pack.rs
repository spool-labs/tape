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
