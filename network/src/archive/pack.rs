use anyhow::Result;
use std::sync::Arc;
use super::queue::Rx;
use solana_sdk::pubkey::Pubkey;

use crate::store::TapeStore;

/// Spawn task C – CPU-heavy preprocessing.
pub async fn run(mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {
    while let Some(job) = rx.recv().await {
        let store = store.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={} size={}", job.tape, job.seg_no, job.data.len());

            // let solved = process_segment(&miner, &job.data, store.packing_difficulty())?;
            // store.write_segment(&job.tape, job.seg_no, solved)?;
            Ok(())
        });
    }

    Ok(())
}
