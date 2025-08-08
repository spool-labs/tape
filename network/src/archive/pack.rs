use anyhow::Result;
use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;
use tape_client::get_epoch_account;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::store::TapeStore;
use super::queue::Rx;
use super::process::process_segment;

/// Spawn task C – CPU-heavy preprocessing.
pub async fn run(rpc: Arc<RpcClient>, mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {

    // Fetch packing difficulty once before the loop
    // TODO: update this once packing difficulty can change dynamically
    let epoch = get_epoch_account(&rpc).await?.0;
    let packing_difficulty = epoch.packing_difficulty;

    while let Some(job) = rx.recv().await {
        let store = store.clone();
        let miner = miner.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={} size={}", job.tape, job.seg_no, job.data.len());

            let solved = process_segment(&miner, &job.data, packing_difficulty)?;
            store.put_segment(&job.tape, job.seg_no, solved)?;
            Ok(())
        })
        .await??;
    }

    Ok(())
}
