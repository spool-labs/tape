use anyhow::Result;
use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;
use solana_client::nonblocking::rpc_client::RpcClient;

use super::queue::{Tx, SegmentJob};

/// Spawn task A – stream live blocks and push raw segments into `tx`.
pub async fn run(_rpc: Arc<RpcClient>, tx: Tx) -> Result<()> {
    let tape = Pubkey::default();
    let mut seg_no: u64 = 0;

    loop {
        let job = SegmentJob {
            tape,
            seg_no,
            data: vec![0u8; 128],
        };

        tx.send(job).await?;// back-pressure if queue full

        seg_no += 1;
        
        // 1. fetch next block  -> let block = utils::next_block(&rpc).await?;
        // 2. for each segment  -> tx.send(SegmentJob{..}).await?;
        log::debug!("fetching next block and pushing segments to tx");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await; // placeholder
    }
}
