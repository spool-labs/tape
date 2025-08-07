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
