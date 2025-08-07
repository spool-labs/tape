use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use crate::store::TapeStore;
use super::queue::Tx;

/// Spawn task B – periodic miner-challenge sync.
pub async fn run(
    _rpc: Arc<RpcClient>,
    _store: Arc<TapeStore>,
    _miner: Pubkey,
    _tx: Tx,
) -> anyhow::Result<()> {
    loop {
        // 1. compute current challenge
        // 2. fetch / sync required tape
        // 3. push missing segments into `tx`
        log::debug!("syncing miner challenge and pushing segments to tx");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await; // placeholder
    }
}
