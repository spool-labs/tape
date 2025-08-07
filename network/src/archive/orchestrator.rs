use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::store::TapeStore;
use crate::utils::wait_for_shutdown;
use super::{ queue, live, challenge, pack };

/// Orchestrator for the archive processing tasks.
pub async fn run(miner: Pubkey, store: Arc<TapeStore>, rpc: Arc<RpcClient>) -> Result<()> {
    let (tx, rx) = queue::channel();

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // A – live updates
    tasks.spawn(live::run(rpc.clone(), tx.clone()));

    // B – miner challenge / tape sync
    tasks.spawn(challenge::run(rpc.clone(), store.clone(), miner, tx));

    // C – pack segments
    tasks.spawn(pack::run(rx, miner, store));


    wait_for_shutdown(tasks).await
}
