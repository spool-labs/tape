use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::store::TapeStore;
use crate::utils::wait_for_shutdown;
use crate::metrics::{run_metrics_server, Process};
use super::{ queue, live, challenge, pack, sync };

/// Orchestrator for the archive processing tasks.
pub async fn run(
    miner: Pubkey, 
    store: Arc<TapeStore>, 
    rpc: Arc<RpcClient>,
    trusted_peer: Option<String>,
) -> Result<()> {
    let (tx, rx) = queue::channel();

    init(
        &store.clone(), 
        &rpc.clone(), 
        trusted_peer.clone()
    ).await?;

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // A – live updates
    tasks.spawn(live::run(rpc.clone(), store.clone(), tx.clone()));

    // B – miner challenge / tape sync
    tasks.spawn(challenge::run(rpc.clone(), store.clone(), miner, trusted_peer, tx));

    // C – pack segments
    tasks.spawn(pack::run(rpc.clone(), rx, miner, store));

    wait_for_shutdown(tasks).await
}

pub async fn init(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer: Option<String>,
) ->Result<()> {
    run_metrics_server(Process::Archive)?;

    sync::get_tape_addresses(
        store, client, trusted_peer
    ).await?;

    Ok(())
}
