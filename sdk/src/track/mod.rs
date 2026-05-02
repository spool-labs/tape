use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::{Api, ProtocolState, fetch::fetch_state};

use crate::error::TapedriveError;
use crate::metrics::{Operation, Phase};
use crate::tapedrive::Tapedrive;

mod mutations;
mod queries;
mod read;
pub mod write;

pub async fn bootstrap_network_state<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operation: Option<Operation>,
) -> Result<arc_swap::Guard<Arc<ProtocolState>>, TapedriveError> {
    let state = client.state();
    if !state.committee.is_empty() {
        return Ok(state);
    }
    drop(state);

    let state = match operation {
        Some(operation) => {
            let timer = client.timer(operation, Phase::Bootstrap);
            let result = fetch_state(&client.rpc).await;
            timer.finish_result(&result);
            result?
        }
        None => fetch_state(&client.rpc).await?,
    };

    match operation {
        Some(operation) => {
            let timer = client.timer(operation, Phase::ResolvePeers);
            let result = client.peer_manager.resolve_peers(&client.rpc, &state).await;
            timer.finish_result(&result);
            result?;
        }
        None => {
            client.peer_manager.resolve_peers(&client.rpc, &state).await?;
        }
    }

    client.state.store(Arc::new(state));
    Ok(client.state())
}
