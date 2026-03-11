use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::{Api, ProtocolState};

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

mod mutations;
mod queries;
mod read;
pub mod write;

pub async fn bootstrap_network_state<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
) -> Result<arc_swap::Guard<Arc<ProtocolState>>, TapedriveError> {
    let state = client
        .peer_manager
        .bootstrap(&client.rpc)
        .await
        .map_err(TapedriveError::Network)?;
    client.state.store(Arc::new(state));
    Ok(client.state())
}
