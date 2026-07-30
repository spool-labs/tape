use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::{Api, ProtocolState, fetch::fetch_state_current};

use crate::bootstrap::{NetworkKey, Prediction};
use crate::error::TapedriveError;
use crate::metrics::{Operation, Phase};
use crate::tapedrive::Tapedrive;

mod delete;
mod query;
mod read;
pub mod write;

pub(crate) use query::{query_track_proof, queryable_peers};

pub async fn bootstrap_network_state<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operation: Option<Operation>,
) -> Result<arc_swap::Guard<Arc<ProtocolState>>, TapedriveError> {
    let state = client.state();
    if !state.current.committee.is_empty() {
        return Ok(state);
    }
    drop(state);

    let state = match operation {
        Some(operation) => {
            let timer = client.timer(operation, Phase::Bootstrap);
            let result = fetch_state_current(&client.rpc).await;
            timer.finish_result(&result);
            result?
        }
        None => fetch_state_current(&client.rpc).await?,
    };

    match operation {
        Some(operation) => {
            let timer = client.timer(operation, Phase::ResolvePeers);
            let result = client.peer_manager.resolve_peers(&state);
            timer.finish_result(&result);
            result?;
        }
        None => {
            client.peer_manager.resolve_peers(&state)?;
        }
    }

    remember_for_next_run(client, &state).await;

    client.state.store(Arc::new(state));
    Ok(client.state())
}

/// Record what this run learned, so the next one can skip discovery.
///
/// Nothing here is load bearing. Every failure is swallowed, because a client
/// that cannot write a hint file must still complete the command it was asked
/// to run.
async fn remember_for_next_run<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    state: &ProtocolState,
) {
    // Which chain this is completes the cache key, and it is only knowable
    // from the node we are talking to.
    if let Ok(genesis) = client.rpc.rpc().get_genesis_hash().await {
        client.reputation.rekey(NetworkKey {
            program_id: tape_api::program::tapedrive::id().into(),
            genesis: tape_crypto::hash::Hash(genesis.to_bytes()),
        });
    }

    client.reputation.set_prediction(Prediction {
        epoch: state.current.epoch.id,
        total_groups: state.current.epoch.total_groups,
        // Negative only if the chain reports a pre-epoch timestamp, which
        // would make the guess meaningless anyway, so clamp to zero.
        epoch_start: state.current.epoch.start_time.max(0) as u64,
        epoch_duration: state.current.epoch.preferences.epoch_duration.0,
    });

    // Peers that left the network should not linger in the file forever.
    let known: Vec<_> = state.peers.iter().map(|peer| peer.node).collect();
    client.reputation.prune(&known);
    client.reputation.flush();
}
