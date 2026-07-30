use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::fetch::{EpochGuess, fetch_state_current, fetch_state_speculative};
use tape_protocol::{Api, ProtocolState};

use crate::bootstrap::{NetworkKey, Prediction, now_secs};
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
            let result = discover(client).await;
            timer.finish_result(&result);
            result?
        }
        None => discover(client).await?,
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

/// Fetch protocol state, guessing the epoch when a previous run left one.
///
/// The guess only ever changes how many round trips this costs. Whether it was
/// right is decided by the freshly read system row, inside the speculative
/// fetch, so a stale or foreign guess falls back rather than misleading us.
async fn discover<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
) -> Result<ProtocolState, TapedriveError> {
    let guess = client.reputation.prediction().map(|prediction| EpochGuess {
        epoch: prediction.epoch_at(now_secs()),
        total_groups: prediction.total_groups,
    });

    let state = match guess {
        Some(guess) => fetch_state_speculative(&client.rpc, guess).await?,
        None => fetch_state_current(&client.rpc).await?,
    };
    Ok(state)
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
    // Which chain this is completes the cache key, and it is only knowable from
    // the node we are talking to. Asked once, on the run that does not know yet,
    // rather than on every bootstrap: this sits on the path the cache exists to
    // shorten, so a needless round trip here costs exactly what was saved.
    let unconfirmed = client
        .reputation
        .network()
        .map(|network| network.genesis == tape_crypto::hash::Hash([0u8; 32]))
        .unwrap_or(true);
    if unconfirmed {
        if let Ok(genesis) = client.rpc.rpc().get_genesis_hash().await {
            client.reputation.rekey(NetworkKey {
                program_id: tape_api::program::tapedrive::id().into(),
                genesis: tape_crypto::hash::Hash(genesis.to_bytes()),
            });
        }
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
