//! fetch_state — fetch protocol state from on-chain accounts.

use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_core::system::EpochPhase;

use crate::ProtocolState;

/// Fetch current protocol state from on-chain accounts.
///
/// Makes 2 RPC calls: `get_system()` + `get_epoch()`.
/// Does NOT fetch individual Node accounts (network addresses).
pub async fn fetch_state<R: Rpc>(rpc: &RpcClient<R>)
-> Result<ProtocolState, RpcError> {

    let system = rpc.get_system().await?;
    let epoch = rpc.get_epoch().await?;

    let phase = EpochPhase::try_from(epoch.state.phase)
        .unwrap_or(EpochPhase::Unknown);

    let committee = system.committee.iter().cloned().collect();
    let committee_prev = system.committee_prev.iter().cloned().collect();
    let committee_next = system.committee_next.iter().cloned().collect();

    Ok(ProtocolState {
        epoch: epoch.id,
        phase,
        nonce: epoch.nonce,
        committee,
        committee_prev,
        committee_next,
        spools: system.spools,
        spools_prev: system.spools_prev,
    })
}
