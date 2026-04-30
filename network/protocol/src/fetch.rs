use rpc::{CommitmentLevel, Rpc, RpcError};
use rpc_client::RpcClient;
use tape_api::state::{Epoch, System};
use tape_core::system::EpochPhase;

use crate::ProtocolState;

pub async fn fetch_state<R: Rpc>(rpc: &RpcClient<R>) -> Result<ProtocolState, RpcError> {

    let system = rpc
        .get_system_with_commitment(CommitmentLevel::Finalized)
        .await?;

    let epoch = rpc
        .get_epoch_with_commitment(CommitmentLevel::Finalized)
        .await?;

    Ok(protocol_state_from(system, epoch))
}

fn protocol_state_from(system: System, epoch: Epoch) -> ProtocolState {
    let phase = EpochPhase::try_from(epoch.state.phase)
        .unwrap_or(EpochPhase::Unknown);

    let committee = system.committee.iter().cloned().collect();
    let committee_prev = system.committee_prev.iter().cloned().collect();
    let committee_next = system.committee_next.iter().cloned().collect();

    ProtocolState {
        epoch: epoch.id,
        phase,
        last_epoch: epoch.last_epoch,
        nonce: epoch.nonce,
        committee,
        committee_prev,
        committee_next,
        spools: system.spools,
        spools_prev: system.spools_prev,
    }
}
