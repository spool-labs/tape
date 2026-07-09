use rpc::{CommitmentLevel, Rpc, RpcError};
use rpc_client::RpcClient;
use tape_api::state::Epoch;
use tape_core::types::EpochNumber;

use crate::{EpochBundle, ProtocolState};

pub async fn fetch_state<R: Rpc>(rpc: &RpcClient<R>) -> Result<ProtocolState, RpcError> {
    fetch_state_with_commitment(rpc, rpc.rpc().commitment()).await
}

pub async fn fetch_state_with_commitment<R: Rpc>(
    rpc: &RpcClient<R>,
    commitment: CommitmentLevel,
) -> Result<ProtocolState, RpcError> {

    let system = rpc
        .get_system_with_commitment(commitment)
        .await?;

    let next = system.current_epoch.next();
    let candidate = system.current_epoch.saturating_add(EpochNumber(2));
    let prev = system.current_epoch.prev();

    let current_epoch = rpc
        .get_epoch_with_commitment(system.current_epoch, commitment)
        .await?;

    let current = build_epoch_bundle_with_commitment(rpc, current_epoch, commitment)
        .await?;

    let (peer_capacity, peers) = rpc
        .get_peer_set_account_with_commitment(commitment)
        .await?;

    let previous = if system.current_epoch.is_zero() {
        None
    } else {
        maybe_fetch_epoch_bundle_with_commitment(rpc, prev, commitment)
            .await?
    };

    let next_epoch = optional_account(
        rpc.get_epoch_with_commitment(next, commitment)
        .await
    )?;

    let next_committee_account = optional_account(
        rpc.get_committee_account_with_commitment(next, commitment)
        .await
    )?;
    let (next_committee_capacity, next_committee) = match next_committee_account {
        Some((capacity, members)) => (Some(capacity), Some(members)),
        None => (None, None),
    };

    let candidate_epoch = optional_account(
        rpc.get_epoch_with_commitment(candidate, commitment)
        .await
    )?;

    let candidate_committee_capacity = optional_account(
        rpc.get_committee_account_with_commitment(candidate, commitment)
        .await
    )?
    .map(|(capacity, _)| capacity);

    Ok(ProtocolState {
        system,
        peers,
        peer_capacity,
        current,
        previous,
        next_epoch,
        next_committee,
        next_committee_capacity,
        candidate_epoch,
        candidate_committee_capacity,
    })
}

pub async fn fetch_epoch_bundle<R: Rpc>(
    rpc: &RpcClient<R>,
    epoch: EpochNumber,
) -> Result<EpochBundle, RpcError> {
    fetch_epoch_bundle_with_commitment(rpc, epoch, rpc.rpc().commitment()).await
}

pub async fn fetch_epoch_bundle_with_commitment<R: Rpc>(
    rpc: &RpcClient<R>,
    epoch: EpochNumber,
    commitment: CommitmentLevel,
) -> Result<EpochBundle, RpcError> {
    let epoch = rpc
        .get_epoch_with_commitment(epoch, commitment)
        .await?;

    build_epoch_bundle_with_commitment(rpc, epoch, commitment).await
}

async fn maybe_fetch_epoch_bundle_with_commitment<R: Rpc>(
    rpc: &RpcClient<R>,
    epoch: EpochNumber,
    commitment: CommitmentLevel,
) -> Result<Option<EpochBundle>, RpcError> {

    let epoch = match rpc.get_epoch_with_commitment(epoch, commitment).await {
        Ok(epoch) => epoch,
        Err(RpcError::AccountNotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };

    build_epoch_bundle_with_commitment(rpc, epoch, commitment)
        .await
        .map(Some)
}

async fn build_epoch_bundle_with_commitment<R: Rpc>(
    rpc: &RpcClient<R>,
    epoch: Epoch,
    commitment: CommitmentLevel,
) -> Result<EpochBundle, RpcError> {

    let committee = rpc
        .get_committee_with_commitment(epoch.id, commitment)
        .await?;

    let groups = rpc
        .get_groups_with_commitment(epoch.id, epoch.total_groups, commitment)
        .await?;

    Ok(EpochBundle {
        epoch,
        committee,
        groups,
    })
}

fn optional_account<T>(result: Result<T, RpcError>) -> Result<Option<T>, RpcError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(RpcError::AccountNotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
