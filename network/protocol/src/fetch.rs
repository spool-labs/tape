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

    // Every account below has a known address once the system row is read, so
    // the reads collapse into one concurrent round; groups follow in a second
    // round because their counts come from the epoch rows.
    let has_previous = !system.current_epoch.is_zero();
    let (
        current_epoch,
        current_committee,
        peer_set,
        prev_epoch,
        prev_committee,
        next_epoch,
        next_committee_account,
        candidate_epoch,
        candidate_committee_account,
    ) = tokio::join!(
        rpc.get_epoch_with_commitment(system.current_epoch, commitment),
        rpc.get_committee_with_commitment(system.current_epoch, commitment),
        rpc.get_peer_set_account_with_commitment(commitment),
        async {
            match has_previous {
                true => rpc.get_epoch_with_commitment(prev, commitment).await.map(Some),
                false => Ok(None),
            }
        },
        async {
            match has_previous {
                true => rpc.get_committee_with_commitment(prev, commitment).await.map(Some),
                false => Ok(None),
            }
        },
        rpc.get_epoch_with_commitment(next, commitment),
        rpc.get_committee_account_with_commitment(next, commitment),
        rpc.get_epoch_with_commitment(candidate, commitment),
        rpc.get_committee_account_with_commitment(candidate, commitment),
    );

    let current_epoch = current_epoch?;
    let current_committee = current_committee?;
    let (peer_capacity, peers) = peer_set?;
    let prev_epoch = match prev_epoch {
        Ok(epoch) => epoch,
        Err(RpcError::AccountNotFound(_)) => None,
        Err(error) => return Err(error),
    };
    let prev_committee = match (&prev_epoch, prev_committee) {
        (None, _) => Ok(None),
        (Some(_), result) => result,
    };
    let next_epoch = optional_account(next_epoch)?;
    let (next_committee_capacity, next_committee) = match optional_account(next_committee_account)? {
        Some((capacity, members)) => (Some(capacity), Some(members)),
        None => (None, None),
    };
    let candidate_epoch = optional_account(candidate_epoch)?;
    let candidate_committee_capacity =
        optional_account(candidate_committee_account)?.map(|(capacity, _)| capacity);

    let (current_groups, prev_groups) = tokio::join!(
        rpc.get_groups_with_commitment(current_epoch.id, current_epoch.total_groups, commitment),
        async {
            match &prev_epoch {
                Some(epoch) => rpc
                    .get_groups_with_commitment(epoch.id, epoch.total_groups, commitment)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        },
    );

    let prev_groups = match (&prev_epoch, prev_groups) {
        (None, _) => Ok(None),
        (Some(_), result) => result,
    };

    let current = EpochBundle {
        epoch: current_epoch,
        committee: current_committee,
        groups: current_groups?,
    };

    // A missing previous epoch drops the whole bundle; a present epoch with a
    // failing committee or group read is a real error, as before.
    let previous = match prev_epoch {
        None => None,
        Some(epoch) => {
            let committee = prev_committee?.ok_or_else(|| {
                RpcError::Deserialization("previous committee missing".into())
            })?;
            let groups = prev_groups?.ok_or_else(|| {
                RpcError::Deserialization("previous groups missing".into())
            })?;
            Some(EpochBundle {
                epoch,
                committee,
                groups,
            })
        }
    };

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
