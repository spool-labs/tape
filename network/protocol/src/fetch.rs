use rpc::{CommitmentLevel, Rpc, RpcError};
use rpc_client::{EpochBatch, RpcClient};
use tape_api::state::Epoch;
use tape_core::types::EpochNumber;

use crate::{EpochBundle, ProtocolState};

/// How much of the protocol state a caller actually needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchScope {
    /// The current epoch only.
    ///
    /// Enough to route a read or a write: the client picks peers from the
    /// current committee and maps spools through the current groups. The
    /// previous and next bundles exist for node-side recovery and consensus,
    /// which no client path reads.
    Current,

    /// Current, previous, next and candidate epochs.
    Full,
}

pub async fn fetch_state<R: Rpc>(rpc: &RpcClient<R>) -> Result<ProtocolState, RpcError> {
    fetch_state_with_commitment(rpc, rpc.rpc().commitment()).await
}

/// Fetch only what a client needs to route traffic.
pub async fn fetch_state_current<R: Rpc>(rpc: &RpcClient<R>) -> Result<ProtocolState, RpcError> {
    fetch_state_scoped(rpc, rpc.rpc().commitment(), FetchScope::Current).await
}

pub async fn fetch_state_with_commitment<R: Rpc>(
    rpc: &RpcClient<R>,
    commitment: CommitmentLevel,
) -> Result<ProtocolState, RpcError> {
    fetch_state_scoped(rpc, commitment, FetchScope::Full).await
}

pub async fn fetch_state_scoped<R: Rpc>(
    rpc: &RpcClient<R>,
    commitment: CommitmentLevel,
    scope: FetchScope,
) -> Result<ProtocolState, RpcError> {
    let want_full = matches!(scope, FetchScope::Full);

    let system = rpc
        .get_system_with_commitment(commitment)
        .await?;

    let next = system.current_epoch.next();
    let candidate = system.current_epoch.saturating_add(EpochNumber(2));
    let prev = system.current_epoch.prev();

    // Every account below has a known address once the system row is read, so
    // the reads collapse into one concurrent round; groups follow in a second
    // round because their counts come from the epoch rows.
    let has_previous = want_full && !system.current_epoch.is_zero();
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
        async {
            match want_full {
                true => rpc.get_epoch_with_commitment(next, commitment).await,
                false => Err(RpcError::AccountNotFound(tape_crypto::address::Address::default())),
            }
        },
        async {
            match want_full {
                true => rpc.get_committee_account_with_commitment(next, commitment).await,
                false => Err(RpcError::AccountNotFound(tape_crypto::address::Address::default())),
            }
        },
        async {
            match want_full {
                true => rpc.get_epoch_with_commitment(candidate, commitment).await,
                false => Err(RpcError::AccountNotFound(tape_crypto::address::Address::default())),
            }
        },
        async {
            match want_full {
                true => rpc.get_committee_account_with_commitment(candidate, commitment).await,
                false => Err(RpcError::AccountNotFound(tape_crypto::address::Address::default())),
            }
        },
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


/// A guess at which epoch is current, carried over from a previous run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochGuess {
    pub epoch: EpochNumber,
    pub total_groups: u64,
}

/// Fetch current state in one round trip when the guess is right.
///
/// Discovery normally costs three sequential rounds because epoch and
/// committee addresses derive from the system row, and group addresses derive
/// from the epoch row. A guess from the last run breaks that chain: everything
/// is requested at once, and the system row in the reply says whether the guess
/// held.
///
/// Nothing speculative is adopted unless the system row confirms the epoch, so
/// a stale guess costs one wasted round and can never produce state from the
/// wrong epoch. A miss falls back to the ordinary scoped fetch.
pub async fn fetch_state_speculative<R: Rpc>(
    rpc: &RpcClient<R>,
    guess: EpochGuess,
) -> Result<ProtocolState, RpcError> {
    let commitment = rpc.rpc().commitment();
    let batch = rpc
        .get_epoch_batch_with_commitment(guess.epoch, guess.total_groups, commitment)
        .await?;

    match assemble_speculative(&batch, guess) {
        Some(state) => Ok(state),
        None => fetch_state_scoped(rpc, commitment, FetchScope::Current).await,
    }
}

/// Build state from a speculative batch, or nothing if the guess missed.
///
/// Kept separate so the acceptance rules can be tested without an RPC.
fn assemble_speculative(batch: &EpochBatch, guess: EpochGuess) -> Option<ProtocolState> {
    // The whole safety argument sits on this line: cached addresses are only
    // ever adopted when the freshly read system row agrees.
    if batch.system.current_epoch != guess.epoch {
        return None;
    }

    let epoch = batch.epoch?;
    let committee = batch.committee.clone()?;
    let (peer_capacity, peers) = batch.peer_set.clone()?;

    // The group count is part of the guess, so it can be short or long even
    // when the epoch is right. Anything less than complete is a miss.
    if epoch.total_groups != guess.total_groups {
        return None;
    }
    let groups: Option<Vec<_>> = batch.groups.iter().cloned().collect();
    let groups = groups?;

    Some(ProtocolState {
        system: batch.system,
        peers,
        peer_capacity,
        current: EpochBundle {
            epoch,
            committee,
            groups,
        },
        previous: None,
        next_epoch: None,
        next_committee: None,
        next_committee_capacity: None,
        candidate_epoch: None,
        candidate_committee_capacity: None,
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

#[cfg(test)]
mod speculative_tests {
    use bytemuck::Zeroable;
    use tape_api::state::{Epoch, Group, System};

    use super::*;

    fn batch(current: u64, epoch_id: u64, groups: u64) -> EpochBatch {
        let mut system = System::zeroed();
        system.current_epoch = EpochNumber(current);

        let mut epoch = Epoch::zeroed();
        epoch.id = EpochNumber(epoch_id);
        epoch.total_groups = groups;

        EpochBatch {
            system,
            epoch: Some(epoch),
            committee: Some(Vec::new()),
            peer_set: Some((0, Vec::new())),
            groups: (0..groups).map(|_| Some(Group::zeroed())).collect(),
        }
    }

    fn guess(epoch: u64, groups: u64) -> EpochGuess {
        EpochGuess {
            epoch: EpochNumber(epoch),
            total_groups: groups,
        }
    }

    // the happy path: the system row confirms the guess
    #[test]
    fn confirmed_guess_is_adopted() {
        let state = assemble_speculative(&batch(7, 7, 3), guess(7, 3)).expect("adopted");
        assert_eq!(state.current.epoch.id, EpochNumber(7));
        assert_eq!(state.current.groups.len(), 3);
        assert!(state.previous.is_none(), "speculation only ever yields the current bundle");
    }

    // the whole safety argument: a stale guess is never adopted
    #[test]
    fn epoch_advanced_since_the_guess() {
        assert!(assemble_speculative(&batch(8, 7, 3), guess(7, 3)).is_none());
    }

    // the epoch can be right while the group count moved
    #[test]
    fn group_count_disagrees() {
        assert!(assemble_speculative(&batch(7, 7, 4), guess(7, 3)).is_none());
    }

    // a partial reply is a miss, not a truncated state
    #[test]
    fn missing_group_is_a_miss() {
        let mut partial = batch(7, 7, 3);
        partial.groups[1] = None;
        assert!(assemble_speculative(&partial, guess(7, 3)).is_none());
    }

    // an empty slot anywhere means the guessed address held nothing
    #[test]
    fn missing_accounts_are_a_miss() {
        let mut no_epoch = batch(7, 7, 2);
        no_epoch.epoch = None;
        assert!(assemble_speculative(&no_epoch, guess(7, 2)).is_none());

        let mut no_committee = batch(7, 7, 2);
        no_committee.committee = None;
        assert!(assemble_speculative(&no_committee, guess(7, 2)).is_none());

        let mut no_peers = batch(7, 7, 2);
        no_peers.peer_set = None;
        assert!(assemble_speculative(&no_peers, guess(7, 2)).is_none());
    }
}
