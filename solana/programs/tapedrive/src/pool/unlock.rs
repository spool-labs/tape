use tape_api::program::prelude::*;
use tape_api::event::StakeUnlockRequested;
use crate::pool::helpers::resolve_rate;

pub fn process_request_stake_unlock(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RequestStakeUnlock::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        stake_info,
        system_info,
        node_info,
        history_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let (stake_address, _) = stake_pda((*authority_info.key).into());
    let (history_address, _) = history_pda((*node_info.key).into());

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let current = system.current_epoch;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let history_tape = history_info
        .has_address(&history_address.into())?
        .as_account::<Tape>(&tapedrive::ID)?;

    if !history_tape.is_history_tape(node.id) {
        return Err(ProgramError::InvalidAccountData);
    }

    let stake = stake_info
        .has_address(&stake_address.into())?
        .is_writable()?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if stake.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    if stake.pool != (*node_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let prev = current.prev();
    let node_stale = node.latest_advance_epoch < prev;

    if node_stale {
        return Err(TapeError::NodeStale.into());
    }

    let withdraw_epoch;
    let staked_tape = &mut stake.inner;
    let not_yet_active = staked_tape.activation_epoch > current;

    // If the stake hasn't activated yet, we can cancel and return tokens immediately.
    if not_yet_active {
        node.pool
            .request_cancel(staked_tape, current)
            .map_err(|_| TapeError::StakingFailed)?;

        withdraw_epoch = current;

    // Otherwise, we schedule a normal withdrawal with the standard E+2 delay.
    } else {
        // Activation rate is the snapshot used by process_scheduled_additions during
        // AdvancePool(activation_epoch). It's captured by the closing span [_, activation_epoch).
        // Look it up via activation_epoch.prev() so check_contains lands in that span.
        let activation_rate = resolve_rate(
            node,
            history_tape,
            history_address,
            (*node_info.key).into(),
            staked_tape.activation_epoch.prev(),
            args.rate,
        )?;

        node.pool
            .request_withdraw(staked_tape, current, activation_rate)
            .map_err(|_| TapeError::StakingFailed)?;

        withdraw_epoch = current + EpochNumber(2);
    }

    StakeUnlockRequested {
        stake: stake_address,
        authority: (*authority_info.key).into(),
        pool: (*node_info.key).into(),
        amount: staked_tape.amount,
        withdraw_epoch,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::staking::RateSpan;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind};
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, MerkleTree};
    use tape_crypto::Hash;
    use tape_test::*;

    fn make_closed_span(
        node_id: NodeId,
        history_address: Address,
        span: RateSpan,
    ) -> (Tape, PoolRate) {
        let track = CompressedTrack {
            tape: history_address,
            key: span.key(),
            track_number: TrackNumber(0),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(core::mem::size_of::<RateSpan>() as u64),
            group: GroupIndex(0),
            value_hash: span.value_hash(),
        };
        let leaf_hash = track.get_hash();

        let mut tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        tree.add_leaf_hash(leaf_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] =
            create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(&[leaf_hash], 0)
                .expect("track proof")
                .try_into()
                .expect("proof length");

        let mut tape = Tape::history(node_id, span.end_epoch);
        tape.tracks = TrackArchive {
            tree,
            next_number: TrackNumber(1),
            num_tracks: 1,
        };
        let pool_rate = PoolRate::new(span, CompressedTrackProof { state: track, proof });
        (tape, pool_rate)
    }

    #[test]
    fn request_stake_unlock() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let e0: EpochNumber = EpochNumber(42);     // stake activation epoch
        let e2: EpochNumber = e0.saturating_add(EpochNumber(2)); // current epoch
        let e4: EpochNumber = e2 + EpochNumber(2); // unstake epoch

        let (system_address, _) = system_pda();
        let (stake_address, _) = stake_pda(authority.into());
        let (history_address, _) = history_pda(pool_address.into());

        let activation_rate = ExchangeRate { tape: 1000, other: 9000 };

        // Closed span containing the activator's mint epoch (e0).
        // Lookup at e0.prev() falls within [e0-1, e0+1).
        let span = RateSpan {
            node: pool_address.into(),
            start_epoch: e0.prev(),
            end_epoch: e0 + EpochNumber(1),
            rate: activation_rate,
        };
        let (history_tape, pool_rate) =
            make_closed_span(NodeId(5), history_address.into(), span);

        let instruction = build_request_stake_unlock_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
            pool_rate,
        );

        let system = System {
            current_epoch: e2,
            ..System::zeroed()
        };

        let mut node = Node::zeroed();
        node.id = NodeId(5);
        node.latest_advance_epoch = e2;
        node.rate_span_start = e0 + EpochNumber(1);
        node.pool.stake = TAPE(activation_rate.tape);
        node.pool.shares = ShareAmount(activation_rate.other);

        let mut stake = Stake::zeroed();
        stake.authority = authority.into();
        stake.pool = pool_address.into();
        stake.inner = StakedTape::new(TAPE(1000), e0);

        let shares = activation_rate.convert_to_other_amount(stake.inner.amount.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(stake_address, stake.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history_tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(stake_address)).data(
                    Stake {
                        inner: StakedTape {
                            state: StakeState {
                                phase: StakePhase::Unlocking.into(),
                                unstake_epoch: e4,
                            },
                            unlock_shares: ShareAmount(shares),
                            ..stake.inner
                        },
                        ..stake
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(pool_address)).data(
                    Node {
                        pool: StakingPool {
                            schedule: PoolSchedule {
                                outgoing_shares: EpochValues::try_from(
                                    &[e4],
                                    &[shares],
                                ).expect("schedule outgoing"),
                                ..node.pool.schedule
                            },
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
