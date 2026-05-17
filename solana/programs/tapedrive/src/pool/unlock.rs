use tape_api::program::prelude::*;
use tape_api::event::StakeUnlockRequested;

pub fn process_request_stake_unlock(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RequestStakeUnlock::try_from_bytes(data)?;
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

    let history = history_info
        .has_address(&history_address.into())?
        .as_account::<History>(&tapedrive::ID)?
        .assert(|h| h.node == (*node_info.key).into())?;

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

    let prev = current.saturating_sub(EpochNumber(1));
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
        let activation_rate = history.inner
            .rate_at(staked_tape.activation_epoch)
            .ok_or(TapeError::RateMissing)?;

        node.pool
            .request_withdraw(staked_tape, current, activation_rate)
            .map_err(|_| TapeError::StakingFailed)?;

        withdraw_epoch = current + EpochNumber(2);
    }

    StakeUnlockRequested {
        stake: stake_address,
        authority: (*authority_info.key).into(),
        pool: (*node_info.key).into(),
        amount: staked_tape.amount.pack(),
        withdraw_epoch,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn request_stake_unlock() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let e0: EpochNumber = EpochNumber(42);     // stake activation epoch
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1); // current epoch
        let e4: EpochNumber = e2 + EpochNumber(2); // unstake epoch

        let instruction = build_request_stake_unlock_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
        );

        let (system_address, _) = system_pda();
        let (stake_address, _) = stake_pda(authority.into());
        let (history_address, _) = history_pda(pool_address.into());

        let system = System {
            current_epoch: e2,
            ..System::zeroed()
        };

        let mut node = Node::zeroed();
        node.id = NodeId(5);
        node.latest_advance_epoch = e2;
        node.pool.stake = TAPE(5000);

        let mut history = History::zeroed();
        history.node = pool_address.into();
        history.inner.push(e0, ExchangeRate { tape: 1000, other: 9000 });
        history.inner.push(e1, ExchangeRate { tape: 1100, other: 8900 });
        history.inner.push(e2, ExchangeRate { tape: 1200, other: 8800 });

        let mut stake = Stake::zeroed();
        stake.authority = authority.into();
        stake.pool = pool_address.into();
        stake.inner = StakedTape::new(TAPE(1000), e0);

        let shares = history.inner.rate_at(e0)
            .expect("rate at activation")
            .convert_to_other_amount(stake.inner.amount.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(stake_address, stake.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
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
