use crate::error::*;
use tape_api::prelude::*;
use tape_solana::*;

pub fn process_request_stake_unlock(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RequestStakeUnlock::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        stake_info,
        epoch_info,
        node_info,
        history_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let (history_address, _) = history_pda(*node_info.key);

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let history = history_info
        .has_address(&history_address)?
        .as_account::<History>(&tapedrive::ID)?
        .assert(|h| h.node == *node_info.key)?;

    if node.latest_epoch < prev_epoch(epoch) {
        return Err(TapeError::NodeStale.into());
    }

    let (stake_address, _) = stake_pda(*authority_info.key);

    let stake = stake_info
        .has_address(&stake_address)?
        .is_writable()?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if stake.authority != *authority_info.key || stake.pool != *node_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let staked_tape = &mut stake.inner;
    let activation_rate = history.inner
        .rate_at(staked_tape.activation_epoch)
        .ok_or(TapeError::RateMissing)?;

    solana_program::msg!("Activation rate: {:?}", activation_rate);

    node.pool
        .request_withdraw(staked_tape, current_epoch(epoch), activation_rate)
        .map_err(|_| TapeError::StakingFailed)?;

    // TODO: update/advance the node's state?

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_request_stake_unlock() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_request_stake_unlock_ix(fee_payer, authority, pool_address);

        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(authority);
        let (history_address, _) = history_pda(pool_address);

        // Setup existing accounts
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();
        let mut stake = Stake::zeroed();
        let mut history = History::zeroed();

        let e0: EpochNumber = EpochNumber(42);     // stake activation epoch
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1); // current epoch
        let e3: EpochNumber = e2 + EpochNumber(1);
        let e4: EpochNumber = e3 + EpochNumber(1); // unstake epoch

        epoch.id = e2;

        node.id = NodeId(5);
        node.latest_epoch = e2;
        node.pool.stake = TAPE(5000);

        history.node = pool_address;
        history.inner.push(e0, ExchangeRate { tape: 1000, other: 9000 });
        history.inner.push(e1, ExchangeRate { tape: 1100, other: 8900 });
        history.inner.push(e2, ExchangeRate { tape: 1200, other: 8800 });

        stake.authority = authority;
        stake.pool = pool_address;
        stake.inner = StakedTape::new(TAPE(1000), e0);

        // Calculate shares at activation
        let shares = history.inner.rate_at(e0)
            .expect("rate at activation")
            .convert_to_other_amount(stake.inner.amount.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(stake_address, stake.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&stake_address).data(
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
                Check::account(&pool_address).data(
                    Node {
                        pool: StakingPool {
                            schedule: PoolSchedule {
                                outgoing_shares: EpochValues::try_from(
                                    &[e4],
                                    &[shares],
                                ).expect("schedule incoming"),
                                ..node.pool.schedule
                            },
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address) // unchanged
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&history_address) // unchanged
                    .data(history.pack().as_ref())
                    .build(),
            ]
        );
    }
}
