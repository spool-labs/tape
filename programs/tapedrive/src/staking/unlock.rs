//use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_request_stake_unlock(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RequestStakeUnlock::try_from_bytes(data)?;
    let [
        signer_info,
        stake_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let (stake_address, _) = stake_pda(*signer_info.key, *node_info.key);

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let stake = stake_info
        .has_address(&stake_address)?
        .is_writable()?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    let staked_tape = &mut stake.inner;
    let activation_rate = node.history
        .rate_at(staked_tape.activation_epoch)
        .ok_or(ProgramError::Custom(0))?;
    //  .ok_or(TapeError::MissingExchangeRate)?;

    solana_program::msg!("Activation rate: {:?}", activation_rate);

    node.pool
        .request_withdraw(staked_tape, current_epoch(epoch), activation_rate)
        .map_err(|_| ProgramError::Custom(1))?;
    //  .map_err(|_| TapeError::StakingFailed)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_request_stake_unlock() {
        let signer = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_request_stake_unlock_ix(signer, pool_address);

        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(signer, pool_address);

        // Setup existing accounts
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();
        let mut stake = Stake::zeroed();

        let e0: EpochNumber = EpochNumber(42);     // stake activation epoch
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1); // current epoch
        let e3: EpochNumber = e2 + EpochNumber(1);
        let e4: EpochNumber = e3 + EpochNumber(1); // unstake epoch

        epoch.id = e2;

        node.id = NodeId(5);
        node.pool.stake = TAPE(5000);
        node.history.push(e0, ExchangeRate { tape: 1000, other: 9000 });
        node.history.push(e1, ExchangeRate { tape: 1100, other: 8900 });
        node.history.push(e2, ExchangeRate { tape: 1200, other: 8800 });

        stake.authority = signer;
        stake.pool = pool_address;
        stake.inner = StakedTape::new(TAPE(1000), e0);

        // Calculate shares at activation
        let shares = node.history.rate_at(e0)
            .expect("rate at activation")
            .convert_to_other_amount(stake.inner.amount.into());

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(stake_address, stake.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
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
            ]
        );
    }
}
