use tape_api::prelude::*;
use steel::*;

pub fn process_unstake_from_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeFromPool::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        stake_info,
        vault_info,
        epoch_info,
        node_info,

        token_program_info,
        staking_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;


    token_program_info
        .is_program(&spl_token::ID)?;
    staking_program_info
        .is_program(&staking::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;


    let (stake_address, _) = stake_pda(*signer_info.key, *node_info.key);
    let (vault_address, _) = vault_pda(stake_address);

    let stake = stake_info
        .has_address(&stake_address)?
        .is_writable()?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if stake.authority != *signer_info.key || stake.pool != *node_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    vault_info
        .has_address(&vault_address)?
        .is_writable()?;

    let staked_tape = &mut stake.inner;

    // Must be in withdrawing state and withdraw epoch must have arrived
    if !staked_tape.is_withdrawing() {
        return Err(ProgramError::Custom(0));
    }

    let withdraw_epoch = staked_tape
        .state
        .withdraw_epoch()
        .ok_or(ProgramError::InvalidInstructionData)?;

    if withdraw_epoch > current_epoch(epoch) {
        return Err(ProgramError::Custom(1)); // Epoch not reached
    }

    // Compute owed rewards based on activation and withdraw exchange rates
    // Note: If withdraw <= activation, owed = 0 (per StakingPool::unstake)

    let activation_rate = node.history
        .rate_at(staked_tape.activation_epoch)
        .ok_or(ProgramError::Custom(2))?;

    let withdraw_rate = node.history
        .rate_at(withdraw_epoch)
        .ok_or(ProgramError::Custom(3))?;

    // Convert principal (TAPE) -> shares at activation, 
    // then shares -> TAPE at withdraw

    let shares = activation_rate
        .convert_to_other_amount(staked_tape.amount.into());

    if shares == 0 {
        return Err(ProgramError::Custom(4)); // Zero shares
    }

    let tokens_at_withdraw = withdraw_rate
        .convert_to_tape_amount(shares);

    let owed_rewards = tokens_at_withdraw
        .saturating_sub(staked_tape.amount.into());

    // Update pool accounting and stake state
    // TODO: pay the rewards from pool rewards balance
    let _total_rewards = node.pool
        .unstake(staked_tape, current_epoch(epoch), owed_rewards.into())
        .map_err(|_| ProgramError::Custom(5))?;

    // CPI into staking program to move tokens from 
    // vault -> signer ATA and close the vault

    solana_program::program::invoke(
        &build_unstake_ix(*signer_info.key, *node_info.key),
        &[
            signer_info.clone(),
            signer_ata_info.clone(),

            node_info.clone(),
            vault_info.clone(),
        ],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_unstake_from_pool() {
        // Principal and rates
        let principal: u64 = 1_000;

        let signer = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_unstake_from_pool_ix(signer, pool_address);

        let signer_ata = ata_address(&signer);
        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(signer, pool_address);
        let (vault_address, _) = vault_pda(stake_address);

        // Epoch timeline
        let e0: EpochNumber = EpochNumber(42);     // activation epoch
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);
        let e3: EpochNumber = e2 + EpochNumber(1);
        let e4: EpochNumber = e3 + EpochNumber(1); // withdraw epoch (== current)

        // Existing accounts
        let mut epoch = Epoch::zeroed();
        epoch.id = e4; // current epoch equals withdraw epoch

        let mut node = Node::zeroed();
        node.id = NodeId(7);

        // History: rate at activation and withdraw
        // arbitrary but consistent values
        let activation_rate = ExchangeRate { tape: 1000, other: 9000 };
        let withdraw_rate   = ExchangeRate { tape: 1200, other: 8800 };

        node.history.push(e0, activation_rate);
        node.history.push(e4, withdraw_rate);

        // Compute expected owed rewards for assertion
        let shares = activation_rate
            .convert_to_other_amount(TAPE(principal).into());
        let tokens_at_withdraw = withdraw_rate
            .convert_to_tape_amount(shares);
        let owed_rewards = tokens_at_withdraw
            .saturating_sub(principal);

        // Fund rewards so we can pay fully
        node.pool.rewards = owed_rewards.into();

        // Stake account prepared in "unlocking" state with withdraw at e4
        let stake = Stake {
            authority: signer,
            pool: pool_address,
            inner: StakedTape {
                amount: TAPE(principal),
                activation_epoch: e0,
                state: StakeState {
                    phase: StakePhase::Unlocking.into(),
                    unstake_epoch: e4,
                },
            },
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, 0),

            pda(stake_address, stake.pack(), tapedrive::ID),
            token(vault_address, vault_address, principal),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),

            token_program(),
            staking_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // Stake state moved to Withdrawn
                Check::account(&stake_address).data(
                    Stake {
                        authority: signer,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: TAPE(principal),
                            activation_epoch: e0,
                            state: *StakeState::new().set_withdrawn(),
                        },
                    }.pack().as_ref()
                ).build(),

                //// Pool rewards reduced by owed_rewards (cap was exact)
                //Check::account(&pool_address).data(
                //    Node {
                //        // TODO
                //        ..node
                //    }.pack().as_ref()
                //).build(),

                // Signer gets principal tokens and vault gets closed, rent refunded
                Check::account(&signer_ata).data(
                    token(
                        signer_ata,
                        signer,
                        principal
                    ).1.data.as_ref()
                ).build(),
                Check::account(&vault_address)
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&signer)
                    .lamports(1_000_000_000 + rent_token())
                    .build(),
            ],
        );
    }
}
