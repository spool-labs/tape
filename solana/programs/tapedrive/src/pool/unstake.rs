use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::StakeWithdrawn;

pub fn process_unstake_from_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeFromPool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        archive_info,
        archive_ata_info,

        stake_info,
        vault_info,
        system_info,
        node_info,
        history_info,

        token_program_info,
        staking_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let (history_address, _) = history_pda((*node_info.key).into());

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    archive_info
        .is_archive()?;

    archive_ata_info
        .is_writable()?
        .is_archive_ata()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    staking_program_info
        .is_program(&staking::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let current = system.current_epoch;
    let prev = current.saturating_sub(EpochNumber(1));

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let history = history_info
        .has_address(&history_address.into())?
        .as_account::<History>(&tapedrive::ID)?
        .assert(|h| h.node == (*node_info.key).into())?;

    if node.latest_advance_epoch < prev {
        return Err(TapeError::NodeStale.into());
    }

    let (stake_address, _) = stake_pda((*authority_info.key).into());
    let (vault_address, _) = vault_pda(stake_address);

    let stake = stake_info
        .is_writable()?
        .has_address(&stake_address.into())?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if stake.authority != (*authority_info.key).into() || stake.pool != (*node_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    vault_info
        .has_address(&vault_address.into())?
        .is_writable()?;

    let staked_tape = &mut stake.inner;

    // Must be in withdrawing state and withdraw epoch must have arrived
    if !staked_tape.is_withdrawing() {
        return Err(TapeError::BadStakeState.into());
    }

    let withdraw_epoch = staked_tape
        .state
        .withdraw_epoch()
        .ok_or(ProgramError::InvalidInstructionData)?;

    if withdraw_epoch > current {
        return Err(TapeError::EpochNotReached.into());
    }

    // Compute owed rewards based on activation and withdraw exchange rates
    // Note: If withdraw <= activation, owed = 0 (per StakingPool::unstake)

    let activation_rate = history.inner
        .rate_at(staked_tape.activation_epoch)
        .ok_or(TapeError::RateMissing)?;

    let withdraw_rate = history.inner
        .rate_at(withdraw_epoch)
        .ok_or(TapeError::RateMissing)?;

    let shares = activation_rate
        .convert_to_other_amount(staked_tape.amount.into());

    if shares == 0 {
        return Err(TapeError::ZeroShares.into());
    }

    let tokens_at_withdraw = withdraw_rate
        .convert_to_tape_amount(shares);

    let owed_rewards = tokens_at_withdraw
        .saturating_sub(staked_tape.amount.into());

    // Update pool accounting and stake state
    let total_rewards = node.pool
        .unstake_from_pool(staked_tape, current, owed_rewards.into())
        .map_err(|_| TapeError::StakingFailed)?;

    solana_program::msg!(
        "Unstaking {} (owed rewards: {}, total rewards paid: {})",
        staked_tape.amount,
        owed_rewards,
        total_rewards,
    );

    // Transfer owed rewards from archive to authority ATA
    transfer_signed(
        archive_info,
        archive_ata_info,
        authority_ata_info,
        token_program_info,
        total_rewards.into(),
        &[ARCHIVE],
    )?;

    // Transfer out the principal, and close vault
    solana_program::program::invoke(
        &build_unstake_ix(
            (*fee_payer_info.key).into(),
            (*authority_info.key).into(),
        ),
        &[
            fee_payer_info.clone(),
            authority_info.clone(),
            authority_ata_info.clone(),
            vault_info.clone(),
            token_program_info.clone(),
        ],
    )?;

    StakeWithdrawn {
        stake: stake_address,
        authority: (*authority_info.key).into(),
        pool: (*node_info.key).into(),
        principal: staked_tape.amount.as_u64().to_le_bytes(),
        rewards: total_rewards.as_u64().to_le_bytes(),
    }.log();

    close_account(
        stake_info,
        fee_payer_info,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn unstake_from_pool() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let authority_ata = ata_address(&authority);
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (system_address, _) = system_pda();
        let (pool_address, _)  = node_pda(pool_owner.into());
        let (history_address, _) = history_pda(pool_address.into());
        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);

        let e0: EpochNumber = EpochNumber(42);     // activation epoch
        let e3: EpochNumber = e0 + EpochNumber(3);
        let e4: EpochNumber = e0 + EpochNumber(4); // withdraw epoch (== current)

        let instruction = build_unstake_from_pool_ix(
            fee_payer.into(),
            authority.into(),
            pool_address,
        );

        let system = System {
            current_epoch: e4,
            ..System::zeroed()
        };
        let archive = Archive::zeroed();
        let mut node = Node::zeroed();
        let mut history = History::zeroed();

        node.id = NodeId(7);
        node.latest_advance_epoch = e3;
        node.authority = pool_owner.into();

        let activation_rate = ExchangeRate { tape: 1000, other: 9000 };
        let withdraw_rate   = ExchangeRate { tape: 1200, other: 8800 };

        history.node = pool_address.into();
        history.inner.push(e0, activation_rate);
        history.inner.push(e4, withdraw_rate);

        let principal: u64 = 1_000;
        let shares = activation_rate
            .convert_to_other_amount(TAPE(principal).into());
        let tokens_at_withdraw = withdraw_rate
            .convert_to_tape_amount(shares);
        let reward = tokens_at_withdraw
            .saturating_sub(principal);

        node.pool.rewards = reward.into();

        let stake = Stake {
            authority: authority.into(),
            pool: pool_address.into(),
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
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 0),

            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, reward),

            pda(stake_address, stake.pack(), tapedrive::ID),
            token(vault_address, vault_address, principal),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),

            token_program(),
            staking_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(fee_payer))
                    .lamports(1_000_000_000 + rent(Stake::get_size()))
                    .build(),
                Check::account(&Pubkey::from(authority))
                    .lamports(rent_token())
                    .build(),
                Check::account(&Pubkey::from(stake_address))
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&Pubkey::from(vault_address))
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&Pubkey::from(archive_ata)).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(authority_ata)).data(
                    token(authority_ata, authority, principal + reward).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(pool_address)).data(
                    Node {
                        pool: StakingPool {
                            rewards: node.pool.rewards - TAPE(reward),
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }
}
