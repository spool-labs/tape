use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_merge_pool_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergePoolStake::try_from_bytes(data)?;
    let [
        fee_payer_info,
        source_authority_info,
        dest_authority_info,

        node_info,
        source_stake_info,
        dest_stake_info,
        source_vault_info,
        dest_vault_info,

        token_program_info,
        staking_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    source_authority_info
        .is_signer()?;

    dest_authority_info
        .is_signer()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    staking_program_info
        .is_program(&staking::ID)?;

    // Derive addresses
    let (source_stake_address, _) = stake_pda((*source_authority_info.key).into());
    let (dest_stake_address, _) = stake_pda((*dest_authority_info.key).into());

    let (source_vault_address, _) = vault_pda(source_stake_address);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    // Load/validate source stake
    source_stake_info
        .has_address(&source_stake_address.into())?
        .is_writable()?
        .is_type::<Stake>(&tapedrive::ID)?;

    let source_stake = source_stake_info
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if source_stake.authority != (*source_authority_info.key).into() || source_stake.pool != (*node_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    // Load/validate destination stake
    dest_stake_info
        .has_address(&dest_stake_address.into())?
        .is_writable()?
        .is_type::<Stake>(&tapedrive::ID)?;

    let dest_stake = dest_stake_info
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if dest_stake.authority != (*dest_authority_info.key).into() || dest_stake.pool != (*node_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    // For now, only allow merging staked positions with same activation epoch/state
    if !source_stake.inner.is_staked() || !dest_stake.inner.is_staked() {
        return Err(TapeError::NotStaked.into());
    }

    if source_stake.inner.activation_epoch != dest_stake.inner.activation_epoch {
        return Err(TapeError::EpochMismatch.into());
    }

    // Validate vaults
    source_vault_info
        .has_address(&source_vault_address.into())?
        .is_writable()?;
    dest_vault_info
        .has_address(&dest_vault_address.into())?
        .is_writable()?;

    // Amount to merge
    let amount = source_stake.inner.amount;

    // Update state: move amount to dest, zero out source
    dest_stake.inner.amount = dest_stake.inner.amount.saturating_add(amount);
    source_stake.inner.amount = TAPE::zero();

    // CPI into staking program to move all funds and close source vault
    solana_program::program::invoke(
        &build_merge_stake_ix(
            (*fee_payer_info.key).into(),
            (*source_authority_info.key).into(),
            (*dest_authority_info.key).into(),
        ),
        &[
            fee_payer_info.clone(),

            source_authority_info.clone(),
            dest_authority_info.clone(),
            source_vault_info.clone(),
            dest_vault_info.clone(),

            token_program_info.clone(),
        ],
    )?;

    close_account(
        source_stake_info,
        fee_payer_info,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_merge_pool_stake() {
        let amount: u64 = 1_000;
        let initial_dest_balance: u64 = 1_500;

        let fee_payer = Pubkey::new_unique();
        let source_authority = Pubkey::new_unique();
        let dest_authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_merge_pool_stake_ix(fee_payer.into(), source_authority.into(), pool_address.into(), dest_authority.into());

        let (source_stake_address, _) = stake_pda(source_authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(dest_authority.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        // Prepare node
        let node = Node::zeroed();

        // Both stakes in staked state with matching activation epoch
        let e0: EpochNumber = EpochNumber(73);

        let source_stake = Stake {
            authority: source_authority.into(),
            pool: pool_address.into(),
            inner: StakedTape {
                amount: TAPE(amount),
                activation_epoch: e0,
                state: *StakeState::new().set_staked(),
            },
        };

        let dest_stake = Stake {
            authority: dest_authority.into(),
            pool: pool_address.into(),
            inner: StakedTape {
                amount: TAPE(initial_dest_balance),
                activation_epoch: e0,
                state: *StakeState::new().set_staked(),
            },
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(source_authority, 0),
            sol(dest_authority, 0),

            pda(pool_address, node.pack(), tapedrive::ID),
            pda(source_stake_address, source_stake.pack(), tapedrive::ID),
            pda(dest_stake_address, dest_stake.pack(), tapedrive::ID),
            token(source_vault_address, source_vault_address, amount),
            token(dest_vault_address, dest_vault_address, initial_dest_balance),

            token_program(),
            staking_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // fee_payer gets stake account rent refund (not vault rent - that goes to authority)
                Check::account(&Pubkey::from(fee_payer))
                    .lamports(1_000_000_000 + rent(source_stake.pack().len()))
                    .build(),
                // source_authority receives vault rent refund
                Check::account(&Pubkey::from(source_authority))
                    .lamports(rent_token())
                    .build(),
                Check::account(&Pubkey::from(source_stake_address))
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&Pubkey::from(source_vault_address))
                    .lamports(0)
                    .closed()
                    .build(),

                // Destination stake receives principal; source amount becomes zero
                Check::account(&Pubkey::from(dest_stake_address)).data(
                    Stake {
                        authority: dest_authority.into(),
                        pool: pool_address.into(),
                        inner: StakedTape {
                            amount: TAPE(initial_dest_balance + amount),
                            activation_epoch: e0,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),

                // Vaults: move tokens and close source vault (rent refunded to authority)
                Check::account(&Pubkey::from(dest_vault_address)).data(
                    token(
                        dest_vault_address,
                        dest_vault_address,
                        initial_dest_balance + amount
                    ).1.data.as_ref()
                ).build(),
            ],
        );
    }
}
