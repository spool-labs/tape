use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_split_pool_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitPoolStake::try_from_bytes(data)?;
    let [
        fee_payer_info,
        source_authority_info,
        dest_authority_info,

        node_info,
        source_stake_info,
        dest_stake_info,
        source_vault_info,
        dest_vault_info,

        mint_info,
        token_program_info,
        system_program_info,
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
    system_program_info
        .is_program(&system_program::ID)?;
    staking_program_info
        .is_program(&staking::ID)?;
    mint_info
        .is_mint()?;

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    // Derive stake/vault addresses
    let (source_stake_address, _) = stake_pda(*source_authority_info.key);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    let (dest_stake_address, _) = stake_pda(*dest_authority_info.key);
    let (dest_vault_address, _) = vault_pda(dest_stake_address);

    // Validate source stake
    source_stake_info
        .has_address(&source_stake_address)?
        .is_writable()?
        .is_type::<Stake>(&tapedrive::ID)?;

    let source_stake = source_stake_info
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if source_stake.authority != *source_authority_info.key || source_stake.pool != *node_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Only allow splitting an actively staked position for now
    if !source_stake.inner.is_staked() {
        return Err(TapeError::NotStaked.into());
    }

    if source_stake.inner.amount < amount {
        return Err(ProgramError::InsufficientFunds);
    }

    // Validate source vault
    source_vault_info
        .has_address(&source_vault_address)?
        .is_writable()?;

    // Destination stake: must be empty (we'll create it), or already be the correct PDA and empty
    dest_stake_info
        .has_address(&dest_stake_address)?
        .is_writable()?
        .is_empty()?; // Enforce creation for simplicity

    // Create new destination Stake state account
    create_program_account::<Stake>(
        dest_stake_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[STAKE, dest_authority_info.key.as_ref()],
    )?;

    let dest_stake = dest_stake_info
        .is_type::<Stake>(&tapedrive::ID)?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    // Initialize dest stake with same activation epoch/state, amount = split amount
    dest_stake.authority = *dest_authority_info.key;
    dest_stake.pool      = *node_info.key;
    dest_stake.inner     = StakedTape {
        amount,
        activation_epoch: source_stake.inner.activation_epoch,
        state: source_stake.inner.state,
    };

    // Reduce the source amount
    source_stake.inner.amount = source_stake.inner.amount
        .saturating_sub(amount);

    // Validate destination vault
    // It may be empty (will be created by the staking program CPI) or already exist.
    dest_vault_info
        .has_address(&dest_vault_address)?
        .is_writable()?;

    // CPI into staking program to split the underlying vaults
    solana_program::program::invoke(
        &build_split_stake_ix(
            *fee_payer_info.key,
            *source_authority_info.key,
            *dest_authority_info.key,
            amount,
        ),
        &[
            fee_payer_info.clone(),

            source_authority_info.clone(),
            dest_authority_info.clone(),
            source_vault_info.clone(),
            dest_vault_info.clone(),

            mint_info.clone(),
            token_program_info.clone(),
            system_program_info.clone(),
        ],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use tape_test::*;

    #[test]
    fn test_split_pool_stake() {
        let amount: u64 = 1_000;
        let initial_source_balance: u64 = 5_000;

        let fee_payer = Pubkey::new_unique();
        let source_authority = Pubkey::new_unique();
        let dest_authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_split_pool_stake_ix(fee_payer, source_authority, pool_address, dest_authority, amount.into());

        let (source_stake_address, _) = stake_pda(source_authority);
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(dest_authority);
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        // Prepare a minimal node
        let node = Node::zeroed();

        // Source stake in staked state
        let e0: EpochNumber = EpochNumber(100);
        let source_stake = Stake {
            authority: source_authority,
            pool: pool_address,
            inner: StakedTape {
                amount: TAPE(initial_source_balance),
                activation_epoch: e0,
                state: *StakeState::new().set_staked(),
            },
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(source_authority, 0),
            sol(dest_authority, 0),

            pda(pool_address, node.pack(), tapedrive::ID),

            // Stake state accounts
            pda(source_stake_address, source_stake.pack(), tapedrive::ID),
            empty(dest_stake_address),

            // Vaults
            token(source_vault_address, source_vault_address, initial_source_balance),
            empty(dest_vault_address),

            // Programs and mint
            mint(0),
            token_program(),
            system_program(),
            staking_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // Source stake amount reduced
                Check::account(&source_stake_address).data(
                    Stake {
                        authority: source_authority,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: TAPE(initial_source_balance - amount),
                            activation_epoch: e0,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),

                // Destination stake account created with split amount, same activation/state
                Check::account(&dest_stake_address).data(
                    Stake {
                        authority: dest_authority,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: TAPE(amount),
                            activation_epoch: e0,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),

                // Vault balances moved, destination vault created
                Check::account(&source_vault_address).data(
                    token(
                        source_vault_address,
                        source_vault_address,
                        initial_source_balance - amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&dest_vault_address).data(
                    token(
                        dest_vault_address,
                        dest_vault_address,
                        amount
                    ).1.data.as_ref()
                ).build(),
            ],
        );
    }
}
