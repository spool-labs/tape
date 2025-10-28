use tape_api::prelude::*;
use steel::*;

pub fn process_merge_pool_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergePoolStake::try_from_bytes(data)?;
    let [
        signer_info,
        recipient_info,

        source_stake_info,
        dest_stake_info,

        node_info,

        source_vault_info,
        dest_vault_info,

        token_program_info,
        staking_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    staking_program_info
        .is_program(&staking::ID)?;

    //let node = node_info
    //    .is_writable()?
    //    .as_account_mut::<Node>(&tapedrive::ID)?;

    // Derive addresses
    let (source_stake_address, _) = stake_pda(*signer_info.key, *node_info.key);
    let (dest_stake_address, _)   = stake_pda(*recipient_info.key, *node_info.key);

    let (source_vault_address, _) = vault_pda(source_stake_address);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    // Load/validate source stake
    source_stake_info
        .has_address(&source_stake_address)?
        .is_writable()?
        .is_type::<Stake>(&tapedrive::ID)?;

    let source_stake = source_stake_info
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if source_stake.authority != *signer_info.key || source_stake.pool != *node_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Load/validate destination stake
    dest_stake_info
        .has_address(&dest_stake_address)?
        .is_writable()?
        .is_type::<Stake>(&tapedrive::ID)?;

    let dest_stake = dest_stake_info
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    if dest_stake.authority != *recipient_info.key || dest_stake.pool != *node_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // For now, only allow merging staked positions with same activation epoch/state
    if !source_stake.inner.is_staked() || !dest_stake.inner.is_staked() {
        return Err(ProgramError::Custom(20));
    }

    if source_stake.inner.activation_epoch != dest_stake.inner.activation_epoch {
        return Err(ProgramError::Custom(21));
    }

    // Validate vaults
    source_vault_info
        .has_address(&source_vault_address)?
        .is_writable()?;
    dest_vault_info
        .has_address(&dest_vault_address)?
        .is_writable()?;

    // Amount to merge
    let amount = source_stake.inner.amount;

    // Update state: move amount to dest, zero out source
    dest_stake.inner.amount = dest_stake.inner.amount.saturating_add(amount);
    source_stake.inner.amount = TAPE::zero();

    // CPI into staking program to move all funds and close source vault
    solana_program::program::invoke(
        &build_merge_stake_ix(
            *signer_info.key,
            *node_info.key,
            *recipient_info.key,
        ),
        &[
            signer_info.clone(),
            recipient_info.clone(),

            node_info.clone(),
            source_vault_info.clone(),
            dest_vault_info.clone(),
        ],
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

        let signer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_merge_pool_stake_ix(signer, pool_address, recipient);

        let (source_stake_address, _) = stake_pda(signer, pool_address);
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient, pool_address);
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        // Prepare node
        let node = Node::zeroed();

        // Both stakes in staked state with matching activation epoch
        let e0: EpochNumber = EpochNumber(73);

        let source_stake = Stake {
            authority: signer,
            pool: pool_address,
            inner: StakedTape {
                amount: TAPE(amount),
                activation_epoch: e0,
                state: *StakeState::new().set_staked(),
            },
        };

        let dest_stake = Stake {
            authority: recipient,
            pool: pool_address,
            inner: StakedTape {
                amount: TAPE(initial_dest_balance),
                activation_epoch: e0,
                state: *StakeState::new().set_staked(),
            },
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            sol(recipient, 0),

            pda(source_stake_address, source_stake.pack(), tapedrive::ID),
            pda(dest_stake_address, dest_stake.pack(), tapedrive::ID),

            pda(pool_address, node.pack(), tapedrive::ID),

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

                // Destination stake receives principal; source amount becomes zero
                Check::account(&dest_stake_address).data(
                    Stake {
                        authority: recipient,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: TAPE(initial_dest_balance + amount),
                            activation_epoch: e0,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
                Check::account(&source_stake_address).data(
                    Stake {
                        authority: signer,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: TAPE(0),
                            activation_epoch: e0,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),

                // Vaults: move tokens and close source vault (rent refunded to signer)
                Check::account(&dest_vault_address).data(
                    token(
                        dest_vault_address,
                        dest_vault_address,
                        initial_dest_balance + amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&source_vault_address)
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

