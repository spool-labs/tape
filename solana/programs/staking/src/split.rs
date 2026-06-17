use tape_api::program::prelude::*;

pub fn process_split_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitStake::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        recipient_info,

        source_vault_info,
        dest_vault_info,

        mint_info,
        token_program_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    recipient_info
        .is_signer()?;

    // No check done against "pool_info" to reduce risks of stake being locked due to parent
    // program changes

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;

    let amount = args.amount;
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    // Source vault token account
    let (source_stake_address, _) = stake_pda((*authority_info.key).into());
    let (source_vault_address, source_bump) = vault_pda(source_stake_address);

    source_vault_info
        .has_address(&source_vault_address.into())?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *source_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    // Destination vault token account must be empty; we'll create it
    let (dest_stake_address, _) = stake_pda((*recipient_info.key).into());
    let (dest_vault_address, dest_bump)     = vault_pda(dest_stake_address);

    dest_vault_info
        .is_writable()?
        .has_address(&dest_vault_address.into())?;

    // If the PDA token account doesn't exist yet, create it; otherwise validate it.
    if dest_vault_info.is_empty().is_ok() {
        create_token_account(
            fee_payer_info,
            dest_vault_info,
            mint_info,
            system_program_info,
            &staking::ID,
            &[VAULT, dest_stake_address.as_ref()],
            dest_bump,
        )?;
    } else {
        dest_vault_info
            .as_token_account()?
            .assert(|t| t.owner() == *dest_vault_info.key)?
            .assert(|t| t.mint() == MINT_ADDRESS.into())?;
    }

    transfer_signed_with_bump(
        source_vault_info,
        source_vault_info,
        dest_vault_info,
        token_program_info,
        amount.into(),
        &[VAULT, source_stake_address.as_ref()],
        source_bump,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn to_pubkey(address: impl Into<Pubkey>) -> Pubkey {
        address.into()
    }

    #[test]
    fn test_split_stake() {
        let amount: u64 = 1_000;
        let initial_source_balance: u64 = 5_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let instruction = build_split_stake_ix(
            fee_payer.into(),
            authority.into(),
            recipient.into(),
            amount.into(),
        );

        let (source_stake_address, _) = stake_pda(authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            token(
                to_pubkey(source_vault_address),
                to_pubkey(source_vault_address),
                initial_source_balance,
            ),
            empty(to_pubkey(dest_vault_address)),

            mint(0),
            token_program(),
            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&fee_payer)
                    .lamports(1_000_000_000 - rent_token())
                    .build(),
                Check::account(&to_pubkey(source_vault_address)).data(
                    token(
                        to_pubkey(source_vault_address),
                        to_pubkey(source_vault_address),
                        initial_source_balance - amount
                    ).1.data.as_ref(),
                ).build(),
                Check::account(&to_pubkey(dest_vault_address)).data(
                    token(
                        to_pubkey(dest_vault_address),
                        to_pubkey(dest_vault_address),
                        amount
                    ).1.data.as_ref(),
                ).build(),
            ],
        );
    }

    #[test]
    fn test_split_existing() {
        let amount: u64 = 1_000;
        let initial_source_balance: u64 = 5_000;
        let initial_dest_balance: u64 = 1_500;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let instruction = build_split_stake_ix(
            fee_payer.into(),
            authority.into(),
            recipient.into(),
            amount.into(),
        );

        let (source_stake_address, _) = stake_pda(authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            token(
                to_pubkey(source_vault_address),
                to_pubkey(source_vault_address),
                initial_source_balance
            ),
            token(
                to_pubkey(dest_vault_address),
                to_pubkey(dest_vault_address),
                initial_dest_balance
            ),

            mint(0),
            token_program(),
            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&fee_payer)
                    .lamports(1_000_000_000)
                    .build(),
                Check::account(&to_pubkey(source_vault_address)).data(
                    token(
                        to_pubkey(source_vault_address),
                        to_pubkey(source_vault_address),
                        initial_source_balance - amount
                    ).1.data.as_ref(),
                ).build(),
                Check::account(&to_pubkey(dest_vault_address)).data(
                    token(
                        to_pubkey(dest_vault_address),
                        to_pubkey(dest_vault_address),
                        initial_dest_balance + amount
                    ).1.data.as_ref(),
                ).build(),
            ],
        );
    }
}
