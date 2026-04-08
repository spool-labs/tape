use tape_api::program::prelude::*;

pub fn process_stake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeTokens::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        pool_info,
        vault_info,

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

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    pool_info
        .not_empty()?
        .has_owner(&tapedrive::ID)?;

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;

    let (stake_address, _) = stake_pda((*authority_info.key).into());
    let (vault_address, bump)   = vault_pda(stake_address);

    vault_info
        .has_address(&vault_address.into())?
        .is_writable()?;

    // If the PDA token account doesn't exist yet, create it; otherwise validate it.
    if vault_info.data_is_empty() {
        create_token_account(
            fee_payer_info,
            vault_info,
            mint_info,
            system_program_info,
            &[VAULT, stake_address.as_ref()],
            bump,
        )?;
    } else {
        vault_info
            .as_token_account()?
            .assert(|t| t.owner() == *vault_info.key)?
            .assert(|t| t.mint() == MINT_ADDRESS.into())?;
    }

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    transfer(
        authority_info,
        authority_ata_info,
        vault_info,
        token_program_info,
        amount.into(),
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
    fn test_stake() {
        let amount: u64 = 1000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction =
            build_stake_ix(fee_payer.into(), authority.into(), pool_address.into(), amount.into());

        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);
        let authority_ata = ata_address(&authority);

        let pool = Node::zeroed();

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(pool_address, pool.pack(), tapedrive::ID),
            empty(to_pubkey(vault_address)),
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
                Check::account(&authority_ata).data(
                    token(
                        authority_ata,
                        authority,
                        initial_token_balance - amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&to_pubkey(vault_address)).data(
                    token(
                        to_pubkey(vault_address),
                        to_pubkey(vault_address),
                        amount
                    ).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_stake_existing() {
        let amount: u64 = 2_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction =
            build_stake_ix(fee_payer.into(), authority.into(), pool_address.into(), amount.into());

        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);
        let authority_ata = ata_address(&authority);

        let pool = Node::zeroed();

        let initial_token_balance: u64 = 10_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(pool_address, pool.pack(), tapedrive::ID),
            token(to_pubkey(vault_address), to_pubkey(vault_address), 0),
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
                // No rent change since the vault already existed
                Check::account(&fee_payer)
                    .lamports(1_000_000_000)
                    .build(),
                Check::account(&authority_ata).data(
                    token(
                        authority_ata,
                        authority,
                        initial_token_balance - amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&to_pubkey(vault_address)).data(
                    token(
                        to_pubkey(vault_address),
                        to_pubkey(vault_address),
                        amount
                    ).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
