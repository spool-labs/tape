use tape_api::program::prelude::*;

pub fn process_unstake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeTokens::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,
        vault_info,
        token_program_info,
        stake_authority_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    // Funds only move when the parent program co-signs with its stake authority,
    // which keeps withdrawals bound to the parent unbonding and pool accounting.
    let (stake_authority_address, _) = stake_authority_pda();
    stake_authority_info
        .is_signer()?
        .has_address(&stake_authority_address.into())?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let (stake_address, _) = stake_pda((*authority_info.key).into());
    let (vault_address, bump) = vault_pda(stake_address);

    vault_info
        .has_address(&vault_address.into())?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    let amount = vault_info
        .as_token_account()?
        .amount();

    transfer_signed_with_bump(
        vault_info,
        vault_info,
        authority_ata_info,
        token_program_info,
        amount,
        &[VAULT, stake_address.as_ref()],
        bump,
    )?;

    close_token_account_signed_with_bump(
        vault_info,
        authority_info,
        vault_info,
        token_program_info,
        &[VAULT, stake_address.as_ref()],
        bump,
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
    fn test_unstake() {
        let amount: u64 = 1000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_unstake_ix(fee_payer.into(), authority.into());

        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);
        let (stake_authority_address, _) = stake_authority_pda();
        let authority_ata = ata_address(&authority);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 0),
            token(to_pubkey(vault_address), to_pubkey(vault_address), amount),
            token_program(),
            sol(to_pubkey(stake_authority_address), 0),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&authority)
                    .lamports(rent_token()).build(),
                Check::account(&authority_ata).data(
                    token(
                        authority_ata,
                        authority,
                        amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&to_pubkey(vault_address))
                    .lamports(0)
                    .closed()
                    .build(),
            ]
        );
    }

    // a direct call without the parent stake authority signature is rejected
    #[test]
    fn unstake_requires_authority() {
        let amount: u64 = 1000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let mut instruction = build_unstake_ix(fee_payer.into(), authority.into());
        // An attacker cannot produce the parent program's PDA signature.
        instruction
            .accounts
            .last_mut()
            .expect("stake authority meta")
            .is_signer = false;

        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);
        let (stake_authority_address, _) = stake_authority_pda();
        let authority_ata = ata_address(&authority);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 0),
            token(to_pubkey(vault_address), to_pubkey(vault_address), amount),
            token_program(),
            sol(to_pubkey(stake_authority_address), 0),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::MissingRequiredSignature)],
        );
    }
}
