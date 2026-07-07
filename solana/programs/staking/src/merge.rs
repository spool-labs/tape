use tape_api::program::prelude::*;

pub fn process_merge_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergeStake::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        recipient_info,

        source_vault_info,
        dest_vault_info,

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

    recipient_info
        .is_signer()?;

    // Vault moves require the parent program to co-sign with its stake authority,
    // so pool accounting cannot be desynced by merging outside the parent.
    let (stake_authority_address, _) = stake_authority_pda();
    stake_authority_info
        .is_signer()?
        .has_address(&stake_authority_address.into())?;

    token_program_info
        .is_program(&spl_token::ID)?;


    // Source vault token account
    let (source_stake_address, _) = stake_pda((*authority_info.key).into());
    let (source_vault_address, bump)  = vault_pda(source_stake_address);

    source_vault_info
        .has_address(&source_vault_address.into())?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *source_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;


    // Destination vault token account
    let (dest_stake_address, _) = stake_pda((*recipient_info.key).into());
    let (dest_vault_address, _)       = vault_pda(dest_stake_address);


    dest_vault_info
        .has_address(&dest_vault_address.into())?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *dest_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    let amount = source_vault_info
        .as_token_account()?
        .amount();

    transfer_signed_with_bump(
        source_vault_info,
        source_vault_info,
        dest_vault_info,
        token_program_info,
        amount,
        &[VAULT, source_stake_address.as_ref()],
        bump,
    )?;

    close_token_account_signed_with_bump(
        source_vault_info,
        authority_info,
        source_vault_info,
        token_program_info,
        &[VAULT, source_stake_address.as_ref()],
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
    fn test_merge_stake() {
        let amount: u64 = 1_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let instruction =
            build_merge_stake_ix(fee_payer.into(), authority.into(), recipient.into());

        let (source_stake_address, _) = stake_pda(authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let (stake_authority_address, _) = stake_authority_pda();

        let initial_balance: u64 = 1_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            token(
                to_pubkey(source_vault_address),
                to_pubkey(source_vault_address),
                amount,
            ),
            token(
                to_pubkey(dest_vault_address),
                to_pubkey(dest_vault_address),
                initial_balance,
            ),

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
                Check::account(&to_pubkey(source_vault_address))
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&to_pubkey(dest_vault_address)).data(
                    token(
                        to_pubkey(dest_vault_address),
                        to_pubkey(dest_vault_address),
                        amount + initial_balance
                    ).1.data.as_ref(),
                ).build(),
            ],
        );
    }

    // a merge without the recipient's signature is rejected
    #[test]
    fn merge_requires_recipient() {
        let amount: u64 = 1_000;
        let initial_balance: u64 = 1_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mut instruction =
            build_merge_stake_ix(fee_payer.into(), authority.into(), recipient.into());
        instruction.accounts[2].is_signer = false;

        let (source_stake_address, _) = stake_pda(authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let (stake_authority_address, _) = stake_authority_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            token(
                to_pubkey(source_vault_address),
                to_pubkey(source_vault_address),
                amount,
            ),
            token(
                to_pubkey(dest_vault_address),
                to_pubkey(dest_vault_address),
                initial_balance,
            ),

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

    // a direct call without the parent stake authority signature is rejected
    #[test]
    fn merge_requires_authority() {
        let amount: u64 = 1_000;
        let initial_balance: u64 = 1_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mut instruction =
            build_merge_stake_ix(fee_payer.into(), authority.into(), recipient.into());
        // An attacker cannot produce the parent program's PDA signature.
        instruction
            .accounts
            .last_mut()
            .expect("stake authority meta")
            .is_signer = false;

        let (source_stake_address, _) = stake_pda(authority.into());
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient.into());
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let (stake_authority_address, _) = stake_authority_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            token(
                to_pubkey(source_vault_address),
                to_pubkey(source_vault_address),
                amount,
            ),
            token(
                to_pubkey(dest_vault_address),
                to_pubkey(dest_vault_address),
                initial_balance,
            ),

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
