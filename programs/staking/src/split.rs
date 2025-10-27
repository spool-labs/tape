use tape_api::prelude::*;
use steel::*;

pub fn process_split_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitStake::try_from_bytes(data)?;
    let [
        signer_info,
        recipient_info,

        pool_info,

        source_vault_info,
        source_vault_ata_info,

        dest_vault_info,
        dest_vault_ata_info,

        mint_info,

        token_program_info,
        associated_token_program_info,
        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    pool_info
        .not_empty()?
        .has_owner(&tapedrive::ID)?;

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Source vault/ATA validation
    let (source_stake_address, _)     = stake_pda(*signer_info.key, *pool_info.key);
    let (source_vault_address, bump)  = vault_pda(source_stake_address);
    let (source_vault_ata, _)         = vault_ata(source_vault_address);

    source_vault_info
        .has_address(&source_vault_address)?;

    source_vault_ata_info
        .is_writable()?
        .has_address(&source_vault_ata)?
        .as_token_account()?
        .assert(|t| t.owner() == *source_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    // Destination must be empty
    let (dest_stake_address, _) = stake_pda(*recipient_info.key, *pool_info.key);
    let (dest_vault_address, _) = vault_pda(dest_stake_address);
    let (dest_vault_ata, _)     = vault_ata(dest_vault_address);

    dest_vault_info
        .has_address(&dest_vault_address)?;

    dest_vault_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&dest_vault_ata)?;

    let amount = TAPE::unpack(args.amount);
    if amount == TAPE::zero() {
        return Err(ProgramError::InvalidArgument);
    }

    create_associated_token_account(
        signer_info,
        dest_vault_info,
        dest_vault_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    transfer_signed_with_bump(
        source_vault_info,
        source_vault_ata_info,
        dest_vault_ata_info,
        token_program_info,
        amount.into(),
        &[VAULT, source_stake_address.as_ref()],
        bump,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_split_stake() {
        let amount: u64 = 1_000;
        let initial_source_balance: u64 = 5_000;

        let signer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_split_stake_ix(signer, pool_address, recipient, amount.into());

        // Derive source and destination vaults/ATAs
        let (source_stake_address, _) = stake_pda(signer, pool_address);
        let (source_vault_address, _) = vault_pda(source_stake_address);
        let source_vault_ata = ata_address(&source_vault_address);

        let (dest_stake_address, _) = stake_pda(recipient, pool_address);
        let (dest_vault_address, _) = vault_pda(dest_stake_address);
        let dest_vault_ata = ata_address(&dest_vault_address);

        let pool = Node::zeroed();

        let accounts = vec![
            // signer, recipient
            sol(signer, 1_000_000_000),
            sol(recipient, 0),

            // pool
            pda(pool_address, pool.pack(), tapedrive::ID),

            // source vault and ATA
            empty(source_vault_address),
            token(source_vault_ata, source_vault_address, initial_source_balance),

            // destination vault and ATA (ATA must be empty and will be created)
            empty(dest_vault_address),
            empty(dest_vault_ata),

            // mint and programs
            mint(0),
            token_program(),
            ata_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&signer)
                    .lamports(1_000_000_000 - rent_token())
                    .build(),
                Check::account(&source_vault_ata).data(
                    token(source_vault_ata, source_vault_address, initial_source_balance - amount).1.data.as_ref(),
                ).build(),
                Check::account(&dest_vault_ata).data(
                    token(dest_vault_ata, dest_vault_address, amount).1.data.as_ref(),
                ).build(),
            ],
        );
    }
}
