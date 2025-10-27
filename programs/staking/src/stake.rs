use tape_api::prelude::*;
use steel::*;

pub fn process_stake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeTokens::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        vault_info,
        vault_ata_info,
        pool_info,
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

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let (vault_address, _)     = vault_pda(*signer_info.key, *pool_info.key);
    let (vault_ata_address, _) = vault_ata(vault_address);

    vault_info
        .has_address(&vault_address)?;

    vault_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&vault_ata_address)?;

    pool_info
        .is_type::<Node>(&tapedrive::ID)?;

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

    let amount = TAPE::unpack(args.amount);

    create_associated_token_account(
        signer_info,
        vault_info,
        vault_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    transfer(
        signer_info,
        signer_ata_info,
        vault_ata_info,
        token_program_info,
        amount.into(),
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_stake() {
        let signer = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let amount: u64 = 1000;

        let instruction = build_stake_ix(signer, pool, amount.into());

        let (vault_address, _) = vault_pda(signer, pool);
        let vault_ata = ata_address(&vault_address);
        let signer_ata = ata_address(&signer);

        let node = Node::zeroed();

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),
            empty(vault_address),
            empty(vault_ata),

            pda(pool, node.pack(), tapedrive::ID),
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
                Check::account(&vault_address)
                    .space(0)
                    .build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_token_balance - amount).1.data.as_ref()
                ).build(),
                Check::account(&vault_ata).data(
                    token(vault_ata, vault_address, amount).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
