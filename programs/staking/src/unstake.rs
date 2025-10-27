use tape_api::prelude::*;
use steel::*;

pub fn process_unstake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeTokens::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        vault_info,
        vault_ata_info,
        pool_info,
        token_program_info,
        system_program_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    solana_program::msg!("1");

    signer_info
        .is_signer()?;

    solana_program::msg!("1");
    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    solana_program::msg!("1");
    // TODO: might need a bump stored somewhere or use signer_info.key instead of vault_info.key.
    // not sure we need the vault_info.key at all actually.
    let (vault_address, _)     = vault_pda(*signer_info.key, *pool_info.key);
    let (vault_ata_address, bump) = vault_ata(vault_address);

    solana_program::msg!("1");
    vault_info
        .has_address(&vault_address)?;

    solana_program::msg!("1");
    vault_ata_info
        .is_writable()?
        .has_address(&vault_ata_address)?
        .as_token_account()?
        .assert(|t| t.owner() == *vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    solana_program::msg!("1");
    pool_info
        .is_type::<Node>(&tapedrive::ID)?;

    solana_program::msg!("1");
    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;

    let amount = vault_ata_info
        .as_token_account()?
        .amount();

    solana_program::msg!("1");
    transfer_signed_with_bump(
        vault_info,
        vault_ata_info,
        signer_ata_info,
        token_program_info,
        amount,
        &[VAULT, signer_info.key.as_ref(), pool_info.key.as_ref()],
        bump,
    )?;

    solana_program::msg!("1");
    close_token_account_signed_with_bump(
        vault_ata_info, 
        signer_info, 
        vault_info, 
        token_program_info, 
        &[VAULT, signer_info.key.as_ref(), pool_info.key.as_ref()],
        bump,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_unstake() {
        let signer = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let amount: u64 = 1000;

        let instruction = build_unstake_ix(signer, pool);

        let (vault_address, _) = vault_pda(signer, pool);
        let vault_ata = ata_address(&vault_address);
        let signer_ata = ata_address(&signer);

        let node = Node::zeroed();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, 0),
            empty(vault_address),
            token(vault_ata, vault_address, amount),

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
                Check::account(&signer).lamports(1002039280).build(),
                Check::account(&vault_address)
                    .space(0)
                    .build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, amount).1.data.as_ref()
                ).build(),
                Check::account(&vault_ata).closed().build(),
            ]
        );
    }
}
