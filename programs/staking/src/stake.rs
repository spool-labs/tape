use tape_api::prelude::*;
use steel::*;

pub fn process_stake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeTokens::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        pool_info,
        vault_info,

        mint_info,
        token_program_info,
        system_program_info,
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

    pool_info
        .not_empty()?
        .has_owner(&tapedrive::ID)?;

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;

    let (stake_address, _)      = stake_pda(*signer_info.key, *pool_info.key);
    let (vault_address, bump)   = vault_pda(stake_address);

    vault_info
        .has_address(&vault_address)?
        .is_writable()?;

    // If the PDA token account doesn't exist yet, create it; otherwise validate it.
    if vault_info.is_empty().is_ok() {

        create_token_account(
            signer_info,
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
            .assert(|t| t.mint() == MINT_ADDRESS)?;

    }

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    transfer(
        signer_info,
        signer_ata_info,
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

    #[test]
    fn test_stake() {
        let amount: u64 = 1000;

        let signer = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_stake_ix(signer, pool_address, amount.into());

        let (stake_address, _) = stake_pda(signer, pool_address);
        let (vault_address, _) = vault_pda(stake_address);
        let signer_ata = ata_address(&signer);

        let pool = Node::zeroed();

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),

            pda(pool_address, pool.pack(), tapedrive::ID),
            empty(vault_address),
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
                Check::account(&signer)
                    .lamports(1_000_000_000 - rent_token())
                    .build(),
                Check::account(&signer_ata).data(
                    token(
                        signer_ata,
                        signer,
                        initial_token_balance - amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&vault_address).data(
                    token(
                        vault_address,
                        vault_address,
                        amount
                    ).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
