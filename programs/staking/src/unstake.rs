use tape_api::prelude::*;
use steel::*;

pub fn process_unstake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeTokens::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        stake_info,
        vault_info,
        vault_ata_info,

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

    stake_info
        .not_empty()?
        .has_owner(&tapedrive::ID)?;

    let (vault_address, bump)  = vault_pda(*signer_info.key, *stake_info.key);
    let (vault_ata, _)         = vault_ata(vault_address);

    vault_info
        .has_address(&vault_address)?;

    vault_ata_info
        .is_writable()?
        .has_address(&vault_ata)?
        .as_token_account()?
        .assert(|t| t.owner() == *vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;

    let amount = vault_ata_info
        .as_token_account()?
        .amount();

    transfer_signed_with_bump(
        vault_info,
        vault_ata_info,
        signer_ata_info,
        token_program_info,
        amount,
        &[VAULT, signer_info.key.as_ref(), stake_info.key.as_ref()],
        bump,
    )?;

    close_token_account_signed_with_bump(
        vault_ata_info, 
        signer_info, 
        vault_info, 
        token_program_info, 
        &[VAULT, signer_info.key.as_ref(), stake_info.key.as_ref()],
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
        let amount: u64 = 1000;

        let signer = Pubkey::new_unique();
        let stake_address = Pubkey::new_unique();

        let instruction = build_unstake_ix(signer, stake_address);

        let (vault_address, _) = vault_pda(signer, stake_address);
        let vault_ata = ata_address(&vault_address);
        let signer_ata = ata_address(&signer);

        let stake = Stake::zeroed();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, 0),

            pda(stake_address, stake.pack(), tapedrive::ID),
            empty(vault_address),
            token(vault_ata, vault_address, amount),

            token_program(),
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
                    .lamports(1002039280).build(),
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
