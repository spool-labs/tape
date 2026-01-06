use tape_api::prelude::*;
use steel::*;

pub fn process_unstake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = UnstakeTokens::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        pool_info,
        vault_info,

        token_program_info,
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
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    // No check done against "pool_info" to reduce risks of stake being locked due to parent
    // program changes

    token_program_info
        .is_program(&spl_token::ID)?;

    let (stake_address, _)   = stake_pda(*authority_info.key);
    let (vault_address, bump) = vault_pda(stake_address);

    vault_info
        .has_address(&vault_address)?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

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

    #[test]
    fn test_unstake() {
        let amount: u64 = 1000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_unstake_ix(fee_payer, authority, pool_address);

        let (stake_address, _) = stake_pda(authority);
        let (vault_address, _) = vault_pda(stake_address);
        let authority_ata = ata_address(&authority);

        let pool = Node::zeroed();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 0),

            pda(pool_address, pool.pack(), tapedrive::ID),
            token(vault_address, vault_address, amount),

            token_program(),
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
                Check::account(&vault_address)
                    .lamports(0)
                    .closed()
                    .build(),
            ]
        );
    }
}
