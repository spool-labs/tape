use tape_api::prelude::*;
use steel::*;

pub fn process_merge_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergeStake::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        recipient_info,

        pool_info,
        source_vault_info,
        dest_vault_info,

        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    // No check done against "pool_info" to reduce risks of stake being locked due to parent
    // program changes

    token_program_info
        .is_program(&spl_token::ID)?;


    // Source vault token account
    let (source_stake_address, _)     = stake_pda(*authority_info.key);
    let (source_vault_address, bump)  = vault_pda(source_stake_address);

    source_vault_info
        .has_address(&source_vault_address)?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *source_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;


    // Destination vault token account
    let (dest_stake_address, _)       = stake_pda(*recipient_info.key);
    let (dest_vault_address, _)       = vault_pda(dest_stake_address);


    dest_vault_info
        .has_address(&dest_vault_address)?
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *dest_vault_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

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

    #[test]
    fn test_merge_stake() {
        let amount: u64 = 1_000;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();

        let instruction = build_merge_stake_ix(fee_payer, authority, pool_address, recipient);

        let (source_stake_address, _) = stake_pda(authority);
        let (source_vault_address, _) = vault_pda(source_stake_address);

        let (dest_stake_address, _) = stake_pda(recipient);
        let (dest_vault_address, _) = vault_pda(dest_stake_address);

        let pool = Node::zeroed();

        let initial_balance: u64 = 1_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            sol(recipient, 0),

            pda(pool_address, pool.pack(), tapedrive::ID),

            token(source_vault_address, source_vault_address, amount),
            token(dest_vault_address, dest_vault_address, initial_balance),

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
                Check::account(&source_vault_address)
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&dest_vault_address).data(
                    token(
                        dest_vault_address,
                        dest_vault_address,
                        amount + initial_balance
                    ).1.data.as_ref(),
                ).build(),
            ],
        );
    }
}
