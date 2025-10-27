use tape_api::prelude::*;
use tape_api::program::staking::STAKE;
use steel::*;

pub fn process_stake_tokens(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeTokens::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        pool_info,
        stake_ata_info,
        mint_info,

        token_program_info,
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

    mint_info
        .is_mint()?;

    pool_info
        .is_type::<Node>(&tapedrive::ID)?;

    let (stake_ata, bump) = stake_ata(*signer_info.key, *pool_info.key);

    stake_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_ata)?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let amount = TAPE::unpack(args.amount);

    create_token_account(
        signer_info,
        stake_ata_info,
        mint_info,
        system_program_info,
        rent_info,
        &[STAKE, signer_info.key.as_ref(), pool_info.key.as_ref()],
        bump
    )?;

    transfer(
        signer_info,
        signer_ata_info,
        stake_ata_info,
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

        let signer_ata = ata_address(&signer);
        let (stake_ata, _) = stake_ata(signer, pool);

        let node = Node::zeroed();

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),

            pda(pool, node.pack(), tapedrive::ID),
            empty(stake_ata),
            mint(0),

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
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_token_balance - amount).1.data.as_ref()
                ).build(),
                Check::account(&stake_ata).data(
                    token(stake_ata, stake_ata, amount).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
