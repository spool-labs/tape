use steel::*;
use tape_api::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

pub fn process_create_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateCommittee::try_from_bytes(data)?;
    let [
        signer_info, 
        committee_info,
        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let committee_number = CommitteeNumber::unpack(args.id);
    if !committee_number.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    let (committee_address, _) = committee_pda(committee_number);

    committee_info
        .is_empty()?
        .is_writable()?
        .has_address(&committee_address)?;

    let size = MAX_PERMITTED_DATA_INCREASE
        .min(Committee::get_size());
    
    create_account_with_size::<Committee>(
        committee_info,
        system_program_info,
        signer_info,
        size,
        &tape_api::ID,
        &[COMMITTEE, &committee_number.pack()],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_create() {
        let signer = Pubkey::new_unique();
        let committee_number = CommitteeNumber(0);

        let instruction = build_create_committee_ix(signer, committee_number);
        let (committee_address, _) = committee_pda(committee_number);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(committee_address),

            system_program(),
            rent_sysvar(),
        ];

        let size = MAX_PERMITTED_DATA_INCREASE
            .min(Committee::get_size());

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&committee_address)
                    .space(size)
                    .owner(&tape_api::ID)
                    .data_slice(0, &[Committee::discriminator()])
                    .build(),
            ]
        );
    }
}
