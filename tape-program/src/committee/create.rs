use steel::*;
use tape_api::prelude::*;

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

    let epoch_number = EpochNumber::unpack(args.epoch);
    let (committee_address, _) = committee_pda(epoch_number);
    committee_info
        .is_empty()?
        .is_writable()?
        .has_address(&committee_address)?;

    // Check programs and sysvars.
    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    create_program_account::<Committee>(
        committee_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[COMMITTEE, &epoch_number.pack()],
    )?;

    let committee = committee_info.as_account_mut::<Committee>(&tape_api::ID)?;
    committee.epoch = epoch_number;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_create() {
        let signer = Pubkey::new_unique();
        let epoch_number = EpochNumber(0);

        let instruction = build_create_committee(signer, epoch_number);
        let (committee_address, _) = committee_pda(epoch_number);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(committee_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(&instruction, &accounts);
    }
}
