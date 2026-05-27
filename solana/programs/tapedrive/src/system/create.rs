use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_create_system(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateSystem::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (system_address, _) = system_pda();

    system_info
        .is_empty()?
        .is_writable()?
        .has_address(&system_address.into())?;

    let committee_size = args.committee_size;
    let spool_groups = args.spool_groups;
    let min_version = args.min_version;
    let min_epoch_duration = args.min_epoch_duration;
    let max_epoch_duration = args.max_epoch_duration;

    if committee_size < MIN_COMMITTEE_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if spool_groups == 0 {
        return Err(ProgramError::InvalidArgument);
    }
    if min_epoch_duration.0 == 0 || min_epoch_duration > max_epoch_duration {
        return Err(ProgramError::InvalidArgument);
    }

    create_program_account::<System>(
        system_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SYSTEM],
    )?;

    let system = system_info.as_account_mut::<System>(&tapedrive::ID)?;
    system.current_epoch = EpochNumber(0);
    system.min_version = min_version;
    system.total_nodes = 0;
    system.committee_size = committee_size;
    system.target_group_count = spool_groups;
    system.live_group_count = 0;
    system.min_epoch_duration = min_epoch_duration;
    system.max_epoch_duration = max_epoch_duration;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_system() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let config = GenesisConfig::local();
        let instruction = build_create_system_ix(
            fee_payer.into(),
            authority.into(),
            &config,
        );
        let (system_address, _) = system_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(system_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address))
                    .space(System::get_size())
                    .owner(&tapedrive::ID)
                    .data(System {
                        committee_size: config.committee_size,
                        target_group_count: config.spool_groups,
                        min_version: config.min_version,
                        min_epoch_duration: config.min_epoch_duration,
                        max_epoch_duration: config.max_epoch_duration,
                        ..System::zeroed()
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
