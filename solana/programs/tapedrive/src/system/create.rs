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
    let min_epoch_duration = args.min_epoch_duration;
    let max_epoch_duration = args.max_epoch_duration;

    let trust = TrustModel::try_from(args.trust_model)
        .map_err(|_| ProgramError::InvalidArgument)?;
    let code = validate_profile(trust, args.profile)
        .map_err(|_| ProgramError::InvalidArgument)?;

    if committee_size < u64::from(code.n()) {
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
    system.total_nodes = 0;
    system.committee_size = committee_size;
    system.target_group_count = spool_groups;
    system.live_group_count = 0;
    system.min_epoch_duration = min_epoch_duration;
    system.max_epoch_duration = max_epoch_duration;
    system.profile = args.profile;
    system.trust_model = args.trust_model;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{ClayParams, RSParams};
    use tape_test::*;

    fn args_of(config: &GenesisConfig) -> CreateSystem {
        CreateSystem {
            committee_size: config.committee_size,
            spool_groups: config.spool_groups,
            min_epoch_duration: config.min_epoch_duration,
            max_epoch_duration: config.max_epoch_duration,
            profile: config.profile.into(),
            trust_model: config.trust_model.into(),
        }
    }

    fn create_case(args: CreateSystem, check: Check) {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (system_address, _) = system_pda();

        let mut instruction = build_create_system_ix(
            fee_payer.into(),
            authority.into(),
            &GenesisConfig::simnet(),
        );
        instruction.data = args.to_bytes();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(system_address),

            system_program(),
            rent_sysvar(),
        ];

        test_env().process_instruction(&instruction, &accounts, &[check]);
    }

    #[test]
    fn create_system() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let config = GenesisConfig::simnet();
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
                        min_epoch_duration: config.min_epoch_duration,
                        max_epoch_duration: config.max_epoch_duration,
                        ..permissionless_system()
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    // a trusted cluster starts on its own profile with a committee of n
    #[test]
    fn custom() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let config = GenesisConfig::try_custom(profile, TrustModel::Trusted).expect("3 of 4");
        let (system_address, _) = system_pda();

        create_case(
            args_of(&config),
            Check::account(&Pubkey::from(system_address))
                .data(System {
                    committee_size: 4,
                    target_group_count: config.spool_groups,
                    min_epoch_duration: config.min_epoch_duration,
                    max_epoch_duration: config.max_epoch_duration,
                    ..trusted_system(profile)
                }.pack().as_ref())
                .build(),
        );
    }

    // a profile its code or trust model refuses can't create a network
    #[test]
    fn bad_profile() {
        let simnet = args_of(&GenesisConfig::simnet());
        let above_open_limit = EncodingProfile::clay(ClayParams::new(20, 8, 16));
        let no_parity = EncodingProfile::reed_solomon(RSParams::new(20, 0));
        let bad_helpers = EncodingProfile::clay(ClayParams::new(20, 7, 7));

        for profile in [above_open_limit, no_parity, bad_helpers, EncodingProfile::unknown()] {
            create_case(
                CreateSystem { profile, ..simnet },
                Check::err(ProgramError::InvalidArgument),
            );
        }
    }

    // a missing or unassigned trust model can't create a network
    #[test]
    fn bad_trust() {
        let simnet = args_of(&GenesisConfig::simnet());

        for trust_model in [0, 3] {
            create_case(
                CreateSystem { trust_model, ..simnet },
                Check::err(ProgramError::InvalidArgument),
            );
        }
    }

    // the committee holds at least one node per spool of a group
    #[test]
    fn small_committee() {
        let simnet = args_of(&GenesisConfig::simnet());

        create_case(
            CreateSystem { committee_size: 19, ..simnet },
            Check::err(TapeError::InsufficientCommittee.into()),
        );
    }
}
