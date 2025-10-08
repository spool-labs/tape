use steel::*;
use tape_api::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

pub fn process_expand_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ExpandEpoch::try_from_bytes(data)?;
    let [
        signer_info, 
        epoch_info,
        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (epoch_address, _) = epoch_pda();

    epoch_info
        .is_type::<Epoch>(&tape_api::ID)?
        .is_writable()?
        .has_address(&epoch_address)?;

    let current_size = epoch_info.data_len();
    let required_size = Epoch::get_size();

    if current_size == 0 {
        return Err(ProgramError::UninitializedAccount);
    }

    if current_size >= required_size {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    let new_size = current_size
        .saturating_add(MAX_PERMITTED_DATA_INCREASE)
        .min(required_size);

    resize_account(
        epoch_info,
        system_program_info,
        signer_info,
        new_size,
    )?;

    if new_size == required_size {
        let epoch = epoch_info.as_account_mut::<Epoch>(&tape_api::ID)?;
        epoch.id = EpochNumber::zero();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_epoch_expand() {
        let signer = Pubkey::new_unique();

        let instruction = build_expand_epoch_ix(signer);
        let (epoch_address, _) = epoch_pda();

        // Create an epoch account with half the required size
        let partial_account = Epoch::zeroed()
            .pack()[..Epoch::get_size()/2].to_vec();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(epoch_address, partial_account),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch { 
                        id: EpochNumber::zero(),
                        state: EpochState::zeroed(),
                        last_epoch_ms: 0,
                        leaders: CandidateSet::zeroed(),
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
