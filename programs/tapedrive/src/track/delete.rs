use tape_api::prelude::*;

pub fn process_delete_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = DeleteTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        tape_info,
        track_info,

        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let track = track_info
        .is_writable()?
        .as_account_mut::<Track>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(tape.authority);
    let (track_address, _) = track_pda(tape.authority, track.key);

    if tape.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if tape_address != *tape_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track_address != *track_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track.tape != tape_address {
        return Err(ProgramError::InvalidAccountData);
    }

    tape.used = tape.used
        .checked_sub(track.size)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    tape.track_count = tape.track_count
        .checked_sub(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    close_account(track_info, fee_payer_info)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_delete_track() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(authority);
        let (track_address, _) = track_pda(authority, bucket_hash);

        let track = Track {
            id: TrackNumber(100),
            tape: tape_address,
            key: bucket_hash,
            size: StorageUnits(250),
            data: TrackData::new(
                EpochNumber(10),
                Hash::new_unique(),
            ),
        };

        let tape = Tape {
            authority: authority,
            capacity: StorageUnits(1000),
            used: StorageUnits(250),
            active_epoch: EpochNumber(15),
            expiry_epoch: EpochNumber(100),
            track_count: 1,
            ..Tape::zeroed()
        };

        let instruction = build_delete_track_ix(fee_payer, authority, bucket_hash);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(track_address, track.pack(), tapedrive::ID),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&fee_payer)
                    .lamports(1_000_000_000 + rent(Track::get_size()))
                    .build(),
                Check::account(&track_address)
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&tape_address).data(
                    Tape {
                        used: StorageUnits(0),
                        track_count: 0,
                        ..tape
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }
}
