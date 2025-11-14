use steel::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_register_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterTrack::try_from_bytes(data)?;
    let [
        signer_info,

        epoch_info,
        tape_info,
        track_info,

        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(*signer_info.key);
    let (track_address, _) = track_pda(*signer_info.key, args.id);

    let tape = tape_info
        .is_writable()?
        .has_address(&tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    track_info
        .is_empty()?
        .is_writable()?
        .has_address(&track_address)?;

    if tape.expiry_epoch <= current_epoch(epoch) {
        return Err(TapeError::TapeExpired.into());
    }

    let total_units = StorageUnits::unpack(args.size);

    create_program_account::<Track>(
        track_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[TRACK, signer_info.key.as_ref(), args.id.as_ref()],
    )?;

    let track_number = tape.track_count;
    tape.track_count = tape.track_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let track = track_info.as_account_mut::<Track>(&tapedrive::ID)?;

    track.id = track_number.into();
    track.tape = tape_address;
    track.key = args.id;
    track.size = total_units;
    track.root = args.root;
    track.data = BlobData::new(
        current_epoch(epoch),
        args.commitment,
    );

    let new_used = tape.used
         .checked_add(total_units)
         .ok_or(ProgramError::ArithmeticOverflow)?;

     if new_used > tape.capacity { 
         return Err(TapeError::NoSpace.into()); 
     }

    tape.used = new_used;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_register_track() {
        let signer = Pubkey::new_unique();
        let storage_units = StorageUnits(100);

        let data_root = Hash::new_unique();
        let erasure_root = Hash::new_unique();
        let bucket_hash = Hash::new_unique();

        let instruction = build_register_track_ix(
            signer,
            storage_units,
            data_root,
            erasure_root,
            bucket_hash,
        );

        let (epoch_address, _) = epoch_pda();
        let (tape_address, _) = tape_pda(signer);
        let (track_address, _) = track_pda(signer, bucket_hash);

        // Setup existing accounts

        let epoch = Epoch::zeroed();
        let tape = Tape {
            authority: signer,
            capacity: StorageUnits(1000),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(100),
            track_count: 100,
            ..Tape::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),

            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            empty(track_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&track_address).data(
                    Track {
                        id: TrackNumber(100),
                        tape: tape_address,
                        key: bucket_hash,
                        size: storage_units,
                        root: data_root,
                        data: BlobData::new(
                            EpochNumber(0),
                            erasure_root,
                        ),
                    }.pack().as_ref()
                ).build(),
                Check::account(&tape_address).data(
                    Tape {
                        authority: signer,
                        capacity: tape.capacity,
                        used: storage_units,
                        active_epoch: tape.active_epoch,
                        expiry_epoch: tape.expiry_epoch,
                        track_count: 101,
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
