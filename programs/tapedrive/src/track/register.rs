use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackRegistered;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_COUNT};
use tape_core::encoding::EncodingProfile;
use tape_crypto::merkle::root_from_leaf_hashes;
use crate::error::*;

pub fn process_register_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        epoch_info,
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

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(*authority_info.key);
    let (track_address, _) = track_pda(*authority_info.key, args.key);

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
        fee_payer_info,
        &tapedrive::ID,
        &[TRACK, authority_info.key.as_ref(), args.key.as_ref()],
    )?;

    let track_number = tape.track_count;
    let spool_group = track_number % SPOOL_GROUP_COUNT as u64;
    tape.track_count = tape.track_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let track = track_info.as_account_mut::<Track>(&tapedrive::ID)?;

    track.id   = track_number.into();
    track.tape = tape_address;
    track.key  = args.key;
    track.size = total_units;
    track.data = TrackData::new(
        current_epoch(epoch),
        args.commitment,
        spool_group,
    );
    let profile = EncodingProfile::unpack(args.profile);
    track.data.profile = profile;

    // Verify leaf hashes produce the commitment root
    let computed_root = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&args.leaves);
    if computed_root != args.commitment {
        return Err(TapeError::InvalidCommitment.into());
    }

    let new_used = tape.used
         .checked_add(total_units)
         .ok_or(ProgramError::ArithmeticOverflow)?;

     if new_used > tape.capacity { 
         return Err(TapeError::NoSpace.into()); 
     }

    tape.used = new_used;

    TrackRegistered {
        track: *track_info.key,
        tape: tape_address,
        key: args.key,
        size: total_units,
        commitment: args.commitment,
        epoch: current_epoch(epoch),
        profile,
        spool_group: spool_group.to_le_bytes(),
        stripe_size: args.stripe_size,
        stripe_count: args.stripe_count,
        leaves: args.leaves,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_register_track() {
        use tape_core::encoding::EncodingProfile;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let storage_units = StorageUnits(100);

        let data_root = Hash::new_unique();
        let bucket_hash = Hash::new_unique();
        let profile = EncodingProfile::clay_default();

        let leaves = [Hash::default(); SPOOL_GROUP_SIZE];
        // Compute valid commitment from leaves
        let commitment = tape_crypto::merkle::root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        let instruction = build_register_track_ix(
            fee_payer,
            authority,
            storage_units,
            data_root,
            commitment,
            bucket_hash,
            profile,
            0,
            0,
            leaves,
        );

        let (epoch_address, _) = epoch_pda();
        let (tape_address, _) = tape_pda(authority);
        let (track_address, _) = track_pda(authority, bucket_hash);

        // Setup existing accounts

        let epoch = Epoch::zeroed();
        let tape = Tape {
            authority: authority,
            capacity: StorageUnits(1000),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(100),
            track_count: 100,
            ..Tape::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            empty(track_address),

            system_program(),
            rent_sysvar(),
        ];

        // Build expected track data with profile
        // spool_group = tape.track_count % SPOOL_GROUP_COUNT = 100 % 50 = 0
        let mut expected_data = TrackData::new(EpochNumber(0), commitment, 0);
        expected_data.profile = profile;

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
                        data: expected_data,
                    }.pack().as_ref()
                ).build(),
                Check::account(&tape_address).data(
                    Tape {
                        authority: authority,
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
