use bytemuck::bytes_of;
use tape_api::event::SnapshotFinalized;
use tape_api::program::prelude::*;
use tape_crypto::hash::hash as hash_bytes;

pub fn process_finalize_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = FinalizeSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        target_epoch_info,
        snapshot_tape_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let target_epoch_id = EpochNumber::unpack(args.epoch);
    let target_epoch = target_epoch_info
        .is_epoch(target_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if !target_epoch.has_snapshot_hash() {
        return Err(TapeError::UnexpectedState.into());
    }

    let tape_hash = hash_bytes(bytes_of(&args.tape));
    if tape_hash != target_epoch.snapshot_hash {
        return Err(TapeError::InvalidCommitment.into());
    }

    if !args.tape.is_snapshot_tape(target_epoch_id) {
        return Err(TapeError::UnexpectedState.into());
    }

    let (snapshot_tape_address, _) = snapshot_tape_pda(target_epoch_id);

    snapshot_tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_tape_address.into())?;

    create_program_account::<Tape>(
        snapshot_tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT_TAPE, &target_epoch_id.pack()],
    )?;

    let tape = snapshot_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;
    *tape = args.tape;

    SnapshotFinalized {
        epoch: target_epoch_id,
        hash: target_epoch.snapshot_hash,
        snapshot_tape: snapshot_tape_address,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn finalize_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let target_epoch_id = EpochNumber(11);

        let tape = Tape {
            id: TapeNumber(0),
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch: target_epoch_id,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };

        let snapshot_hash = hash_bytes(bytes_of(&tape));

        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(target_epoch_id);

        let target_epoch = Epoch {
            id: target_epoch_id,
            snapshot_hash,
            ..Epoch::zeroed()
        };

        let instruction =
            build_finalize_snapshot_ix(fee_payer.into(), target_epoch_id, tape);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(snapshot_tape_address),
            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .owner(&tapedrive::ID)
                    .data(tape.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_malformed_snapshot_tape() {
        let fee_payer = Pubkey::new_unique();
        let target_epoch_id = EpochNumber(11);

        let tape = Tape {
            id: TapeNumber(7),
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch: target_epoch_id,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };
        let snapshot_hash = hash_bytes(bytes_of(&tape));

        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(target_epoch_id);

        let target_epoch = Epoch {
            id: target_epoch_id,
            snapshot_hash,
            ..Epoch::zeroed()
        };

        let instruction =
            build_finalize_snapshot_ix(fee_payer.into(), target_epoch_id, tape);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(snapshot_tape_address),
            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::UnexpectedState.into())],
        );
    }
}
