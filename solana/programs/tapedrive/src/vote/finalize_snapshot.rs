use bytemuck::bytes_of;
use tape_api::event::SnapshotFinalized;
use tape_api::program::prelude::*;
use tape_crypto::hash::hash as hash_bytes;

pub fn process_finalize_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = FinalizeSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        voting_epoch_info,
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

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let voting_epoch_id = system.current_epoch;
    let target_epoch_id = voting_epoch_id.prev();

    if args.epoch != target_epoch_id {
        return Err(TapeError::BadEpochId.into());
    }

    let voting_epoch = voting_epoch_info
        .is_writable()?
        .is_epoch(voting_epoch_id)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if voting_epoch.state.phase != EpochPhase::Snapshot as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let target_epoch = target_epoch_info
        .is_epoch(target_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if !target_epoch.has_snapshot_hash() {
        return Err(TapeError::UnexpectedState.into());
    }

    let snapshot_hash = target_epoch.snapshot_hash;

    let tape_hash = hash_bytes(bytes_of(&args.tape));
    if tape_hash != snapshot_hash {
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

    voting_epoch.state.phase = EpochPhase::Active as u64;

    SnapshotFinalized {
        epoch: target_epoch_id,
        hash: snapshot_hash,
        snapshot_tape: snapshot_tape_address,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn finalize_accounts(
        fee_payer: Pubkey,
        target_epoch_id: EpochNumber,
        voting_phase: EpochPhase,
        target_epoch: Epoch,
    ) -> Vec<(Pubkey, solana_account::Account)> {
        let voting_epoch_id = target_epoch_id.next();

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(target_epoch_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        let voting_epoch = Epoch {
            id: voting_epoch_id,
            state: EpochState {
                phase: voting_phase as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(snapshot_tape_address),
            system_program(),
        ]
    }

    #[test]
    fn finalize_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let target_epoch_id = EpochNumber(11);
        let voting_epoch_id = target_epoch_id.next();

        let tape = Tape::snapshot(target_epoch_id);
        let snapshot_hash = hash_bytes(bytes_of(&tape));

        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(target_epoch_id);

        let target_epoch = Epoch {
            id: target_epoch_id,
            snapshot_hash,
            ..Epoch::zeroed()
        };

        let instruction =
            build_finalize_snapshot_ix(fee_payer.into(), target_epoch_id, tape);

        let accounts =
            finalize_accounts(fee_payer, target_epoch_id, EpochPhase::Snapshot, target_epoch);

        let expected_voting_epoch = Epoch {
            id: voting_epoch_id,
            state: EpochState {
                phase: EpochPhase::Active as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

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
                Check::account(&Pubkey::from(voting_epoch_address))
                    .data(expected_voting_epoch.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_malformed_snapshot_tape() {
        let fee_payer = Pubkey::new_unique();
        let target_epoch_id = EpochNumber(11);

        let mut tape = Tape::snapshot(target_epoch_id);
        tape.id = TapeNumber(7);
        let snapshot_hash = hash_bytes(bytes_of(&tape));

        let target_epoch = Epoch {
            id: target_epoch_id,
            snapshot_hash,
            ..Epoch::zeroed()
        };

        let instruction =
            build_finalize_snapshot_ix(fee_payer.into(), target_epoch_id, tape);

        let accounts =
            finalize_accounts(fee_payer, target_epoch_id, EpochPhase::Snapshot, target_epoch);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::UnexpectedState.into())],
        );
    }

    #[test]
    fn rejects_when_not_snapshot_phase() {
        let fee_payer = Pubkey::new_unique();
        let target_epoch_id = EpochNumber(11);

        let tape = Tape::snapshot(target_epoch_id);
        let snapshot_hash = hash_bytes(bytes_of(&tape));

        let target_epoch = Epoch {
            id: target_epoch_id,
            snapshot_hash,
            ..Epoch::zeroed()
        };

        let instruction =
            build_finalize_snapshot_ix(fee_payer.into(), target_epoch_id, tape);

        let accounts =
            finalize_accounts(fee_payer, target_epoch_id, EpochPhase::Active, target_epoch);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }
}
