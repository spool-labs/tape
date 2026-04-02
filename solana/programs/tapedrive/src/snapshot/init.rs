use tape_api::prelude::*;

use crate::error::*;

pub fn process_init_snapshot_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = InitSnapshotEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        archive_info,
        snapshot_state_info,
        manifest_info,
        snapshot_tape_info,
        system_program_info,
        rent_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info.is_signer()?.is_writable()?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;

    let (system_address, _) = system_pda();
    system_info.is_system()?.has_address(&system_address)?;

    let epoch = epoch_info.is_epoch()?.as_account::<Epoch>(&tapedrive::ID)?;
    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;
    let snapshot_state = snapshot_state_info
        .is_snapshot_state()?
        .as_account::<SnapshotState>(&tapedrive::ID)?;

    let snapshot_epoch = EpochNumber::unpack(args.snapshot_epoch);
    let current_epoch = current_epoch(epoch);
    let expected_epoch = required_snapshot_epoch(current_epoch)?;
    let expected_parent = snapshot_state
        .tail_epoch
        .checked_add(EpochNumber(1))
        .ok_or(ProgramError::ArithmeticOverflow)?;

    if snapshot_epoch != expected_epoch || snapshot_epoch != expected_parent {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    manifest_info
        .is_empty()?
        .is_writable()?
        .has_address(&manifest_address)?;

    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);
    snapshot_tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_tape_address)?;

    create_program_account::<SnapshotManifest>(
        manifest_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT_MANIFEST, &snapshot_epoch.pack()],
    )?;

    create_program_account::<Tape>(
        snapshot_tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT_TAPE, &snapshot_epoch.pack()],
    )?;

    let tape_id = TapeNumber(
        archive
            .tape_count
            .checked_add(1)
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );
    archive.tape_count = tape_id.as_u64();

    let snapshot_tape = snapshot_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;
    snapshot_tape.id = tape_id;
    snapshot_tape.authority = system_address;
    snapshot_tape.active_epoch = snapshot_epoch;
    snapshot_tape.expiry_epoch = EpochNumber(u64::MAX);
    snapshot_tape.capacity = StorageUnits(u64::MAX);
    snapshot_tape.used = StorageUnits::zero();

    let manifest = manifest_info.as_account_mut::<SnapshotManifest>(&tapedrive::ID)?;
    manifest.epoch = snapshot_epoch;
    manifest.parent_epoch = snapshot_state.tail_epoch;
    manifest.tape = snapshot_tape_address;
    manifest.certified_count = 0;
    manifest.group_bitmap = SnapshotGroupBitmap::zeroed();
    manifest.reserved = [0; 7];
    manifest.groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];

    SnapshotEpochInitialized {
        epoch: snapshot_epoch,
        parent_epoch: manifest.parent_epoch,
        tape: snapshot_tape_address,
    }
    .log();

    Ok(())
}

fn required_snapshot_epoch(current_epoch: EpochNumber) -> Result<EpochNumber, ProgramError> {
    if current_epoch <= EpochNumber(1) {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    current_epoch
        .checked_sub(EpochNumber(1))
        .ok_or(TapeError::SnapshotEpochClosed.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_init_snapshot_epoch() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(1);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(2),
            ..Epoch::zeroed()
        };
        let archive = Archive {
            tape_count: 4,
            ..Archive::zeroed()
        };
        let snapshot_state = SnapshotState {
            tail_epoch: EpochNumber(0),
        };

        let instruction = build_init_snapshot_epoch_ix(fee_payer, snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            empty(manifest_address),
            empty(snapshot_tape_address),
            system_program(),
            rent_sysvar(),
        ];

        let expected_manifest = SnapshotManifest {
            epoch: snapshot_epoch,
            parent_epoch: EpochNumber(0),
            tape: snapshot_tape_address,
            certified_count: 0,
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            reserved: [0; 7],
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let expected_tape = Tape {
            id: TapeNumber(5),
            authority: system_address,
            capacity: StorageUnits(u64::MAX),
            used: StorageUnits::zero(),
            active_epoch: snapshot_epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&archive_address)
                    .data(
                        Archive {
                            tape_count: 5,
                            ..archive
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
                Check::account(&manifest_address)
                    .data(expected_manifest.pack().as_ref())
                    .build(),
                Check::account(&snapshot_tape_address)
                    .data(expected_tape.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_init_snapshot_epoch_rejects_closed_epoch() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(1);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(3),
            ..Epoch::zeroed()
        };
        let snapshot_state = SnapshotState {
            tail_epoch: EpochNumber(1),
        };

        let instruction = build_init_snapshot_epoch_ix(fee_payer, snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            empty(manifest_address),
            empty(snapshot_tape_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::SnapshotEpochClosed.into())],
        );
    }
}
