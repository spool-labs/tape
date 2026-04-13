use tape_api::program::prelude::*;
use tape_core::types::SnapshotGroupBitmap;


pub fn process_init_snapshot_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = InitSnapshotEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        archive_info,
        manifest_info,
        snapshot_tape_info,
        system_program_info,
        rent_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?.is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    // todo: remove this account, we're not using it
    system_info
        .is_system()?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    // todo: we should be using the epoch from the epoch account, why are we passing it in as an arg.
    let snapshot_epoch = EpochNumber::unpack(args.epoch);

    let current_epoch = current_epoch(epoch);
    if current_epoch <= EpochNumber(1) {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    let expected_epoch = current_epoch - EpochNumber(1);
    if snapshot_epoch != expected_epoch {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    manifest_info
        .is_empty()?
        .is_writable()?
        .has_address(&manifest_address.into())?;

    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);
    snapshot_tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_tape_address.into())?;

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
    snapshot_tape.authority = system_address.into();
    snapshot_tape.active_epoch = snapshot_epoch;
    snapshot_tape.expiry_epoch = EpochNumber(u64::MAX);
    snapshot_tape.capacity = StorageUnits(u64::MAX);
    snapshot_tape.used = StorageUnits::zero();

    let manifest = manifest_info.as_account_mut::<SnapshotManifest>(&tapedrive::ID)?;
    manifest.epoch = snapshot_epoch;
    manifest.group_bitmap = SnapshotGroupBitmap::zeroed();
    manifest.chunk_size = StorageUnits::zero();
    manifest.groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];

    SnapshotInit {
        epoch: snapshot_epoch,
    }
    .log();

    Ok(())
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

        let instruction = build_init_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            empty(manifest_address),
            empty(snapshot_tape_address),
            system_program(),
            rent_sysvar(),
        ];

        let expected_manifest = SnapshotManifest {
            epoch: snapshot_epoch,
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            chunk_size: StorageUnits::zero(),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let expected_tape = Tape {
            id: TapeNumber(5),
            authority: system_address.into(),
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
                Check::account(&Pubkey::from(archive_address))
                    .data(
                        Archive {
                            tape_count: 5,
                            ..archive
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(manifest_address))
                    .data(expected_manifest.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .data(expected_tape.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_init_snapshot_epoch_rejects_closed_epoch() {
        // current_epoch = 3 → expected snapshot_epoch = 2, but we submit 1.
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(1);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(3),
            ..Epoch::zeroed()
        };

        let instruction = build_init_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
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
