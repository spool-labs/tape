use tape_api::prelude::*;

use crate::error::*;

pub fn process_finalize_snapshot_epoch(
    accounts: &[AccountInfo<'_>],
    data: &[u8],
) -> ProgramResult {
    let args = FinalizeSnapshotEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        epoch_info,
        snapshot_state_info,
        manifest_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info.is_signer()?.is_writable()?;

    let epoch = epoch_info.is_epoch()?.as_account::<Epoch>(&tapedrive::ID)?;
    let snapshot_state = snapshot_state_info
        .is_writable()?
        .is_snapshot_state()?
        .as_account_mut::<SnapshotState>(&tapedrive::ID)?;

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
    let manifest = manifest_info
        .has_address(&manifest_address.into())?
        .is_snapshot_manifest()?
        .as_account::<SnapshotManifest>(&tapedrive::ID)?;

    if manifest.parent_epoch != snapshot_state.tail_epoch {
        return Err(TapeError::SnapshotParentMismatch.into());
    }

    if manifest.certified_count != SPOOL_GROUP_COUNT as u16
        || manifest.group_bitmap.count_ones() != SPOOL_GROUP_COUNT
    {
        return Err(TapeError::SnapshotIncomplete.into());
    }

    snapshot_state.tail_epoch = manifest.epoch;

    SnapshotEpochFinalized {
        epoch: manifest.epoch,
        parent_epoch: manifest.parent_epoch,
        tail_epoch: snapshot_state.tail_epoch,
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

    fn full_group_bitmap() -> SnapshotGroupBitmap {
        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        for group_index in 0..SPOOL_GROUP_COUNT {
            group_bitmap.set(group_index);
        }
        group_bitmap
    }

    #[test]
    fn test_finalize_snapshot_epoch() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);

        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(43),
            ..Epoch::zeroed()
        };
        let snapshot_state = SnapshotState {
            tail_epoch: EpochNumber(41),
        };
        let manifest = SnapshotManifest {
            epoch: snapshot_epoch,
            parent_epoch: EpochNumber(41),
            tape: snapshot_tape_address,
            certified_count: SPOOL_GROUP_COUNT as u16,
            group_bitmap: full_group_bitmap(),
            reserved: [0; 7],
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let instruction = build_finalize_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            pda(manifest_address, manifest.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(snapshot_state_address))
                    .data(
                        SnapshotState {
                            tail_epoch: snapshot_epoch,
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }

    #[test]
    fn test_finalize_snapshot_epoch_rejects_incomplete_manifest() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);

        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(43),
            ..Epoch::zeroed()
        };
        let snapshot_state = SnapshotState {
            tail_epoch: EpochNumber(41),
        };
        let manifest = SnapshotManifest {
            epoch: snapshot_epoch,
            parent_epoch: EpochNumber(41),
            tape: snapshot_tape_address,
            certified_count: (SPOOL_GROUP_COUNT - 1) as u16,
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            reserved: [0; 7],
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let instruction = build_finalize_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            pda(manifest_address, manifest.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::SnapshotIncomplete.into())],
        );
    }
}
