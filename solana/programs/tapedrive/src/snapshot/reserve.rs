use tape_api::program::prelude::*;
use tape_core::{snapshot::types::SnapshotState, types::GroupBitmap};

pub fn process_reserve_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ReserveSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        epoch_info,
        snapshot_info,
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

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    // Snapshot epoch is always for the previous epoch, the advance_epoch instruction ensures we
    // cannot go forward without certifying the previous epoch snapshot, so this guarantees the
    // snapshot epoch is always closed.

    let snapshot_epoch = prev_epoch(epoch);

    let (snapshot_address, _) = snapshot_pda(snapshot_epoch);
    snapshot_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_address.into())?;

    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);
    snapshot_tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_tape_address.into())?;

    create_program_account::<Snapshot>(
        snapshot_info,
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

    let tape = snapshot_tape_info
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    tape.authority = SYSTEM_ADDRESS;
    tape.active_epoch = snapshot_epoch;
    tape.id = TapeNumber(0);                   // snapshot tapes always have id 0, making them easy to find
    tape.expiry_epoch = EpochNumber(u64::MAX); // snapshot tapes never expire
    tape.capacity = StorageUnits(u64::MAX);    // snapshot tapes have no capacity limit
    tape.used = StorageUnits::zero();

    let snapshot = snapshot_info
        .as_account_mut::<Snapshot>(&tapedrive::ID)?;

    snapshot.epoch = snapshot_epoch;
    snapshot.group_bitmap = GroupBitmap::zeroed();
    snapshot.state = SnapshotState::Registered.into();

    SnapshotReserved {
        epoch: snapshot_epoch,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_reserve_snapshot() {

        todo!();
    }
}
