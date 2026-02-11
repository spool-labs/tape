use tape_solana::*;
use tape_api::prelude::*;

/// Create the system-owned snapshot tape.
///
/// Called once during network initialization (via `tape admin init`).
/// The tape is owned by the system PDA and used to store all epoch
/// snapshot tracks per epoch.
pub fn process_reserve_snapshot_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ReserveSnapshotTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        tape_info,
        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (system_address, _) = system_pda();

    system_info
        .is_writable()?
        .is_system()?
        .has_address(&system_address)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(system_address);

    tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&tape_address)?;

    // Create the tape account (PDA owned by system authority)
    create_program_account::<Tape>(
        tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[RESOURCE, system_address.as_ref()],
    )?;

    let tape = tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    tape.id = TapeNumber(0); // System tape, ID 0
    tape.authority = system_address;
    tape.active_epoch = current_epoch(epoch);
    tape.expiry_epoch = EpochNumber(u64::MAX); // Indefinite
    tape.capacity = StorageUnits(u64::MAX); // Unlimited
    tape.used = StorageUnits::zero();

    Ok(())
}
