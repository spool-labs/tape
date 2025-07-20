use tape_api::prelude::*;
use steel::*;

pub fn process_finalize(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Finalize::try_from_bytes(data)?;
    let [
        signer_info, 
        tape_info,
        writer_info, 
        archive_info,
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let tape = tape_info
        .as_account_mut::<Tape>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    let writer = writer_info
        .as_account_mut::<Writer>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.tape == *tape_info.key,
            ProgramError::InvalidAccountData,
        )?;

    let archive = archive_info
        .is_archive()?
        .as_account_mut::<Archive>(&tape_api::ID)?;

    let (tape_address, _tape_bump) = tape_pda(*signer_info.key, &tape.name);
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    tape_info.has_address(&tape_address)?;
    writer_info.has_address(&writer_address)?;

    system_program_info
        .is_program(&system_program::ID)?;

    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Can't finalize if the tape with no data on it.
    check_condition(
        tape.state.eq(&u64::from(TapeState::Writing)),
        TapeError::UnexpectedState,
    )?;

    archive.tapes_stored = archive.tapes_stored.saturating_add(1);
    archive.bytes_stored = archive.bytes_stored.saturating_add(tape.total_size);

    tape.number            = archive.tapes_stored;
    tape.state             = TapeState::Finalized.into();
    tape.merkle_root       = writer.state.get_root().into();
    tape.header            = args.header;

    // Close the writer and return rent to signer.
    writer_info.close(signer_info)?;

    solana_program::msg!(
        "Finalizing tape {}",
        tape.number,
    );

    FinalizeEvent {
        tape: tape.number,
        address: tape_address.to_bytes()
    }
    .log();

    Ok(())
}

