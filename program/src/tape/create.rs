use tape_api::prelude::*;
use tape_api::instruction::tape::Create;
use steel::*;

pub fn process_tape_create(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let current_slot = Clock::get()?.slot;
    let args = Create::try_from_bytes(data)?;
    let [
        signer_info, 
        tape_info,
        writer_info, 
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let (tape_address, _tape_bump) = tape_pda(*signer_info.key, &args.name);
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&tape_address)?;

    writer_info
        .is_empty()?
        .is_writable()?
        .has_address(&writer_address)?;

    system_program_info
        .is_program(&system_program::ID)?;

    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    create_program_account::<Tape>(
        tape_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[TAPE, signer_info.key.as_ref(), &args.name],
    )?;

    create_program_account::<Writer>(
        writer_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[WRITER, tape_info.key.as_ref()],
    )?;

    let tape = tape_info.as_account_mut::<Tape>(&tape_api::ID)?;
    let writer = writer_info.as_account_mut::<Writer>(&tape_api::ID)?;

    tape.number            = 0; // (tapes get a number when finalized)
    tape.authority         = *signer_info.key;
    tape.name              = args.name;
    tape.state             = TapeState::Created.into();
    tape.total_segments    = 0;
    tape.merkle_root       = [0; 32];
    tape.header            = [0; HEADER_SIZE];
    tape.first_slot        = current_slot; 
    tape.tail_slot         = current_slot;

    writer.tape            = *tape_info.key;
    writer.state           = SegmentTree::new(&[tape_info.key.as_ref()]);

    Ok(())
}
