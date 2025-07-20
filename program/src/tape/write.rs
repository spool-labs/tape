use tape_api::prelude::*;
use steel::*;

pub fn process_write(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let current_slot = Clock::get()?.slot;
    let [
        signer_info, 
        tape_info,
        writer_info,
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

    let (tape_address, _tape_bump) = tape_pda(*signer_info.key, &tape.name);
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    tape_info.has_address(&tape_address)?;
    writer_info.has_address(&writer_address)?;
        
    check_condition(
        tape.state.eq(&u64::from(TapeState::Created)) ||
        tape.state.eq(&u64::from(TapeState::Writing)),
        TapeError::UnexpectedState,
    )?;

    check_condition(
        tape.total_size as usize + data.len() <= MAX_TAPE_SIZE,
        TapeError::TapeTooLong,
    )?;

    // Convert the data to a canonical segments of data 
    // and write them to the Merkle tree (all segments are 
    // written as SEGMENT_SIZE bytes, no matter the size 
    // of the data)

    let segments = data.chunks(SEGMENT_SIZE);
    let segment_count = segments.len() as u64;
    for (segment_number, segment) in segments.enumerate() {
        let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

        write_segment(
            &mut writer.state,
            tape.total_segments + segment_number as u64,
            &canonical_segment,
        )?;
    }

    let prev_slot = tape.tail_slot;

    tape.total_segments   += segment_count;
    tape.total_size       += data.len() as u64;
    tape.merkle_root       = writer.state.get_root().to_bytes();
    tape.state             = TapeState::Writing.into();
    tape.tail_slot         = current_slot;

    WriteEvent {
        prev_slot,
        num_added: segment_count,
        num_total: tape.total_segments,
        address: tape_address.to_bytes(),
    }
    .log();

    Ok(())
}
