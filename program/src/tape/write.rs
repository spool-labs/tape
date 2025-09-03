use tape_api::prelude::*;
use steel::*;

pub fn process_tape_write(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let [
        signer_info, 
        tape_info,
        writer_info,
        clock_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let current_slot = Clock::from_account_info(clock_info)?.slot;

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

    let tape_address = tape_derive_pda(signer_info.key, &tape.name, tape.pda_bump as u8);
    let writer_address = writer_derive_pda(&tape_address, writer.pda_bump as u8);

    tape_info.has_address(&tape_address)?;
    writer_info.has_address(&writer_address)?;
        
    check_condition(
        tape.state.eq(&u64::from(TapeState::Created)) ||
        tape.state.eq(&u64::from(TapeState::Writing)),
        TapeError::UnexpectedState,
    )?;

    // Convert the data to a canonical segments of data 
    // and write them to the Merkle tree (all segments are 
    // written as SEGMENT_SIZE bytes, no matter the size 
    // of the data)

    let segments = data.chunks(SEGMENT_SIZE);
    let segment_count = segments.len() as u64;

    check_condition(
        tape.total_segments + segment_count <= MAX_SEGMENTS_PER_TAPE as u64,
        TapeError::TapeTooLong,
    )?;

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
