use brine_tree::Leaf;
use tape_api::prelude::*;
use steel::*;

pub fn process_update(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let current_slot = Clock::get()?.slot;
    let args = Update::try_from_bytes(data)?;

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

    let segment_number = args.segment_number;
    let merkle_proof   = args.proof;

    assert!(args.old_data.len() == SEGMENT_SIZE);
    assert!(args.new_data.len() == SEGMENT_SIZE);
    assert!(merkle_proof.len() == PROOF_LEN);

    let old_leaf = Leaf::new(&[
        segment_number.as_ref(), // u64_le_bytes
        args.old_data.as_ref(),
    ]);

    let new_leaf = Leaf::new(&[
        segment_number.as_ref(), // u64_le_bytes
        args.new_data.as_ref(),
    ]);

    writer.state.try_replace_leaf(
        &merkle_proof,
        old_leaf, 
        new_leaf
    )
    .map_err(|_| TapeError::WriteFailed)?;

    let prev_slot = tape.tail_slot;

    tape.merkle_root = writer.state.get_root().to_bytes();
    tape.tail_slot   = current_slot;

    UpdateEvent {
        prev_slot,
        segment_number: u64::from_le_bytes(segment_number),
        address: tape_address.to_bytes(),
    }
    .log();

    Ok(())
}

