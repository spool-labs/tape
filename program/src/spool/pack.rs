use tape_api::prelude::*;
use tape_api::instruction::spool::Pack;
use brine_tree::Leaf;
use steel::*;

pub fn process_spool_pack(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let pack_args = Pack::try_from_bytes(data)?;
    let [
        signer_info, 
        spool_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let spool = spool_info
        .as_account_mut::<Spool>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    let tape = tape_info
        .as_account::<Tape>(&tape_api::ID)?
        .assert_err(
            |p| p.state  == u64::from(TapeState::Finalized),
            TapeError::UnexpectedState.into()
        )?
        .assert_err(
            |p| p.number > 0,
            TapeError::UnexpectedState.into()
        )?;

    check_condition(
        spool.total_tapes as usize <= MAX_TAPES_PER_SPOOL,
        TapeError::SpoolTooManyTapes,
    )?;

    let tape_id = tape.number.to_le_bytes();
    let leaf = Leaf::new(&[
        tape_id.as_ref(), // u64 (8 bytes)
        &pack_args.value,
    ]);

    check_condition(
        spool.state.try_add_leaf(leaf).is_ok(),
        TapeError::SpoolPackFailed,
    )?;
    
    spool.total_tapes += 1;

    Ok(())
}
