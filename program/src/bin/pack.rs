use tape_api::prelude::*;
use tape_api::instruction::bin::Pack;
use brine_tree::Leaf;
use steel::*;

pub fn process_bin_pack(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let pack_args = Pack::try_from_bytes(data)?;
    let [
        signer_info, 
        tape_info,
        bin_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let bin = bin_info
        .as_account_mut::<Bin>(&tape_api::ID)?
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
        bin.total_tapes as usize + 1 < MAX_TAPES_PER_BIN,
        TapeError::BinTooManyTapes,
    )?;

    let tape_id = tape.number.to_le_bytes();
    let leaf = Leaf::new(&[
        tape_id.as_ref(), // u64 (8 bytes)
        &pack_args.value,
    ]);

    check_condition(
        bin.state.try_add_leaf(leaf).is_ok(),
        TapeError::BinPackFailed,
    )?;
    
    bin.total_tapes += 1;

    Ok(())
}
