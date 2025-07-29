use tape_api::prelude::*;
use tape_api::instruction::tape::SetHeader;
use steel::*;

pub fn process_tape_set_header(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetHeader::try_from_bytes(data)?;
    let [
        signer_info, 
        tape_info,
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

    let (tape_address, _tape_bump) = tape_pda(*signer_info.key, &tape.name);

    tape_info.has_address(&tape_address)?;

    check_condition(
        tape.state.eq(&u64::from(TapeState::Writing)),
        TapeError::UnexpectedState,
    )?;

    tape.header = args.header;

    Ok(())
}

