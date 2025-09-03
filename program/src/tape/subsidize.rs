use tape_api::prelude::*;
use tape_api::instruction::tape::Subsidize;
use steel::*;

pub fn process_tape_subsidize_rent(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Subsidize::try_from_bytes(data)?;
    let [
        signer_info, 
        ata_info,
        tape_info,
        treasury_ata_info, 
        token_program_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // We don't require the owner of the tape to be the 
    // signer; anyone can subsidize any tape.
    let tape = tape_info
        .as_account_mut::<Tape>(&tape_api::ID)?;

    treasury_ata_info
        .is_treasury_ata()?;

    let amount = u64::from_le_bytes(args.amount);

    transfer(
        signer_info,
        ata_info,
        treasury_ata_info,
        token_program_info,
        amount,
    )?;

    tape.balance = tape.balance.saturating_add(amount);

    Ok(())
}

