use tape_api::prelude::*;
use steel::*;

pub fn process_pay_rent(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = PayRent::try_from_bytes(data)?;
    let [
        signer_info, 
        ata_info,
        treasury_ata_info, 
        tape_info,
        token_program_info, 
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

    treasury_ata_info
        .is_writable()?;

    token_program_info
        .is_program(&spl_token::ID)?;

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

