use tape_api::prelude::*;
use steel::*;

pub fn process_deposit_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = DepositTape::try_from_bytes(data)?;
    let [
        signer_info, 
        signer_ata_info,
        exchange_info,
        exchange_ata_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&tape_api::ID)?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let amount = TAPE::unpack(args.amount);

    transfer(
        signer_info,
        signer_ata_info,
        exchange_ata_info,
        token_program_info,
        amount.as_u64()
    )?;

    exchange.balance_tape = exchange.balance_tape
        .checked_add(amount)
        .ok_or(TapeError::UnexpectedState)?;

    Ok(())
}
