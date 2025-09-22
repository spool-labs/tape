use tape_api::prelude::*;
use steel::*;

pub fn process_withdraw_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = WithdrawTape::try_from_bytes(data)?;
    let [
        signer_info, 
        signer_ata_info,
        exchange_info,
        exchange_ata_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&tape_api::ID)?;

    signer_info
        .is_signer()?
        .is_writable()?
        .has_address(&exchange.authority)?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let mut amount = TAPE::unpack(args.amount);

    // Check if the exchange has enough balance
    if amount > exchange.balance_tape {
        return Err(TapeError::InsufficientFunds.into());
    }

    // If amount is zero, withdraw the entire balance
    if amount.is_zero() {
        amount = exchange.balance_tape;
    }

    transfer_signed(
        exchange_info,
        exchange_ata_info,
        signer_ata_info,
        token_program_info,
        amount.as_u64(),
        &[EXCHANGE, exchange.authority.as_ref()],
    )?;

    exchange.balance_tape = exchange.balance_tape
        .checked_sub(amount)
        .ok_or(TapeError::UnexpectedState)?;

    Ok(())
}
