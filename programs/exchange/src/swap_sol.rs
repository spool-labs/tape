use steel::*;
use tape_api::prelude::*;
use solana_program::sysvar::rent::Rent;

pub fn process_swap_for_sol(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SwapForSol::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        exchange_info,
        exchange_ata_info,
        token_program_info,
        rent_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_writable()?
        .is_signer()?;

    let (exchange_ata, _) = exchange_ata(*exchange_info.key);

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    exchange_ata_info
        .is_writable()?
        .has_address(&exchange_ata)?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    token_program_info
        .is_program(&spl_token::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Amount in TAPE from user
    let amount_in_tape = TAPE::unpack(args.amount_tape);
    if amount_in_tape.is_zero() {
        return Err(TapeError::UnexpectedState.into());
    }

    // Validate rate
    let rate = exchange.rate;
    if rate.other == 0 || rate.tape == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let amount_out_sol = exchange.rate
        .convert_to_other_amount(amount_in_tape.as_u64());

    if amount_out_sol > exchange.balance_sol.as_u64() {
        return Err(TapeError::InsufficientFunds.into());
    }

    // Transfer TAPE from signer to exchange_ata
    transfer(
        signer_info,
        signer_ata_info,
        exchange_ata_info,
        token_program_info,
        amount_in_tape.as_u64(),
    )?;

    // Transfer SOL from exchange to signer
    let rent_exempt_reserve = Rent::get()?
        .minimum_balance(exchange_info.data_len());

    // Transfer lamports
    let new_exchange_lamports = (**exchange_info.lamports.borrow())
        .checked_sub(amount_out_sol)
        .ok_or(TapeError::Underflow)?;
    let new_signer_lamports = (**signer_info.lamports.borrow())
        .checked_add(amount_out_sol)
        .ok_or(TapeError::Overflow)?;

    if new_exchange_lamports < rent_exempt_reserve {
        return Err(TapeError::InsufficientFunds.into());
    }

    **exchange_info.try_borrow_mut_lamports()? = new_exchange_lamports;
    **signer_info.try_borrow_mut_lamports()? = new_signer_lamports;

    // Update exchange balances safely
    exchange.balance_tape = exchange
        .balance_tape
        .checked_add(amount_in_tape)
        .ok_or(TapeError::Overflow)?;

    let amount_out_sol: SOL = amount_out_sol.into();
    exchange.balance_sol = exchange
        .balance_sol
        .checked_sub(amount_out_sol)
        .ok_or(TapeError::Underflow)?;

    Ok(())
}
