use tape_api::prelude::*;
use steel::*;

pub fn process_swap_for_sol(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SwapForSol::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        exchange_info,
        exchange_ata_info,
        token_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_writable()?
        .is_signer()?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&tape_api::ID)?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    exchange_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS))?;

    token_program_info
        .is_program(&spl_token::ID)?;

    // Amount in TAPE from user
    let amount_in_tape = TAPE::unpack(args.amount_tape);
    if amount_in_tape.is_zero() {
        return Err(TapeError::UnexpectedState.into());
    }

    // Validate rate
    let rate = exchange.rate;
    if rate.sol == 0 || rate.tape == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    // Compute SOL out: amount_out = amount_in * sol / tape
    let amount_in_u64 = amount_in_tape.as_u64();
    let amount_out_sol_u64 = (amount_in_u64 as u128)
        .checked_mul(rate.sol as u128)
        .ok_or(TapeError::Overflow)?
        .checked_div(rate.tape as u128)
        .ok_or(TapeError::Overflow)? as u64;

    if amount_out_sol_u64 == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    // Check liquidity
    if amount_out_sol_u64 > exchange.balance_sol.as_u64() {
        return Err(TapeError::InsufficientFunds.into());
    }

    // Transfer TAPE from signer to exchange_ata
    transfer(
        signer_info,
        signer_ata_info,
        exchange_ata_info,
        token_program_info,
        amount_in_u64,
    )?;

    // Transfer SOL from exchange to signer
    let new_exchange_lamports = (**exchange_info.lamports.borrow())
        .checked_sub(amount_out_sol_u64)
        .ok_or(TapeError::Overflow)?;
    let new_signer_lamports = (**signer_info.lamports.borrow())
        .checked_add(amount_out_sol_u64)
        .ok_or(TapeError::Overflow)?;

    **exchange_info.try_borrow_mut_lamports()? = new_exchange_lamports;
    **signer_info.try_borrow_mut_lamports()? = new_signer_lamports;

    // Update exchange balances safely
    exchange.balance_tape = exchange
        .balance_tape
        .checked_add(amount_in_tape)
        .ok_or(TapeError::Overflow)?;

    let amount_out_sol: SOL = amount_out_sol_u64.into();
    exchange.balance_sol = exchange
        .balance_sol
        .checked_sub(amount_out_sol)
        .ok_or(TapeError::Overflow)?;

    Ok(())
}
