use solana_program::{program::invoke, system_instruction};
use tape_api::prelude::*;
use steel::*;

pub fn process_swap_for_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SwapForTape::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        exchange_info,
        exchange_ata_info,
        system_program_info,
        token_program_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Basic checks
    signer_info
        .is_writable()?
        .is_signer()?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

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

    // Amount in SOL from user
    let amount_in_sol = SOL::unpack(args.amount_sol);
    if amount_in_sol.is_zero() {
        return Err(TapeError::UnexpectedState.into());
    }

    // Validate rate
    let rate = exchange.rate;
    if rate.other == 0 || rate.tape == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    // Compute tape out: amount_out = amount_in * tape / sol
    let amount_out_tape = exchange.rate
        .convert_to_tape_amount(amount_in_sol.as_u64());

    if amount_out_tape > exchange.balance_tape.as_u64() {
        return Err(TapeError::InsufficientFunds.into());
    }

    // Transfer SOL from signer to exchange (CPI to system program)
    invoke(
        &system_instruction::transfer(
            signer_info.key,
            exchange_info.key,
            amount_in_sol.as_u64(),
        ),
        &[
            signer_info.clone(),
            exchange_info.clone(),
            system_program_info.clone(),
        ],
    )?;

    // Transfer TAPE from exchange_ata to signer_ata
    transfer_signed(
        exchange_info,
        exchange_ata_info,
        signer_ata_info,
        token_program_info,
        amount_out_tape,
        &[EXCHANGE, exchange.authority.as_ref()],
    )?;

    // Update exchange balances
    exchange.balance_sol = exchange
        .balance_sol
        .checked_add(amount_in_sol)
        .ok_or(TapeError::Overflow)?;

    // Convert output u64 to TAPE coin and subtract
    let amount_out_tape: TAPE = amount_out_tape.into();
    exchange.balance_tape = exchange
        .balance_tape
        .checked_sub(amount_out_tape)
        .ok_or(TapeError::Underflow)?;

    Ok(())
}
