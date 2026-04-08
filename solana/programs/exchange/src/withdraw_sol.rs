use tape_solana::*;
use solana_program::sysvar::rent::Rent;
use tape_api::program::prelude::*;

pub fn process_withdraw_sol(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = WithdrawSol::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        exchange_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    authority_info
        .is_signer()?
        .is_writable()?
        .has_address(&exchange.authority.into())?;

    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let mut amount = SOL::unpack(args.amount);

    // Check if the exchange has enough balance
    // (without dipping into rent-exempt reserve)
    if amount > exchange.balance_sol {
        return Err(ExchangeError::InsufficientFunds.into());
    }

    // If amount is zero, withdraw the entire balance
    if amount.is_zero() {
        amount = exchange.balance_sol;
    }

    let rent_exempt_reserve = Rent::get()?
        .minimum_balance(exchange_info.data_len());

    // Transfer lamports
    let new_exchange_lamports = (**exchange_info.lamports.borrow())
        .checked_sub(amount.as_u64())
        .ok_or(ExchangeError::Underflow)?;
    let new_authority_lamports = (**authority_info.lamports.borrow())
        .checked_add(amount.as_u64())
        .ok_or(ExchangeError::Overflow)?;

    if new_exchange_lamports < rent_exempt_reserve {
        return Err(ExchangeError::InsufficientFunds.into());
    }

    **exchange_info.try_borrow_mut_lamports()? = new_exchange_lamports;
    **authority_info.try_borrow_mut_lamports()? = new_authority_lamports;

    // Update exchange state
    exchange.balance_sol = exchange.balance_sol
        .checked_sub(amount)
        .ok_or(ExchangeError::Underflow)?;

    Ok(())
}
