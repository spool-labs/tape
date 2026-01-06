use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_withdraw_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = WithdrawTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,
        exchange_info,
        exchange_ata_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let (exchange_ata, _) = exchange_ata(*exchange_info.key);

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    authority_info
        .is_signer()?
        .has_address(&exchange.authority)?;

    authority_ata_info
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

    let mut amount = TAPE::unpack(args.amount);

    // Check if the exchange has enough balance
    if amount > exchange.balance_tape {
        return Err(ExchangeError::InsufficientFunds.into());
    }

    // If amount is zero, withdraw the entire balance
    if amount.is_zero() {
        amount = exchange.balance_tape;
    }

    transfer_signed(
        exchange_info,
        exchange_ata_info,
        authority_ata_info,
        token_program_info,
        amount.as_u64(),
        &[EXCHANGE, exchange.authority.as_ref()],
    )?;

    exchange.balance_tape = exchange.balance_tape
        .checked_sub(amount)
        .ok_or(ExchangeError::Underflow)?;

    Ok(())
}
