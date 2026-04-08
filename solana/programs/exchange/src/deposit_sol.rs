#[allow(deprecated)]
use solana_program::system_instruction;
use solana_program::program::invoke;

use tape_api::program::prelude::*;

pub fn process_deposit_sol(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = DepositSol::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        exchange_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?
        .is_writable()?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    system_program_info
        .is_program(&system_program::ID)?;

    let amount = SOL::unpack(args.amount);

    invoke(
        &system_instruction::transfer(
            authority_info.key,
            exchange_info.key,
            amount.as_u64()
        ),
        &[
            authority_info.clone(),
            exchange_info.clone(),
            system_program_info.clone(),
        ],
    )?;

    exchange.balance_sol = exchange.balance_sol
        .checked_add(amount)
        .ok_or(ExchangeError::Overflow)?;

    Ok(())
}
