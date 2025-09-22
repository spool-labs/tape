use solana_program::{system_instruction, program::invoke};

use tape_api::prelude::*;
use steel::*;

pub fn process_deposit_sol(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = DepositSol::try_from_bytes(data)?;
    let [
        signer_info, 
        exchange_info,
        system_program_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    exchange_info
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let amount = SOL::unpack(args.amount);

    invoke(
        &system_instruction::transfer(
            signer_info.key,
            exchange_info.key,
            amount.as_u64()
        ),
        &[
            signer_info.clone(),
            exchange_info.clone(),
            system_program_info.clone(),
        ],
    )?;

    let exchange = exchange_info.as_account_mut::<Exchange>(&tape_api::ID)?;

    exchange.balance_sol = exchange.balance_sol
        .checked_add(amount)
        .ok_or(TapeError::UnexpectedState)?;

    Ok(())
}
