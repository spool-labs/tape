use crate::error::*;
use tape_api::prelude::*;

pub fn process_deposit_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = DepositTape::try_from_bytes(data)?;
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

    authority_info
        .is_signer()?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS.into()))?;

    let (exchange_ata, _) = exchange_ata((*exchange_info.key).into());

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    exchange_ata_info
        .is_writable()?
        .has_address(&exchange_ata.into())?
        .as_token_account()?
        .assert(|a| a.mint().eq(&MINT_ADDRESS.into()))?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let amount = TAPE::unpack(args.amount);

    transfer(
        authority_info,
        authority_ata_info,
        exchange_ata_info,
        token_program_info,
        amount.as_u64()
    )?;

    exchange.balance_tape = exchange.balance_tape
        .checked_add(amount)
        .ok_or(ExchangeError::Overflow)?;

    Ok(())
}
