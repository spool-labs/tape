use crate::error::*;
use tape_api::prelude::*;

pub fn process_set_exchange_rate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetExchangeRate::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        exchange_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    // Only exchange authority may update the rate
    authority_info
        .is_signer()?
        .has_address(&exchange.authority)?;

    // Parse and validate new rate
    let tape_per_unit = TAPE::unpack(args.tape).as_u64();
    let sol_per_unit = SOL::unpack(args.sol).as_u64();

    if tape_per_unit == 0 || sol_per_unit == 0 {
        return Err(ExchangeError::UnexpectedState.into());
    }

    exchange.rate = ExchangeRate {
        other: sol_per_unit,
        tape: tape_per_unit,
    };

    Ok(())
}
