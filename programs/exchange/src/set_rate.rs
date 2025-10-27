use tape_api::prelude::*;
use steel::*;

pub fn process_set_exchange_rate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetExchangeRate::try_from_bytes(data)?;
    let [
        signer_info, 
        exchange_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let exchange = exchange_info
        .is_writable()?
        .as_account_mut::<Exchange>(&exchange::ID)?;

    // Only exchange authority may update the rate
    signer_info
        .is_signer()?
        .has_address(&exchange.authority)?;

    // Parse and validate new rate
    let tape_per_unit = TAPE::unpack(args.tape).as_u64();
    let sol_per_unit = SOL::unpack(args.sol).as_u64();

    if tape_per_unit == 0 || sol_per_unit == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    exchange.rate = ExchangeRate {
        other: sol_per_unit,
        tape: tape_per_unit,
    };

    Ok(())
}
