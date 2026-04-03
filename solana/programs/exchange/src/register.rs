use tape_api::prelude::*;

pub fn process_register_exchange(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RegisterExchange::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        exchange_info,
        exchange_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_writable()?
        .is_signer()?;

    authority_info
        .is_signer()?;

    mint_info
        .has_address(&MINT_ADDRESS.into())?;

    let (exchange_address, _) = exchange_pda((*authority_info.key).into());
    let (exchange_ata, _) = exchange_ata(exchange_address);

    exchange_info
        .is_empty()?
        .is_writable()?
        .has_address(&exchange_address.into())?;

    exchange_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&exchange_ata.into())?;

    // Check programs and sysvars.
    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Initialize exchange.
    create_program_account::<Exchange>(
        exchange_info,
        system_program_info,
        fee_payer_info,
        &exchange::ID,
        &[EXCHANGE, authority_info.key.as_ref()],
    )?;

    let exchange = exchange_info.as_account_mut::<Exchange>(&exchange::ID)?;

    exchange.authority = (*authority_info.key).into();
    exchange.balance_sol = SOL::zero();
    exchange.balance_tape = TAPE::zero();
    exchange.rate = ExchangeRate::flat();

    // Initialize exchange token account.
    create_associated_token_account(
        fee_payer_info,
        exchange_info,
        exchange_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    Ok(())
}
