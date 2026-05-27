use tape_api::program::prelude::*;

pub fn process_set_burn_fee_bps(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetBurnFeeBps::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let burn_fee_bps = args.burn_fee_bps;
    if !burn_fee_bps.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    node.preferences.burn_fee_bps = burn_fee_bps;

    Ok(())
}

