use tape_api::program::prelude::*;

pub fn process_set_subsidy_decay_bps(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetSubsidyDecayBps::try_from_bytes(data)?;
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

    let subsidy_decay_bps = BasisPoints::unpack(args.subsidy_decay_bps);
    if !subsidy_decay_bps.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    node.preferences.subsidy_decay_bps = subsidy_decay_bps;

    Ok(())
}

