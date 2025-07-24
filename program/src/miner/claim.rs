use tape_api::prelude::*;
use steel::*;

pub fn process_claim(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Claim::try_from_bytes(data)?;
    let [
        signer_info, 
        beneficiary_info, 
        proof_info, 
        treasury_info, 
        treasury_ata_info, 
        token_program_info,
        _rest@..
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    beneficiary_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let miner = proof_info
        .as_account_mut::<Miner>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    treasury_info
        .is_treasury()?;

    treasury_ata_info
        .is_writable()?
        .is_treasury_ata()?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let mut amount = u64::from_le_bytes(args.amount);

    // If amount is zero, we claim the entire unclaimed rewards.
    if amount == 0 {
        amount = miner.unclaimed_rewards;
    }

    // Update miner balance.
    miner.unclaimed_rewards = miner
        .unclaimed_rewards
        .checked_sub(amount)
        .ok_or(TapeError::ClaimTooLarge)?;

    // Transfer tokens from treasury to beneficiary.
    transfer_signed(
        treasury_info,
        treasury_ata_info,
        beneficiary_info,
        token_program_info,
        amount,
        &[TREASURY],
    )?;

    Ok(())
}
