use tape_api::prelude::*;
use tape_api::instruction::miner::Claim;
use steel::*;

pub fn process_claim(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Claim::try_from_bytes(data)?;
    let [
        signer_info, 
        beneficiary_info, 
        miner_info, 
        treasury_info, 
        treasury_ata_info, 
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let miner = miner_info
        .as_account_mut::<Miner>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    treasury_info
        .is_treasury()?;

    treasury_ata_info
        .is_treasury_ata()?;

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
    transfer_signed_with_bump(
        treasury_info,
        treasury_ata_info,
        beneficiary_info,
        token_program_info,
        amount,
        &[TREASURY],
        treasury_pda().1
    )?;

    Ok(())
}
