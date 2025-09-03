use steel::*;
use tape_api::prelude::*;
use tape_api::instruction::program::Airdrop;

pub fn process_airdrop(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Airdrop::try_from_bytes(data)?;
    let [
        beneficiary_ata_info,
        mint_info,
        treasury_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Verify accounts
    let (mint_address, _mint_bump) = mint_pda();

    mint_info
        .has_address(&mint_address)?;

    // Parse amount
    let amount = u64::from_le_bytes(args.amount);

    // Mint tokens to beneficiary's ATA
    mint_to_signed_with_bump(
        mint_info,
        beneficiary_ata_info,
        treasury_info,
        token_program_info,
        amount,
        &[TREASURY],
        treasury_pda().1,
    )?;

    Ok(())
}
