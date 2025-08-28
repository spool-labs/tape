use steel::*;
use tape_api::prelude::*;
use tape_api::instruction::program::Airdrop;

pub fn process_airdrop(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Airdrop::try_from_bytes(data)?;
    let [
        signer_info,
        beneficiary_info,
        mint_info,
        treasury_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Verify signer
    signer_info.is_signer()?;
    beneficiary_info.has_address(signer_info.key)?;

    // Verify accounts
    let (mint_address, _mint_bump) = mint_pda();

    mint_info
        .is_writable()?
        .has_address(&mint_address)?;

    // Parse amount
    let amount = u64::from_le_bytes(args.amount);

    // Mint tokens to beneficiary's ATA
    mint_to_signed(
        mint_info,
        beneficiary_info,
        treasury_info,
        token_program_info,
        amount,
        &[TREASURY],
    )?;

    Ok(())
}
