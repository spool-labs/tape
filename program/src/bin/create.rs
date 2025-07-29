use tape_api::prelude::*;
use tape_api::instruction::bin::Create;
use solana_program::{
    keccak::hashv,
    slot_hashes::SlotHash,
};
use steel::*;

pub fn process_bin_create(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let current_time = Clock::get()?.unix_timestamp;
    let args = Create::try_from_bytes(data)?;
    let [
        signer_info,
        miner_info,
        bin_info,
        system_program_info, 
        rent_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;

    let bin_number = u64::from_le_bytes(args.number);
    let (bin_pda, _bump) = bin_pda(*miner_info.key, bin_number);

    bin_info
        .is_empty()?
        .is_writable()?
        .has_address(&bin_pda)?;

    miner_info
        .as_account::<Miner>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    // Create bin account.
    create_program_account::<Bin>(
        bin_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[BIN, miner_info.key.as_ref(), &args.number],
    )?;

    let bin = bin_info.as_account_mut::<Bin>(&tape_api::ID)?;

    let empty_seed = hashv(&[
        bin_info.key.as_ref(),
        &slot_hashes_info.data.borrow()[
            0..core::mem::size_of::<SlotHash>()
        ],
    ]);

    bin.number            = bin_number;
    bin.authority         = *signer_info.key;
    bin.last_proof_at     = current_time;
    bin.last_proof_block  = 0;
    bin.state             = TapeTree::new(&[empty_seed.as_ref()]);
    bin.contains          = [0; 32];
    bin.total_tapes       = 0;

    Ok(())
}
