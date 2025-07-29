use brine_tree::{Leaf, verify};
use tape_api::prelude::*;
use tape_api::instruction::bin::Unpack;
use steel::*;

pub fn process_bin_commit(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Unpack::try_from_bytes(data)?;
    let [
        signer_info, 
        miner_info,
        bin_info, 
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

    let bin = bin_info
        .as_account::<Bin>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    let merkle_proof = args.proof;
    let merkle_root  = merkle_proof.last()
        .ok_or(ProgramError::InvalidArgument)?;
        
    // Check that the bin has previously unpacked the tape we're trying to commit to
    check_condition(
        bin.contains.eq(merkle_root),
        TapeError::BinCommitFailed,
    )?;

    let segment_id = args.index;
    let leaf = Leaf::new(&[
        segment_id.as_ref(), // u64 (8 bytes)
        &args.value,
    ]);

    check_condition(
        verify(*merkle_root, &merkle_proof, leaf),
        TapeError::BinUnpackFailed,
    )?;

    miner.commitment = args.value;

    Ok(())
}

