use brine_tree::Leaf;
use tape_api::prelude::*;
use tape_api::instruction::bin::Unpack;
use steel::*;

pub fn process_bin_unpack(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Unpack::try_from_bytes(data)?;
    let [
        signer_info, 
        bin_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let bin = bin_info
        .as_account_mut::<Bin>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    let merkle_proof = args.proof;

    let tape_id = args.index;
    let leaf = Leaf::new(&[
        tape_id.as_ref(), // u64 (8 bytes)
        &args.value,
    ]);

    check_condition(
        bin.state.contains_leaf(&merkle_proof, leaf),
        TapeError::BinUnpackFailed,
    )?;

    bin.contains = args.value;

    Ok(())
}

