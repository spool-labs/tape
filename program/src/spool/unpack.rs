use brine_tree::Leaf;
use tape_api::prelude::*;
use tape_api::instruction::spool::Unpack;
use steel::*;

pub fn process_spool_unpack(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Unpack::try_from_bytes(data)?;
    let [
        signer_info, 
        spool_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let spool = spool_info
        .as_account_mut::<Spool>(&tape_api::ID)?
        .assert_mut_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    let merkle_proof = args.proof;
    assert!(merkle_proof.len() == TAPE_PROOF_LEN);

    let tape_id = args.index;
    let leaf = Leaf::new(&[
        tape_id.as_ref(), // u64 (8 bytes)
        &args.value,
    ]);

    check_condition(
        spool.state.contains_leaf(&merkle_proof, leaf),
        TapeError::SpoolUnpackFailed,
    )?;

    spool.contains = args.value;

    Ok(())
}

