use tape_solana::*;
use tape_api::program::prelude::*;
use tape_crypto::Hash;

pub fn process_remove_from_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RemoveFromBlacklist::try_from_bytes(data)?;
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

    // Node authority must sign
    authority_info.is_signer()?;

    // Load and validate node
    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    // Extract parameters from args 
    // (track may not exist any more, so we take these from the args)
    let blob_hash: Hash = args.hash;
    let units: StorageUnits = StorageUnits::unpack(args.size);
    let index: u64 = u64::from_le_bytes(args.index);

    // Remove from blacklist using provided Merkle proof
    node.blacklist
        .remove(index, &args.proof, blob_hash, units)
        .map_err(|_| TapeError::BadProof)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    // Helper to convert Vec<Hash> into a fixed-size array required by the IX builder
    fn vec_to_fixed<const N: usize>(v: Vec<Hash>) -> [Hash; N] {
        assert_eq!(v.len(), N, "proof length mismatch");
        let mut arr = [Hash::zeroed(); N];
        arr.copy_from_slice(&v[..]);
        arr
    }

    #[test]
    fn test_remove_from_blacklist_success() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority.into());

        // Build a node with a single blacklisted track
        let blob_hash = Hash::new_unique();
        let units = StorageUnits::mb(500);

        let mut node = Node::zeroed();
        node.authority = authority.into();
        node.blacklist = Blacklist::new();
        node.blacklist.add(blob_hash, units).expect("add");

        // Build Merkle proof for that single entry (client-side)

        let leaf = blacklist_entry(blob_hash, units);
        let leaves = [leaf];
        let proof = node
            .blacklist
            .state
            .create_proof(&leaves, 0)
            .expect("valid proof for single blacklist entry");
        let proof = vec_to_fixed::<BLACKLIST_SIZE>(proof);

        // Test the merkle proof is valid
        assert!(node.blacklist.contains(0, &proof, blob_hash, units));

        // Build instruction
        let instruction = build_remove_from_blacklist_ix(fee_payer.into(), authority.into(),
            node_address,
            0,
            blob_hash,
            units,
            proof,
        );

        // Accounts
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected node after removal

        node.blacklist
            .remove(0, &proof, blob_hash, units)
            .expect("remove");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node_address))
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_remove_from_blacklist_bad_proof() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority.into());

        let blob_hash = Hash::new_unique();
        let units = StorageUnits::mb(123);

        // Node with one blacklisted entry
        let mut node = Node::zeroed();
        node.authority = authority.into();
        node.blacklist = Blacklist::new();
        node.blacklist.add(blob_hash, units).expect("add");

        // Provide an invalid proof (all zeros)
        let bad_proof: [Hash; BLACKLIST_SIZE] = [Hash::zeroed(); BLACKLIST_SIZE];

        let instruction = build_remove_from_blacklist_ix(fee_payer.into(), authority.into(),
            node_address,
            0,
            blob_hash,
            units,
            bad_proof,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::BadProof.into()),
            ],
        );
    }
}
