use tape_api::prelude::*;
use steel::*;

pub fn process_remove_from_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RemoveFromBlacklist::try_from_bytes(data)?;
    let [
        signer_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Node authority must sign
    signer_info.is_signer()?;

    // Load and validate node
    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Extract parameters from args 
    // (track may not exist any more, so we take these from the args)
    let blob_hash: Hash = args.hash;
    let units: StorageUnits = StorageUnits::unpack(args.size);

    // Remove from blacklist using provided Merkle proof
    node.blacklist
        .remove(&args.proof, blob_hash, units)
        .map_err(|_| ProgramError::Custom(1))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_crypto::merkle::{Leaf, MerkleTree};

    // Helper to convert Vec<Hash> into a fixed-size array required by the IX builder
    fn vec_to_fixed<const N: usize>(v: Vec<Hash>) -> [Hash; N] {
        assert_eq!(v.len(), N, "proof length mismatch");
        let mut arr = [Hash::zeroed(); N];
        arr.copy_from_slice(&v[..]);
        arr
    }

    #[test]
    fn test_remove_from_blacklist_success() {
        let signer = Pubkey::new_unique();
        let (node_address, _) = node_pda(signer);

        // Build a node with a single blacklisted track
        let blob_hash = Hash::new_unique();
        let units = StorageUnits(500);

        let mut node = Node::zeroed();
        node.authority = signer;
        node.blacklist = Blacklist::new();
        node.blacklist.add(blob_hash, units).expect("add");

        // Build Merkle proof for that single entry (client-side)
        let leaf = Leaf::new(&[blob_hash.as_ref(), units.pack().as_ref()]);
        let leaves = [leaf];
        let tree = MerkleTree::<BLACKLIST_SIZE>::new(&[BLACKLIST]);
        let proof_vec = tree.get_proof(&leaves, 0);
        let proof = vec_to_fixed::<BLACKLIST_SIZE>(proof_vec);

        // Test the merkle proof is valid
        assert!(node.blacklist.contains(&proof, blob_hash, units));

        // Build instruction
        let instruction = build_remove_from_blacklist_ix(
            signer,
            node_address,
            blob_hash,
            units,
            proof,
        );

        // Accounts
        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected node after removal

        node.blacklist
            .remove(&proof, blob_hash, units)
            .expect("remove");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&node_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_remove_from_blacklist_bad_proof() {
        let signer = Pubkey::new_unique();
        let (node_address, _) = node_pda(signer);

        let blob_hash = Hash::new_unique();
        let units = StorageUnits(123);

        // Node with one blacklisted entry
        let mut node = Node::zeroed();
        node.authority = signer;
        node.blacklist = Blacklist::new();
        node.blacklist.add(blob_hash, units).expect("add");

        // Provide an invalid proof (all zeros)
        let bad_proof: [Hash; BLACKLIST_SIZE] = [Hash::zeroed(); BLACKLIST_SIZE];

        let instruction = build_remove_from_blacklist_ix(
            signer,
            node_address,
            blob_hash,
            units,
            bad_proof,
        );

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::Custom(1)),
            ],
        );
    }
}
