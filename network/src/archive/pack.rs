use std::sync::Arc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use brine_tree::{Leaf, Hash, MerkleTree};
use tape_api::prelude::*;
use tape_client::get_epoch_account;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::store::*;
use super::queue::Rx;

/// Orchestrator Task C – CPU-heavy preprocessing (packx)
pub async fn run(rpc: Arc<RpcClient>, mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {
    let epoch = get_epoch_account(&rpc).await?.0;
    let packing_difficulty = epoch.packing_difficulty;

    while let Some(job) = rx.recv().await {
        let store = store.clone();
        //let rpc = rpc.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={}", job.tape, job.seg_no);

            let solved = pack_segment(&miner, &job.data, packing_difficulty)?;
            store.put_segment(&job.tape, job.seg_no, solved)?;

            // TODO: need a way to check if we need to update the sector root

            // let handle = tokio::runtime::Handle::current();
            // handle.block_on(update_sector_root(&store, &rpc, &job.tape, job.seg_no))?;

            Ok(())
        })
        .await??;
    }

    Ok(())
}

/// Packs a segment using the packx algorithm.
/// Can be quite CPU intensive if the difficulty is high.
pub fn pack_segment(miner_address: &Pubkey, segment: &[u8], packing_difficulty: u64) -> Result<Vec<u8>> {
    let miner_address: [u8; 32] = miner_address.to_bytes();
    let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

    let solution = packx::solve(&miner_address, &canonical_segment, packing_difficulty as u32)
        .ok_or_else(|| anyhow!("Failed to find solution"))?;

    if !packx::verify(&miner_address, &canonical_segment, &solution, packing_difficulty as u32) {
        return Err(anyhow!("Solution verification failed"));
    }

    let segment_bytes = solution.to_bytes();
    Ok(segment_bytes.to_vec())
}

/// Computes the Merkle root for the entire tape by using a cached canopy of sector roots.
pub fn get_tape_root(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
) -> Result<Hash> {
    // canopy height = number of levels above the sector layer up to the root
    const CANOPY_HEIGHT: usize = SEGMENT_TREE_HEIGHT - SECTOR_TREE_HEIGHT;

    // All zero values for the full-height segment tree
    let zeros_full = get_or_create_empty_hashes(store, tape_address)?;
    if zeros_full.len() != SEGMENT_TREE_HEIGHT {
        return Err(anyhow!(
            "Invalid zero_values len: expected {}, got {}",
            SEGMENT_TREE_HEIGHT,
            zeros_full.len()
        ));
    }

    // Zero values for the canopy tree (start at the sector layer)
    let canopy_zeros: [Hash; CANOPY_HEIGHT] = zeros_full
        [SECTOR_TREE_HEIGHT .. SEGMENT_TREE_HEIGHT]
        .try_into()
        .map_err(|_| anyhow!(
            "Invalid canopy zeros slice: expected {}, got {}",
            CANOPY_HEIGHT,
            zeros_full.len().saturating_sub(SECTOR_TREE_HEIGHT)
        ))?;

    // Build canopy over sector roots
    type CanopyTree = MerkleTree<{ SEGMENT_TREE_HEIGHT - SECTOR_TREE_HEIGHT }>;
    let mut canopy = CanopyTree::from_zeros(canopy_zeros);

    // Load sector roots cached at the sector layer
    let sector_roots = store.get_layer(tape_address, SECTOR_TREE_HEIGHT as u8)?;
    for root_bytes in sector_roots.iter() {
        let leaf = Leaf::from(*root_bytes);
        canopy.try_add_leaf(leaf).expect("Failed to add sector root");
    }

    Ok(canopy.get_root())
}

/// Updates the Merkle subtree for a given segment number.
pub fn update_segment_root(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
    segment_number: u64,
) -> Result<()> {
    let sector_number = segment_number / SECTOR_LEAVES as u64;
    update_sector_root(store, tape_address, sector_number)
}

/// Updates the Merkle subtree for a given sector number.
pub fn update_sector_root(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
    sector_number: u64,
) -> Result<()> {

    let empty_hashes = get_or_create_empty_hashes(store, tape_address)?;
    let empty_leaf = empty_hashes.first().unwrap().as_leaf();

    let leaves = compute_sector_leaves(store, tape_address, sector_number, empty_leaf)?;
    let root = compute_sector_root(&leaves, &empty_hashes)?;

    update_sector_canopy(store, tape_address, sector_number, root)
}

/// Computes leaves for a sector from the store, filling with empty_leaf as needed.
pub fn compute_sector_leaves(
    store: &TapeStore,
    tape_address: &Pubkey,
    sector_number: u64,
    empty_leaf: Leaf,
) -> Result<Vec<Leaf>> {
    let sector = store.get_sector(tape_address, sector_number)?;

    let mut leaves = vec![empty_leaf; SECTOR_LEAVES];
    for (i, leaf) in leaves.iter_mut().enumerate() {
        if let Some(segment) = sector.get_segment(i) {
            let segment_id = (sector_number * SECTOR_LEAVES as u64) + i as u64;
            *leaf = Leaf::new(&[
                &segment_id.to_le_bytes(),
                segment
            ]);
        }
    }
    Ok(leaves)
}

/// Computes the root node for a sector at the specified layer.
pub fn compute_sector_root(
    leaves: &[Leaf],
    empty_hashes: &[Hash],
) -> Result<Hash> {
    let mut tree = SegmentTree::from_zeros(
        empty_hashes
            .try_into()
            .map_err(|_| {
                anyhow!(
                    "Invalid empty hashes length: expected {}, got {}", 
                    SEGMENT_TREE_HEIGHT,
                    empty_hashes.len()
                )
    })?);

    tree.next_index = leaves.len() as u64;

    let layer_nodes = tree.get_layer_nodes(leaves, SECTOR_TREE_HEIGHT);
    if layer_nodes.len() != 1 {
        return Err(anyhow!(
            "Invalid layer nodes length: expected 1, got {}",
            layer_nodes.len()
        ));
    }

    Ok(*layer_nodes.first().unwrap())
}

/// Updates the layer in the store with the new sector root. 
pub fn update_sector_canopy(
    store: &TapeStore,
    tape_address: &Pubkey,
    sector_number: u64,
    root: Hash,
) -> Result<()> {

    let mut layer = match store.get_layer(tape_address, SECTOR_TREE_HEIGHT as u8) {
        Ok(layer) => layer,
        Err(_) => vec![[0u8; 32]; (sector_number + 1) as usize],
    };

    if (sector_number as usize) >= layer.len() {
        layer.resize(sector_number as usize + 1, [0u8; 32]);
    }

    layer[sector_number as usize] = root.to_bytes();
    store.put_layer(tape_address, SECTOR_TREE_HEIGHT as u8, &layer)?;

    Ok(())
}

/// Helper to create or init the zero values for a tape
pub fn get_or_create_empty_hashes(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
) -> Result<Vec<Hash>> {

    let empty_values = match store.get_zero_values(tape_address) {
        Ok(empty_values) => empty_values,
        Err(_) => {
            // Create an empty SegmentTree to get the zero values
            let tree = SegmentTree::new(&[&tape_address.as_ref()]);
            let empty_values = tree.zero_values;
            let seeds_bytes = empty_values
                .into_iter()
                .map(|h| h.to_bytes())
                .collect::<Vec<_>>();

            // Throw the empty_values into the store for future use
            store.put_zero_values(tape_address, &seeds_bytes)?;
            seeds_bytes
        }
    };

    if empty_values.len() != SEGMENT_TREE_HEIGHT {
        return Err(
            anyhow!("Invalid number of zero values: expected {}, got {}", 
                SEGMENT_TREE_HEIGHT,
                empty_values.len()
            ));
    }

    // Convert empty values to Hash type
    let empty_hashes: Vec<_> = empty_values
        .into_iter()
        .map(Hash::from)
        .collect();

    Ok(empty_hashes)
}


#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use tempdir::TempDir;
    use brine_tree::{Leaf, get_cached_merkle_proof};

    fn setup_store() -> Result<(Arc<TapeStore>, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((Arc::new(store), temp_dir))
    }

    fn create_segment_data(marker: u8, miner: &Pubkey) -> Vec<u8> {
        let data = &[marker; SEGMENT_SIZE];
        let canonical_segment = padded_array::<SEGMENT_SIZE>(data);
        let solution = packx::solve(
            &miner.to_bytes(), 
            &canonical_segment,
            0
            ).expect("Failed to pack segment");

        solution.to_bytes().to_vec()
    }

    #[test]
    fn test_subtree_update() -> Result<()> {
        // Setup store and tape address
        let (store, _temp_dir) = setup_store()?;

        // Setup our tape and miner
        let miner_address = Pubkey::new_unique();
        let tape_address = Pubkey::new_unique();
        let mut tape_tree = SegmentTree::new(&[tape_address.as_ref()]);
        let mut leaves = vec![];

        let empty_values = get_or_create_empty_hashes(&store, &tape_address)?;
        let empty_leaf = empty_values[0].as_leaf();
        assert_eq!(empty_values.len(), SEGMENT_TREE_HEIGHT);
        assert_eq!(empty_values, empty_values);

        // Fill the tape with some segments (2.5 sectors worth)
        let leaf_count = (SECTOR_LEAVES as f64 * 2.5) as usize;
        for i in 0..leaf_count {
            let segment_id = i as u64;
            let segment_data = create_segment_data(1, &miner_address);
            let leaf = Leaf::new(&[
                &segment_id.to_le_bytes(),
                &segment_data
            ]);

            tape_tree.try_add_leaf(leaf).expect("Failed to add leaf");
            store.put_segment(&tape_address, segment_id, segment_data)?;
            leaves.push(leaf);
        }

        let expected_vals = tape_tree.get_layer_nodes(&leaves, SECTOR_TREE_HEIGHT);
        let expected_canopy = expected_vals
            .iter()
            .map(|h| h.to_bytes())
            .collect::<Vec<_>>();

        // Calculate sector roots and update the stored canopy values
        update_sector_root(&store, &tape_address, 0)?;
        update_sector_root(&store, &tape_address, 1)?;
        update_sector_root(&store, &tape_address, 2)?;

        let actual_canopy = store.get_layer(&tape_address, SECTOR_TREE_HEIGHT as u8)?;
        assert_eq!(expected_canopy.len(), actual_canopy.len());
        assert_eq!(expected_canopy, actual_canopy);

        // Lets try a Merkle proof for a segment
        let segment_number : usize = 1234;
        let sector_number = (segment_number as u64) / SECTOR_LEAVES as u64;
        let sector = store.get_sector(&tape_address, sector_number)?;

        let expected_proof = tape_tree.get_proof(&leaves, segment_number); // <- Requires all leaves in memory (bad)
        let actual_proof = get_cached_merkle_proof(                        // <- Only one sector in memory     (good)
            &tape_tree,
            segment_number,
            SECTOR_TREE_HEIGHT,
            &expected_vals,
            |i| { 
                let local_idx = i % SECTOR_LEAVES;
                match sector.get_segment(local_idx) { // <- Look ma, no store access here!
                    Some(data) => Some(
                        Leaf::new(&[
                            &(i as u64).to_le_bytes(),
                            &data
                        ])),
                    None => Some(empty_leaf),
                }

                // Same as above, but with store access (slower)
                // match store.get_segment(&tape_address, i as u64) {
                //     Ok(data) => Some(
                //         Leaf::new(&[
                //             &(i as u64).to_le_bytes(),
                //             &data
                //         ])),
                //     Err(_) => Some(empty_leaf),
                // }
            }
        );

        assert_eq!(expected_proof, actual_proof);

        //Verify the tape root matches our computed root
        let computed_root = get_tape_root(&store, &tape_address)?;
        assert_eq!(computed_root, tape_tree.get_root());

        Ok(())
    }
}
