use std::sync::Arc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use brine_tree::{Leaf, Hash, MerkleTree};
use tape_api::prelude::*;
use tape_client::get_epoch_account;
use solana_client::nonblocking::rpc_client::RpcClient;
use packx::{solve_with_memory, build_memory, SolverMemory};

use crate::store::*;
use super::queue::Rx;

type CanopyTree = MerkleTree<{ SEGMENT_TREE_HEIGHT - SECTOR_TREE_HEIGHT }>;

/// Orchestrator Task C – CPU-heavy preprocessing (packx)
pub async fn run(rpc: Arc<RpcClient>, mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {
    let epoch = get_epoch_account(&rpc).await?.0;
    let packing_difficulty = epoch.packing_difficulty;
    let miner_bytes = miner.to_bytes();
    let mem = Arc::new(build_memory(&miner_bytes));

    // TODO: scale the thread count based on job queue depth (but also don't get in the way of the
    // miner threads)

    while let Some(job) = rx.recv().await {
        let store = store.clone();
        let mem = mem.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={} diff={}", job.tape, job.seg_no, packing_difficulty);

            pack_segment(
                &store,
                &mem,
                &miner, 
                &job.tape, 
                job.data, 
                job.seg_no,
                packing_difficulty
            )?;

            Ok(())
        })
        .await??;
    }

    Ok(())
}

/// Packs a segment using the packx algorithm.
/// Can be quite CPU intensive if the difficulty is high.
pub fn pack_segment(
    store: &Arc<TapeStore>,
    mem: &Arc<SolverMemory>,
    miner_address: &Pubkey,
    tape_address: &Pubkey,
    segment_data: Vec<u8>,
    segment_number: u64,
    difficulty: u64,
) -> anyhow::Result<()> {

    let miner_bytes = miner_address.to_bytes();
    let segment_bytes = padded_array::<SEGMENT_SIZE>(&segment_data);

    // Pack the segment into a miner-specific solution
    let solution = solve_with_memory(
        &segment_bytes, 
        mem,
        difficulty as u32
    ).ok_or_else(|| anyhow!("Failed to find solution"))?;

    // Verify the solution before storing
    if !packx::verify(
        &miner_bytes, 
        &segment_bytes, 
        &solution, 
        difficulty as u32
    ) {
        return Err(anyhow!("Solution verification failed"));
    }

    let packed_segment = solution.to_bytes();

    store.put_segment(
        tape_address, 
        segment_number, 
        packed_segment.to_vec()
    )?;

    // TODO: only update the canopy if this segment is the last for this sector
    update_merkle_canopy_for_segment(
        &store,
        miner_address,
        tape_address,
        segment_number,
    )?;

    Ok(())
}

/// Updates the Merkle canopy for the sector containing the specified segment.
fn update_merkle_canopy_for_segment(
    store: &Arc<TapeStore>,
    miner_address: &Pubkey,
    tape_address: &Pubkey,
    segment_number: u64,
) -> anyhow::Result<()> {
    let sector_number = segment_number / SECTOR_LEAVES as u64;

    update_merkle_canopy_for_sector(
        store,
        miner_address,
        tape_address,
        sector_number,
    )
}

/// Updates the Merkle canopy for the provided sector number.
fn update_merkle_canopy_for_sector(
    store: &Arc<TapeStore>,
    miner_address: &Pubkey,
    tape_address: &Pubkey,
    sector_number: u64,
) -> anyhow::Result<()> {

    let empty_hashes = get_or_create_empty_hashes(store, tape_address)?;
    let empty_leaf = empty_hashes.first().unwrap().as_leaf();

    let leaves_unpacked = compute_sector_leaves_unpacked(
        store,
        miner_address,
        tape_address,
        sector_number,
        empty_leaf,
    )?;

    let leaves_packed = compute_sector_leaves_packed(
        store,
        tape_address,
        sector_number,
        empty_leaf,
    )?;

    let root_unpacked = compute_sector_root(&leaves_unpacked, &empty_hashes)?;
    let root_packed   = compute_sector_root(&leaves_packed,   &empty_hashes)?;

    update_sector_canopy_with_key(
        store.as_ref(),
        sector_number,
        root_unpacked,
        MerkleCacheKey::UnpackedTapeLayer {
            address: *tape_address,
            layer: SECTOR_TREE_HEIGHT as u8,
        },
    )?;

    update_sector_canopy_with_key(
        store.as_ref(),
        sector_number,
        root_packed,
        MerkleCacheKey::PackedTapeLayer {
            address: *tape_address,
            layer: SECTOR_TREE_HEIGHT as u8,
        },
    )?;

    Ok(())
}


/// Computes the Merkle root for the entire tape by using a cached canopy of sector roots.
pub fn get_tape_root(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
) -> Result<Hash> {
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
    let mut canopy = CanopyTree::from_zeros(canopy_zeros);

    // Load sector roots cached at the sector layer
    let sector_roots = store.get_merkle_cache(
        &MerkleCacheKey::UnpackedTapeLayer {
            address: *tape_address,
            layer: SECTOR_TREE_HEIGHT as u8
        }
    )?;

    for root_bytes in sector_roots.iter() {
        let leaf = Leaf::from(*root_bytes);
        canopy.try_add_leaf(leaf).expect("Failed to add sector root");
    }

    Ok(canopy.get_root())
}

/// Computes packed leaves (stored solution bytes).
fn compute_sector_leaves_packed(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
    sector_number: u64,
    empty_leaf: Leaf,
) -> Result<Vec<Leaf>> {
    let mut leaves = vec![empty_leaf; SECTOR_LEAVES];

    for i in 0..SECTOR_LEAVES {
        let segment_number = (sector_number * SECTOR_LEAVES as u64) + i as u64;
        if let Ok(packed) = store.get_segment(tape_address, segment_number) {
            let segment_id = (sector_number * SECTOR_LEAVES as u64) + i as u64;
            leaves[i] = Leaf::new(&[
                &segment_id.to_le_bytes(),
                &packed,
            ]);
        }
    }

    Ok(leaves)
}

/// Computes unpacked leaves (reconstructed 128-byte data from solutions).
fn compute_sector_leaves_unpacked(
    store: &Arc<TapeStore>,
    miner_address: &Pubkey,
    tape_address: &Pubkey,
    sector_number: u64,
    empty_leaf: Leaf,
) -> Result<Vec<Leaf>> {
    let miner_bytes = miner_address.to_bytes();

    let mut data = [0u8; PACKED_SEGMENT_SIZE];
    let mut leaves = vec![empty_leaf; SECTOR_LEAVES];

    for i in 0..SECTOR_LEAVES {
        let segment_number = (sector_number * SECTOR_LEAVES as u64) + i as u64;
        if let Ok(packed) = store.get_segment(tape_address, segment_number) {
            data.copy_from_slice(&packed[..PACKED_SEGMENT_SIZE]);

            let solution = packx::Solution::from_bytes(&data);
            let data_unpacked = solution.unpack(&miner_bytes);

            let segment_id = (sector_number * SECTOR_LEAVES as u64) + i as u64;
            leaves[i] = Leaf::new(&[
                &segment_id.to_le_bytes(),
                &data_unpacked,
            ]);
        }
    }

    Ok(leaves)
}

/// Computes the root node for a sector at the specified layer.
fn compute_sector_root(
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

/// Helper to update a specific MerkleCacheKey layer with the new sector root.
fn update_sector_canopy_with_key(
    store: &TapeStore,
    sector_number: u64,
    root: Hash,
    key: MerkleCacheKey,
) -> Result<()> {
    let mut layer = match store.get_merkle_cache(&key) {
        Ok(layer) => layer,
        Err(_) => vec![[0u8; 32]; (sector_number + 1) as usize],
    };

    if (sector_number as usize) >= layer.len() {
        layer.resize(sector_number as usize + 1, [0u8; 32]);
    }

    layer[sector_number as usize] = root.to_bytes();
    store.put_merkle_cache(&key, &layer)?;
    Ok(())
}

/// Helper to create or init the zero values for a tape
fn get_or_create_empty_hashes(
    store: &Arc<TapeStore>,
    tape_address: &Pubkey,
) -> Result<Vec<Hash>> {

    let empty_values = match store.get_merkle_cache(
        &MerkleCacheKey::ZeroValues { 
            address: *tape_address 
        }
    ) {
        Ok(empty_values) => empty_values,
        Err(_) => {
            // Create an empty SegmentTree to get the zero values
            let tree = SegmentTree::new(&[tape_address.as_ref()]);
            let empty_values = tree.zero_values;
            let seeds_bytes = empty_values
                .into_iter()
                .map(|h| h.to_bytes())
                .collect::<Vec<_>>();

            // Throw the empty_values into the store for future use
            store.put_merkle_cache(
                &MerkleCacheKey::ZeroValues { address: *tape_address },
                &seeds_bytes
            )?;
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

    fn create_segment_data(marker: u8, mem: &packx::SolverMemory) -> Vec<u8> {
        const TEST_DIFFICULTY: u32 = 0;

        let data = &[marker; SEGMENT_SIZE];
        let canonical_segment = padded_array::<SEGMENT_SIZE>(data);
        let solution = packx::solve_with_memory(
            &canonical_segment,
            mem,
            TEST_DIFFICULTY
        ).expect("Failed to pack segment");

        solution.to_bytes().to_vec()
    }

    fn test_with_larger_stack() -> Result<()> {
        // Setup store and tape address
        let (store, _temp_dir) = setup_store()?;

        // Setup our tape and miner
        let miner_address = Pubkey::new_unique();
        let tape_address = Pubkey::new_unique();

        let mem = packx::build_memory(&miner_address.to_bytes());

        let mut tape_tree = SegmentTree::new(&[tape_address.as_ref()]);
        let mut leaves = vec![];

        let empty_values = get_or_create_empty_hashes(&store, &tape_address)?;
        let empty_leaf = empty_values[0].as_leaf();
        assert_eq!(empty_values.len(), SEGMENT_TREE_HEIGHT);
        assert_eq!(empty_values, empty_values);

        // Fill the tape with some segments (2.5 sectors worth)
        let miner_bytes = miner_address.to_bytes();
        let leaf_count = (SECTOR_LEAVES as f64 * 2.5) as usize;
        for i in 0..leaf_count {
            let segment_id = i as u64;
            let segment_data_packed = create_segment_data(1, &mem);

            // For the in-memory expected tree, use UNPACKED bytes
            let mut data = [0u8; PACKED_SEGMENT_SIZE];
            data.copy_from_slice(&segment_data_packed[..PACKED_SEGMENT_SIZE]);
            let solution = packx::Solution::from_bytes(&data);
            let data_unpacked = solution.unpack(&miner_bytes);

            let leaf = Leaf::new(&[
                &segment_id.to_le_bytes(),
                &data_unpacked
            ]);

            tape_tree.try_add_leaf(leaf).expect("Failed to add leaf");
            store.put_segment(&tape_address, segment_id, segment_data_packed)?;
            leaves.push(leaf);
        }

        let expected_vals = tape_tree.get_layer_nodes(&leaves, SECTOR_TREE_HEIGHT);
        let expected_canopy = expected_vals
            .iter()
            .map(|h| h.to_bytes())
            .collect::<Vec<_>>();

        update_merkle_canopy_for_sector(&store, &miner_address, &tape_address, 0)?;
        update_merkle_canopy_for_sector(&store, &miner_address, &tape_address, 1)?;
        update_merkle_canopy_for_sector(&store, &miner_address, &tape_address, 2)?;

        let actual_canopy = store.get_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer {
                address: tape_address,
                layer: SECTOR_TREE_HEIGHT as u8 
            }
        )?;

        assert_eq!(expected_canopy.len(), actual_canopy.len());
        assert_eq!(expected_canopy, actual_canopy);

        // Lets try a Merkle proof for a segment
        let segment_number : usize = 1234;

        let expected_proof = tape_tree.get_proof(&leaves, segment_number); // <- Requires all leaves in memory (bad)
        let actual_proof = get_cached_merkle_proof(                        // <- Only one sector in memory     (good)
            &tape_tree,
            segment_number,
            SECTOR_TREE_HEIGHT,
            &expected_vals,
            |i| { 
                match store.get_segment(&tape_address, i as u64) {
                    Ok(packed) => {
                        let mut data = [0u8; PACKED_SEGMENT_SIZE];
                        data.copy_from_slice(&packed[..PACKED_SEGMENT_SIZE]);
                        let solution = packx::Solution::from_bytes(&data);
                        let data_unpacked = solution.unpack(&miner_bytes);

                        Some(Leaf::new(&[
                            &(i as u64).to_le_bytes(),
                            &data_unpacked
                        ]))
                    }
                    _ => Some(empty_leaf),
                }
            }
        );

        assert_eq!(expected_proof, actual_proof);

        // Verify the tape root matches our computed root
        let computed_root = get_tape_root(&store, &tape_address)?;
        assert_eq!(computed_root, tape_tree.get_root());

        Ok(())
    }

    #[test]
    fn test_subtree_update() -> Result<()> {
        // TODO: get to the bottom of what is eating the stack space in this test

        let _ = std::thread::Builder::new()
            .name("larger_stack".into())
            .stack_size(4 * 1024 * 1024)
            .spawn(test_with_larger_stack)
            .unwrap()
            .join()
            .unwrap();

        Ok(())
    }
}
