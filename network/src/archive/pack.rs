use log::info;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use brine_tree::{Leaf, Hash};
use tape_api::prelude::*;
use tape_client::{get_epoch_account, get_tape_account};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::store::*;
use super::queue::Rx;

const LAYER_NUMBER: u8 = 10;

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

/// Updates the Merkle subtree for a given sector number.
pub async fn update_sector_root(
    store: &Arc<TapeStore>,
    rpc: &Arc<RpcClient>,
    tape_address: &Pubkey,
    sector_number: u64,
) -> Result<()> {

    let empty_hashes = get_or_create_empty_hashes(store, rpc, tape_address).await?;
    let empty_leaf = empty_hashes.first().unwrap().as_leaf();

    let leaves = compute_sector_leaves(store, tape_address, sector_number, empty_leaf)?;
    let root = compute_sector_root(&leaves, &empty_hashes, LAYER_NUMBER)?;

    update_layer(store, tape_address, LAYER_NUMBER, sector_number, root)?;

    Ok(())
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
    layer_number: u8,
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

    let layer_nodes = tree.get_layer_nodes(leaves, layer_number as usize);
    if layer_nodes.len() != 1 {
        return Err(anyhow!(
            "Invalid layer nodes length: expected 1, got {}",
            layer_nodes.len()
        ));
    }

    Ok(*layer_nodes.first().unwrap())
}

/// Updates the layer in the store with the new sector root.
pub fn update_layer(
    store: &TapeStore,
    tape_address: &Pubkey,
    layer_number: u8,
    sector_number: u64,
    root: Hash,
) -> Result<()> {
    let mut layer = match store.get_layer(tape_address, layer_number) {
        Ok(layer) => layer,
        Err(_) => vec![[0u8; 32]; (sector_number + 1) as usize],
    };

    if (sector_number as usize) >= layer.len() {
        layer.resize(sector_number as usize + 1, [0u8; 32]);
    }

    layer[sector_number as usize] = root.to_bytes();
    store.put_layer(tape_address, layer_number, &layer)?;
    Ok(())
}

/// Helper to create or init the zero values for a tape
/// Note: This only needs to be done once per tape, the zero values are the height of the tree and
/// are calculated from the tape's merkle seed
pub async fn get_or_create_empty_hashes(
    store: &Arc<TapeStore>,
    rpc: &Arc<RpcClient>,
    tape_address: &Pubkey,
) -> Result<Vec<Hash>> {
    const H: usize = SEGMENT_TREE_HEIGHT;

    let empty_values = match store.get_zero_values(tape_address) {
        Ok(empty_values) => empty_values,
        Err(_) => {
            info!("Updating zeros tape {}", tape_address);
            let tape = get_tape_account(rpc, tape_address).await?.0;

            // Just in case we don't have the tape number mapped, store it now
            if tape.number != 0 {
                store.put_tape_address(tape.number, tape_address)?;
            }

            // Create an empty SegmentTree to get the zero values
            let tree = SegmentTree::new(&[&tape.merkle_seed]);
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

    if empty_values.len() != H {
        return Err(anyhow!("Invalid number of zero values: expected {}, got {}", H, empty_values.len()));
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
    use brine_tree::{Leaf, Hash};

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((store, temp_dir))
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

    fn create_empty_hashes(seed: &[u8]) -> Vec<Hash> {
        let tree = SegmentTree::new(&[seed]);
        tree.zero_values.iter().copied().collect()
    }

    #[test]
    fn test_subtree_update() -> Result<()> {
        // Setup store and tape address
        let (store, _temp_dir) = setup_store()?;
        let tape_address = Pubkey::new_unique();
        let miner_address = Pubkey::new_unique();

        // Create empty hashes and store them
        let seed = b"test_seed";
        let empty_hashes = create_empty_hashes(seed);
        let empty_hashes_bytes = empty_hashes.iter().map(|h| h.to_bytes()).collect::<Vec<_>>();
        store.put_zero_values(&tape_address, &empty_hashes_bytes)?;
        let empty_leaf = empty_hashes.first().unwrap().as_leaf();

        // Create a bunch of leaves (fill 2.5 sectors)
        let leaf_count = (SECTOR_LEAVES as f64 * 2.5) as usize;
        let mut tree = SegmentTree::new(&[seed]);
        let mut leaves = vec![];

        for i in 0..leaf_count {
            let segment_id = i as u64;
            let segment_data = create_segment_data(1, &miner_address);
            let leaf = Leaf::new(&[
                &segment_id.to_le_bytes(),
                &segment_data
            ]);

            // Add leaf to the tree
            tree.try_add_leaf(leaf).expect("Failed to add leaf");

            // Store the segment in a sector
            let sector_number = segment_id / SECTOR_LEAVES as u64;
            let local_seg_idx = (segment_id % SECTOR_LEAVES as u64) as usize;
            let mut sector = store
                .get_sector(&tape_address, sector_number)
                .unwrap_or_else(|_| Sector::new());
            sector.set_segment(local_seg_idx, &segment_data);
            store.put_sector(&tape_address, sector_number, &sector)?;

            // Collect leaves so we can verify later
            leaves.push(leaf);
        }

        // Verify sector count
        assert_eq!(store.get_sector_count(&tape_address)?, 3);

        // Add a new leaf and test subtree update
        let segment_number = leaves.len() as u64;
        let sector_number = segment_number / SECTOR_LEAVES as u64;
        let segment_data = create_segment_data(42, &miner_address);
        let new_leaf = Leaf::new(&[
            &segment_number.to_le_bytes(),
            &segment_data
        ]);

        tree.try_add_leaf(new_leaf).expect("Failed to add new leaf");
        leaves.push(new_leaf);

        // Store the leaf so that compute_sector_leaves can find it
        let mut sector = store
            .get_sector(&tape_address, sector_number)
            .unwrap_or_else(|_| Sector::new());
        let local_seg_idx = (segment_number % SECTOR_LEAVES as u64) as usize;
        sector.set_segment(local_seg_idx, &segment_data);
        store.put_sector(&tape_address, sector_number, &sector)?;

        // Compute leaves for the sector
        let sector_leaves = compute_sector_leaves(&store, &tape_address, sector_number, empty_leaf)?;

        assert_eq!(sector_leaves.len(), SECTOR_LEAVES, "Incorrect number of leaves");
        assert_eq!(sector_leaves[local_seg_idx], new_leaf);

        // Compute the sector root
        let root = compute_sector_root(&sector_leaves, &empty_hashes, 10)?;

        // Verify root using the existing tree
        let layer_nodes = tree.get_layer_nodes(&leaves, 10);

        assert_eq!(layer_nodes.len(), 3);
        assert_eq!(root, layer_nodes[sector_number as usize]);

        // Update the layer
        update_layer(&store, &tape_address, 10, sector_number, root)?;

        // Verify the layer
        let layer = store.get_layer(&tape_address, 10)?;
        assert_eq!(layer.len(), sector_number as usize + 1);

        // The first two will be empty because we never called update_layer for them
        assert_eq!(layer[0], [0u8; 32]);
        assert_eq!(layer[1], [0u8; 32]);
        assert_eq!(layer[sector_number as usize], root.to_bytes());

        Ok(())
    }
}
