use log::info;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use brine_tree::{Leaf, Hash, MerkleTree};
use tape_api::prelude::*;
use tape_client::{get_epoch_account, get_tape_account};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::store::*;
use super::queue::Rx;

/// Orchestrator Task C – CPU-heavy preprocessing (packx)
pub async fn run(rpc: Arc<RpcClient>, mut rx: Rx, miner: Pubkey, store: Arc<TapeStore>) -> Result<()> {

    // Fetch packing difficulty once before the loop
    // TODO: update this once packing difficulty can change dynamically
    let epoch = get_epoch_account(&rpc).await?.0;
    let packing_difficulty = epoch.packing_difficulty;

    while let Some(job) = rx.recv().await {
        let store = store.clone();
        let _rpc = rpc.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            log::info!("packx: tape={} seg={}", job.tape, job.seg_no);

            let solved = pack_segment(&miner, &job.data, packing_difficulty)?;
            store.put_segment(&job.tape, job.seg_no, solved)?;

            // update_tree(&store, &rpc, &job.tape, job.seg_no)?;

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

// TODO: this is a work in progress still... 
pub fn update_tree(
    store: &Arc<TapeStore>,
    rpc: &Arc<RpcClient>,
    tape_address: &Pubkey,
    segment_number: u64,
) -> Result<()> {
    const H: usize = SEGMENT_TREE_HEIGHT;
    let sector_number = segment_number / SECTOR_LEAVES as u64;

    // Initialize zero values if not present
    let seeds = match store.get_zeros(tape_address) {
        Ok(seeds) => seeds,
        Err(_) => {
            info!("Updating zeros tape {} segment {}", tape_address, segment_number);
            let handle = tokio::runtime::Handle::current();
            let tape = handle.block_on(
                get_tape_account(rpc, tape_address)
            )?.0;

            if tape.number != 0 {
                store.put_tape_address(tape.number, tape_address)?;
            }

            let tree = SegmentTree::new(&[&tape.merkle_seed]);
            let seeds = tree.zero_values;
            let seeds_bytes = seeds.into_iter().map(|h| h.to_bytes()).collect::<Vec<_>>();

            store.put_zeros(tape_address, &seeds_bytes)?;
            seeds_bytes
        }
    };

    let sector = store.get_sector(tape_address, sector_number)?;

    let zero_values: Vec<_> = seeds
        .into_iter()
        .map(Hash::from)
        .collect();

    if zero_values.len() != H {
        return Err(anyhow!(
            "Invalid number of zero values: expected {}, got {}",
            H,
            zero_values.len()
        ));
    }

    info!("Zero values: {:?}", zero_values.len());
    let empty = zero_values.first().unwrap().as_leaf();

    let seeds_arr: [Hash; H] = zero_values
        .try_into()
        .map_err(|v: Vec<_>| {
            anyhow!(
                "Invalid seed array length: expected {} elements, got {}",
                H,
                v.len()
            )
        })?;

    let mut leaves = vec![empty; SECTOR_LEAVES];
    for (i, leaf) in leaves.iter_mut().enumerate() {
        if let Some(segment) = sector.get_segment(i) {
            let segment_id = (sector_number * SECTOR_LEAVES as u64) + i as u64;
            *leaf = Leaf::new(&[&segment_id.to_le_bytes(), segment]);
        }
    }

    info!("Leaves: {:?}", leaves.len());

    let tree = MerkleTree::<H>::from_zeros(seeds_arr);
    let root = tree.get_layer_nodes(&leaves, 2);

    info!("Tree node: {:?}", root.len());

    Ok(())
}
