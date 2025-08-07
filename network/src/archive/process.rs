use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use tape_api::prelude::*;

pub fn process_segment(miner_address: &Pubkey, segment: &[u8], packing_difficulty: u64) -> Result<Vec<u8>> {
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

fn padded_array<const N: usize>(data: &[u8]) -> [u8; N] {
    let mut result = [0u8; N];
    let len = data.len().min(N);
    result[..len].copy_from_slice(&data[..len]);
    result
}
