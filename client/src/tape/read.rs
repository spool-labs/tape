use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status_client_types::TransactionDetails;
use std::collections::{BinaryHeap, HashMap, HashSet};
use tape_api::prelude::*;
use crate::utils::*;

pub struct ReadState {
    segments: HashMap<u64, Vec<u8>>,
    visited: HashSet<u64>,
    queue: BinaryHeap<u64>,
}

impl ReadState {
    pub fn segments_len(&self) -> usize {
        self.segments.len()
    }
}

pub fn init_read(start_slot: u64) -> ReadState {
    let mut queue = BinaryHeap::new();
    queue.push(start_slot);
    ReadState {
        segments: HashMap::new(),
        visited: HashSet::new(),
        queue,
    }
}

pub async fn process_next_block(
    client: &RpcClient,
    tape_address: &Pubkey,
    state: &mut ReadState,
) -> Result<bool> {
    if let Some(current_slot) = state.queue.pop() {
        if !state.visited.insert(current_slot) {
            return Ok(!state.queue.is_empty());
        }

        let block = get_block_by_number(client, current_slot, TransactionDetails::Full).await?;
        let processed = process_block(block, current_slot)?;

        let mut parents: HashSet<u64> = HashSet::new();

        for (key, data) in &processed.segment_writes {
            if key.address != *tape_address {
                continue;
            }

            // TODO: Check if this works for updates (if we go to a previous slot it might
            // overwrite)

            state.segments
                .entry(key.segment_number)
                .or_insert(data.clone());

            if key.prev_slot != 0 {
                if key.prev_slot > current_slot {
                    return Err(anyhow!("Parent slot must be earlier than current"));
                }

                parents.insert(key.prev_slot);
            }
        }

        for parent in parents {
            state.queue.push(parent);
        }

        Ok(!state.queue.is_empty())
    } else {
        Ok(false)
    }
}

pub fn finalize_read(state: ReadState) -> Result<Vec<u8>> {
    // Sort segments by key and concatenate data
    let mut keys: Vec<u64> = state.segments.keys().cloned().collect();
    keys.sort();

    let mut output = Vec::new();
    for key in keys {
        let segment = padded_array::<SEGMENT_SIZE>(&state.segments[&key]);
        output.extend_from_slice(&segment);
    }

    Ok(output)
}

pub async fn read_from_block(
    client: &RpcClient,
    tape_address: &Pubkey,
    slot: u64,
) -> Result<Vec<u8>> {
    let mut state = init_read(slot);

    while process_next_block(client, tape_address, &mut state).await? {}

    finalize_read(state)
}
