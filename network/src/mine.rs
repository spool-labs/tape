use anyhow::{Result, anyhow};
use log::{debug, error};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, pubkey::Pubkey};
use tape_client::mine::mine::perform_mining;
use tokio::time::{sleep, Duration};

use tape_client::utils::*;
use tape_api::prelude::*;

use brine_tree::MerkleTree;
use crankx::equix::SolverMemory;
use crankx::{
    solve_with_memory,
    Solution, 
    CrankXError
};

use super::store::TapeStore;

use std::sync::{Arc, mpsc::{channel, Sender, Receiver}};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use num_cpus;

pub async fn mine_loop(
    store: &TapeStore, 
    client: &RpcClient, 
    miner_address: &Pubkey,
    signer: &Keypair,
) -> Result<()> {
    let interval = Duration::from_secs(1);

    loop {
        match try_mine_iteration(store, client, miner_address, signer).await {
            Ok(()) => debug!("Mining iteration completed successfully"),
            Err(e) => {
                // Log the error (you can use a proper logger like `log::error!` if set up)
                error!("Mining iteration failed: {:?}", e);
            }
        }

        debug!("Waiting for next interval...");
        sleep(interval).await;
    }
}

async fn try_mine_iteration(
    store: &TapeStore,
    client: &RpcClient,
    miner_address: &Pubkey,
    signer: &Keypair,
) -> Result<()> {
    debug!("Starting mine process...");

    let epoch = get_epoch_account(client)
        .await
        .map_err(|e| anyhow!("Failed to get epoch account: {}", e))?.0;

    let block = get_block_account(client)
        .await
        .map_err(|e| anyhow!("Failed to get block account: {}", e))?.0;

    let miner = get_miner_account(client, miner_address)
        .await
        .map_err(|e| anyhow!("Failed to get miner account: {}", e))?.0;

    let miner_challenge = compute_challenge(
        &block.challenge,
        &miner.challenge,
    );

    let tape_number = compute_recall_tape(
        &miner_challenge,
        block.challenge_set
    );

    debug!("Recall tape number: {:?}", tape_number);

    let tape_address = store.get_tape_address(tape_number);

    if let Ok(tape_address) = tape_address {
        let tape = get_tape_account(client, &tape_address)
            .await
            .map_err(|e| anyhow!("Failed to get tape account: {}", e))?.0;

        let segment_number = compute_recall_segment(
            &miner_challenge,
            tape.total_segments
        );

        // Find the slot for the segment
        let segment_slot = store.get_slot(tape_number, segment_number)?;

        // Get the entire tape
        let segments = store.get_tape_segments(&tape_address)?;
        if segments.len() != tape.total_segments as usize {
            return Err(anyhow!("Local store is missing some segments for tape number {}: expected {}, got {}", 
                tape_address, tape.total_segments, segments.len()));
        }

        debug!("Recall tape {}, segment {}, slot: {:?}", tape_number, segment_number, segment_slot);

        let (solution, recall_segment, merkle_proof) = 
            compute_challenge_solution(
                &tape,
                &miner_challenge,
                segment_number,
                segments,
                epoch.target_difficulty,
            )?;

        let sig = perform_mining(
            client, 
            signer, 
            *miner_address, 
            tape_address, 
            solution, 
            recall_segment, 
            merkle_proof,
        ).await?;

        debug!("Mining successful! Signature: {:?}", sig);
    } else {
        debug!("Tape not found, continuing...");
    }

    debug!("Catching up with primary...");
    store.catch_up_with_primary()?;

    Ok(())
}

fn compute_challenge_solution(
    tape: &Tape,
    miner_challenge: &[u8; 32],
    segment_number: u64,
    segments: Vec<(u64, Vec<u8>)>,
    epoch_difficulty: u64,
) -> Result<(Solution, [u8; SEGMENT_SIZE], [[u8; 32]; TREE_HEIGHT])> {

    let mut leaves = Vec::new();
    let mut recall_segment = [0; SEGMENT_SIZE];
    let mut merkle_tree = MerkleTree::<{TREE_HEIGHT}>::new(&[tape.merkle_seed.as_ref()]);

    for (segment_id, segment_data) in segments.iter() {
        if *segment_id == segment_number {
            recall_segment.copy_from_slice(segment_data);
        }

        // Create our canonical segment of exactly SEGMENT_SIZE bytes 
        // and compute the merkle leaf

        let data = padded_array::<SEGMENT_SIZE>(segment_data);
        let leaf = compute_leaf(
            *segment_id,
            &data,
        );

        leaves.push(leaf);

        // TODO: we don't actually need to do this, this is just for 
        // debugging and making sure the local root matches the tape root
        merkle_tree.try_add_leaf(leaf).map_err(|e| {
            anyhow!("Failed to add leaf to Merkle tree: {:?}", e)
        })?;
    }

    let merkle_proof = merkle_tree.get_merkle_proof(&leaves, segment_number as usize);
    let merkle_proof = merkle_proof
        .iter()
        .map(|v| v.to_bytes())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    if merkle_tree.get_root() != tape.merkle_root.into() {
        return Err(anyhow!("Merkle root mismatch"));
    } else {
        debug!("Merkle root matches tape root!");
    }

    let solution = solve_challenge(
        *miner_challenge, 
        &recall_segment, 
        epoch_difficulty
    )?;

    debug!("Solution difficulty: {:?}", solution.difficulty());

    solution.is_valid(&miner_challenge, &recall_segment)
        .map_err(|_| anyhow!("Invalid solution"))?;

    debug!("Solution is valid!");

    Ok((solution, recall_segment, merkle_proof))
}

fn solve_challenge<const N: usize>(
    challenge: [u8; 32],
    data: &[u8; N],
    difficulty: u64,
) -> Result<Solution, CrankXError> {
    let num_threads = num_cpus::get();
    let (tx, rx): (Sender<Solution>, Receiver<Solution>) = channel();
    let found = Arc::new(AtomicBool::new(false));
    let challenge_arc = Arc::new(challenge);
    let data_arc = Arc::new(*data);
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let tx_clone = tx.clone();
        let found_clone = found.clone();
        let challenge_clone = challenge_arc.clone();
        let data_clone = data_arc.clone();

        let handle = thread::spawn(move || {
            let mut memory = SolverMemory::new();
            let mut nonce: u64 = i as u64;

            loop {
                if found_clone.load(Ordering::Relaxed) {
                    break;
                }

                if let Ok(solution) = solve_with_memory(
                    &mut memory,
                    &*challenge_clone,
                    &*data_clone,
                    &nonce.to_le_bytes(),
                ) {
                    if solution.difficulty() >= difficulty as u32 {
                        found_clone.store(true, Ordering::Relaxed);
                        let _ = tx_clone.send(solution);
                        break;
                    }
                }
                // If solve_with_memory returns Err, skip and continue, as in the original

                nonce += num_threads as u64;
            }
        });

        handles.push(handle);
    }

    let solution = rx.recv().map_err(|_| CrankXError::EquiXFailure)?;

    // Ensure all threads stop
    found.store(true, Ordering::Relaxed);

    // Wait for all threads to finish
    for handle in handles {
        let _ = handle.join();
    }

    Ok(solution)
}
