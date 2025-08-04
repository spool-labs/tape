use anyhow::{anyhow, Result};
use log::{debug, error};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, pubkey::Pubkey};
use tape_client::mine::mine::perform_mining;
use tokio::time::{sleep, Duration};

use tape_client::utils::*;
use tape_api::prelude::*;

use crankx::equix::SolverMemory;
use crankx::{
    solve_with_memory,
    Solution, 
    CrankXError
};

use crate::store::run_refresh_store;

use super::store::TapeStore;

use std::sync::{Arc, mpsc::{channel, Sender, Receiver}};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use num_cpus;

pub async fn mine_loop(
    store: TapeStore, 
    client: &Arc<RpcClient>, 
    miner_address: &Pubkey,
    signer: &Keypair,
) -> Result<()> {
    let store = Arc::new(store);

    let interval = Duration::from_secs(1);

    let refresh_store_instance = store.clone();
    
    run_refresh_store(&refresh_store_instance);

    loop {
        match try_mine_iteration(&store, client, miner_address, signer).await {
            Ok(()) => debug!("Mining iteration completed successfully"),
            Err(e) => {
                // Log the error (you can use a proper logger like `log::error!` if set up)
                error!("Mining iteration failed: {e:?}");
            }
        }

        debug!("Waiting for next interval...");
        sleep(interval).await;
    }
}

async fn get_mining_accounts(
    client: &Arc<RpcClient>,
    miner_address: &Pubkey
) -> Result<(Epoch, Block, Miner)> {
     
    let (epoch_res, block_res, miner_res) = tokio::join!(
        get_epoch_account(client),
        get_block_account(client),
        get_miner_account(client, miner_address),
    );

    let (epoch, block, miner) = (
        epoch_res.map_err(|e| anyhow!("Failed to get epoch account: {}", e))?.0,
        block_res.map_err(|e| anyhow!("Failed to get block account: {}", e))?.0,
        miner_res.map_err(|e| anyhow!("Failed to get miner account: {}", e))?.0,
    );

    Ok((epoch, block, miner))
}

async fn try_mine_iteration(
    store: &TapeStore,
    client: &Arc<RpcClient>,
    miner_address: &Pubkey,
    signer: &Keypair,
) -> Result<()> {
    debug!("Starting mine process...");

    // fetch epoch, block and miner accounts concurrently
    let (epoch, block, miner) = get_mining_accounts(client, miner_address).await?;

    let miner_challenge = compute_challenge(
        &block.challenge,
        &miner.challenge,
    );

    let tape_number = compute_recall_tape(
        &miner_challenge,
        block.challenge_set
    );

    debug!("Recall tape number: {tape_number:?}");

    let tape_address = store.read_tape_address(tape_number);

    if let Ok(tape_address) = tape_address {

        debug!("Tape address: {tape_address:?}");

        let tape = get_tape_account(client, &tape_address)
            .await
            .map_err(|e| anyhow!("Failed to get tape account: {}", e))?.0;
        
        let (solution, recall_segment, merkle_proof) = if tape.has_minimum_rent() {
            
            // This tape has minimum rent, we can recall a segment
            let segment_number = compute_recall_segment(
                &miner_challenge,
                tape.total_segments
            );

            // Get the entire tape
            let segments = store.read_tape_segments(&tape_address)?;
            if segments.len() != tape.total_segments as usize {
                return Err(anyhow!("Local store is missing some segments for tape number {}: expected {}, got {}", 
                    tape_address, tape.total_segments, segments.len()));
            }

            debug!("Recall tape {tape_number}, segment {segment_number}");

            compute_challenge_solution(
                &tape,
                &miner_challenge,
                segment_number,
                segments,
                epoch.mining_difficulty,
            )?

        // This tape does not have minimum rent, we use an empty segment
        } else {

            debug!("Tape {tape_address} does not have minimum rent, using empty segment");

            let solution = solve_challenge(
                miner_challenge,
                &EMPTY_SEGMENT,
                epoch.mining_difficulty,
            )?;

            (solution, EMPTY_SEGMENT, EMPTY_PROOF)
        };

        let sig = perform_mining(
            client, 
            signer, 
            *miner_address, 
            tape_address, 
            solution, 
            recall_segment, 
            merkle_proof,
        ).await?;

        debug!("Mining successful! Signature: {sig:?}");

        let (miner, _) = get_miner_account(client, miner_address)
            .await
            .map_err(|e| anyhow!("Failed to get miner account after mining: {}", e))?;

        debug!("Miner {} has unclaimed rewards: {}", miner_address, miner.unclaimed_rewards);

    } else {
        debug!("Tape not found, continuing...");
    }

    debug!("Catching up with primary...");

    Ok(())
}

fn compute_challenge_solution(
    tape: &Tape,
    miner_challenge: &[u8; 32],
    segment_number: u64,
    segments: Vec<(u64, Vec<u8>)>,
    epoch_difficulty: u64,
) -> Result<(Solution, [u8; SEGMENT_SIZE], [[u8; 32]; SEGMENT_TREE_HEIGHT])> {

    let mut leaves = Vec::new();
    let mut recall_segment = [0; SEGMENT_SIZE];
    let mut merkle_tree = SegmentTree::new(&[tape.merkle_seed.as_ref()]);

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
        .map_err(|_|anyhow!("failed to get merkle proof"))?;

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

    solution.is_valid(miner_challenge, &recall_segment)
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
                    &challenge_clone,
                    &data_clone,
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
