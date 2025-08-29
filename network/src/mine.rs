use num_cpus;
use anyhow::{anyhow, Result};
use bytemuck::Zeroable;
use log::{debug, error};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, pubkey::Pubkey};
use tape_client::mine::mine::perform_mining;
use tokio::time::{sleep, Duration};
use brine_tree::{Hash, Leaf, get_cached_merkle_proof};

use crankx::equix::SolverMemory;
use crankx::{
    solve_with_memory,
    Solution, 
    CrankXError
};

use crate::metrics::{
    inc_tape_mining_attempts_total, 
    inc_tape_mining_challenges_solved_total, 
    observe_tape_mining_duration, 
    run_metrics_server, 
    set_current_mining_iteration, 
    Process
};
use crate::store::*;

use std::sync::{Arc, mpsc::{channel, Sender, Receiver}};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use std::time::Instant;
use tape_client::utils::*;
use tape_api::prelude::*;

pub async fn mine_loop(
    store: TapeStore, 
    client: &Arc<RpcClient>, 
    miner_address: &Pubkey,
    signer: &Keypair,
) -> Result<()> {

    // run metrics server
    run_metrics_server(Process::Mine)?;

    let store = Arc::new(store);
    let interval = Duration::from_secs(1);
    let refresh_store_instance = store.clone();
    
    run_refresh_store(&refresh_store_instance);

    let mut iteration = 0;

    loop {
        set_current_mining_iteration(iteration);
        match try_mine_iteration(&store, client, miner_address, signer).await {
            Ok(()) => debug!("Mining iteration completed successfully"),
            Err(e) => {
                // Log the error (you can use a proper logger like `log::error!` if set up)
                error!("Mining iteration failed: {e:?}");
            }
        }

        debug!("Waiting for next interval...");
        sleep(interval).await;
        iteration += 1;
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

    let (epoch, block, miner) = get_mining_accounts(client, miner_address).await?;

    let miner_challenge = compute_challenge(
        &block.challenge,
        &miner.challenge,
    );

    let tape_number = compute_recall_tape(
        &miner_challenge,
        block.challenge_set
    );

    let res = store.get_tape_address(tape_number);
    if res.is_err() {
        debug!("Tape address not found in local db, nothing to do for now...");
        return Ok(());
    }

    let tape_address = res.unwrap();
    debug!("Tape address: {tape_address:?}");

    let (tape, _) = get_tape_account(client, &tape_address)
        .await
        .map_err(|e| anyhow!("Failed to get tape account: {}", e))?;


    if tape.has_minimum_rent() {
        // We need to provide a PoA solution

        let segment_number = compute_recall_segment(
            &miner_challenge, 
            tape.total_segments
        );

        let canopy_values = store.get_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer {
                address: tape_address,
                layer: SECTOR_TREE_HEIGHT as u8 
            }
        )?;

        let canopy_hashes: Vec<_> = canopy_values
            .into_iter()
            .map(Hash::from)
            .collect();

        let miner_bytes = miner_address.to_bytes();
        let merkle_tree = SegmentTree::new(&[tape_address.as_ref()]);

        // This only fetches 1024 segments max, which is fine for now (~150kb in total)
        let get_leaf = |i| { 
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
                _ => Some(merkle_tree.get_empty_leaf()),
            }
        };

        // Get the Merkle proof for the segment (using a pre-cached canopy)
        let proof_nodes = get_cached_merkle_proof(
            &merkle_tree,
            segment_number as usize,
            SECTOR_TREE_HEIGHT,
            &canopy_hashes,
            get_leaf
        );

        let proof_nodes: Vec<[u8; 32]> = proof_nodes
            .into_iter()
            .map(|h| h.to_bytes())
            .collect();

        let proof_path = ProofPath::from_slice(&proof_nodes).unwrap();

        let segment = store.get_segment(&tape_address, segment_number)?;
        let mut packed_segment = [0; PACKED_SEGMENT_SIZE];
        packed_segment.copy_from_slice(&segment[..PACKED_SEGMENT_SIZE]);
        let solution = packx::Solution::from_bytes(&packed_segment);
        let unpacked_segment = solution.unpack(&miner_address.to_bytes());

        let poa_solution = packx::Solution::from_bytes(&packed_segment);
        let pow_solution = solve_challenge(
            miner_challenge,
            &unpacked_segment, 
            epoch.mining_difficulty
        ).unwrap();

        debug_assert!(pow_solution.is_valid(&miner_challenge, &unpacked_segment).is_ok());

        let pow = PoW::from_solution(&pow_solution);
        let poa = PoA::from_solution(&poa_solution, proof_path);

        // Tx1: load the packed tape leaf from the spool onto the miner commitment field
        // TODO: leaving this out for now, as it requries managing miner spools

        //commit_for_mining(
        //    svm, 
        //    &payer, 
        //    &stored_spool, 
        //    tape_index, 
        //    segment_number
        //);

        // Tx2: perform mining with PoW and PoA
        perform_mining(
            client,
            signer,
            *miner_address,
            tape_address,
            pow,
            poa
        ).await?;


    } else {

        let solution = solve_challenge(
            miner_challenge, 
            &EMPTY_SEGMENT, 
            epoch.mining_difficulty
        ).unwrap();

        let pow = PoW::from_solution(&solution);
        let poa = PoA::zeroed();

        perform_mining(
            client,
            signer,
            *miner_address,
            tape_address,
            pow,
            poa
        ).await?;
    }

    Ok(())
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
            let start = Instant::now();
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
                        let elapsed = start.elapsed();
                        observe_tape_mining_duration(elapsed.as_secs_f64());
                        found_clone.store(true, Ordering::Relaxed);
                        inc_tape_mining_challenges_solved_total();
                        let _ = tx_clone.send(solution);
                        break;
                    }
                }
                // If solve_with_memory returns Err, skip and continue, as in the original

                nonce += num_threads as u64;
            }
        });

        handles.push(handle);
        inc_tape_mining_attempts_total();
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
