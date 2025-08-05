#![cfg(test)]
pub mod utils;
use utils::*;

use steel::Zeroable;
use solana_sdk::{
    signer::Signer,
    transaction::Transaction,
    pubkey::Pubkey,
    signature::Keypair,
    clock::Clock,
    instruction::Instruction,
};

use brine_tree::Leaf;
use tape::miner::get_base_rate;
use tape_api::prelude::*;
use tape_api::instruction;
use litesvm::LiteSVM;

use packx;
use crankx::equix::SolverMemory;
use crankx::{
    solve_with_memory,
    Solution, 
    CrankXError
};

struct StoredSpool {
    //number: u64,
    address: Pubkey,
    miner: Pubkey,
    tree: TapeTree,
    tapes: Vec<PackedTape>,
    //account: Spool,
}

struct StoredTape {
    number: u64,
    address: Pubkey,
    segments: Vec<Vec<u8>>,
    account: Tape,
}

struct PackedTape {
    number: u64,
    address: Pubkey,
    tree: SegmentTree,
    data: Vec<Vec<u8>>,
}



#[test]
fn run_integration() {

    let mut svm = SvmWithCUTracker::new();

    // Initialize program
    initialize_program(&mut svm);

    let owner_pk = svm.payer.pubkey();
    // Register miner
    let miner_name = "miner-name";
    let miner_address = register_miner(&mut svm, miner_name);
    let ata = create_ata(&mut svm, &MINT_ADDRESS, &owner_pk);

    // Create a miner spool
    let spool_number = 1;
    let mut stored_spool = create_spool(&mut svm, miner_address, spool_number);

    // Fetch and store genesis tape
    let genesis_tape = get_genesis_tape(&mut svm);

    // Pack the tape into a miner specific representation
    pack_tape(&mut svm, &genesis_tape, &mut stored_spool);

    // Verify initial accounts
    verify_archive_account(&svm, 1);
    verify_epoch_account(&svm);
    verify_block_account(&svm);
    verify_treasury_account(&svm);
    verify_mint_account(&svm);
    verify_metadata_account(&svm);
    verify_treasury_ata(&svm);

    // Mine the genesis tape (to earn some tokens)
    do_mining_run(&mut svm, &stored_spool, 5);
    claim_rewards(&mut svm, miner_address, ata);

    let ata_balance = get_ata_balance(&svm, &ata);
    assert!(ata_balance > 0);

    println!("ATA balance after claiming rewards: {ata_balance}");

    // Advance clock
    let mut initial_clock = svm.svm.get_sysvar::<Clock>();
    initial_clock.slot = 10;
    svm.svm.set_sysvar::<Clock>(&initial_clock);

    // Create tapes
    let tape_count = 5;
    for tape_index in 1..tape_count {
        let stored_tape = create_and_verify_tape(&mut svm, ata, tape_index);
        let _packed_tape = pack_tape(&mut svm, &stored_tape, &mut stored_spool);
    }

    // Verify archive account after tape creation
    verify_archive_account(&svm, tape_count);

    // Mine again with more tapes this time
    do_mining_run(&mut svm, &stored_spool, 5);

    svm.commit_cus_change_log();
}

fn setup_environment() -> (LiteSVM, Keypair) {
    let mut svm = setup_svm();
    let payer = create_payer(&mut svm);
    (svm, payer)
}

fn subsidize_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    ata: Pubkey,
    tape_address: Pubkey,
    amount: u64,
) {
    let payer_pk = payer.pubkey();

    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_subsidize_ix(
        payer_pk, 
        ata, 
        tape_address, 
        amount
    );

    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::TapeSubsidize,res.unwrap().compute_units_consumed);

    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();
    assert!(tape.balance >= amount);
}

fn claim_rewards(
    svm: &mut SvmWithCUTracker,
    miner_address: Pubkey,
    miner_ata: Pubkey,
) {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    let payer_pk = payer.pubkey();

    let blockhash = svm.latest_blockhash();
    let ix = instruction::miner::build_claim_ix(
        payer_pk, 
        miner_address, 
        miner_ata, 
        0 // Claim all unclaimed rewards
    );

    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::MinerClaim, res.unwrap().compute_units_consumed);

    // Verify miner account after claiming rewards
    let account = svm.get_account(&miner_address).unwrap();
    let miner = Miner::unpack(&account.data).unwrap();

    assert!(miner.unclaimed_rewards == 0);
}

fn do_mining_run(
    svm: &mut SvmWithCUTracker,
    stored_spool: &StoredSpool,
    num_iterations: u64,
) {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    for _ in 0..num_iterations {
        // We need to expire the blockhash because we're not checking if the mining commitment
        // needs to change (when it doesn't, we get a AlreadyProcessed error). Todo, check before
        // submitting the transaction if the commitment is still valid.

        let mut current_clock = svm.get_sysvar::<Clock>();
        current_clock.slot = current_clock.slot + 10;
        svm.set_sysvar::<Clock>(&current_clock);
        svm.expire_blockhash();

        let (epoch_address, _epoch_bump) = epoch_pda();
        let epoch_account = svm.get_account(&epoch_address).unwrap();
        let epoch = Epoch::unpack(&epoch_account.data).unwrap();

        let (block_address, _block_bump) = block_pda();
        let block_account = svm.get_account(&block_address).unwrap();
        let block = Block::unpack(&block_account.data).unwrap();

        let miner_account = svm.get_account(&stored_spool.miner).unwrap();
        let miner = Miner::unpack(&miner_account.data).unwrap();

        let miner_challenge = compute_challenge(
            &block.challenge,
            &miner.challenge,
        );

        let recall_tape = compute_recall_tape(
            &miner_challenge,
            block.challenge_set
        );

        // Compute challenge solution (proof of work challenge)

        let tape_index = recall_tape - 1; // index in spool (not the tape_number)
        let packed_tape = &stored_spool.tapes[tape_index as usize];
        let tape_account = svm.get_account(&packed_tape.address).unwrap();
        let tape = Tape::unpack(&tape_account.data).unwrap();

        // Check if we need to provide a PoA solution based on whether the tape has minimum rent.
        // (Note: We always need to provide a PoW solution)

        if tape.has_minimum_rent() {
            // We need to provide a PoA solution

            let miner_address = stored_spool.miner;
            let segment_number = compute_recall_segment(
                &miner_challenge, 
                tape.total_segments
            );

            // Unpack the whole tape 
            // (todo: this could be up to 32Mb and not really trival with ~262k segments)

            let mut leaves = Vec::new();
            let mut packed_segment = [0; packx::SOLUTION_SIZE];
            let mut unpacked_segment = [0; SEGMENT_SIZE];

            for (segment_id, packed_data) in packed_tape.data.iter().enumerate() {
                let mut data = [0u8; packx::SOLUTION_SIZE];
                data.copy_from_slice(&packed_data[..packx::SOLUTION_SIZE]);

                let solution = packx::Solution::from_bytes(&data);
                let segement_data = solution.unpack(&miner_address.to_bytes());

                let leaf = compute_leaf(
                    segment_id as u64,
                    &segement_data,
                );

                leaves.push(leaf);

                if segment_id == segment_number as usize {
                    packed_segment.copy_from_slice(&data);
                    unpacked_segment.copy_from_slice(&segement_data);
                }
            }

            assert_eq!(leaves.len(), tape.total_segments as usize);

            println!("Recall segment: {segment_number}");

            let poa_solution = packx::Solution::from_bytes(&packed_segment);
            let pow_solution = solve_challenge(miner_challenge, &unpacked_segment, epoch.mining_difficulty).unwrap();
            assert!(pow_solution.is_valid(&miner_challenge, &unpacked_segment).is_ok());

            let merkle_tree = SegmentTree::new(&[tape.merkle_seed.as_ref()]);
            let merkle_proof = merkle_tree.get_merkle_proof(&leaves, segment_number as usize);
            let merkle_proof = merkle_proof
                .iter()
                .map(|v| v.to_bytes())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap();

            let pow = PoW::from_solution(&pow_solution);
            let poa = PoA::from_solution(&poa_solution, merkle_proof);

            // Tx1: load the packed tape leaf from the spool onto the miner commitment field
            commit_for_mining(
                svm, 
                &payer, 
                cu_tracker,
                &stored_spool, 
                tape_index, 
                segment_number
            );

            // Tx2: perform mining with PoW and PoA
            perform_mining(
                svm,
                payer,
                cu_tracker,
                stored_spool.miner,
                packed_tape.address,
                pow,
                poa
            );

        } else {

            let solution = solve_challenge(
                miner_challenge, 
                &EMPTY_SEGMENT, 
                epoch.mining_difficulty
            ).unwrap();

            let pow = PoW::from_solution(&solution);
            let poa = PoA::zeroed();

            perform_mining(
                svm,
                payer,
                cu_tracker,
                stored_spool.miner,
                packed_tape.address,
                pow,
                poa
            );
        }
    }
}

fn get_genesis_tape(svm: &mut SvmWithCUTracker) -> StoredTape {
    let SvmWithCUTracker { svm, cu_tracker:_, payer } = svm;
    let genesis_name = "genesis".to_string();
    let genesis_name_bytes = to_name(&genesis_name);
    let (genesis_pubkey, _) = tape_find_pda(payer.pubkey(), &genesis_name_bytes);

    let account = svm.get_account(&genesis_pubkey).expect("Genesis tape should exist");
    let tape = Tape::unpack(&account.data).expect("Failed to unpack genesis tape");

    assert!(tape.can_finalize());

    let genesis_data = b"hello, world";
    let genesis_segment = padded_array::<SEGMENT_SIZE>(genesis_data).to_vec();
    let segments = vec![genesis_segment];

    let stored_genesis = StoredTape {
        number: tape.number,
        address: genesis_pubkey,
        segments,
        account: *tape,
    };

    stored_genesis
}


fn initialize_program(svm: &mut SvmWithCUTracker) {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    let payer_pk = payer.pubkey();
    let ix = instruction::program::build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::ProgramInitialize, res.unwrap().compute_units_consumed);
}

fn verify_archive_account(svm: &SvmWithCUTracker, expected_tapes_stored: u64) {
    let SvmWithCUTracker {svm,..} = svm;

    let (archive_address, _archive_bump) = archive_pda();
    let account = svm
        .get_account(&archive_address)
        .expect("Archive account should exist");
    let archive = Archive::unpack(&account.data).expect("Failed to unpack Archive account");
    assert_eq!(archive.tapes_stored, expected_tapes_stored);
}

fn verify_epoch_account(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (epoch_address, _epoch_bump) = epoch_pda();
    let account = svm
        .get_account(&epoch_address)
        .expect("Epoch account should exist");
    let epoch = Epoch::unpack(&account.data).expect("Failed to unpack Epoch account");
    assert_eq!(epoch.number, 1);
    assert_eq!(epoch.progress, 0);
    assert_eq!(epoch.mining_difficulty, MIN_MINING_DIFFICULTY);
    assert_eq!(epoch.packing_difficulty, MIN_PACKING_DIFFICULTY);
    assert_eq!(epoch.target_participation, MIN_PARTICIPATION_TARGET);
    assert_eq!(epoch.reward_rate, get_base_rate(1));
    assert_eq!(epoch.duplicates, 0);
    assert_eq!(epoch.last_epoch_at, 0);
}

fn verify_block_account(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (block_address, _block_bump) = block_pda();
    let account = svm
        .get_account(&block_address)
        .expect("Block account should exist");
    let block = Block::unpack(&account.data).expect("Failed to unpack Block account");
    assert_eq!(block.number, 1);
    assert_eq!(block.progress, 0);
    assert_eq!(block.last_proof_at, 0);
    assert_eq!(block.last_block_at, 0);
    assert_eq!(block.challenge_set, 1);
    assert!(block.challenge.ne(&[0u8; 32]));
}

fn verify_treasury_account(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (treasury_address, _treasury_bump) = treasury_pda();
    let _treasury_account = svm
        .get_account(&treasury_address)
        .expect("Treasury account should exist");
}

fn verify_mint_account(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (mint_address, _mint_bump) = mint_pda();
    let mint = get_mint(svm, &mint_address);
    assert_eq!(mint.supply, MAX_SUPPLY);
    assert_eq!(mint.decimals, TOKEN_DECIMALS);
}

fn verify_metadata_account(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (mint_address, _mint_bump) = mint_pda();
    let (metadata_address, _metadata_bump) = metadata_find_pda(mint_address);
    let account = svm
        .get_account(&metadata_address)
        .expect("Metadata account should exist");
    assert!(!account.data.is_empty());
}

fn verify_treasury_ata(svm: &SvmWithCUTracker) {
    let SvmWithCUTracker {svm,..} = svm;

    let (treasury_ata_address, _ata_bump) = treasury_ata();
    let account = svm
        .get_account(&treasury_ata_address)
        .expect("Treasury ATA should exist");
    assert!(!account.data.is_empty());
}


fn create_and_verify_tape(
    svm: &mut SvmWithCUTracker,
    ata: Pubkey,
    tape_index: u64,
) -> StoredTape {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    let payer_pk = payer.pubkey();
    let tape_name = format!("tape-name-{tape_index}");

    let (tape_address, _tape_bump) = tape_find_pda(payer_pk, &to_name(&tape_name));
    let (writer_address, _writer_bump) = writer_find_pda(tape_address);

    // Create tape and verify initial state
    let mut stored_tape = create_tape(
        svm, 
        payer, 
        cu_tracker,
        &tape_name, 
        tape_address, 
        writer_address
    );

    let tape_seed = &[stored_tape.account.merkle_seed.as_ref()];
    let mut writer_tree = SegmentTree::new(tape_seed);

    write_tape(
        svm,
        payer,
        cu_tracker,
        tape_address,
        writer_address,
        &mut stored_tape,
        &mut writer_tree,
    );

    update_tape(
        svm,
        payer,
        cu_tracker,
        tape_address,
        writer_address,
        &mut stored_tape,
        &mut writer_tree,
    );

    let min_rent = min_finalization_rent(
        stored_tape.account.total_segments,
    );

    subsidize_tape(
        svm, 
        payer, 
        cu_tracker,
        ata,
        tape_address, 
        min_rent,
    );

    finalize_tape(
        svm,
        payer,
        cu_tracker,
        tape_address,
        writer_address,
        &mut stored_tape,
        tape_index,
    );

    stored_tape
}

fn create_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    tape_name: &str,
    tape_address: Pubkey,
    writer_address: Pubkey,
) -> StoredTape {
    let payer_pk = payer.pubkey();

    // Create tape
    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_create_ix(payer_pk, tape_name);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::TapeCreate,res.unwrap().compute_units_consumed);

    // Verify tape account
    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();
    assert_eq!(tape.authority, payer_pk);
    assert_eq!(tape.name, to_name(tape_name));
    assert_eq!(tape.state, u64::from(TapeState::Created));
    assert_ne!(tape.merkle_seed, [0; 32]);
    assert_eq!(tape.merkle_root, [0; 32]);
    assert_eq!(tape.header, [0; HEADER_SIZE]);
    assert_eq!(tape.number, 0);

    // Verify writer account
    let account = svm.get_account(&writer_address).unwrap();
    let writer = Writer::unpack(&account.data).unwrap();
    assert_eq!(writer.tape, tape_address);

    let writer_tree = SegmentTree::new(&[tape.merkle_seed.as_ref()]);
    assert_eq!(writer.state, writer_tree);

    StoredTape {
        number: 0,
        address: tape_address,
        segments: vec![],
        account: *tape,
    }
}

fn write_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    tape_address: Pubkey,
    writer_address: Pubkey,
    stored_tape: &mut StoredTape,
    writer_tree: &mut SegmentTree,
) {
    let payer_pk = payer.pubkey();

    for write_index in 0..5u64 {
        let data = format!("<segment_{write_index}_data>").into_bytes();

        let blockhash = svm.latest_blockhash();
        let ix = instruction::tape::build_write_ix(payer_pk, tape_address, writer_address, &data);
        let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
        let res = send_tx(svm, tx);
        assert!(res.is_ok());
        cu_tracker.track_cus(ProgramIx::TapeWrite,res.unwrap().compute_units_consumed);

        // Update local state
        let segments = data.chunks(SEGMENT_SIZE);
        for (segment_number, segment) in segments.enumerate() {
            let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

            assert!(write_segment(
                writer_tree,
                (stored_tape.segments.len() + segment_number) as u64,
                &canonical_segment,
            )
            .is_ok());

            stored_tape.segments.push(canonical_segment.to_vec());
        }

        // Verify writer state
        let account = svm.get_account(&writer_address).unwrap();
        let writer = Writer::unpack(&account.data).unwrap();
        assert_eq!(writer.state.get_root(), writer_tree.get_root());

        // Verify and update tape state
        let account = svm.get_account(&tape_address).unwrap();
        let tape = Tape::unpack(&account.data).unwrap();
        assert_eq!(tape.total_segments, stored_tape.segments.len() as u64);
        assert_eq!(tape.state, u64::from(TapeState::Writing));
        assert_eq!(tape.merkle_root, writer_tree.get_root().to_bytes());
        assert_eq!(tape.header, stored_tape.account.header);

        // Update stored_tape.account
        stored_tape.account = *tape;
    }
}

fn update_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    tape_address: Pubkey,
    writer_address: Pubkey,
    stored_tape: &mut StoredTape,
    writer_tree: &mut SegmentTree,
) {
    let payer_pk = payer.pubkey();
    let target_segment: u64 = 0;

    // Reconstruct leaves for proof
    let mut leaves = Vec::new();
    for (segment_id, segment_data) in stored_tape.segments.iter().enumerate() {
        let data_array = padded_array::<SEGMENT_SIZE>(segment_data);
        let leaf = compute_leaf(
            segment_id as u64, 
            &data_array
        );
        leaves.push(leaf);
    }

    // Compute Merkle proof
    let merkle_proof_vec = writer_tree.get_merkle_proof(&leaves, target_segment as usize);
    let merkle_proof: [[u8; 32]; SEGMENT_PROOF_LEN] = merkle_proof_vec
        .iter()
        .map(|v| v.to_bytes())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    // Prepare data
    let old_data_array: [u8; SEGMENT_SIZE] = stored_tape.segments[target_segment as usize]
        .clone()
        .try_into()
        .unwrap();
    let new_raw = b"<segment_0_updated>";
    let new_data_array = padded_array::<SEGMENT_SIZE>(new_raw);

    // Send update transaction
    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_update_ix(
        payer_pk,
        tape_address,
        writer_address,
        target_segment,
        old_data_array,
        new_data_array,
        merkle_proof,
    );
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::TapeUpdate,res.unwrap().compute_units_consumed);

    // Update local tree
    assert!(update_segment(
        writer_tree,
        target_segment,
        &old_data_array,
        &new_data_array,
        &merkle_proof,
    )
    .is_ok());

    // Update stored tape segments
    stored_tape.segments[target_segment as usize] = new_data_array.to_vec();

    // Verify writer state
    let account = svm.get_account(&writer_address).unwrap();
    let writer = Writer::unpack(&account.data).unwrap();
    assert_eq!(writer.state, *writer_tree);

    // Verify and update tape state
    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();
    assert_eq!(tape.total_segments, 5);
    assert_eq!(tape.state, u64::from(TapeState::Writing));
    assert_eq!(tape.merkle_root, writer_tree.get_root().to_bytes());
    assert_eq!(tape.header, stored_tape.account.header);

    // Update stored_tape.account
    stored_tape.account = *tape;
}

fn finalize_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    tape_address: Pubkey,
    writer_address: Pubkey,
    stored_tape: &mut StoredTape,
    tape_index: u64,
) {
    let payer_pk = payer.pubkey();

    // Finalize tape
    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_finalize_ix(payer_pk, tape_address, writer_address);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::TapeFinalize,res.unwrap().compute_units_consumed);
    // Verify update fails after finalization
    let target_segment: u64 = 0;

    let old_data_array: [u8; SEGMENT_SIZE] = stored_tape.segments[target_segment as usize]
        .clone()
        .try_into()
        .unwrap();

    let new_raw = b"<segment_0_updated>";
    let new_data_array = padded_array::<SEGMENT_SIZE>(new_raw);
    let merkle_proof = [[0u8; 32]; SEGMENT_PROOF_LEN]; // Stale proof, but should fail due to state

    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_update_ix(
        payer_pk,
        tape_address,
        writer_address,
        target_segment,
        old_data_array,
        new_data_array,
        merkle_proof,
    );
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_err());

    // Verify finalized tape
    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();
    assert_eq!(tape.state, u64::from(TapeState::Finalized));
    assert_eq!(tape.number, tape_index + 1);
    assert_eq!(tape.total_segments, 5);
    assert_eq!(tape.merkle_root, stored_tape.account.merkle_root);

    // Verify writer account is closed
    let account = svm.get_account(&writer_address).unwrap();
    assert!(account.data.is_empty());

    // Update stored_tape
    stored_tape.number = tape_index + 1;
}

fn register_miner(svm: &mut SvmWithCUTracker, miner_name: &str) -> Pubkey {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    let payer_pk = payer.pubkey();
    let (miner_address, _miner_bump) = miner_find_pda(payer_pk, to_name(miner_name));

    let blockhash = svm.latest_blockhash();
    let ix = instruction::miner::build_register_ix(payer_pk, miner_name);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::MinerRegister, res.unwrap().compute_units_consumed);

    let account = svm.get_account(&miner_address).unwrap();
    let miner = Miner::unpack(&account.data).unwrap();

    assert_eq!(miner.authority, payer_pk);
    assert_eq!(miner.name, to_name(miner_name));
    assert_eq!(miner.unclaimed_rewards, 0);
    assert_eq!(miner.multiplier, 0);
    assert_eq!(miner.last_proof_block, 0);
    assert_eq!(miner.last_proof_at, 0);
    assert_eq!(miner.total_proofs, 0);
    assert_eq!(miner.total_rewards, 0);

    miner_address
}

fn create_spool(svm: &mut SvmWithCUTracker, miner_address: Pubkey, number: u64) -> StoredSpool {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;

    let payer_pk = payer.pubkey();
    let (spool_address, _bump) = spool_find_pda(miner_address, number);

    let blockhash = svm.latest_blockhash();
    let ix = instruction::spool::build_create_ix(payer_pk, miner_address, number);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::SpoolCreate, res.unwrap().compute_units_consumed);

    let account = svm.get_account(&spool_address).unwrap();
    let spool = Spool::unpack(&account.data).unwrap();

    assert_eq!(spool.authority, payer_pk);
    assert_eq!(spool.number, number);
    assert_ne!(spool.seed, [0; 32]);
    assert_eq!(spool.contains, [0; 32]);
    assert_eq!(spool.total_tapes, 0);
    assert_eq!(spool.last_proof_block, 0);
    assert_eq!(spool.last_proof_at, 0);

    StoredSpool {
        //number,
        address: spool_address,
        miner: miner_address,
        tree: TapeTree::new(&[spool.seed.as_ref()]),
        tapes: vec![],
        //account: *spool,
    }
}

fn get_packed_segments(
    miner_address: Pubkey,
    stored_tape: &StoredTape,
    difficulty: u32,
) -> Vec<Vec<u8>> {

    let mut packed_segments: Vec<Vec<u8>> = vec![];
    for segment_data in &stored_tape.segments {
        let canonical_segment = padded_array::<SEGMENT_SIZE>(segment_data);
        let solution = packx::solve(
            &miner_address.to_bytes(),
            &canonical_segment,
            difficulty
        ).expect("Failed to pack segment data");

        packed_segments.push(solution.to_bytes().to_vec());
    }

    packed_segments
}

fn get_packed_tape(
    miner_address: Pubkey,
    stored_tape: &StoredTape,
    difficulty: u32,
) -> PackedTape {

    let packed_segments = get_packed_segments(miner_address, stored_tape, difficulty);

    let mut merkle_tree = SegmentTree::new(&[stored_tape.account.merkle_seed.as_ref()]);
    for (segment_number, packed_data) in packed_segments.iter().enumerate() {
        let segment_id = segment_number.to_le_bytes();
        let leaf = Leaf::new(&[
            segment_id.as_ref(),
            &packed_data,
        ]);
        
        merkle_tree.try_add_leaf(leaf)
            .expect("Failed to add leaf to Merkle tree");
    }

    return PackedTape {
        number: stored_tape.number,
        address: stored_tape.address,
        tree: merkle_tree,
        data: packed_segments,
    };
}

fn commit_for_mining(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    stored_spool: &StoredSpool,
    tape_index: u64,
    segment_index: u64,
) {
    let payer_pk = payer.pubkey();
    let blockhash = svm.latest_blockhash();

    let [unpack_ix,commit_ix] = [
        unpack_tape_ix(
            payer, 
            stored_spool, 
            tape_index
        ),
        commit_data_ix(
            payer, 
            stored_spool, 
            tape_index, 
            segment_index
        ),
    ];

    let tx = Transaction::new_signed_with_payer(&[unpack_ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::SpoolUnpack, res.unwrap().compute_units_consumed);

    let tx = Transaction::new_signed_with_payer(&[commit_ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::SpoolCommit, res.unwrap().compute_units_consumed);

    // Verify that the mining account has the leaf we need
    let account = svm.get_account(&stored_spool.miner)
        .expect("Miner account should exist");
    let miner = Miner::unpack(&account.data)
        .expect("Failed to unpack Miner account");

    let leaf = Leaf::new(&[
        segment_index.to_le_bytes().as_ref(),
        &stored_spool.tapes[tape_index as usize].data[segment_index as usize],
    ]);

    assert!(miner.commitment.eq(&leaf.to_bytes()));
}

fn commit_data_ix(
    payer: &Keypair,
    stored_spool: &StoredSpool,
    tape_index: u64,
    segment_index: u64,
) -> Instruction {
    let payer_pk = payer.pubkey();

    let packed_tape = stored_spool.tapes
        .get(tape_index as usize)
        .expect("Tape index out of bounds");

    let leaves = packed_tape.data.iter().enumerate()
        .map(|(segment_id, packed_data)| {
            Leaf::new(&[
                segment_id.to_le_bytes().as_ref(),
                packed_data.as_ref(),
            ])
        })
        .collect::<Vec<_>>();

    //let data = packed_tape.data[segment_index as usize].clone();

    let data = leaves[segment_index as usize]
        .to_bytes();

    let merkle_proof = packed_tape.tree.get_merkle_proof(
        &leaves,
        segment_index as usize
    );
    let merkle_proof = merkle_proof
        .iter()
        .map(|v| v.to_bytes())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    instruction::spool::build_commit_ix(
        payer_pk,
        stored_spool.miner,
        stored_spool.address,
        tape_index,
        merkle_proof,
        data,
    )
}

fn unpack_tape_ix(
    payer: &Keypair,
    stored_spool: &StoredSpool,
    index: u64,
) -> Instruction {
    let payer_pk = payer.pubkey();

    let packed_tape = stored_spool.tapes
        .get(index as usize)
        .expect("Tape index out of bounds");
    let tape_root = packed_tape.tree.get_root();

    let leaves = stored_spool.tapes.iter()
        .map(|tape| {
            Leaf::new(&[
                tape.number.to_le_bytes().as_ref(),
                tape.tree.get_root().as_ref(),
            ])
        })
        .collect::<Vec<_>>();

    let merkle_proof = stored_spool.tree.get_merkle_proof(
        &leaves,
        index as usize
    );
    let merkle_proof = merkle_proof
        .iter()
        .map(|v| v.to_bytes())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    instruction::spool::build_unpack_ix(
        payer_pk,
        stored_spool.address,
        packed_tape.number,
        merkle_proof,
        tape_root.to_bytes(),
    )
}

fn pack_tape(
    svm: &mut SvmWithCUTracker,
    stored_tape: &StoredTape, 
    stored_spool: &mut StoredSpool,
) {
    let SvmWithCUTracker { svm, cu_tracker, payer } = svm;
    // Get the required difficulty for packing
    let (epoch_address, _epoch_bump) = epoch_pda();
    let epoch_account = svm.get_account(&epoch_address).unwrap();
    let epoch = Epoch::unpack(&epoch_account.data).unwrap();
    let difficulty = epoch.packing_difficulty as u32;

    // Compute packed tape for this miner
    let packed_tape = get_packed_tape(stored_spool.miner, stored_tape, difficulty);

    // Publicly commit the packed tape to the provided spool address
    let payer_pk = payer.pubkey();
    let blockhash = svm.latest_blockhash();
    let ix = instruction::spool::build_pack_ix(
        payer_pk,
        stored_spool.address,
        stored_tape.address,
        packed_tape.tree.get_root().to_bytes()
    );
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::SpoolPack, res.unwrap().compute_units_consumed);

    stored_spool.tree.try_add_leaf(
        Leaf::new(&[
            stored_tape.number.to_le_bytes().as_ref(),
            packed_tape.tree.get_root().as_ref(),
        ])
    ).expect("Failed to add leaf to spool tree");

    stored_spool.tapes.push(packed_tape);
}


fn perform_mining(
    svm: &mut LiteSVM,
    payer: &Keypair,
    cu_tracker: &mut ComputeUnitsTracker,
    miner_address: Pubkey,
    tape_address: Pubkey,
    pow: PoW,
    poa: PoA,
) {
    let payer_pk = payer.pubkey();

    let blockhash = svm.latest_blockhash();
    let ix = instruction::miner::build_mine_ix(
        payer_pk,
        miner_address,
        tape_address,
        pow,
        poa,
    );

    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
    cu_tracker.track_cus(ProgramIx::MinerMine, res.unwrap().compute_units_consumed);

    let account = svm.get_account(&miner_address).unwrap();
    let miner = Miner::unpack(&account.data).unwrap();
    assert!(miner.unclaimed_rewards > 0);
}

fn solve_challenge<const N: usize>(
    challenge: [u8; 32],
    data: &[u8; N],
    difficulty: u64,
) -> Result<Solution, CrankXError> {
    let mut memory = SolverMemory::new();
    let mut nonce: u64 = 0;

    loop {
        if let Ok(solution) = solve_with_memory(&mut memory, &challenge, data, &nonce.to_le_bytes()) {
            if solution.difficulty() >= difficulty as u32 {
                return Ok(solution);
            }
        }
        nonce += 1;
    }
}
