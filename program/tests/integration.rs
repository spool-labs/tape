#![cfg(test)]
pub mod utils;
use utils::*;

use solana_sdk::{
    signer::Signer,
    transaction::Transaction,
    pubkey::Pubkey,
    signature::Keypair,
    clock::Clock,
};

use tape::miner::get_base_rate;
use tape_api::prelude::*;
use tape_api::instruction;
use litesvm::LiteSVM;

use crankx::equix::SolverMemory;
use crankx::{
    solve_with_memory,
    Solution, 
    CrankXError
};

struct StoredTape {
    pubkey: Pubkey,
    segments: Vec<Vec<u8>>,  // (segment_data, slot_number)
    account: Tape,
}

#[test]
fn run_integration() {
    // Setup environment
    let (mut svm, payer) = setup_environment();

    // Initialize program
    initialize_program(&mut svm, &payer);

    // Fetch and store genesis tape
    let mut tape_db = vec![];
    fetch_genesis_tape(&mut svm, &payer, &mut tape_db);

    // Verify initial accounts
    verify_archive_account(&svm, 1);
    verify_epoch_account(&svm);
    verify_block_account(&svm);
    verify_treasury_account(&svm);
    verify_mint_account(&svm);
    verify_metadata_account(&svm);
    verify_treasury_ata(&svm);

    // Register miner
    let miner_name = "miner-name";
    let miner_address = register_miner(&mut svm, &payer, miner_name);
    let ata = create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());

    // Mine the genesis tape (to earn some tokens)
    do_mining_run(&mut svm, &payer, miner_address, &mut tape_db, 5);
    claim_rewards(&mut svm, &payer, miner_address, ata);

    let ata_balance = get_ata_balance(&svm, &ata);
    assert!(ata_balance > 0);

    println!("ATA balance after claiming rewards: {ata_balance}");

    // Advance clock
    let mut initial_clock = svm.get_sysvar::<Clock>();
    initial_clock.slot = 10;
    svm.set_sysvar::<Clock>(&initial_clock);

    // Create tapes
    let tape_count = 5;
    for tape_idx in 1..tape_count {
        create_and_verify_tape(&mut svm, &payer, ata, tape_idx, &mut tape_db);
    }

    // Verify archive account after tape creation
    verify_archive_account(&svm, tape_count);

    // Mine again with more tapes this time
    do_mining_run(&mut svm, &payer, miner_address, &mut tape_db, 5);
}

fn setup_environment() -> (LiteSVM, Keypair) {
    let mut svm = setup_svm();
    let payer = create_payer(&mut svm);
    (svm, payer)
}

fn subsidize_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
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

    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();
    assert!(tape.balance >= amount);
}

fn claim_rewards(
    svm: &mut LiteSVM,
    payer: &Keypair,
    miner_address: Pubkey,
    miner_ata: Pubkey,
) {
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

    // Verify miner account after claiming rewards
    let account = svm.get_account(&miner_address).unwrap();
    let miner = Miner::unpack(&account.data).unwrap();

    assert!(miner.unclaimed_rewards == 0);
}

fn do_mining_run(
    svm: &mut LiteSVM,
    payer: &Keypair,
    miner_address: Pubkey,
    tape_db: &mut Vec<StoredTape>,
    num_iterations: u64,
) {
    for _ in 0..num_iterations {

        let (epoch_address, _epoch_bump) = epoch_pda();
        let epoch_account = svm.get_account(&epoch_address).unwrap();
        let epoch = Epoch::unpack(&epoch_account.data).unwrap();

        let (block_address, _block_bump) = block_pda();
        let block_account = svm.get_account(&block_address).unwrap();
        let block = Block::unpack(&block_account.data).unwrap();

        let miner_account = svm.get_account(&miner_address).unwrap();
        let miner = Miner::unpack(&miner_account.data).unwrap();

        let miner_challenge = compute_challenge(
            &block.challenge,
            &miner.challenge,
        );

        let recall_tape = compute_recall_tape(
            &miner_challenge,
            block.challenge_set
        );

        // Compute challenge solution
        let stored_tape = &tape_db[(recall_tape - 1) as usize];
        let tape_account = svm.get_account(&stored_tape.pubkey).unwrap();
        let tape = Tape::unpack(&tape_account.data).unwrap();

        let (solution, recall_segment, merkle_proof) = 
            if tape.has_minimum_rent() {
                compute_challenge_solution(stored_tape, miner, epoch, block)
            } else {

                let solution = solve_challenge(
                    miner_challenge, 
                    &EMPTY_SEGMENT, 
                    epoch.target_difficulty
                ).unwrap();

                (solution, EMPTY_SEGMENT, EMPTY_PROOF)
            };

        perform_mining(
            svm,
            payer,
            miner_address,
            stored_tape.pubkey,
            solution,
            recall_segment,
            merkle_proof,
        );

    }
}

fn fetch_genesis_tape(svm: &mut LiteSVM, payer: &Keypair, tape_db: &mut Vec<StoredTape>) {
    let genesis_name = "genesis".to_string();
    let genesis_name_bytes = to_name(&genesis_name);
    let (genesis_pubkey, _) = tape_pda(payer.pubkey(), &genesis_name_bytes);

    let account = svm.get_account(&genesis_pubkey).expect("Genesis tape should exist");
    let tape = Tape::unpack(&account.data).expect("Failed to unpack genesis tape");

    assert!(tape.can_finalize());

    let genesis_data = b"hello, world";
    let genesis_segment = padded_array::<SEGMENT_SIZE>(genesis_data).to_vec();
    let segments = vec![genesis_segment];

    let stored_genesis = StoredTape {
        pubkey: genesis_pubkey,
        segments,
        account: *tape,
    };

    tape_db.push(stored_genesis);
}


fn initialize_program(svm: &mut LiteSVM, payer: &Keypair) {
    let payer_pk = payer.pubkey();
    let ix = instruction::program::build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());
}

fn verify_archive_account(svm: &LiteSVM, expected_tapes_stored: u64) {
    let (archive_address, _archive_bump) = archive_pda();
    let account = svm
        .get_account(&archive_address)
        .expect("Archive account should exist");
    let archive = Archive::unpack(&account.data).expect("Failed to unpack Archive account");
    assert_eq!(archive.tapes_stored, expected_tapes_stored);
}

fn verify_epoch_account(svm: &LiteSVM) {
    let (epoch_address, _epoch_bump) = epoch_pda();
    let account = svm
        .get_account(&epoch_address)
        .expect("Epoch account should exist");
    let epoch = Epoch::unpack(&account.data).expect("Failed to unpack Epoch account");
    assert_eq!(epoch.number, 1);
    assert_eq!(epoch.progress, 0);
    assert_eq!(epoch.target_difficulty, MIN_DIFFICULTY);
    assert_eq!(epoch.target_participation, MIN_PARTICIPATION_TARGET);
    assert_eq!(epoch.reward_rate, get_base_rate(1));
    assert_eq!(epoch.duplicates, 0);
    assert_eq!(epoch.last_epoch_at, 0);
}

fn verify_block_account(svm: &LiteSVM) {
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

fn verify_treasury_account(svm: &LiteSVM) {
    let (treasury_address, _treasury_bump) = treasury_pda();
    let _treasury_account = svm
        .get_account(&treasury_address)
        .expect("Treasury account should exist");
}

fn verify_mint_account(svm: &LiteSVM) {
    let (mint_address, _mint_bump) = mint_pda();
    let mint = get_mint(svm, &mint_address);
    assert_eq!(mint.supply, MAX_SUPPLY);
    assert_eq!(mint.decimals, TOKEN_DECIMALS);
}

fn verify_metadata_account(svm: &LiteSVM) {
    let (mint_address, _mint_bump) = mint_pda();
    let (metadata_address, _metadata_bump) = metadata_pda(mint_address);
    let account = svm
        .get_account(&metadata_address)
        .expect("Metadata account should exist");
    assert!(!account.data.is_empty());
}

fn verify_treasury_ata(svm: &LiteSVM) {
    let (treasury_ata_address, _ata_bump) = treasury_ata();
    let account = svm
        .get_account(&treasury_ata_address)
        .expect("Treasury ATA should exist");
    assert!(!account.data.is_empty());
}

fn create_and_verify_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    ata: Pubkey,
    tape_idx: u64,
    tape_db: &mut Vec<StoredTape>,
) {
    let payer_pk = payer.pubkey();
    let tape_name = format!("tape-name-{tape_idx}");

    let (tape_address, _tape_bump) = tape_pda(payer_pk, &to_name(&tape_name));
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    // Create tape and verify initial state
    let mut stored_tape = create_tape(
        svm, 
        payer, 
        &tape_name, 
        tape_address, 
        writer_address
    );

    let tape_seed = &[stored_tape.account.merkle_seed.as_ref()];
    let mut writer_tree = SegmentTree::new(tape_seed);

    write_tape(
        svm,
        payer,
        tape_address,
        writer_address,
        &mut stored_tape,
        &mut writer_tree,
    );

    update_tape(
        svm,
        payer,
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
        ata,
        tape_address, 
        min_rent,
    );

    finalize_tape(
        svm,
        payer,
        tape_address,
        writer_address,
        &stored_tape,
        tape_idx,
    );

    tape_db.push(stored_tape);
}

fn create_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
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
        pubkey: tape_address,
        segments: vec![],
        account: *tape,
    }
}

fn write_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
    stored_tape: &mut StoredTape,
    writer_tree: &mut SegmentTree,
) {
    let payer_pk = payer.pubkey();

    for write_index in 0..10u64 {
        let data = format!("<segment_{write_index}_data>").into_bytes();

        let blockhash = svm.latest_blockhash();
        let ix = instruction::tape::build_write_ix(payer_pk, tape_address, writer_address, &data);
        let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
        let res = send_tx(svm, tx);
        assert!(res.is_ok());

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
        assert_eq!(tape.total_size, tape.total_segments * 128);
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
    assert_eq!(tape.total_segments, 10);
    assert_eq!(tape.total_size, tape.total_segments * 128);
    assert_eq!(tape.state, u64::from(TapeState::Writing));
    assert_eq!(tape.merkle_root, writer_tree.get_root().to_bytes());
    assert_eq!(tape.header, stored_tape.account.header);

    // Update stored_tape.account
    stored_tape.account = *tape;
}

fn finalize_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
    stored_tape: &StoredTape,
    tape_idx: u64,
) {
    let payer_pk = payer.pubkey();

    // Finalize tape
    let blockhash = svm.latest_blockhash();
    let ix = instruction::tape::build_finalize_ix(payer_pk, tape_address, writer_address);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());

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
    assert_eq!(tape.number, tape_idx + 1);
    assert_eq!(tape.total_segments, 10);
    assert_eq!(tape.total_size, tape.total_segments * 128);
    assert_eq!(tape.merkle_root, stored_tape.account.merkle_root);

    // Verify writer account is closed
    let account = svm.get_account(&writer_address).unwrap();
    assert!(account.data.is_empty());
}

fn register_miner(svm: &mut LiteSVM, payer: &Keypair, miner_name: &str) -> Pubkey {
    let payer_pk = payer.pubkey();
    let (miner_address, _miner_bump) = miner_pda(payer_pk, to_name(miner_name));

    let blockhash = svm.latest_blockhash();
    let ix = instruction::miner::build_register_ix(payer_pk, miner_name);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());

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

fn compute_challenge_solution(
    stored_tape: &StoredTape,
    miner: &Miner,
    epoch: &Epoch,
    block: &Block,
) -> (Solution, [u8; SEGMENT_SIZE], [[u8; 32]; SEGMENT_PROOF_LEN]) {
    let miner_challenge = compute_challenge(
        &block.challenge,
        &miner.challenge,
    );

    let segment_number = compute_recall_segment(
        &miner_challenge, 
        stored_tape.account.total_segments
    ) as usize;

    let mut leaves = Vec::new();
    let mut recall_segment = [0; SEGMENT_SIZE];

    for (segment_id, segment_data) in stored_tape.segments.iter().enumerate() {
        if segment_id == segment_number {
            recall_segment.copy_from_slice(segment_data);
        }

        let data = padded_array::<SEGMENT_SIZE>(segment_data);
        let leaf = compute_leaf(
            segment_id as u64,
            &data,
        );

        leaves.push(leaf);
    }

    assert_eq!(leaves.len(), stored_tape.account.total_segments as usize);

    println!("Recall segment: {segment_number}");

    let solution = solve_challenge(miner_challenge, &recall_segment, epoch.target_difficulty).unwrap();
    assert!(solution.is_valid(&miner_challenge, &recall_segment).is_ok());

    let merkle_tree = SegmentTree::new(&[stored_tape.account.merkle_seed.as_ref()]);
    let merkle_proof = merkle_tree.get_merkle_proof(&leaves, segment_number);
    let merkle_proof = merkle_proof
        .iter()
        .map(|v| v.to_bytes())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    (solution, recall_segment, merkle_proof)
}

fn perform_mining(
    svm: &mut LiteSVM,
    payer: &Keypair,
    miner_address: Pubkey,
    tape_address: Pubkey,
    solution: Solution,
    recall_segment: [u8; SEGMENT_SIZE],
    merkle_proof: [[u8; 32]; SEGMENT_PROOF_LEN],
) {
    let payer_pk = payer.pubkey();

    let blockhash = svm.latest_blockhash();
    let ix = instruction::miner::build_mine_ix(
        payer_pk,
        miner_address,
        tape_address,
        solution,
        recall_segment,
        merkle_proof,
    );

    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());

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
