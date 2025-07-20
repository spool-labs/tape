use crankx::Solution;
use brine_tree::{Leaf, verify};
use steel::*;
use tape_api::prelude::*;

pub fn process_mine(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let current_time = Clock::get()?.unix_timestamp;
    let args = Mine::try_from_bytes(data)?;
    let [
        signer_info, 
        epoch_info, 
        block_info,
        miner_info, 
        tape_info,
        archive_info,
        slot_hashes_info
    ] = accounts else { 
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let archive = archive_info
        .is_archive()?
        .as_account_mut::<Archive>(&tape_api::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account_mut::<Epoch>(&tape_api::ID)?;

    let block = block_info
        .is_block()?
        .as_account_mut::<Block>(&tape_api::ID)?;

    let tape = tape_info
        .as_account::<Tape>(&tape_api::ID)?;

    let miner = miner_info
        .as_account_mut::<Miner>(&tape_api::ID)?;

    let (miner_address, _miner_bump) = miner_pda(miner.authority, miner.name);

    check_condition(
        miner_info.key.eq(&miner_address),
        ProgramError::InvalidSeeds
    )?;

    check_condition(
        signer_info.key.eq(&miner.authority),
        ProgramError::InvalidAccountOwner,
    )?;

    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;

    let solution   = Solution::new(args.digest, args.nonce);
    let difficulty = solution.difficulty();

    check_condition(
        difficulty >= epoch.target_difficulty as u32,
        TapeError::SolutionTooEasy,
    )?;

    // Check if we should allow duplicate solutions 
    // (same miner submitting multiple times in a block).

    if miner.last_proof_block == block.number {
        if has_stalled(block, current_time) {
            epoch.duplicates = epoch.duplicates.saturating_add(1);
        } else {
            return Err(TapeError::SolutionInvalid.into());
        }
    }

    let miner_challenge = compute_challenge(
        &block.challenge,
        &miner.challenge,
    );

    let recall_tape = compute_recall_tape(
        &miner_challenge,
        block.challenge_set,
    );

    check_condition(
        tape.number == recall_tape,
        TapeError::SolutionInvalid,
    )?;

    let segment_number = compute_recall_segment(
        &miner_challenge, 
        tape.total_segments
    );

    // Validate that the miner actually has the data for the tape

    let merkle_root = tape.merkle_root;
    let merkle_proof = &args.recall_proof;
    let leaf = Leaf::new(&[
        (segment_number as u64).to_le_bytes().as_ref(),
        args.recall_segment.as_ref(),
    ]);

    assert!(merkle_proof.len() == PROOF_LEN as usize);

    check_condition(
        verify(
            merkle_root,
            merkle_proof,
            leaf
        ),
        TapeError::SolutionInvalid,
    )?;

    // Verify that the PoW solution is good
    check_condition(
        solution.is_valid(&miner_challenge, &args.recall_segment).is_ok(),
        TapeError::SolutionInvalid,
    )?;

    // Update miner multiplier
    update_miner_multiplier(miner, block);

    let final_reward = get_scaled_reward(
        epoch.reward_rate,
        miner.multiplier
    );

    let next_miner_challenge = compute_next_challenge(
        &miner.challenge,
        slot_hashes_info
    );

    // Update miner
    miner.unclaimed_rewards   += final_reward;
    miner.total_rewards       += final_reward;
    miner.total_proofs        += 1;
    miner.last_proof_at        = current_time;
    miner.last_proof_block     = block.number;
    miner.challenge            = next_miner_challenge;

    // Update block
    block.progress = block.progress
        .saturating_add(1);

    // Check if we need to advance the block
    if block.progress >= epoch.target_participation {
        advance_block(block, current_time)?;

        let next_block_challenge = compute_next_challenge(
            &block.challenge,
            slot_hashes_info
        );

        block.challenge = next_block_challenge;
        block.challenge_set = archive.tapes_stored;
    }

    // Check if we need to advance the epoch
    if epoch.progress >= EPOCH_BLOCKS {
        advance_epoch(epoch, current_time)?;

        // Update the reward rate for the new epoch
        let storage_rate   = get_storage_rate(archive.bytes_stored);
        let base_rate = get_base_rate(epoch.number);

        epoch.reward_rate = storage_rate
            .saturating_add(base_rate);
    } else {

        // Epoch is still in progress, increment the progress
        epoch.progress = epoch.progress
            .saturating_add(1);
    }

    Ok(())
}

// Helper: Check if the block has stalled, meaning no solutions have been submitted for a while.
fn has_stalled(block: &Block, current_time: i64) -> bool {
    current_time > block.last_proof_at
        .saturating_add(BLOCK_DURATION_SECONDS as i64)
}

// Helper: Update miner multiplier based on timing of this solution.
//
// Miners that consistently submit solutions on-time will have a higher multiplier number.
//
// This encourages miners to come up with strategies that allow them quick access to the tape data
// needed to solve the challenge.
#[inline(always)]
fn update_miner_multiplier(miner: &mut Miner, block: &Block) {
    if miner.last_proof_block.saturating_add(1) == block.number {
        miner.multiplier = miner.multiplier
            .saturating_add(1)
            .min(MAX_CONSISTENCY_MULTIPLIER);
    } else {
        miner.multiplier = miner.multiplier
            .saturating_sub(1)
            .max(MIN_CONSISTENCY_MULTIPLIER);
    }
}

// Helper: Get the scaled reward based on miner's consistency multiplier.
fn get_scaled_reward(reward: u64, multiplier: u64) -> u64 {
    assert!(multiplier >= MIN_CONSISTENCY_MULTIPLIER);
    assert!(multiplier <= MAX_CONSISTENCY_MULTIPLIER);
    
    reward
        .saturating_mul(multiplier)
        .saturating_div(MAX_CONSISTENCY_MULTIPLIER)
}

// Helper: Advance the block state
#[inline(always)]
fn advance_block(
    block: &mut Block,
    current_time: i64,
) -> ProgramResult {

    // Reset the block state
    block.number            = block.number.saturating_add(1);
    block.progress          = 0;
    block.last_proof_at     = current_time;
    block.last_block_at     = current_time;

    Ok(())
}

// Helper: Advance the epoch state
#[inline(always)]
fn advance_epoch(
    epoch: &mut Epoch,
    current_time: i64,
) -> ProgramResult {

    adjust_participation(epoch);
    adjust_difficulty(epoch, current_time);

    epoch.number                = epoch.number.saturating_add(1);
    epoch.target_difficulty     = epoch.target_difficulty.max(MIN_DIFFICULTY);
    epoch.target_participation  = epoch.target_participation.max(MIN_PARTICIPATION_TARGET);
    epoch.progress              = 0;
    epoch.duplicates            = 0;
    epoch.last_epoch_at         = current_time;

    Ok(())
}


// Every epoch, the protocol adjusts the minimum required difficulty for a block solution.
//
// Proof Difficulty:
// If blocks were solved faster than 1 minute on average, increase difficulty.
// If blocks were slower, decrease difficulty.
//
// This keeps block times near the 1-minute target.
#[inline(always)]
fn adjust_difficulty(epoch: &mut Epoch, current_time: i64) {

    let elapsed_time = current_time.saturating_sub(epoch.last_epoch_at);
    let average_time_per_block = elapsed_time / EPOCH_BLOCKS as i64;

    // If blocks were solved faster than 1 minute, increase difficulty
    if average_time_per_block < BLOCK_DURATION_SECONDS as i64 {
        epoch.target_difficulty = epoch.target_difficulty
            .saturating_add(1)

    // If they were slower, decrease difficulty
    } else {
        epoch.target_difficulty = epoch.target_difficulty
            .saturating_sub(1)
            .max(MIN_DIFFICULTY);
    }
}

// Every epoch, the protocol adjusts the minimum required unique proofs for a single block. This
// is referred to as the participation target.
//
// Participation Target (X):
// * If all submissions during the epoch came from unique miners, increase X by 1.
// * If any duplicates occurred (same miner submitting multiple times in a block), decrease X by 1.
//
// This helps tune how many miners can share in a block reward, balancing inclusivity and competitiveness.
#[inline(always)]
fn adjust_participation(epoch: &mut Epoch) {
    // If all miner submissions were unique, increase by 1
    if epoch.duplicates == 0 {
        epoch.target_participation = epoch.target_participation
            .saturating_add(1)
            .min(MAX_PARTICIPATION_TARGET);

    // If there were duplicates, decrease target by 1
    } else {
        epoch.target_participation = epoch.target_participation
            .saturating_sub(1)
            .max(MIN_PARTICIPATION_TARGET);
    }
}

