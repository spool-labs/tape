use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackRegistered;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_COUNT};
use tape_core::encoding::EncodingProfile;
use tape_crypto::merkle::root_from_leaf_hashes;
use crate::error::*;

pub fn process_register_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        node_info,
        system_info,
        epoch_info,
        tape_info,
        track_info,
        snapshot_state_info,
        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (system_address, _) = system_pda();

    let system = system_info
        .is_system()?
        .has_address(&system_address)?
        .as_account::<System>(&tapedrive::ID)?;

    // Committee check: fee_payer must be a registered node in the current committee
    let (node_address, _) = node_pda(*fee_payer_info.key);

    let node = node_info
        .has_address(&node_address)?
        .as_account::<Node>(&tapedrive::ID)?;

    if node.authority != *fee_payer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if !system.committee.contains(&node.id) {
        return Err(TapeError::NotInCommittee.into());
    }

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let chunk_index = ChunkIndex::unpack(args.chunk_index);
    if chunk_index.as_usize() >= SPOOL_GROUP_COUNT {
        return Err(ProgramError::InvalidArgument);
    }

    let epoch_number = EpochNumber::unpack(args.epoch);

    // Derive expected PDA for this snapshot track
    let (tape_address, _) = tape_pda(system_address);
    let (track_address, _) = snapshot_pda(epoch_number, chunk_index);

    let tape = tape_info
        .is_writable()?
        .has_address(&tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    track_info
        .is_empty()?
        .is_writable()?
        .has_address(&track_address)?;

    let snapshot_state = snapshot_state_info
        .is_writable()?
        .is_snapshot_state()?
        .as_account_mut::<SnapshotState>(&tapedrive::ID)?;

    // Enforce sequential registration: chunk_index must match tape.track_count % SPOOL_GROUP_COUNT.
    // This prevents gaps and ensures chunks are registered in order (0..49 per epoch).
    let expected_chunk = ChunkIndex(tape.track_count % SPOOL_GROUP_COUNT as u64);
    if chunk_index != expected_chunk {
        return Err(TapeError::InvalidTrackOrder.into());
    }

    // Verify leaf hashes produce the commitment root
    let computed_root = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&args.leaves);
    if computed_root != args.commitment {
        return Err(TapeError::InvalidCommitment.into());
    }

    // Create the track account using snapshot PDA seeds
    create_program_account::<Track>(
        track_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT, &epoch_number.pack(), &chunk_index.pack()],
    )?;

    let track_number = tape.track_count;
    tape.track_count = tape.track_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Compute track size from stripe data
    let stripe_size = u64::from_le_bytes(args.stripe_size);
    let stripe_count = u64::from_le_bytes(args.stripe_count);
    let track_size = StorageUnits(stripe_size.saturating_mul(stripe_count));

    // Back-pointer: store the previous tail address as a Hash so bootstrap
    // can walk the linked list backward.
    let back_pointer = Hash(snapshot_state.tail.to_bytes());

    let track = track_info.as_account_mut::<Track>(&tapedrive::ID)?;

    track.id   = track_number.into();
    track.tape = tape_address;
    track.key  = back_pointer;
    track.size = track_size;
    track.data = TrackData::new(
        current_epoch(epoch),
        args.commitment,
        chunk_index.as_u64(),
    );
    let profile = EncodingProfile::unpack(args.profile);
    track.data.profile = profile;

    snapshot_state.tail = *track_info.key;
    snapshot_state.commitment = args.commitment;
    snapshot_state.count += 1;
    snapshot_state.total_size = StorageUnits(
        snapshot_state.total_size.as_u64().saturating_add(track_size.as_u64())
    );

    TrackRegistered {
        track: *track_info.key,
        tape: tape_address,
        key: back_pointer,
        size: track_size,
        commitment: args.commitment,
        epoch: current_epoch(epoch),
        profile,
        spool_group: chunk_index.pack(),
        stripe_size: args.stripe_size,
        stripe_count: args.stripe_count,
        leaves: args.leaves,
    }.log();

    Ok(())
}
