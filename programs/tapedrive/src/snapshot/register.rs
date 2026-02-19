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

    let spool_group = u64::from_le_bytes(args.spool_group);
    if (spool_group as usize) >= SPOOL_GROUP_COUNT {
        return Err(ProgramError::InvalidArgument);
    }

    let epoch_number = EpochNumber::unpack(args.epoch);

    // Derive expected PDA for this snapshot track (keyed by commitment)
    let (tape_address, _) = tape_pda(system_address);
    let (track_address, _) = snapshot_pda(epoch_number, args.commitment);

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

    // Verify leaf hashes produce the commitment root
    let computed_root = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&args.leaves);
    if computed_root != args.commitment {
        return Err(TapeError::InvalidCommitment.into());
    }

    // Create the track account using snapshot PDA seeds (epoch + commitment)
    create_program_account::<Track>(
        track_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT, &epoch_number.pack(), &args.commitment.0],
    )?;

    let track_number = tape.track_count;
    tape.track_count = tape.track_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Compute track size from stripe data (stripe_size is in bytes, convert to MB)
    let stripe_size = u64::from_le_bytes(args.stripe_size);
    let stripe_count = u64::from_le_bytes(args.stripe_count);
    let total_bytes = stripe_size.saturating_mul(stripe_count);
    let track_size = StorageUnits(total_bytes / 1_000_000);

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
        spool_group,
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
        spool_group: spool_group.to_le_bytes(),
        stripe_size: args.stripe_size,
        stripe_count: args.stripe_count,
        leaves: args.leaves,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_crypto::merkle::hash_leaf;

    #[test]
    fn test_register_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (tape_address, _) = tape_pda(system_address);
        let (node_address, _) = node_pda(fee_payer);
        let (snapshot_state_address, _) = snapshot_state_pda();

        let epoch_number = EpochNumber(5);
        let spool_group = 7u64;

        // Build valid leaf hashes and commitment
        let leaves: [Hash; SPOOL_GROUP_SIZE] = {
            let mut arr = [Hash::default(); SPOOL_GROUP_SIZE];
            for i in 0..SPOOL_GROUP_SIZE {
                arr[i] = hash_leaf(&vec![i as u8; 100]);
            }
            arr
        };
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        // PDA derived from commitment
        let (track_address, _) = snapshot_pda(epoch_number, commitment);

        // Set up a committee with our node
        let node_id = NodeId(42);
        let mut system = System::zeroed();
        system.committee = Committee::from_members(&[CommitteeMember {
            id: node_id,
            stake: TAPE(1_000_000),
            ..CommitteeMember::zeroed()
        }]);

        let node = Node {
            id: node_id,
            authority: fee_payer,
            ..Node::zeroed()
        };

        let tape = Tape {
            authority: system_address,
            capacity: StorageUnits(100_000),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(100),
            ..Tape::zeroed()
        };

        let epoch = Epoch {
            id: EpochNumber(epoch_number.as_u64() + 1),
            nonce: Hash::default(),
            ..Epoch::zeroed()
        };

        let snapshot_state = SnapshotState::zeroed();
        let profile = EncodingProfile::clay_default();

        let instruction = build_register_snapshot_ix(
            fee_payer,
            epoch_number,
            spool_group,
            commitment,
            profile,
            10_000_000, // stripe_size: 10MB
            1,          // stripe_count
            leaves,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            empty(track_address),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
            ],
        );
    }

}
