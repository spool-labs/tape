use tape_solana::*;
use tape_api::program::prelude::*;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::bls12254::min_sig::*;
use tape_core::snapshot::types::SnapshotState;

pub fn process_sign_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SignSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        snapshot_info,
        snapshot_tape_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let snapshot_epoch = prev_epoch(epoch);
    let snapshot_address = snapshot_pda(snapshot_epoch).0;
    let snapshot_tape = snapshot_tape_pda(snapshot_epoch).0;

    let snapshot = snapshot_info
        .is_writable()?
        .has_address(&snapshot_address.into())?
        .as_account_mut::<Snapshot>(&tapedrive::ID)?;

    let tape = snapshot_tape_info
        .is_writable()?
        .has_address(&snapshot_tape.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let spool_group = SpoolGroup::unpack(args.group);

    // verify signature

    let committee = system.committee;
    let weight = args.bitmap.count_ones() as u64;

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let indices = args.bitmap.indices(SPOOL_GROUP_SIZE);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    let group_offset = spool_group.0 * SPOOL_GROUP_SIZE as u64;
    for member_index in &indices {
        // convert from group-local indices to committee-wide indices
        let member_index = member_index + group_offset as usize;
        if let Some(member) = committee.member_at(member_index) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = SnapshotSignMessage::new(
        snapshot_epoch, 
        spool_group
    );
    let message_bytes = message.to_bytes();

    verify_aggregate(
        &message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    // Check if the snapshot has seen enough signatures to be considered complete
    if snapshot.group_bitmap.count_ones() < SPOOL_GROUP_SIZE {
        snapshot.group_bitmap.set(spool_group.0 as usize);
    } else {
        tape.tracks = snapshot.tracks.clone();
        snapshot.state = SnapshotState::Finalized as u64;
    }

    SnapshotSigned {
        epoch: epoch.id,
        group: spool_group,
        state: snapshot.state,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_write_snapshot() {

        todo!();
    }
}
