use tape_solana::*;
use tape_api::program::prelude::*;
use tape_core::{
    erasure::SPOOL_GROUP_SIZE, 
    snapshot::{chunk::snapshot_chunk_key, types::SnapshotState}, 
    track::data::TrackDataSlice
};
use tape_crypto::bls12254::min_sig::*;

pub fn process_write_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = WriteSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        snapshot_info,
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
    let snapshot_blob = BlobInfo::unpack(args.snapshot);
    let meta = TrackDataSlice::Blob(snapshot_blob)
        .meta()
        .ok_or(TapeError::InvalidCommitment)?;

    let snapshot = snapshot_info
        .is_writable()?
        .has_address(&snapshot_address.into())?
        .as_account_mut::<Snapshot>(&tapedrive::ID)?;

    if snapshot.state > SnapshotState::PartiallyCertified as u64 {
        return Err(TapeError::AlreadyCertified.into());
    }

    let track_number = snapshot.tracks.next_number();
    let spool_group = SpoolGroup::unpack(args.group);
    let key = snapshot_chunk_key(epoch.id, spool_group);

    let track = CompressedTrack {
        tape: snapshot_tape,
        key,
        track_number,
        kind: meta.kind as u64,
        state: meta.initial_state as u64,
        size: meta.size,
        spool_group,
        value_hash: meta.value_hash,
    };

    let track_address = track_pda(track.tape, track.track_number).0;
    let track_hash = track.get_hash();

    snapshot.write_track(&track)?;

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

    let message = SnapshotWriteMessage::new(
        snapshot_epoch,
        spool_group, 
        track_hash
    );
    let message_bytes = message.to_bytes();

    verify_aggregate(
        &message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    SnapshotWritten {
        epoch: epoch.id,
        track: track_address,
        group: spool_group,
        track_number,
        track_hash,
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
