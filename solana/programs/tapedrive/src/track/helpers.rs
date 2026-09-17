use tape_api::program::prelude::*;
use tape_api::event::{TrackDeleted, TrackWritten};
use tape_core::spooler::GroupIndex;
use tape_core::track::data::TrackMeta;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::hash::hashv;
use tape_crypto::Hash;

pub fn append_track(
    system: &System,
    tape: &mut Tape,
    slot_hashes_info: &AccountInfo<'_>,
    tape_address: Address,
    key: Hash,
    meta: TrackMeta,
) -> ProgramResult {
    let curr = current_epoch(system);
    if curr < tape.active_epoch || tape.expiry_epoch <= curr {
        return Err(TapeError::TapeExpired.into());
    }

    if system.live_group_count == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let track_number = tape.tracks.next_number();
    let group = select_group(
        tape_address,
        tape.id,
        track_number,
        slot_hash_seed(slot_hashes_info)?,
        system.live_group_count,
    )?;

    let track = CompressedTrack {
        tape: tape_address,
        track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        group,
        value_hash: meta.value_hash,
    };

    let track_address = track_pda(track.tape, track.track_number).0;
    let track_hash = track.get_hash();

    tape.write_track(&track)?;

    TrackWritten {
        epoch: curr,
        track: track_address,
        tape: tape_address,
        group,
        track_number,
        track_hash,
    }
    .log();

    Ok(())
}

pub fn delete_track(
    tape: &mut Tape,
    tape_address: Address,
    proof: CompressedTrackProof,
) -> ProgramResult {
    let track_address = track_pda(proof.state.tape, proof.state.track_number).0;
    let size = proof.state.size;
    let key = proof.state.key;

    tape.delete_track(&proof)
        .map_err(|_| TapeError::BadProof)?;

    TrackDeleted {
        track: track_address,
        tape: tape_address,
        key,
        size,
    }
    .log();

    Ok(())
}

fn select_group(
    tape_address: Address,
    tape_id: TapeNumber,
    track_number: TrackNumber,
    seed: Hash,
    spool_groups: u64,
) -> Result<GroupIndex, ProgramError> {
    let mixed_hash = hashv(&[
        seed.as_ref(),
        tape_address.as_ref(),
        &tape_id.pack(),
        &track_number.pack(),
    ]);
    let mixed = u64::from_le_bytes(
        mixed_hash.0[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    Ok(GroupIndex(mixed % spool_groups))
}

fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;
    let slot_hashes_data = slot_hashes_info.try_borrow_data()?;
    let seed = Hash(
        slot_hashes_data
            .get(16..48)
            .ok_or(TapeError::UnexpectedState)?
            .try_into()
            .map_err(|_| TapeError::UnexpectedState)?,
    );
    Ok(seed)
}
