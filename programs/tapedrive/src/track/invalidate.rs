use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackInvalidated;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::bls12254::min_sig::*;
use crate::error::*;

pub fn process_invalidate_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = InvalidateTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,

        system_info,
        epoch_info,
        tape_info,
        track_info,
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

    let tape = tape_info
        .as_account::<Tape>(&tapedrive::ID)?;

    let track = track_info
        .is_writable()?
        .as_account_mut::<Track>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(tape.authority);
    let (track_address, _) = track_pda(tape.authority, track.key);

    if tape_address != *tape_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track_address != *track_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track.tape != tape_address {
        return Err(ProgramError::InvalidAccountData);
    }

    if !track.data.is_registered() {
        return Err(ProgramError::InvalidAccountData);
    }

    // Verify computed_root differs from on-chain commitment (actual inconsistency)
    if args.computed_root == track.data.commitment_hash {
        return Err(ProgramError::InvalidInstructionData);
    }

    let group = track.data.spool_group();
    let weight = system.spools.group_weight(group, &args.bitmap);

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let committee_size = system.committee.size();
    let indices = args.bitmap.indices(committee_size);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for member_index in &indices {
        if let Some(member) = system.committee.member_at(*member_index) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    // Build invalidation message with domain separation and epoch binding
    let invalidate_message = InvalidateMessage::new(
        current_epoch(epoch),
        track_address.to_bytes(),
        args.computed_root.0,
    );
    let message = invalidate_message.to_bytes();

    verify_aggregate(
        &message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    track.data.set_invalidated();

    TrackInvalidated {
        track: *track_info.key,
        epoch: current_epoch(epoch),
    }.log();

    Ok(())
}
