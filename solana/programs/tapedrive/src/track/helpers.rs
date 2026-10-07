use core::mem::size_of;

use tape_api::program::prelude::*;
use tape_api::event::{TrackDeleted, TrackWritten};
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::tape::TapeFlags;
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
    flags: u64,
) -> ProgramResult {
    let curr = current_epoch(system);
    if curr < tape.active_epoch || tape.expiry_epoch <= curr {
        return Err(TapeError::TapeExpired.into());
    }

    let track_number = tape.tracks.next_number();
    let group = select_group(
        system,
        tape_address,
        tape.id,
        track_number,
        slot_hash_seed(slot_hashes_info)?,
    )?;

    // A hidden or immutable tape makes every track written to it the same.
    let flags = flags | TapeFlags::track_flags(tape.flags);

    let track = CompressedTrack {
        tape: tape_address,
        tape_number: tape.id,
        track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        flags,
        size: meta.size,
        group,
        value_hash: meta.value_hash,
    };

    let track_address = track.address();
    let track_hash = track.get_hash();

    tape.write_track(&track)?;

    TrackWritten {
        epoch: curr,
        track: track_address,
        tape: tape_address,
        tape_number: tape.id,
        group,
        track_number,
        flags,
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
    let track_address = proof.state.address();
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

// Stamp a track with a virtual group in two even picks: a live group among
// the ones that take new tracks, then one of the virtual groups it carries.
//
// A group that retires next epoch takes no new tracks. The stamp resolves to
// the picked group at today's count and at a lower one, and when the count
// rises about half of a group's stamps resolve to the group split from it.
pub fn select_group(
    system: &System,
    tape_address: Address,
    tape_id: TapeNumber,
    track_number: TrackNumber,
    seed: Hash,
) -> Result<VirtualGroup, ProgramError> {
    let writable_groups = system.writable_group_count();
    if writable_groups == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let mixed_hash = hashv(&[
        seed.as_ref(),
        tape_address.as_ref(),
        &tape_id.pack(),
        &track_number.pack(),
    ]);

    let fold = system.fold()?;
    let group = GroupIndex(hash_word(&mixed_hash, 0)? % writable_groups);
    let index = hash_word(&mixed_hash, 1)?
        .checked_rem(fold.carried_count(group))
        .ok_or(TapeError::UnexpectedState)?;

    fold.carried_at(group, index)
        .ok_or_else(|| TapeError::UnexpectedState.into())
}

// The hash's eight-byte word at `index`, so each pick draws its own bits.
fn hash_word(hash: &Hash, index: usize) -> Result<u64, ProgramError> {
    let start = index * size_of::<u64>();
    let word = hash
        .0
        .get(start..start + size_of::<u64>())
        .ok_or(ProgramError::InvalidInstructionData)?
        .try_into()
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    Ok(u64::from_le_bytes(word))
}

pub fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::spooler::{GroupFold, VIRTUAL_GROUP_COUNT};

    const TRACKS: u64 = 40_000;

    fn system(live: u64, target: u64) -> System {
        System {
            live_group_count: live,
            target_group_count: target,
            ..System::zeroed()
        }
    }

    fn stamp(system: &System, track: u64) -> VirtualGroup {
        select_group(
            system,
            Address::new([7u8; 32]),
            TapeNumber(3),
            TrackNumber(track),
            Hash([9u8; 32]),
        )
        .expect("a virtual group")
    }

    fn fold(live: u64) -> GroupFold {
        GroupFold::new(live).expect("live groups")
    }

    // a stamp belongs to a group that takes new tracks, whether the target is
    // above, at or below the live count, and stays with it when the count drops
    #[test]
    fn stamps_resolve() {
        for (live, target) in [(4, 4), (4, 8), (8, 4), (5, 10), (10, 5), (1, 4), (7, 3)] {
            let system = system(live, target);

            for track in 0..2_000 {
                let stamped = stamp(&system, track);
                let group = fold(live).live_group(stamped);

                assert!(stamped.0 < VIRTUAL_GROUP_COUNT);
                assert!(group.0 < live.min(target));
                if target < live {
                    assert_eq!(fold(target).live_group(stamped), group);
                }
            }
        }
    }

    // stamps reach every virtual group and each live group gets an even share
    #[test]
    fn stamps_spread() {
        let system = system(4, 4);
        let mut per_group = [0u64; 4];
        let mut is_stamped = vec![false; VIRTUAL_GROUP_COUNT as usize];

        for track in 0..TRACKS {
            let stamped = stamp(&system, track);

            per_group[fold(4).live_group(stamped).as_usize()] += 1;
            is_stamped[stamped.as_usize()] = true;
        }

        assert!(is_stamped.iter().all(|is_stamped| *is_stamped));
        for count in per_group {
            assert!((9_500..=10_500).contains(&count), "tracks in a group: {count}");
        }
    }

    // doubling the count moves about half of each group's tracks to one new group
    #[test]
    fn doubling_moves_half() {
        let system = system(4, 4);
        let mut kept = [0u64; 4];
        let mut moved = [0u64; 4];

        for track in 0..TRACKS {
            let stamped = stamp(&system, track);
            let before = fold(4).live_group(stamped);
            let after = fold(8).live_group(stamped);

            if after == before {
                kept[before.as_usize()] += 1;
            } else {
                assert!(after.0 >= 4);
                moved[before.as_usize()] += 1;
            }
        }

        for group in 0..4 {
            let total = kept[group] + moved[group];
            assert!(
                (45..=55).contains(&(moved[group] * 100 / total)),
                "group {group} kept {} and moved {}",
                kept[group],
                moved[group],
            );
        }
    }

    // no group is live before the network starts
    #[test]
    fn no_live_groups() {
        let result = select_group(
            &system(0, 4),
            Address::new([7u8; 32]),
            TapeNumber(3),
            TrackNumber(0),
            Hash([9u8; 32]),
        );

        assert!(result.is_err());
    }
}
