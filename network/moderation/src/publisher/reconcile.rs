//! Settling a tape's records against the chain after a write or delete of unknown outcome

use tape_api::program::tapedrive::TrackAddress;
use tape_core::types::TrackNumber;
use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;
use tracing::{info, warn};

use crate::chain::tape_chain::TapeChain;
use crate::publisher::error::PublishError;
use crate::publisher::writer::{TrackWriter, WrittenTrack};
use crate::records::rows::{Adopted, ListTape};
use crate::records::store::Records;
use crate::types::timestamp::Timestamp;

/// Bring a tape's records in line with the chain
///
/// The producer is the only writer of its tapes, so the tape's next track number
/// counts every write that landed, and its track count every one not deleted. A
/// landed track records lack is matched by key to the planned track it is, or
/// recorded as a copy to delete. A sent delete counts as landed once the track
/// count says so. Planned tracks that cannot land on a full tape are abandoned;
/// the caller writes the rest again.
pub async fn reconcile<Chain>(
    records: &Records,
    chain: &Chain,
    tape: &ListTape,
    now: Timestamp,
) -> Result<(), PublishError>
where
    Chain: TapeChain + TrackWriter,
{
    let status = chain
        .tape_status(tape.address)
        .await
        .map_err(|source| PublishError::Read {
            tape: tape.address,
            source,
        })?
        .ok_or(PublishError::MissingTape(tape.address))?;
    records.observe_tape(tape.address, status.observation(), now).await?;

    let landed = records.landed_numbers(tape.address).await?;
    for track_number in unrecorded(&landed, status.next_track) {
        adopt(records, chain, tape, track_number, now).await?;
    }

    let recorded = records.on_chain_tracks(tape.address).await?;
    let sent = records.sent_deletes(tape.address).await?;
    let landed_deletes = recorded
        .0
        .checked_sub(status.present_tracks.0)
        .and_then(|count| usize::try_from(count).ok());
    match landed_deletes {
        Some(0) => {}
        Some(count) if count == sent.len() => records.mark_deleted(sent, now).await?,
        Some(_) | None => {
            return Err(PublishError::TrackCount {
                tape: tape.address,
                recorded,
                chain: status.present_tracks,
            })
        }
    }

    if status.next_track.0 >= MAX_TRACKS_PER_TAPE {
        let abandoned = records.abandon_writing(tape.address, now).await?;
        if abandoned > 0 {
            warn!(tape = %tape.address, abandoned, "planned tracks cannot land on a full tape; their decisions queue again");
        }
    }
    Ok(())
}

/// Record the landed track at a number records lack, found by its key
async fn adopt<Chain>(
    records: &Records,
    chain: &Chain,
    tape: &ListTape,
    track_number: TrackNumber,
    now: Timestamp,
) -> Result<(), PublishError>
where
    Chain: TapeChain + TrackWriter,
{
    let landed = chain
        .landed_track(tape.address, track_number)
        .await
        .map_err(|source| PublishError::Read {
            tape: tape.address,
            source,
        })?;
    let Some(landed) = landed else {
        return Err(PublishError::NotVisible {
            tape: tape.address,
            track_number,
        });
    };

    let written = WrittenTrack {
        track: landed.address(),
        track_number,
    };
    let number = track_number.0;
    match records.adopt_track(tape.address, written, landed.key, now).await? {
        Adopted::Planned(track) => info!(tape = %tape.address, number, %track, "found a planned track that landed"),
        Adopted::Duplicate(track) => warn!(tape = %tape.address, number, %track, "found a track that landed twice; deleting the copy"),
        Adopted::Unknown(track) => warn!(tape = %tape.address, number, %track, "found a track matching nothing recorded; leaving it"),
    }
    Ok(())
}

/// Track numbers below `next` that no recorded track has, lowest first
fn unrecorded(landed: &[TrackNumber], next: TrackNumber) -> Vec<TrackNumber> {
    let mut missing = Vec::new();
    let mut known = landed.iter().peekable();
    for number in 0..next.0 {
        if known.next_if(|known| known.0 == number).is_none() {
            missing.push(TrackNumber(number));
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    // numbers below the next one that no recorded track has are found in order
    #[test]
    fn gaps() {
        let landed = [TrackNumber(0), TrackNumber(2), TrackNumber(3)];

        let missing = unrecorded(&landed, TrackNumber(6));

        assert_eq!(missing, vec![TrackNumber(1), TrackNumber(4), TrackNumber(5)]);
        assert!(unrecorded(&[TrackNumber(0), TrackNumber(1)], TrackNumber(2)).is_empty());
    }
}
