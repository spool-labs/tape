//! ParsedInstruction → ReplayableEvent conversion.
//!
//! Converts each block processing instruction into a replayable event
//! that can be serialized in a snapshot log and later replayed through
//! the same block processing handlers.

use tape_core::snapshot::ReplayableEvent;
use tape_core::types::EpochNumber;

use crate::features::chain::ParsedInstruction;

/// Convert a parsed instruction to a replayable event for the snapshot log.
///
/// Returns `None` for instruction types that are no-ops in the block processor
/// (RegisterNode, JoinNetwork) — these don't affect local state, so replaying
/// them would be pointless. We capture them anyway for completeness since they
/// are cheap and may become stateful in the future.
pub fn to_replayable(
    instruction: &ParsedInstruction,
    current_epoch: EpochNumber,
) -> Option<ReplayableEvent> {
    match instruction {
        ParsedInstruction::AdvanceEpoch { event } => Some(ReplayableEvent::AdvanceEpoch {
            old_epoch: event.old_epoch,
            new_epoch: event.new_epoch,
        }),

        ParsedInstruction::SyncEpoch { event } => Some(ReplayableEvent::SyncEpoch {
            node: event.node.to_bytes(),
            node_id: event.id,
            epoch: event.epoch,
            spools_hash: event.spools_hash,
        }),

        ParsedInstruction::RegisterTrack { track, event, .. } => {
            let event = event.as_ref()?;
            // Store the raw Pod bytes of TrackRegistered. During replay,
            // parse with bytemuck::try_from_bytes::<TrackRegistered>.
            let event_data = bytemuck::bytes_of(event).to_vec();
            Some(ReplayableEvent::RegisterTrack {
                track: track.to_bytes(),
                event_data,
            })
        }

        ParsedInstruction::CertifyTrack { track, event } => {
            Some(ReplayableEvent::CertifyTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            })
        }

        ParsedInstruction::DeleteTrack { track, .. } => Some(ReplayableEvent::DeleteTrack {
            track: track.to_bytes(),
            epoch: current_epoch,
        }),

        ParsedInstruction::InvalidateTrack { track, event } => {
            let epoch = event
                .as_ref()
                .map(|e| e.epoch)
                .unwrap_or(current_epoch);
            Some(ReplayableEvent::InvalidateTrack {
                track: track.to_bytes(),
                epoch,
            })
        }

        ParsedInstruction::ReserveTape {
            owner, tape, event, ..
        } => {
            let (active_epoch, expiry_epoch) = match event {
                Some(e) => (e.active_epoch, e.expiry_epoch),
                None => (current_epoch, current_epoch),
            };
            Some(ReplayableEvent::ReserveTape {
                tape: tape.to_bytes(),
                authority: owner.to_bytes(),
                active_epoch,
                expiry_epoch,
            })
        }

        ParsedInstruction::DestroyTape { tape, .. } => Some(ReplayableEvent::DestroyTape {
            tape: tape.to_bytes(),
            epoch: current_epoch,
        }),

        ParsedInstruction::RegisterNode {
            authority, node, ..
        } => Some(ReplayableEvent::RegisterNode {
            authority: authority.to_bytes(),
            node: node.to_bytes(),
        }),

        ParsedInstruction::JoinNetwork { node, .. } => Some(ReplayableEvent::JoinNetwork {
            node: node.to_bytes(),
        }),
    }
}
