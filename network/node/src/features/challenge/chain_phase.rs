//! The chain's epoch and phase, read off the blocks the challenge lane sees
//!
//! Protocol state applies a block once it promotes, on a task of its own, so
//! when a round's opening block reaches the challenge lane one node's state may
//! hold a phase change from the block before it while another's does not yet.
//! Every node that ingests the chain folds the same blocks here in the same
//! order, so each one reads the same phase for every block.

use tape_blocks::ParsedInstruction;
use tape_api::event::SpoolSynced;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::hash::Hash;

use crate::features::block::ingestor::ParsedBlock;

/// The epoch the chain is in and the phase that epoch has reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainPhase {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
}

/// What one block meant for the chain's phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockPhase {
    /// The phase every block before this one left, which the block was produced in.
    pub produced_in: ChainPhase,
    /// The epoch this block made Active, when it finalized the snapshot that does.
    pub turned_active: Option<EpochNumber>,
}

/// Every phase change the chain has made, as its blocks arrive.
pub struct PhaseLedger {
    /// The phase after the newest block folded in.
    now: ChainPhase,
    /// The phase before each change, kept until the block that made it
    /// promotes, so a block that loses can take its change back.
    undo: Vec<(Hash, ChainPhase)>,
}

impl PhaseLedger {
    /// Starts from the phase the node's state holds before the lane's first block.
    pub fn new(start: ChainPhase) -> Self {
        Self {
            now: start,
            undo: Vec::new(),
        }
    }

    /// Folds one block's changes in and says what the block meant.
    ///
    /// A snapshot finalizing marks the turn into Active even when the start
    /// phase already read Active, since a state fetched ahead of the lane's
    /// first block can say so before the chain has got there.
    pub fn advance(&mut self, block: &ParsedBlock) -> BlockPhase {
        let produced_in = self.now;
        let mut turned_active = None;
        for instruction in &block.instructions {
            if let Some(next) = transition(self.now, instruction) {
                self.undo.push((block.blockhash, self.now));
                self.now = next;
                if next.phase == EpochPhase::Active {
                    turned_active = Some(next.epoch);
                }
            }
        }
        BlockPhase {
            produced_in,
            turned_active,
        }
    }

    /// Takes back every change the lost blocks made.
    ///
    /// The blocks that lose are the newest the lane has seen, so their changes
    /// are the newest held.
    pub fn rolled(&mut self, hashes: &[Hash]) {
        let Some(at) = self.undo.iter().position(|(block, _)| hashes.contains(block)) else {
            return;
        };
        self.now = self.undo[at].1;
        self.undo.truncate(at);
    }

    /// Forgets the undo record up to a block that promoted, which nothing can
    /// take back. Blocks promote in chain order, so every earlier one has too.
    pub fn confirmed(&mut self, hash: Hash) {
        if let Some(at) = self.undo.iter().rposition(|(block, _)| *block == hash) {
            self.undo.drain(..=at);
        }
    }
}

/// The phase an instruction moves the chain to, when it moves it at all.
fn transition(now: ChainPhase, instruction: &ParsedInstruction) -> Option<ChainPhase> {
    match instruction {
        ParsedInstruction::AdvanceEpoch { event } => Some(ChainPhase {
            epoch: event.new_epoch,
            phase: EpochPhase::Sync,
        }),
        // Genesis takes the chain from epoch zero into the first epoch's snapshot.
        ParsedInstruction::StartNetwork => now.epoch.is_zero().then_some(ChainPhase {
            epoch: EpochNumber(1),
            phase: EpochPhase::Snapshot,
        }),
        ParsedInstruction::SyncSpool { event, .. } => synced(now, event),
        // The snapshot of the previous epoch finalizing is what makes this one Active.
        ParsedInstruction::FinalizeSnapshot { event, .. } => {
            (event.epoch.next() == now.epoch).then_some(ChainPhase {
                epoch: now.epoch,
                phase: EpochPhase::Active,
            })
        }
        ParsedInstruction::CommitEpoch { event } => {
            (event.epoch == now.epoch).then_some(ChainPhase {
                epoch: now.epoch,
                phase: EpochPhase::Closing,
            })
        }
        ParsedInstruction::CreateEpoch { .. }
        | ParsedInstruction::CreateCommittee { .. }
        | ParsedInstruction::ResizeCommittee { .. }
        | ParsedInstruction::ResizePeerSet { .. }
        | ParsedInstruction::ProposeSnapshot { .. }
        | ParsedInstruction::VoteSnapshot { .. }
        | ParsedInstruction::ProposeAssignment { .. }
        | ParsedInstruction::VoteAssignment { .. }
        | ParsedInstruction::FinalizeGroup { .. }
        | ParsedInstruction::AdvancePool { .. }
        | ParsedInstruction::StakeWithPool { .. }
        | ParsedInstruction::RequestStakeUnlock { .. }
        | ParsedInstruction::UnstakeFromPool { .. }
        | ParsedInstruction::ClaimCommission { .. }
        | ParsedInstruction::TrackWrite { .. }
        | ParsedInstruction::DeleteTrack { .. }
        | ParsedInstruction::CertifyTrack { .. }
        | ParsedInstruction::InvalidateTrack { .. }
        | ParsedInstruction::ReserveTape { .. }
        | ParsedInstruction::ExtendTape { .. }
        | ParsedInstruction::SetTapeDelegate { .. }
        | ParsedInstruction::AdvanceShareNonce { .. }
        | ParsedInstruction::RegisterNode { .. }
        | ParsedInstruction::SetName { .. }
        | ParsedInstruction::SetNetworkAddress { .. }
        | ParsedInstruction::SetNetworkTls { .. }
        | ParsedInstruction::SetBlsPubkey { .. }
        | ParsedInstruction::JoinCommittee { .. }
        | ParsedInstruction::ProposeEviction { .. }
        | ParsedInstruction::NodeEvicted { .. }
        | ParsedInstruction::AddToBlacklist { .. }
        | ParsedInstruction::RemoveFromBlacklist { .. } => None,
    }
}

/// A spool sync reports the phase its epoch reached, which only moves forward.
fn synced(now: ChainPhase, event: &SpoolSynced) -> Option<ChainPhase> {
    let phase = EpochPhase::try_from(event.phase).ok()?;
    (event.epoch == now.epoch && phase > now.phase).then_some(ChainPhase {
        epoch: now.epoch,
        phase,
    })
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::event::{EpochAdvanced, EpochCommitted, SnapshotFinalized};
    use tape_core::types::SlotNumber;

    use super::*;

    const EPOCH: EpochNumber = EpochNumber(7);

    fn at(phase: EpochPhase) -> ChainPhase {
        ChainPhase { epoch: EPOCH, phase }
    }

    fn block(slot: u64, instructions: Vec<ParsedInstruction>) -> ParsedBlock {
        ParsedBlock {
            slot: SlotNumber(slot),
            parent_slot: SlotNumber(slot - 1),
            blockhash: Hash([slot as u8; 32]),
            instructions,
            ..ParsedBlock::default()
        }
    }

    fn finalize(epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::FinalizeSnapshot {
            epoch,
            event: SnapshotFinalized {
                epoch,
                ..SnapshotFinalized::zeroed()
            },
        }
    }

    fn commit(epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::CommitEpoch {
            event: EpochCommitted {
                epoch,
                ..EpochCommitted::zeroed()
            },
        }
    }

    fn advance(epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch: EpochNumber(epoch.as_u64() - 1),
                new_epoch: epoch,
                ..EpochAdvanced::zeroed()
            },
        }
    }

    // each block reads the phase the blocks before it left, and its own change
    // reaches the block after it
    #[test]
    fn phase_by_block() {
        let mut ledger = PhaseLedger::new(at(EpochPhase::Snapshot));
        let finalized = block(10, vec![finalize(EpochNumber(6))]);
        let committed = block(12, vec![commit(EPOCH)]);
        let advanced = block(14, vec![advance(EpochNumber(8))]);

        let turn = ledger.advance(&finalized);
        assert_eq!(turn.produced_in, at(EpochPhase::Snapshot));
        assert_eq!(turn.turned_active, Some(EPOCH));
        assert_eq!(ledger.advance(&block(11, Vec::new())).produced_in, at(EpochPhase::Active));
        assert_eq!(ledger.advance(&committed).produced_in, at(EpochPhase::Active));
        assert_eq!(ledger.advance(&block(13, Vec::new())).produced_in, at(EpochPhase::Closing));

        ledger.advance(&advanced);
        let next = ledger.advance(&block(15, Vec::new())).produced_in;
        assert_eq!(next, ChainPhase { epoch: EpochNumber(8), phase: EpochPhase::Sync });
    }

    // a finalized snapshot marks the turn into Active even when the start phase
    // already said Active, as a state fetched ahead of the blocks can
    #[test]
    fn turn_from_ahead() {
        let mut ledger = PhaseLedger::new(at(EpochPhase::Active));

        let turn = ledger.advance(&block(10, vec![finalize(EpochNumber(6))]));

        assert_eq!(turn.turned_active, Some(EPOCH));
    }

    // a change meant for another epoch leaves the chain where it was
    #[test]
    fn other_epoch() {
        let mut ledger = PhaseLedger::new(at(EpochPhase::Active));

        ledger.advance(&block(10, vec![commit(EpochNumber(6))]));
        let other = ledger.advance(&block(11, vec![finalize(EPOCH)]));

        assert_eq!(other.turned_active, None);
        assert_eq!(ledger.advance(&block(12, Vec::new())).produced_in, at(EpochPhase::Active));
    }

    // a block that loses takes its change back, and one that promotes keeps it
    #[test]
    fn rolled_block() {
        let mut ledger = PhaseLedger::new(at(EpochPhase::Active));
        let committed = block(10, vec![commit(EPOCH)]);

        ledger.advance(&committed);
        ledger.rolled(&[committed.blockhash]);
        assert_eq!(ledger.advance(&block(11, Vec::new())).produced_in, at(EpochPhase::Active));

        ledger.advance(&block(12, vec![commit(EPOCH)]));
        ledger.confirmed(Hash([12; 32]));
        ledger.rolled(&[Hash([12; 32])]);
        assert_eq!(ledger.advance(&block(13, Vec::new())).produced_in, at(EpochPhase::Closing));
    }
}
