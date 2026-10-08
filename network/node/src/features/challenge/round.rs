//! One round and what peers send each other for it

use tape_core::types::{EpochNumber, RoundNumber, SlotNumber, SpoolIndex};
use tape_crypto::{Address, Hash};

use crate::features::challenge::proof::Proof;
use crate::features::challenge::sample::Sample;

/// A round and the block that seeds it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Round {
    pub epoch: EpochNumber,
    pub number: RoundNumber,
    pub slot: SlotNumber,
    pub seed: Hash,
}

/// One spool's proof for a round. `spool` is the holder's live spool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpoolProof {
    pub spool: SpoolIndex,
    pub track: Address,
    pub proof: Proof,
}

/// A prover's proofs for one peer in one round
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Batch {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub proofs: Vec<SpoolProof>,
}

impl Round {
    pub fn sample(&self, spool: SpoolIndex) -> Sample {
        Sample::new(self.epoch, self.number, spool, self.seed)
    }
}
