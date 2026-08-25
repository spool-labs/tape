//! One round's attestations, gathered so a signer posts once per peer rather
//! than once per answer it verified.

use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::hash::Hash;

/// How long a round's signatures gather before they are sent.
pub const FLUSH_MS: u64 = 120;

/// The round a batch belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchKey {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub block: Hash,
}

/// Signatures waiting to be sent, by round.
#[derive(Default)]
pub struct AttestQueue {
    held: Mutex<HashMap<BatchKey, Vec<(SpoolIndex, BlsSignature)>>>,
}

impl AttestQueue {
    /// Adds one signature; true when it armed this round's flush.
    pub fn push(&self, key: BatchKey, spool: SpoolIndex, signature: BlsSignature) -> bool {
        let Ok(mut held) = self.held.lock() else { return false };
        let entry = held.entry(key).or_default();
        let armed = !entry.is_empty();
        entry.push((spool, signature));
        !armed
    }

    /// Takes everything gathered for a round, leaving nothing behind.
    pub fn take(&self, key: &BatchKey) -> Vec<(SpoolIndex, BlsSignature)> {
        let Ok(mut held) = self.held.lock() else { return Vec::new() };
        held.remove(key).unwrap_or_default()
    }
}
