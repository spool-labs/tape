//! Fold one round's outcome into a peer's record.
//!
//! Both fold sites go through here: a certificate the moment it forms, and the
//! settle pass when the next round opens. The stored per-round outcomes are the
//! dedup, so a repeat never counts twice, a late certificate upgrades a recorded
//! miss, and a miss that settles behind an eager certificate still counts.

use store::Store;
use tape_core::challenge::{Fold, PeerRecord};
use tape_core::types::{EpochNumber, RoundNumber};
use tape_crypto::Address;
use tape_store::TapeStore;
use tape_store::ops::ChallengeOps;
use tracing::debug;

/// Fold an outcome, returning the updated record when it changed anything.
pub fn fold_outcome<Db: Store>(
    store: &TapeStore<Db>,
    peer: Address,
    epoch: EpochNumber,
    round: RoundNumber,
    certified: bool,
) -> Option<PeerRecord> {
    let prior = store.round_outcome(peer, epoch, round).unwrap_or_default();
    let mut record = store.peer_record(peer).unwrap_or_default();

    let fold = record.record(epoch, round, certified, prior);
    if fold == Fold::Ignored {
        return None;
    }

    // The counters say how often; this says which rounds, so a report can name
    // the ones a node failed rather than only count them.
    if let Err(error) = store.put_round_outcome(peer, epoch, round, certified) {
        debug!(%error, node = %peer, "challenge: round outcome not persisted");
    }

    if fold == Fold::Rebuild {
        match store.peer_rounds(peer) {
            Ok(rounds) => record.rebuild_recency(&rounds),
            Err(error) => debug!(%error, node = %peer, "challenge: rounds unavailable for rebuild"),
        }
    }

    if let Err(error) = store.put_peer_record(peer, record) {
        debug!(%error, node = %peer, "challenge: record not persisted");
    }

    debug!(
        node = %peer,
        epoch = epoch.0,
        round = round.0,
        certified,
        misses = record.consecutive_misses,
        opportunities = record.opportunities,
        "challenge: record advanced"
    );

    Some(record)
}
