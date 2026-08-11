use store::Store;
use tape_core::challenge::{Fold, PeerRecord};
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_protocol::ProtocolState;
use tape_store::TapeStore;
use tape_store::ops::ChallengeOps;
use tracing::debug;

/// Returns whether the peer owns the spool in the current or previous epoch.
pub fn holds_spool(state: &ProtocolState, peer: Address, spool: SpoolIndex) -> bool {
    state.spool_owner(spool) == Some(peer) || state.spool_owner_prev(spool) == Some(peer)
}

pub struct Folded {
    pub record: Option<PeerRecord>,
    /// The stored outcome, which may differ from a duplicate input.
    pub certified: bool,
}

/// Applies an outcome once and returns the stored result for the round.
pub fn fold_outcome<Db: Store>(
    store: &TapeStore<Db>,
    peer: Address,
    spool: SpoolIndex,
    epoch: EpochNumber,
    round: RoundNumber,
    certified: bool,
) -> Folded {
    let prior = store
        .round_outcome(peer, spool, epoch, round)
        .unwrap_or_default();
    let mut record = store.peer_record(peer, spool).unwrap_or_default();
    // A success is never downgraded and a recorded miss is upgraded by a late
    // certificate, so this is what the round is worth after the fold either way.
    let stands = prior == Some(true) || certified;

    let fold = record.record(epoch, round, certified, prior);
    if fold == Fold::Ignored {
        return Folded { record: None, certified: stands };
    }

    // The counters say how often. This says which rounds, so a report can name
    // the ones a node failed rather than only count them.
    if let Err(error) = store.put_round_outcome(peer, spool, epoch, round, certified) {
        debug!(%error, node = %peer, "challenge: round outcome not persisted");
    }

    if fold == Fold::Rebuild {
        match store.peer_rounds(peer, spool) {
            Ok(rounds) => record.rebuild_recency(&rounds),
            Err(error) => debug!(%error, node = %peer, "challenge: rounds unavailable for rebuild"),
        }
    }

    if let Err(error) = store.put_peer_record(peer, spool, record) {
        debug!(%error, node = %peer, "challenge: record not persisted");
    }

    debug!(
        node = %peer,
        spool = spool.0,
        epoch = epoch.0,
        round = round.0,
        certified,
        misses = record.consecutive_misses,
        opportunities = record.opportunities,
        "challenge: record advanced"
    );

    Folded { record: Some(record), certified: stands }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_store::TapeStore;

    use super::*;

    const SPOOL: SpoolIndex = SpoolIndex(41);

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn settle_keeps_a_folded_certificate() {
        let store = store();
        let peer = Address::new_unique();
        let (epoch, round) = (EpochNumber(2), RoundNumber(2));

        let certificate = fold_outcome(&store, peer, SPOOL, epoch, round, true);
        assert!(certificate.certified);
        assert_eq!(certificate.record.expect("folded").successes, 1);

        let settled = fold_outcome(&store, peer, SPOOL, epoch, round, false);
        assert!(settled.certified, "a settled round downgraded a certificate");
        assert!(settled.record.is_none(), "an ignored fold reported a change");

        let record = store.peer_record(peer, SPOOL).unwrap_or_default();
        assert_eq!(record.opportunities, 1);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 0);
    }

    #[test]
    fn one_spool_does_not_answer_for_another() {
        let store = store();
        let peer = Address::new_unique();
        let (kept, dropped) = (SpoolIndex(11), SpoolIndex(12));
        let (epoch, round) = (EpochNumber(5), RoundNumber(1));

        assert!(fold_outcome(&store, peer, kept, epoch, round, true).certified);
        assert!(!fold_outcome(&store, peer, dropped, epoch, round, false).certified);

        let answered = store.peer_record(peer, kept).unwrap_or_default();
        let missed = store.peer_record(peer, dropped).unwrap_or_default();
        assert_eq!(answered.successes, 1);
        assert_eq!(answered.consecutive_misses, 0);
        assert_eq!(missed.successes, 0);
        assert_eq!(missed.consecutive_misses, 1, "an answer erased another spool's miss");
    }

    #[test]
    fn a_dropped_spool_trips_its_own_run() {
        let store = store();
        let peer = Address::new_unique();
        let (kept, dropped) = (SpoolIndex(11), SpoolIndex(12));

        for round in 0..3u64 {
            let at = RoundNumber(round);
            fold_outcome(&store, peer, kept, EpochNumber(5), at, true);
            fold_outcome(&store, peer, dropped, EpochNumber(5), at, false);
        }

        assert!(!store.peer_record(peer, kept).unwrap_or_default().run_fires());
        assert!(store.peer_record(peer, dropped).unwrap_or_default().run_fires());

        let worst = store
            .records_for_peer(peer)
            .unwrap_or_default()
            .into_iter()
            .any(|(_, record)| record.run_fires());
        assert!(worst, "the node-level rule cannot see the dropped spool");
    }

    #[test]
    fn a_real_miss_stands_and_counts_once() {
        let store = store();
        let peer = Address::new_unique();
        let (epoch, round) = (EpochNumber(2), RoundNumber(3));

        let first = fold_outcome(&store, peer, SPOOL, epoch, round, false);
        assert!(!first.certified);
        assert!(first.record.is_some());

        let again = fold_outcome(&store, peer, SPOOL, epoch, round, false);
        assert!(!again.certified);
        assert!(again.record.is_none());

        let record = store.peer_record(peer, SPOOL).unwrap_or_default();
        assert_eq!(record.opportunities, 1);
        assert_eq!(record.consecutive_misses, 1);
    }

    #[test]
    fn a_late_certificate_upgrades_the_miss() {
        let store = store();
        let peer = Address::new_unique();
        let (epoch, round) = (EpochNumber(2), RoundNumber(4));

        assert!(!fold_outcome(&store, peer, SPOOL, epoch, round, false).certified);
        let late = fold_outcome(&store, peer, SPOOL, epoch, round, true);
        assert!(late.certified);

        let record = store.peer_record(peer, SPOOL).unwrap_or_default();
        assert_eq!(record.opportunities, 1);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 0);
    }
}
