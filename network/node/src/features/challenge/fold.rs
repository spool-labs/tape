use tape_core::challenge::record::RECENT_ROUNDS;
use tape_core::challenge::{Fold, PeerRecord};
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_protocol::ProtocolState;
use tape_store::TapeStore;
use tape_store::ops::ChallengeOps;
use tape_store_api::Store;
use tracing::debug;

pub struct Folded {
    pub record: Option<PeerRecord>,
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
    // A success is never downgraded and a recorded miss is upgraded by a late
    // certificate, so this is what the round is worth after the fold either way.
    let stands = prior == Some(true) || certified;

    // A round already recorded as proved changes nothing, and every node folds
    // every peer's spools, so the duplicate is the common case and should not
    // cost a record read.
    if prior == Some(true) || (prior == Some(false) && !certified) {
        return Folded {
            record: None,
            certified: stands,
        };
    }

    let mut record = store.peer_record(peer, spool).unwrap_or_default();
    let fold = record.record(epoch, round, certified, prior);
    if fold == Fold::Ignored {
        return Folded {
            record: None,
            certified: stands,
        };
    }

    if fold == Fold::Rebuild {
        // Keep the whole known miss streak, plus its preceding success if it
        // is still stored. The display strip alone cannot reconstruct a long
        // streak, and pruning must not erase the evidence already counted.
        let limit = record
            .consecutive_misses
            .saturating_add(1)
            .max(RECENT_ROUNDS as u64);
        match store.peer_rounds_tail(peer, spool, usize::try_from(limit).unwrap_or(usize::MAX)) {
            Ok(mut rounds) => {
                let retained_misses = rounds
                    .iter()
                    .rev()
                    .take_while(|(_, _, proved)| !proved)
                    .count();
                let preceding_misses = record
                    .consecutive_misses
                    .saturating_sub(retained_misses as u64);
                // This round is written below rather than above, so it is placed
                // into the tail here instead of being read back out of it.
                let at = rounds.partition_point(|(held, at, _)| (*held, *at) < (epoch, round));
                match rounds.get_mut(at) {
                    Some((held, at, outcome)) if (*held, *at) == (epoch, round) => {
                        *outcome = certified;
                    }
                    _ => rounds.insert(at, (epoch, round, certified)),
                }
                record.rebuild_recency(&rounds, preceding_misses);
            }
            Err(error) => debug!(%error, node = %peer, "challenge: rounds unavailable for rebuild"),
        }
    }

    // The outcome says which rounds a node failed where the counters only say
    // how many, and it is written with the record it produced so a crash cannot
    // leave one without the other.
    if let Err(error) = store.put_round_fold(peer, spool, epoch, round, certified, record) {
        debug!(%error, node = %peer, "challenge: fold not persisted");
    }

    Folded {
        record: Some(record),
        certified: stands,
    }
}

/// Returns whether the peer owns the spool in the current or previous epoch.
pub fn holds_spool(state: &ProtocolState, peer: Address, spool: SpoolIndex) -> bool {
    state.spool_owner(spool) == Some(peer) || state.spool_owner_prev(spool) == Some(peer)
}

#[cfg(test)]
mod tests {
    use tape_store::TapeStore;
    use tape_store_memory::MemoryStore;

    use super::*;
    use tape_core::challenge::EvictionPolicy;
    use tape_core::types::BasisPoints;

    const POLICY: EvictionPolicy = EvictionPolicy {
        max_consecutive_misses: 30,
        min_opportunities: 60,
        minimum_success_rate_bps: BasisPoints(5_000),
    };

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
        assert!(
            settled.certified,
            "a settled round downgraded a certificate"
        );
        assert!(
            settled.record.is_none(),
            "an ignored fold reported a change"
        );

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
        assert_eq!(
            missed.consecutive_misses, 1,
            "an answer erased another spool's miss"
        );
    }

    #[test]
    fn a_dropped_spool_trips_its_own_run() {
        let store = store();
        let peer = Address::new_unique();
        let (kept, dropped) = (SpoolIndex(11), SpoolIndex(12));

        for round in 0..POLICY.max_consecutive_misses {
            let at = RoundNumber(round);
            fold_outcome(&store, peer, kept, EpochNumber(5), at, true);
            fold_outcome(&store, peer, dropped, EpochNumber(5), at, false);
        }

        assert!(
            !store
                .peer_record(peer, kept)
                .unwrap_or_default()
                .run_fires(&POLICY)
        );
        assert!(
            store
                .peer_record(peer, dropped)
                .unwrap_or_default()
                .run_fires(&POLICY)
        );

        let worst = store
            .records_for_peer(peer)
            .unwrap_or_default()
            .into_iter()
            .any(|(_, record)| record.run_fires(&POLICY));
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

    #[test]
    fn late_certificate_rebuilds_a_streak_longer_than_the_display_strip() {
        let store = store();
        let peer = Address::new_unique();
        for round in 0..150 {
            fold_outcome(
                &store,
                peer,
                SPOOL,
                EpochNumber(1),
                RoundNumber(round),
                false,
            );
        }
        fold_outcome(&store, peer, SPOOL, EpochNumber(1), RoundNumber(1), true);
        let record = store.peer_record(peer, SPOOL).unwrap();
        assert_eq!(record.consecutive_misses, 148);
        assert_eq!(record.recent_len, 64);
        assert_eq!(record.opportunities, 150);
        assert_eq!(record.successes, 1);
    }

    #[test]
    fn late_outcomes_preserve_pruned_streak_until_a_success_breaks_it() {
        let store = store();
        let peer = Address::new_unique();
        for round in 0..100 {
            fold_outcome(
                &store,
                peer,
                SPOOL,
                EpochNumber(1),
                RoundNumber(round),
                false,
            );
        }
        for round in 0..50 {
            if round != 40 {
                fold_outcome(
                    &store,
                    peer,
                    SPOOL,
                    EpochNumber(2),
                    RoundNumber(round),
                    false,
                );
            }
        }
        store.prune_rounds_before(EpochNumber(2)).unwrap();
        fold_outcome(&store, peer, SPOOL, EpochNumber(2), RoundNumber(40), false);
        let record = store.peer_record(peer, SPOOL).unwrap();
        assert_eq!(record.consecutive_misses, 150);
        assert_eq!(record.opportunities, 150);

        fold_outcome(&store, peer, SPOOL, EpochNumber(2), RoundNumber(10), true);
        let record = store.peer_record(peer, SPOOL).unwrap();
        assert_eq!(record.consecutive_misses, 39);
        assert_eq!(record.opportunities, 150);
        assert_eq!(record.successes, 1);
    }
}
