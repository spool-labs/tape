//! Lists the proofs a node expects in a round and records the ones that arrive

use std::time::Instant;

use tape_core::encoding::ErasureCode;
use tape_core::spooler::{GroupFold, GroupIndex};
use tape_crypto::Address;
use tape_protocol::EpochBundle;
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::features::challenge::board::{BoardRound, Entry, Outcome, Seen};
use crate::features::challenge::error::ChallengeError;
use crate::features::challenge::holders::{holder, peer_spools};
use crate::features::challenge::pick::pick_track;
use crate::features::challenge::proof::{check_proof, ProofCheck};
use crate::features::challenge::round::{Batch, Round, SpoolProof};

/// List an empty entry for every peer spool `node` expects a proof from
///
/// `bundle` is the assignment whose holders answer. A group with no track in
/// the set has nothing to prove, so its spools get no entry.
pub fn expected_entries<Db: Store>(
    store: &TapeStore<Db>,
    bundle: &EpochBundle,
    node: Address,
    round: Round,
) -> Vec<Entry> {
    let Some(fold) = bundle.fold() else {
        return Vec::new();
    };

    // The spools come group by group, so one lookup covers a whole group.
    let mut entries = Vec::new();
    let mut last: Option<(GroupIndex, bool)> = None;
    for (spool, _) in peer_spools(bundle, node) {
        let group = GroupIndex::containing(spool);
        let has_tracks = match last {
            Some((seen, has_tracks)) if seen == group => has_tracks,
            Some(_) | None => group_has_tracks(store, group, fold, round),
        };
        last = Some((group, has_tracks));

        if has_tracks {
            entries.push(Entry { spool, seen: None });
        }
    }
    entries
}

fn group_has_tracks<Db: Store>(
    store: &TapeStore<Db>,
    group: GroupIndex,
    fold: GroupFold,
    round: Round,
) -> bool {
    match pick_track(store, Address::default(), group, fold, round.epoch) {
        Ok(pick) => pick.is_some(),
        Err(error) => {
            warn!(%group, %error, "challenge set lookup failed");
            false
        }
    }
}

/// Record a peer's proofs in the round they are for
///
/// `arrived` is when the batch reached this node, stamped before any check.
/// A proof is skipped when the round expects none for its spool, when one was
/// already recorded, or when the sender is not the spool's answering holder.
pub fn record<Db: Store>(
    store: &TapeStore<Db>,
    bundle: &EpochBundle,
    profile: ErasureCode,
    round: &mut BoardRound,
    sender: Address,
    batch: &Batch,
    arrived: Instant,
) {
    let Some(opened) = round.opened else {
        return;
    };
    let Some(fold) = bundle.fold() else {
        return;
    };
    let ms = signed_ms(opened.at, arrived);

    for proof in &batch.proofs {
        let spool = proof.spool;
        let Some(entry) = round.entries.iter_mut().find(|entry| entry.spool == spool) else {
            continue;
        };
        if entry.seen.is_some() || holder(bundle, spool) != Some(sender) {
            continue;
        }

        match outcome(store, opened.round, proof, fold, profile) {
            Ok(Some(outcome)) => entry.seen = Some(Seen { outcome, ms }),
            Ok(None) => {}
            Err(error) => warn!(%spool, %error, "challenge check failed"),
        }
    }
}

/// Check one proof against this node's own pick for the spool
///
/// Nothing comes back when this node finds no track for the spool, or holds
/// no encoding to check a coded proof against.
fn outcome<Db: Store>(
    store: &TapeStore<Db>,
    round: Round,
    proof: &SpoolProof,
    fold: GroupFold,
    profile: ErasureCode,
) -> Result<Option<Outcome>, ChallengeError> {
    let sample = round.sample(proof.spool);
    let group = GroupIndex::containing(proof.spool);

    let Some(pick) = pick_track(store, sample.start(), group, fold, round.epoch)? else {
        return Ok(None);
    };
    if pick.address != proof.track {
        return Ok(Some(Outcome::WrongTrack));
    }

    let check = check_proof(store, &pick, proof.spool, sample, profile, &proof.proof)?;
    Ok(match check {
        ProofCheck::Valid => Some(Outcome::Ok),
        ProofCheck::Invalid => Some(Outcome::Invalid),
        ProofCheck::NoEncoding => None,
    })
}

/// Milliseconds from `opened` to `arrived`, negative when the proof came first
fn signed_ms(opened: Instant, arrived: Instant) -> i64 {
    match arrived.checked_duration_since(opened) {
        Some(late) => late.as_millis() as i64,
        None => -(opened.duration_since(arrived).as_millis() as i64),
    }
}
