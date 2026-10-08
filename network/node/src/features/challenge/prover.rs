//! Builds a node's proofs for a round and sorts them into one batch for each peer

use std::collections::{BTreeMap, BTreeSet};

use tape_api::state::Group;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::{GroupFold, GroupIndex};
use tape_core::types::SpoolIndex;
use tape_crypto::Address;
use tape_protocol::EpochBundle;
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::features::challenge::error::ChallengeError;
use crate::features::challenge::pick::pick_track;
use crate::features::challenge::proof::build_proof;
use crate::features::challenge::round::{Batch, Round, SpoolProof};

/// Build the proofs `node` owes in a round, as one batch for each peer
///
/// `bundle` is the assignment whose holders answer. A peer gets the proofs of
/// the node's spools in the groups the two share. A spool with no track to
/// prove, or whose data this node lacks, gets no proof.
pub fn build_batches<Db: Store>(
    store: &TapeStore<Db>,
    bundle: &EpochBundle,
    node: Address,
    round: Round,
    profile: ErasureCode,
) -> Vec<(Address, Batch)> {
    let Some(fold) = bundle.fold() else {
        return Vec::new();
    };

    let mut by_peer: BTreeMap<Address, Vec<SpoolProof>> = BTreeMap::new();
    for group in &bundle.groups {
        let proofs = group_proofs(store, group, fold, node, round, profile);
        if proofs.is_empty() {
            continue;
        }

        let mut peers = BTreeSet::new();
        for spool in &group.spools {
            if spool.node != node && spool.node != Address::default() {
                peers.insert(spool.node);
            }
        }
        for peer in peers {
            by_peer.entry(peer).or_default().extend(proofs.iter().cloned());
        }
    }

    let mut batches = Vec::with_capacity(by_peer.len());
    for (peer, proofs) in by_peer {
        batches.push((peer, Batch { epoch: round.epoch, round: round.number, proofs }));
    }
    batches
}

/// Build the proofs for the spools `node` holds in one group
fn group_proofs<Db: Store>(
    store: &TapeStore<Db>,
    group: &Group,
    fold: GroupFold,
    node: Address,
    round: Round,
    profile: ErasureCode,
) -> Vec<SpoolProof> {
    let mut proofs = Vec::new();
    for (position, held) in group.spools.iter().enumerate() {
        if held.node != node {
            continue;
        }

        let spool = group.id.spool_at(position);
        match spool_proof(store, fold, spool, round, profile) {
            Ok(Some(proof)) => proofs.push(proof),
            Ok(None) => {}
            Err(error) => warn!(%spool, %error, "challenge proof failed"),
        }
    }
    proofs
}

fn spool_proof<Db: Store>(
    store: &TapeStore<Db>,
    fold: GroupFold,
    spool: SpoolIndex,
    round: Round,
    profile: ErasureCode,
) -> Result<Option<SpoolProof>, ChallengeError> {
    let sample = round.sample(spool);
    let group = GroupIndex::containing(spool);

    let Some(pick) = pick_track(store, sample.start(), group, fold, round.epoch)? else {
        return Ok(None);
    };
    let Some(proof) = build_proof(store, &pick, spool, sample, profile)? else {
        return Ok(None);
    };

    Ok(Some(SpoolProof { spool, track: pick.address, proof }))
}
