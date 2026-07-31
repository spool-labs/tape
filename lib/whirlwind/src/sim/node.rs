//! Persistent node identity and the adversary and collusion physics.
//!
//! A node keeps a stable identity across epochs, a fixed behavior as a prover,
//! and an optional bloc membership that governs only its verifier conduct. The
//! free functions here are pure model math with no state: how long a prover
//! takes to be ready, whether it delivers to a given observer, whether its proof
//! is valid, and whether an observer is willing to attest for a target. Prover
//! misbehavior and verifier misbehavior are kept cleanly split.

use anyhow::Result;
use serde::Serialize;
use tape_core::bls::{BlsPrivateKey, BlsPubkey};
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::hash::{hashv, Hash};

use crate::crypto::keypair;
use crate::network::simulated::Group;
use crate::sim::latency::Costs;
use crate::sim::scoreboard::EvictionReason;
use crate::types::{GroupPosition, RoundNumber};

/// Domain tag for the deterministic selective-delivery coin.
const SELECTIVE_DOMAIN: &[u8] = b"WHRLWSL1";

/// A prover's conduct when challenged. Governs prover physics only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Behavior {
    Honest,
    NearbyFetch,
    Reconstruct,
    Selective,
    Offline,
    CollusiveFreeRider,
}

impl Behavior {
    /// Short label for reports and the event log.
    pub fn label(&self) -> &'static str {
        match self {
            Behavior::Honest => "honest",
            Behavior::NearbyFetch => "nearby-fetch",
            Behavior::Reconstruct => "reconstruct",
            Behavior::Selective => "selective",
            Behavior::Offline => "offline",
            Behavior::CollusiveFreeRider => "collusive-free-rider",
        }
    }
}

/// Tunables for the time-varying behaviors, kept reproducible by hashing.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct BehaviorConfig {
    /// Probability a selective prover delivers to a given observer in a round.
    pub selective_deliver_probability: f64,
    /// Round after which an offline prover goes silent.
    pub offline_after_round: RoundNumber,
}

/// A storage node with a stable cross-epoch identity.
#[derive(Clone, Copy)]
pub struct Node {
    /// Stable identity that keys the scoreboard across epochs.
    pub id: NodeId,
    /// Prover conduct when challenged.
    pub behavior: Behavior,
    /// Bloc membership governing verifier conduct, none for honest nodes.
    pub bloc: Option<u32>,
    /// Secret key for real BLS attestations and eviction votes.
    pub secret_key: BlsPrivateKey,
    /// Public key checked in real certificates.
    pub public_key: BlsPubkey,
    /// Real city this node sits at for latency.
    pub server_id: usize,
    /// Epoch this node joined the committee.
    pub joined_epoch: EpochNumber,
}

impl Node {
    /// Mint a fresh honest node with a new keypair at the given city.
    pub fn spawn_honest(id: NodeId, server_id: usize, joined_epoch: EpochNumber) -> Result<Self> {
        let (secret_key, public_key) = keypair()?;
        Ok(Self {
            id,
            behavior: Behavior::Honest,
            bloc: None,
            secret_key,
            public_key,
            server_id,
            joined_epoch,
        })
    }
}

/// Extra delay after confirmation before a prover's proof is ready.
///
/// Honest and the on-time misbehaviors add nothing. A nearby-fetch prover adds
/// one small edge round trip plus the decode. A reconstruct prover must rebuild
/// its slice from group helpers first, which costs a full intra-group round trip
/// plus the decode.
pub fn ready_delay_ms(
    behavior: Behavior,
    position: GroupPosition,
    group: &Group,
    costs: &Costs,
    edge_rtt_ms: f64,
) -> f64 {
    match behavior {
        Behavior::NearbyFetch => edge_rtt_ms + costs.decode_ms,
        Behavior::Reconstruct => {
            intra_group_rtt_ms(position, group, costs.helper_count) + costs.decode_ms
        }
        Behavior::Honest
        | Behavior::Selective
        | Behavior::Offline
        | Behavior::CollusiveFreeRider => 0.0,
    }
}

/// Round trip to the d-th nearest group helper, the reconstruction surcharge.
///
/// This mirrors the helper round trip used by the latency histogram exactly.
pub fn intra_group_rtt_ms(position: GroupPosition, group: &Group, helper_count: usize) -> f64 {
    let owner = position.as_usize();
    let mut round_trips: Vec<f64> = (0..group.len())
        .filter(|peer| *peer != owner)
        .map(|peer| group.one_way_ms(owner, peer) + group.one_way_ms(peer, owner))
        .collect();
    round_trips.sort_by(f64::total_cmp);
    let helper_index = helper_count
        .saturating_sub(1)
        .min(round_trips.len().saturating_sub(1));
    round_trips.get(helper_index).copied().unwrap_or(0.0)
}

/// Whether the prover delivers its proof to a given observer this round.
///
/// Reconstruct delivers but only late, so the deadline is what catches it.
/// Selective delivers with a fixed probability drawn from a reproducible coin.
/// Offline delivers until its cutover round and then goes silent.
pub fn delivers(
    behavior: Behavior,
    round: RoundNumber,
    observer_position: GroupPosition,
    entropy: &Hash,
    config: &BehaviorConfig,
) -> bool {
    match behavior {
        Behavior::Honest
        | Behavior::NearbyFetch
        | Behavior::Reconstruct
        | Behavior::CollusiveFreeRider => true,
        Behavior::Selective => {
            selective_coin(round, observer_position, entropy) < config.selective_deliver_probability
        }
        Behavior::Offline => round < config.offline_after_round,
    }
}

/// Whether the prover holds a genuinely valid proof this round.
///
/// False only for the collusive free-rider, which stores nothing and never
/// reconstructs, so it can never build a real proof of possession.
pub fn produces_valid_proof(behavior: Behavior, _round: RoundNumber) -> bool {
    behavior != Behavior::CollusiveFreeRider
}

/// Whether an observer is willing to attest a target's proof.
///
/// An honest observer attests any valid delivered proof. A bloc observer attests
/// only for same-bloc provers, refusing outsiders even when their proof is valid
/// and signing for its own bloc even when the proof is not.
pub fn willing_to_sign(observer: &Node, target: &Node, is_valid: bool, delivered: bool) -> bool {
    match observer.bloc {
        Some(bloc) => target.bloc == Some(bloc),
        None => is_valid && delivered,
    }
}

/// Whether an observer votes to evict a target at the boundary, and why
///
/// A bloc spares its own members and piles on the victim to model a spurious
/// campaign. Everyone else votes their score.
pub fn willing_to_evict(
    observer: &Node,
    target: &Node,
    score_reason: Option<EvictionReason>,
    victim: Option<NodeId>,
) -> Option<EvictionReason> {
    match observer.bloc {
        Some(bloc) => {
            if target.bloc == Some(bloc) {
                return None;
            }
            if let Some(reason) = score_reason {
                return Some(reason);
            }
            (victim == Some(target.id)).then_some(EvictionReason::ConsecutiveMisses)
        }
        None => score_reason,
    }
}

/// The lowest-position honest solo node, the bloc's spurious eviction target.
pub fn designate_victim(committee: &[Node]) -> Option<NodeId> {
    committee
        .iter()
        .find(|node| node.behavior == Behavior::Honest && node.bloc.is_none())
        .map(|node| node.id)
}

/// Deterministic delivery coin in the unit interval for a selective prover.
fn selective_coin(round: RoundNumber, observer_position: GroupPosition, entropy: &Hash) -> f64 {
    let digest = hashv(&[
        SELECTIVE_DOMAIN,
        &round.as_u64().to_le_bytes(),
        &observer_position.as_u64().to_le_bytes(),
        entropy.as_ref(),
    ]);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest.0[..8]);
    u64::from_le_bytes(bytes) as f64 / u64::MAX as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    // only the collusive free-rider fails to hold a valid proof
    #[test]
    fn valid_proofs() {
        assert!(produces_valid_proof(Behavior::Honest, RoundNumber::new(0)));
        assert!(produces_valid_proof(Behavior::NearbyFetch, RoundNumber::new(0)));
        assert!(produces_valid_proof(Behavior::Reconstruct, RoundNumber::new(0)));
        assert!(!produces_valid_proof(Behavior::CollusiveFreeRider, RoundNumber::new(0)));
    }

    // an honest observer signs only a valid proof that was delivered
    #[test]
    fn honest_signing() {
        let (secret_key, public_key) = keypair().unwrap();
        let honest = Node {
            id: NodeId(0),
            behavior: Behavior::Honest,
            bloc: None,
            secret_key,
            public_key,
            server_id: 0,
            joined_epoch: EpochNumber(0),
        };
        let target = Node { id: NodeId(1), ..honest };
        assert!(willing_to_sign(&honest, &target, true, true));
        assert!(!willing_to_sign(&honest, &target, false, true));
        assert!(!willing_to_sign(&honest, &target, true, false));
    }

    // a bloc observer signs for its own members and refuses outsiders
    #[test]
    fn bloc_signing() {
        let (secret_key, public_key) = keypair().unwrap();
        let base = Node {
            id: NodeId(0),
            behavior: Behavior::Honest,
            bloc: Some(0),
            secret_key,
            public_key,
            server_id: 0,
            joined_epoch: EpochNumber(0),
        };
        let same_bloc = Node { id: NodeId(1), bloc: Some(0), ..base };
        let outsider = Node { id: NodeId(2), bloc: None, ..base };
        assert!(willing_to_sign(&base, &same_bloc, false, false));
        assert!(!willing_to_sign(&base, &outsider, true, true));
    }
}
