//! One real Whirlwind challenge round as a pure function.
//!
//! The round derives the challenged spool's byte-weighted sub-leaf sample from
//! the entropy block, builds a real two-level merkle proof for it, and has each
//! honest verifier run the real verification. Two clocks govern the round. The
//! proof deadline runs from block production: an injected one-way network latency
//! is converted to slots to decide whether the proof arrived before the deadline,
//! which is what gates each verifier's signature. Signing opens later on
//! confirmation, and a willing verifier that saw a valid proof in time signs a
//! real BLS attestation; once the threshold sign, the round aggregates and
//! verifies a real certificate, and that certificate is what the scoreboard
//! records.
//!
//! A proof relays through the group, so an owner reaches a peer either directly
//! or through whichever peer already holds it, and withholding only delays.
//!
//! The premise under test is that a spool must answer faster than it could fetch
//! discarded data from its peers, so the proof deadline is the discriminator.

use anyhow::{anyhow, Result};
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::types::EpochNumber;

use crate::crypto::{aggregate, attestation_message, round_seed, sign, verify_certificate};
use crate::network::simulated::Group;
use crate::sim::epoch::{BlsMode, EpochConfig, GROUP_ID};
use crate::sim::latency::Costs;
use crate::sim::node::{delivers, produces_valid_proof, ready_delay_ms, willing_to_sign, Node};
use crate::sim::schedule::EntropyBlock;
use crate::sim::track::SpoolHoldings;
use crate::types::{GroupPosition, RoundNumber};

/// One observer's timing and verdict for a single challenge round.
pub struct ObserverArrival {
    /// One-way proof arrival at this observer in milliseconds from production; at
    /// the challenged position this is the prover's own local readiness.
    pub arrival_ms: f64,
    /// Whether a valid proof landed by the proof deadline, always false at the
    /// challenged position which does not witness itself.
    pub witnessed: bool,
    /// Whether this observer signed an attestation for the round.
    pub signed: bool,
}

/// What one challenge round produced, enough for the epoch loop to score it.
pub struct RoundOutcome {
    /// Milliseconds from production until the prover's proof is ready.
    pub ready_ms: f64,
    /// One entry per committee position, indexed the same as the committee.
    pub observers: Vec<ObserverArrival>,
    /// Distinct in-time willing signers gathered for the round.
    pub signer_count: usize,
    /// Whether a certificate formed and verified.
    pub certificate_formed: bool,
    /// Milliseconds from production until the certificate is standing evidence.
    pub certificate_ms: Option<f64>,
}

/// How far a proof travels after the challenged owner sends it
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayPolicy {
    /// The owner is the only sender, so a withheld peer never sees the proof
    Direct,
    /// One relay layer, the shape Alpenglow's rotor uses
    SingleHop,
    /// Every verified holder forwards, giving shortest-path arrival
    Flood,
}

impl RelayPolicy {
    /// Short label for reports
    pub fn label(&self) -> &'static str {
        match self {
            RelayPolicy::Direct => "direct",
            RelayPolicy::SingleHop => "single-hop",
            RelayPolicy::Flood => "flood",
        }
    }

    /// Links a proof may traverse under this policy
    fn hop_limit(&self, group_size: usize) -> usize {
        match self {
            RelayPolicy::Direct => 1,
            RelayPolicy::SingleHop => 2,
            RelayPolicy::Flood => group_size.saturating_sub(1).max(1),
        }
    }
}

/// Earliest arrival at every position once the group relays the proof onward
///
/// The owner sends only where it chooses, but a relayed proof reaches peers it
/// withheld from, so withholding costs a hop rather than excluding anyone.
/// Infinite means the proof never arrived at all.
pub fn relay_arrivals(
    group: &Group,
    origin: usize,
    ready_ms: f64,
    direct: &[bool],
    hop_ms: f64,
    policy: RelayPolicy,
) -> Vec<f64> {
    let size = group.len();
    let mut arrival = vec![f64::INFINITY; size];
    arrival[origin] = ready_ms;

    for _ in 0..policy.hop_limit(size) {
        let holders = arrival.clone();
        let mut changed = false;
        for (holder, held_at) in holders.iter().enumerate() {
            if !held_at.is_finite() {
                continue;
            }
            for peer in 0..size {
                if peer == holder || peer == origin {
                    continue;
                }
                if holder == origin && !direct[peer] {
                    continue;
                }
                let candidate = held_at + group.one_way_ms(holder, peer) + hop_ms;
                if candidate < arrival[peer] {
                    arrival[peer] = candidate;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    arrival
}

/// Run one real challenge round against a single challenged position.
#[allow(clippy::too_many_arguments)]
pub fn run_round(
    committee: &[Node],
    group: &Group,
    holdings: &SpoolHoldings,
    costs: &Costs,
    entropy: &EntropyBlock,
    epoch: EpochNumber,
    round: RoundNumber,
    target_position: GroupPosition,
    config: &EpochConfig,
) -> Result<RoundOutcome> {
    let size = committee.len();
    let target = committee
        .get(target_position.as_usize())
        .ok_or_else(|| anyhow!("target position {target_position} out of range"))?;
    let schedule = &config.schedule;

    // The seed binds the entropy block hash, epoch, group, round, and challenged
    // spool, so no spool can know its sample before the block is produced.
    let seed = round_seed(
        epoch.as_u64(),
        GROUP_ID,
        round.as_u64(),
        target_position.as_u64(),
        &entropy.hash,
    );
    let sample = holdings.sample(target_position, &seed);
    let proof = holdings.prove(sample)?;
    // Fixed for the whole round, so it verifies once rather than per observer.
    let proof_verified = holdings.verify(sample, &proof);

    let is_valid = produces_valid_proof(target.behavior, round);
    let ready = ready_delay_ms(target.behavior, target_position, group, costs, config.edge_rtt_ms);

    let deadline_ms = schedule.proof_deadline_ms();
    let confirmation_ms = schedule.confirmation_ms();
    let window_close_ms = confirmation_ms + schedule.attestation_window_ms();

    let mut sends_to: Vec<bool> = Vec::with_capacity(size);
    for observer_index in 0..size {
        let observer_position = GroupPosition::new(observer_index);
        sends_to.push(
            observer_index == target_position.as_usize()
                || delivers(target.behavior, round, observer_position, &entropy.hash, &config.behavior),
        );
    }
    let arrivals = relay_arrivals(
        group,
        target_position.as_usize(),
        ready + costs.proof_gen_ms,
        &sends_to,
        costs.verify_ms,
        config.relay,
    );

    let mut observers: Vec<ObserverArrival> = Vec::with_capacity(size);
    let mut signer_pubkeys: Vec<BlsPubkey> = Vec::new();
    let mut signer_secret_keys: Vec<BlsPrivateKey> = Vec::new();
    let mut signer_gathers: Vec<f64> = Vec::new();

    for (observer_index, observer) in committee.iter().enumerate() {
        let is_target = observer_index == target_position.as_usize();
        let arrival_ms = arrivals[observer_index];
        let delivered = arrival_ms.is_finite();
        let in_time = arrival_ms <= deadline_ms;

        // Gates this observer's signature. Scoring reads the certificate.
        let witnessed = !is_target && is_valid && delivered && proof_verified && in_time;

        // The challenged position needs no special case: its arrival is its own
        // readiness and its return hop is the zero diagonal.
        let signed = willing_to_sign(observer, target, is_valid, delivered) && (is_target || in_time);
        if signed {
            let sign_start = confirmation_ms.max(arrival_ms);
            if sign_start <= window_close_ms {
                let gather = sign_start
                    + costs.sign_ms
                    + group.one_way_ms(observer_index, target_position.as_usize());
                signer_pubkeys.push(observer.public_key);
                signer_secret_keys.push(observer.secret_key);
                signer_gathers.push(gather);
            }
        }
        observers.push(ObserverArrival {
            arrival_ms,
            witnessed,
            signed,
        });
    }

    let signer_count = signer_pubkeys.len();
    let certificate_formed = if signer_count < config.threshold {
        false
    } else if real_bls(config, round, target_position, size) {
        let message = attestation_message(
            epoch.as_u64(),
            GROUP_ID,
            round.as_u64(),
            target_position.as_u64(),
            entropy.slot.as_u64(),
            &entropy.hash,
        );
        let partials: Vec<BlsSignature> = signer_secret_keys
            .iter()
            .map(|secret| sign(secret, &message))
            .collect::<Result<_>>()?;
        let aggregate_signature = aggregate(&partials)?;
        verify_certificate(&aggregate_signature, &message, &signer_pubkeys)
    } else {
        true
    };

    let certificate_ms = if certificate_formed {
        signer_gathers.sort_by(f64::total_cmp);
        signer_gathers
            .get(config.threshold.saturating_sub(1))
            .map(|quorum| quorum + schedule.certificate_gossip_ms())
    } else {
        None
    };

    Ok(RoundOutcome {
        ready_ms: ready,
        observers,
        signer_count,
        certificate_formed,
        certificate_ms,
    })
}

/// Whether this target runs real BLS this round given the mode.
fn real_bls(config: &EpochConfig, round: RoundNumber, target_position: GroupPosition, size: usize) -> bool {
    match config.bls_mode {
        BlsMode::Full => true,
        BlsMode::Sampled => target_position.as_usize() == (round.as_usize() % size.max(1)),
        BlsMode::Off => false,
    }
}
