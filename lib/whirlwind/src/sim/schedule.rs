//! The per-round slot schedule and the deterministic entropy block.
//!
//! Round times follow from finalized epoch state and are fixed in Solana slots at
//! epoch start, so every owner computes the same schedule and nobody adjusts it.
//! Each round points to a short span of future slots. The entropy block is the
//! first slot in that span to produce a block that later finalizes; its hash
//! seeds the round. If no slot in the span produces a finalized block the round
//! is void and counts against nobody. A produced block is assumed to finalize,
//! which is realistic for these short spans, so one per-slot probability models
//! both production and the finalized outcome the responses are verified against.

use anyhow::{ensure, Result};
use serde::Serialize;
use tape_crypto::hash::{hashv, Hash};

use crate::types::{EpochNumber, RoundNumber, SlotCount, SlotNumber};

/// Domain tag for the per-round entropy block hash standing in for a real block.
const ENTROPY_DOMAIN: &[u8] = b"WHRLWEB1";
/// Domain tag for the per-slot block production coin.
const PRODUCE_DOMAIN: &[u8] = b"WHRLWPR1";

/// The cadence bounds the node itself runs on, so a change there moves the model
/// with it rather than leaving two copies of one number to drift apart.
pub use tape_core::challenge::schedule::{MAINNET_CADENCE_SLOTS, TARGET_ROUNDS_PER_EPOCH};

/// The cadence for an epoch of the given length
///
/// Short epochs divide down to hold enough rounds, long ones cap at the mainnet
/// cadence, and neither goes below a single round width.
pub fn cadence_for_epoch(epoch_slots: u64, round_width_slots: u64) -> u64 {
    let fitted = epoch_slots / TARGET_ROUNDS_PER_EPOCH.max(1);
    MAINNET_CADENCE_SLOTS.min(fitted).max(round_width_slots)
}

/// When a proof must arrive, in whole slots or in a sub-slot value only the
/// sweep can use because the slot clock cannot schedule it
#[derive(Clone, Copy, Debug, Serialize)]
pub enum ProofDeadline {
    /// Slots after the entropy block slot, the schedulable form.
    Slots(SlotCount),
    /// Milliseconds after production, unschedulable on the slot clock.
    CounterfactualMs(f64),
}

impl ProofDeadline {
    /// The deadline in milliseconds after production.
    pub fn as_ms(&self, slot_ms: f64) -> f64 {
        match self {
            ProofDeadline::Slots(slots) => slots.as_f64() * slot_ms,
            ProofDeadline::CounterfactualMs(ms) => *ms,
        }
    }

    /// Short label for reports, naming the counterfactual form as such.
    pub fn label(&self) -> String {
        match self {
            ProofDeadline::Slots(slots) => format!("{slots} slots"),
            ProofDeadline::CounterfactualMs(ms) => format!("{ms:.0} ms counterfactual"),
        }
    }
}

/// The slot schedule and block production model, fixed for a whole run.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Schedule {
    /// Solana slot time in milliseconds.
    pub slot_ms: f64,
    /// Slots between the start of one round and the start of the next.
    pub round_interval_slots: SlotCount,
    /// Slots in each round's entropy span.
    pub span_slots: SlotCount,
    /// When a proof must arrive, counted from the entropy block's production.
    pub proof_deadline: ProofDeadline,
    /// Slots after production at which the signature period opens on confirmation.
    pub confirmation_slots: SlotCount,
    /// Slots the signature period stays open after confirmation.
    pub attestation_window_slots: SlotCount,
    /// Slots for an aggregated certificate to gossip to the group.
    pub certificate_gossip_slots: SlotCount,
    /// Probability a scheduled slot produces a block that later finalizes.
    pub block_production_probability: f64,
}

/// One round's entropy block: the slot it was produced at and its hash.
#[derive(Clone, Copy, Debug)]
pub struct EntropyBlock {
    /// Absolute slot the entropy block was produced at.
    pub slot: SlotNumber,
    /// The entropy block hash that seeds every spool's sample this round.
    pub hash: Hash,
}

impl Schedule {
    /// The proof deadline in milliseconds after production.
    pub fn proof_deadline_ms(&self) -> f64 {
        self.proof_deadline.as_ms(self.slot_ms)
    }

    /// When the signature period opens, in milliseconds after production.
    pub fn confirmation_ms(&self) -> f64 {
        self.confirmation_slots.as_f64() * self.slot_ms
    }

    /// How long the signature period stays open, in milliseconds.
    pub fn attestation_window_ms(&self) -> f64 {
        self.attestation_window_slots.as_f64() * self.slot_ms
    }

    /// Certificate gossip time in milliseconds.
    pub fn certificate_gossip_ms(&self) -> f64 {
        self.certificate_gossip_slots.as_f64() * self.slot_ms
    }

    /// Wall-clock gap between the start of one round and the start of the next.
    pub fn round_interval_ms(&self) -> f64 {
        self.round_interval_slots.as_f64() * self.slot_ms
    }

    /// Slots from a round opening to its certificate having gossiped
    pub fn round_width_slots(&self) -> u64 {
        let signing_ms = self.confirmation_ms() + self.attestation_window_ms();
        let tail_ms = self.proof_deadline_ms().max(signing_ms) + self.certificate_gossip_ms();
        let tail_slots = (tail_ms / self.slot_ms.max(1.0)).ceil() as u64;
        self.span_slots.as_u64() + tail_slots
    }

    /// Reject a schedule whose rounds would overlap their neighbours.
    pub fn validate(&self) -> Result<()> {
        let interval = self.round_interval_slots.as_u64();
        let width = self.round_width_slots();
        ensure!(
            interval >= width,
            "round interval {interval} slots is shorter than the {width} slot round width",
        );
        Ok(())
    }

    /// First slot of a round's span. Rounds sit on a fixed grid at the interval,
    /// so the whole schedule follows from the epoch and nobody adjusts it.
    pub fn round_base_slot(
        &self,
        epoch: EpochNumber,
        round: RoundNumber,
        rounds_per_epoch: u64,
    ) -> SlotNumber {
        let interval = self.round_interval_slots.as_u64().max(1);
        SlotNumber((epoch.as_u64() * rounds_per_epoch + round.as_u64()) * interval)
    }

    /// The round's entropy block, or none when the whole span produced no block
    /// and the round is void.
    pub fn entropy_block(
        &self,
        base_seed: u64,
        epoch: EpochNumber,
        group: u64,
        round: RoundNumber,
        rounds_per_epoch: u64,
    ) -> Option<EntropyBlock> {
        let base = self.round_base_slot(epoch, round, rounds_per_epoch);
        for offset in 0..self.span_slots.as_u64().max(1) {
            let slot = SlotNumber(base.as_u64() + offset);
            if produced(base_seed, epoch, round, slot, self.block_production_probability) {
                let hash = entropy_hash(base_seed, epoch, group, round, slot);
                return Some(EntropyBlock { slot, hash });
            }
        }
        None
    }
}

/// Whether a scheduled slot produced a block that finalizes, a deterministic coin.
fn produced(
    base_seed: u64,
    epoch: EpochNumber,
    round: RoundNumber,
    slot: SlotNumber,
    probability: f64,
) -> bool {
    let digest = hashv(&[
        PRODUCE_DOMAIN,
        &base_seed.to_le_bytes(),
        &epoch.as_u64().to_le_bytes(),
        &round.as_u64().to_le_bytes(),
        &slot.as_u64().to_le_bytes(),
    ]);
    coin(&digest) < probability
}

/// The entropy block hash for a produced slot, the block-hash stand-in that a
/// spool cannot know before the block is produced because the slot itself depends
/// on the production coin.
fn entropy_hash(
    base_seed: u64,
    epoch: EpochNumber,
    group: u64,
    round: RoundNumber,
    slot: SlotNumber,
) -> Hash {
    hashv(&[
        ENTROPY_DOMAIN,
        &base_seed.to_le_bytes(),
        &epoch.as_u64().to_le_bytes(),
        &group.to_le_bytes(),
        &round.as_u64().to_le_bytes(),
        &slot.as_u64().to_le_bytes(),
    ])
}

/// A deterministic coin in the unit interval from the first eight digest bytes.
fn coin(digest: &Hash) -> f64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest.0[..8]);
    u64::from_le_bytes(bytes) as f64 / u64::MAX as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(probability: f64, span: u64) -> Schedule {
        Schedule {
            slot_ms: 400.0,
            round_interval_slots: SlotCount::new(150),
            span_slots: SlotCount::new(span),
            proof_deadline: ProofDeadline::Slots(SlotCount::new(2)),
            confirmation_slots: SlotCount::new(1),
            attestation_window_slots: SlotCount::new(4),
            certificate_gossip_slots: SlotCount::new(2),
            block_production_probability: probability,
        }
    }

    #[test]
    fn interval_grid() {
        let schedule = schedule(0.95, 4);
        let first = schedule.round_base_slot(EpochNumber(0), RoundNumber::new(0), 48);
        let second = schedule.round_base_slot(EpochNumber(0), RoundNumber::new(1), 48);
        assert_eq!(second - first, SlotNumber(150));
        let next_epoch = schedule.round_base_slot(EpochNumber(1), RoundNumber::new(0), 48);
        assert_eq!(next_epoch, SlotNumber(48 * 150));
    }

    #[test]
    fn preset_cadence() {
        let width = schedule(0.95, 4).round_width_slots();
        let rounds = |epoch_seconds: u64| {
            let epoch_slots = epoch_seconds * 1000 / 400;
            let interval = cadence_for_epoch(epoch_slots, width);
            assert!(interval >= width, "cadence {interval} under the {width} slot round width");
            epoch_slots / interval
        };
        assert_eq!(rounds(604_800), 10_080); // mainnet, one week
        assert_eq!(rounds(3_600), 60); // devnet, one hour
        assert_eq!(rounds(100), 4); // localnet
        assert_eq!(rounds(20), 4); // simnet

        // simnet at its shortest epoch stays legal and only loses rounds
        assert_eq!(rounds(10), 2);
    }

    #[test]
    fn interval_width() {
        // Span 4, signing open through confirmation plus window, then gossip.
        let schedule = schedule(0.95, 4);
        assert_eq!(schedule.round_width_slots(), 11);
        assert!(schedule.validate().is_ok());

        let mut tight = schedule;
        tight.round_interval_slots = SlotCount::new(10);
        assert!(tight.validate().is_err());
    }

    #[test]
    fn certain_production() {
        let schedule = schedule(1.0, 4);
        for round in 0..100 {
            assert!(schedule.entropy_block(1, EpochNumber(0), 0, RoundNumber::new(round), 48).is_some());
        }
    }

    #[test]
    fn no_production() {
        let schedule = schedule(0.0, 4);
        for round in 0..100 {
            assert!(schedule.entropy_block(1, EpochNumber(0), 0, RoundNumber::new(round), 48).is_none());
        }
    }

    #[test]
    fn entropy_slot() {
        let schedule = schedule(0.5, 6);
        for round in 0..200 {
            let round = RoundNumber::new(round);
            if let Some(block) = schedule.entropy_block(7, EpochNumber(2), 0, round, 48) {
                let base = schedule.round_base_slot(EpochNumber(2), round, 48);
                assert!(block.slot >= base && block.slot < base + SlotNumber(6));
            }
        }
    }
}
