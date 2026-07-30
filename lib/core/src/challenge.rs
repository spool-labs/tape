//! The sample one storage challenge round draws from a spool's assigned data.
//!
//! Only the challenger runs this. The target answers bare coordinates and never
//! re-derives the draw, so what has to agree is two challengers of the same spool,
//! not the two sides of one round. Nothing here touches storage, so an owner
//! cannot shrink its own sample set by dropping the data it lost.
//!
//! The draw is uniform over sample leaves rather than over tracks, which makes it
//! byte-weighted: a track spanning more leaves is proportionally more likely to be
//! inspected. Entries must arrive in track-address order; `sort_entries` puts them
//! there for a caller whose source does not already guarantee it.

use tape_crypto::Address;
use tape_crypto::hash::{Hash, hashv};

use crate::erasure::sub_leaf_count;
use crate::types::{EpochNumber, SlotNumber, SpoolIndex, StorageUnits};

/// Domain tag separating a round seed from every other hash in the protocol.
const ROUND_SEED_DOMAIN: &[u8] = b"WHIRLWIND_ROUND";

/// One track a spool is responsible for, and the length of the slice it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleEntry {
    /// Address of the track the slice belongs to.
    pub track: Address,
    /// Byte length of the slice at this spool's position.
    pub slice_len: StorageUnits,
}

/// The sample leaf a round asks one spool for.
///
/// It names no spool: the caller already knows which one it seeded the draw with,
/// and carrying it here only creates a value that can disagree with that seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Track the sampled leaf belongs to.
    pub track: Address,
    /// Index of the sample leaf inside that slice.
    pub sub_leaf: usize,
}

/// Seed for one round against one spool.
///
/// The entropy is a finalized block hash, so no owner can predict its sample
/// before that block exists. Binding the spool means two owners challenged off
/// the same block read different data, and binding epoch and slot means a
/// response cannot be replayed into a later round.
pub fn round_seed(
    entropy: &Hash,
    epoch: EpochNumber,
    slot: SlotNumber,
    spool: SpoolIndex,
) -> Hash {
    hashv(&[
        ROUND_SEED_DOMAIN,
        entropy.as_ref(),
        &epoch.as_u64().to_le_bytes(),
        &slot.as_u64().to_le_bytes(),
        &spool.as_u64().to_le_bytes(),
    ])
}

/// Put entries in the canonical order every challenger has to agree on.
pub fn sort_entries(entries: &mut [SampleEntry]) {
    entries.sort_unstable_by_key(|entry| entry.track);
}

/// Total sample leaves across every entry, the space the draw is uniform over.
fn sample_space(entries: &[SampleEntry]) -> u64 {
    entries
        .iter()
        .map(|entry| sub_leaf_count(entry.slice_len.as_usize()) as u64)
        .sum()
}

/// Draw one sample leaf for a spool from the round seed.
///
/// Returns None when the spool holds nothing, which is a spool with no assigned
/// data rather than a failure to answer.
pub fn draw(seed: &Hash, entries: &[SampleEntry]) -> Option<Sample> {
    let total = sample_space(entries);
    if total == 0 {
        return None;
    }

    // The seed is a hash, so any 8 bytes are uniform. The residual modulo bias is
    // the sample space over 2^64, far below anything a round could observe.
    let mut head = [0u8; 8];
    head.copy_from_slice(&seed.as_ref()[..8]);
    let mut index = u64::from_le_bytes(head) % total;

    for entry in entries {
        let leaves = sub_leaf_count(entry.slice_len.as_usize()) as u64;
        if index < leaves {
            return Some(Sample {
                track: entry.track,
                sub_leaf: index as usize,
            });
        }
        index -= leaves;
    }

    // Unreachable while the loop sums to the same total sample_space returned.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::SUB_LEAF_BYTES;

    fn track(byte: u8) -> Address {
        Address::from([byte; 32])
    }

    fn entry(byte: u8, leaves: usize) -> SampleEntry {
        SampleEntry {
            track: track(byte),
            slice_len: StorageUnits::from_bytes((leaves * SUB_LEAF_BYTES) as u64),
        }
    }

    fn seed_for(round: u64) -> Hash {
        round_seed(
            &hashv(&[b"entropy", &round.to_le_bytes()]),
            EpochNumber(7),
            SlotNumber(round),
            SpoolIndex(3),
        )
    }

    #[test]
    fn the_seed_binds_every_round_coordinate() {
        let entropy = hashv(&[b"block"]);
        let base = round_seed(&entropy, EpochNumber(1), SlotNumber(2), SpoolIndex(3));
        assert_ne!(base, round_seed(&hashv(&[b"other"]), EpochNumber(1), SlotNumber(2), SpoolIndex(3)));
        assert_ne!(base, round_seed(&entropy, EpochNumber(2), SlotNumber(2), SpoolIndex(3)));
        assert_ne!(base, round_seed(&entropy, EpochNumber(1), SlotNumber(3), SpoolIndex(3)));
        assert_ne!(base, round_seed(&entropy, EpochNumber(1), SlotNumber(2), SpoolIndex(4)));
    }

    #[test]
    fn an_empty_spool_draws_nothing() {
        assert!(draw(&seed_for(0), &[]).is_none());
        assert!(draw(&seed_for(0), &[entry(1, 0)]).is_none());
    }

    #[test]
    fn every_draw_lands_inside_its_track() {
        let entries = [entry(1, 10), entry(2, 5), entry(3, 1)];
        for round in 0..500u64 {
            let sample = draw(&seed_for(round), &entries).unwrap();
            let held = entries.iter().find(|e| e.track == sample.track).unwrap();
            assert!(sample.sub_leaf < sub_leaf_count(held.slice_len.as_usize()));
        }
    }

    #[test]
    fn the_sample_follows_the_entropy() {
        // A different block has to ask a different question, or a target could
        // retain one leaf and answer forever.
        let entries = [entry(1, 10), entry(2, 5)];
        let asked: Vec<usize> = [1u8, 2, 3, 4]
            .into_iter()
            .map(|byte| {
                let seed = round_seed(
                    &Hash::from([byte; 32]),
                    EpochNumber(7),
                    SlotNumber(1_000),
                    SpoolIndex(3),
                );
                draw(&seed, &entries).unwrap().sub_leaf
            })
            .collect();
        assert!(asked.iter().any(|leaf| *leaf != asked[0]), "asked {asked:?}");
    }

    #[test]
    fn the_draw_is_byte_weighted() {
        // A track with ten times the leaves is drawn about ten times as often.
        let entries = [entry(1, 100), entry(2, 10)];
        let mut hits = [0u32; 2];
        for round in 0..4_000u64 {
            let sample = draw(&seed_for(round), &entries).unwrap();
            hits[usize::from(sample.track == track(2))] += 1;
        }
        let ratio = hits[0] as f64 / hits[1] as f64;
        assert!(ratio > 7.0 && ratio < 14.0, "ratio {ratio}");
    }

    #[test]
    fn a_trailing_partial_leaf_is_still_drawable() {
        // The last leaf of a slice is short, and it must be sampled like any
        // other or the tail of every slice goes uninspected.
        let short = SampleEntry {
            track: track(9),
            slice_len: StorageUnits::from_bytes((SUB_LEAF_BYTES * 2 + 1) as u64),
        };
        assert_eq!(sample_space(&[short]), 3);
        let mut saw_last = false;
        for round in 0..200u64 {
            if draw(&seed_for(round), &[short]).unwrap().sub_leaf == 2 {
                saw_last = true;
            }
        }
        assert!(saw_last);
    }

    #[test]
    fn the_order_of_entries_decides_the_answer() {
        // Two challengers of the same spool must enumerate the same way, so this
        // pins that reordering the input changes the draw.
        let mut forward = [entry(1, 10), entry(2, 10)];
        let mut reversed = [entry(2, 10), entry(1, 10)];
        let seed = seed_for(1);
        assert_ne!(
            draw(&seed, &forward),
            draw(&seed, &reversed)
        );
        sort_entries(&mut forward);
        sort_entries(&mut reversed);
        assert_eq!(
            draw(&seed, &forward),
            draw(&seed, &reversed)
        );
    }
}
