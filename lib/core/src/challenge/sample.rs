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

use serde::{Deserialize, Serialize};
use tape_crypto::Address;

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use tape_crypto::hash::{Hash, hashv};

use crate::erasure::sub_leaf_count;
use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex, StorageUnits};

/// Domain tag separating a round seed from every other hash in the protocol.
const ROUND_SEED_DOMAIN: &[u8] = b"WHIRLWIND_ROUND";

/// What a track contributes to its group's sample set.
///
/// A coded track contributes one entry per sample leaf of the slice at this
/// spool's position, so the draw over coded data is byte-weighted. An inline
/// track is one bounded entry however large it is: every owner keeps the whole
/// payload rather than a slice, so there is nothing inside it to index.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub enum EntryKind {
    /// A slice of a coded track, of this byte length.
    Coded { slice_len: StorageUnits },
    /// A complete inline payload.
    Inline,
}

impl EntryKind {
    /// Sample leaves this entry puts into the draw.
    pub fn leaves(&self) -> u64 {
        match self {
            EntryKind::Coded { slice_len } => sub_leaf_count(slice_len.as_usize()) as u64,
            EntryKind::Inline => 1,
        }
    }
}

/// One track a spool is responsible for, and what it weighs in the draw.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleEntry {
    /// Address of the track.
    pub track: Address,
    /// What the track contributes.
    pub kind: EntryKind,
}

/// The sample leaf a round asks one spool for.
///
/// It names no spool: the caller already knows which one it seeded the draw with,
/// and carrying it here only creates a value that can disagree with that seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Track the sample belongs to.
    pub track: Address,
    /// What the round is asking for.
    pub leaf: SampleLeaf,
}

/// The thing an owner has to produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleLeaf {
    /// One sample leaf of the slice, by index.
    Coded { sub_leaf: usize },
    /// The whole inline payload.
    Inline,
}

/// Seed for one round against one spool.
///
/// The entropy block's hash combined with the epoch, group, round and spool, as
/// the mechanism specifies. No owner can predict its sample before the block
/// exists, binding the spool means two owners challenged off the same block read
/// different data, and binding the round means a response cannot be replayed into
/// a later one.
pub fn round_seed(
    entropy: &Hash,
    epoch: EpochNumber,
    group: GroupIndex,
    round: RoundNumber,
    spool: SpoolIndex,
) -> Hash {
    hashv(&[
        ROUND_SEED_DOMAIN,
        entropy.as_ref(),
        &epoch.as_u64().to_le_bytes(),
        &group.as_u64().to_le_bytes(),
        &round.as_u64().to_le_bytes(),
        &spool.as_u64().to_le_bytes(),
    ])
}

/// Put entries in the canonical order every challenger has to agree on.
pub fn sort_entries(entries: &mut [SampleEntry]) {
    entries.sort_unstable_by_key(|entry| entry.track);
}

/// Total sample leaves across every entry, the space the draw is uniform over.
pub fn sample_space(entries: &[SampleEntry]) -> u64 {
    entries.iter().map(|entry| entry.kind.leaves()).sum()
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
        let leaves = entry.kind.leaves();
        if index < leaves {
            return Some(Sample {
                track: entry.track,
                leaf: match entry.kind {
                    EntryKind::Coded { .. } => SampleLeaf::Coded {
                        sub_leaf: index as usize,
                    },
                    EntryKind::Inline => SampleLeaf::Inline,
                },
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
            kind: EntryKind::Coded {
                slice_len: StorageUnits::from_bytes((leaves * SUB_LEAF_BYTES) as u64),
            },
        }
    }

    fn inline_entry(byte: u8) -> SampleEntry {
        SampleEntry {
            track: track(byte),
            kind: EntryKind::Inline,
        }
    }

    fn seed_for(round: u64) -> Hash {
        round_seed(
            &hashv(&[b"entropy", &round.to_le_bytes()]),
            EpochNumber(7),
            GroupIndex(1),
            RoundNumber(round),
            SpoolIndex(3),
        )
    }

    // the seed changes with every round coordinate it binds
    #[test]
    fn seed_binds() {
        let entropy = hashv(&[b"block"]);
        let base = round_seed(&entropy, EpochNumber(1), GroupIndex(2), RoundNumber(3), SpoolIndex(4));

        let variants = [
            round_seed(&hashv(&[b"other"]), EpochNumber(1), GroupIndex(2), RoundNumber(3), SpoolIndex(4)),
            round_seed(&entropy, EpochNumber(9), GroupIndex(2), RoundNumber(3), SpoolIndex(4)),
            round_seed(&entropy, EpochNumber(1), GroupIndex(9), RoundNumber(3), SpoolIndex(4)),
            round_seed(&entropy, EpochNumber(1), GroupIndex(2), RoundNumber(9), SpoolIndex(4)),
            round_seed(&entropy, EpochNumber(1), GroupIndex(2), RoundNumber(3), SpoolIndex(9)),
        ];
        for variant in variants {
            assert_ne!(base, variant);
        }
    }

    // a spool holding nothing, or nothing with bytes in it, draws no question
    #[test]
    fn empty_spool() {
        assert!(draw(&seed_for(0), &[]).is_none());
        assert!(draw(&seed_for(0), &[entry(1, 0)]).is_none());
    }

    // every draw names a leaf the drawn track actually holds
    #[test]
    fn draw_in_range() {
        let entries = [entry(1, 10), entry(2, 5), entry(3, 1)];
        for round in 0..500u64 {
            let sample = draw(&seed_for(round), &entries).expect("a drawn sample");
            let held = entries.iter().find(|entry| entry.track == sample.track).expect("the drawn entry");
            let SampleLeaf::Coded { sub_leaf } = sample.leaf else {
                panic!("a coded entry drew an inline sample");
            };
            assert!(sub_leaf < held.kind.leaves() as usize);
        }
    }

    // a different block asks a different question, or a target could keep one
    // leaf and answer forever
    #[test]
    fn follows_entropy() {
        let entries = [entry(1, 10), entry(2, 5)];
        let asked: Vec<usize> = [1u8, 2, 3, 4]
            .into_iter()
            .map(|byte| {
                let seed = round_seed(
                    &Hash::from([byte; 32]),
                    EpochNumber(7),
                    GroupIndex(1),
                    RoundNumber(5),
                    SpoolIndex(3),
                );
                match draw(&seed, &entries).expect("a drawn sample").leaf {
                    SampleLeaf::Coded { sub_leaf } => sub_leaf,
                    SampleLeaf::Inline => usize::MAX,
                }
            })
            .collect();
        assert!(asked.iter().any(|leaf| *leaf != asked[0]), "asked {asked:?}");
    }

    // an inline track is one entry whatever it holds, since every owner keeps
    // the whole payload and there is nothing inside it to index
    #[test]
    fn inline_weighs_one() {
        assert_eq!(sample_space(&[inline_entry(1)]), 1);
        assert_eq!(sample_space(&[inline_entry(1), inline_entry(2)]), 2);
        assert_eq!(sample_space(&[entry(3, 10), inline_entry(1)]), 11);

        let drawn = draw(&seed_for(0), &[inline_entry(1)]).expect("a drawn sample");
        assert_eq!(drawn.leaf, SampleLeaf::Inline);
        assert_eq!(drawn.track, track(1));
    }

    // a coded track of ten leaves outdraws one inline entry about ten to one
    #[test]
    fn coded_outweighs_inline() {
        let entries = [entry(1, 10), inline_entry(2)];
        let mut coded = 0u32;
        for round in 0..2_000u64 {
            if draw(&seed_for(round), &entries).expect("a drawn sample").track == track(1) {
                coded += 1;
            }
        }
        let ratio = coded as f64 / (2_000 - coded) as f64;
        assert!(ratio > 7.0 && ratio < 14.0, "ratio {ratio}");
    }

    // a track with ten times the leaves is drawn about ten times as often
    #[test]
    fn byte_weighted() {
        let entries = [entry(1, 100), entry(2, 10)];
        let mut hits = [0u32; 2];
        for round in 0..4_000u64 {
            let sample = draw(&seed_for(round), &entries).expect("a drawn sample");
            hits[usize::from(sample.track == track(2))] += 1;
        }
        let ratio = hits[0] as f64 / hits[1] as f64;
        assert!(ratio > 7.0 && ratio < 14.0, "ratio {ratio}");
    }

    // the short last leaf of a slice is sampled like any other, or the tail of
    // every slice goes uninspected
    #[test]
    fn partial_leaf() {
        let short = SampleEntry {
            track: track(9),
            kind: EntryKind::Coded {
                slice_len: StorageUnits::from_bytes((SUB_LEAF_BYTES * 2 + 1) as u64),
            },
        };
        assert_eq!(sample_space(&[short]), 3);
        let mut saw_last = false;
        for round in 0..200u64 {
            if draw(&seed_for(round), &[short]).expect("a drawn sample").leaf
                == (SampleLeaf::Coded { sub_leaf: 2 })
            {
                saw_last = true;
            }
        }
        assert!(saw_last);
    }

    // reordering the entries changes the draw, which is why two challengers of
    // one spool have to sort before they enumerate
    #[test]
    fn entry_order() {
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
