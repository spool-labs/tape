//! Encode an epoch's `SnapshotLog` into erasure-coded chunk tracks. Inverse of
//! `decode`. Pure: no transport, no consensus, no store, no on-chain types — the
//! caller supplies the snapshot tape address and the active group count.

use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::GroupIndex;
use tape_core::track::blob::BlobInfo;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{ChunkNumber, EpochNumber, StorageUnits, StripeCount, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
use tape_slicer::{num_stripes, ErasureCoder, OuterCoder, Slicer};

use crate::chunk::{
    pack_segment, snapshot_chunk_key, snapshot_max_segment_bytes, snapshot_outer_k,
    SnapshotChunkPayload,
};
use crate::SnapshotError;

/// One Clay-encoded outer symbol: its derived blob and the GROUP_SIZE slices.
#[derive(Debug, Clone)]
pub struct BuiltChunk {
    pub group: GroupIndex,
    pub chunk: ChunkNumber,
    pub blob: BlobInfo,
    pub slices: [Vec<u8>; GROUP_SIZE],
}

/// One encoded snapshot chunk: the committed track metadata plus its blob and
/// slices. A node persists the slice for the spool it owns; an offline encoder
/// can keep them all.
#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub group: GroupIndex,
    pub chunk: ChunkNumber,
    pub track: CompressedTrack,
    pub blob: BlobInfo,
    pub slices: [Vec<u8>; GROUP_SIZE],
}

/// Encode `log` into the full chunk-track set for `epoch`'s snapshot. The output
/// is deterministic from `(snapshot_tape, epoch, log, total_groups)` and matches
/// the set committed by the on-chain snapshot tape.
pub fn encode_snapshot(
    snapshot_tape: Address,
    epoch: EpochNumber,
    log: &SnapshotLog,
    total_groups: usize,
) -> Result<Vec<SnapshotChunk>, SnapshotError> {
    let outer_k = snapshot_outer_k(total_groups);
    if outer_k == 0 {
        return Err(SnapshotError::NoGroups { epoch: epoch.0 });
    }

    let serialized = log
        .to_bytes()
        .map_err(|e| SnapshotError::Serialize(e.to_string()))?;
    let compressed = lz4_flex::compress_prepend_size(&serialized);

    let max_segment_bytes = snapshot_max_segment_bytes(total_groups);
    let chunk_total = compressed.len().div_ceil(max_segment_bytes).max(1);
    let chunk_size = compressed.len().div_ceil(chunk_total).max(1);

    let mut outer = OuterCoder::new(outer_k, total_groups);
    let mut chunks = Vec::with_capacity(chunk_total * total_groups);

    for chunk_index in 0..chunk_total {
        let start = chunk_index * chunk_size;
        let end = start.saturating_add(chunk_size).min(compressed.len());

        let packed = pack_segment(&compressed[start..end]);
        let symbols = outer
            .encode(&packed)
            .map_err(|e| SnapshotError::OuterEncode(format!("segment={chunk_index}: {e}")))?;

        let chunk = ChunkNumber(chunk_index as u64);

        for (group_index, symbol) in symbols.iter().enumerate() {
            let group = GroupIndex(group_index as u64);
            let built = encode_chunk(epoch, group, chunk, symbol)?;
            let track_number = TrackNumber((chunk_index as u64) * (total_groups as u64) + group.0);

            let track = CompressedTrack {
                tape: snapshot_tape,
                track_number,
                key: snapshot_chunk_key(epoch, group, chunk),
                kind: TrackKind::Blob as u64,
                state: TrackState::Certified as u64,
                size: built.blob.size,
                group,
                value_hash: built.blob.get_hash(),
            };

            chunks.push(SnapshotChunk {
                group,
                chunk,
                track,
                blob: built.blob,
                slices: built.slices,
            });
        }
    }

    Ok(chunks)
}

/// Clay-encode one outer symbol into its GROUP_SIZE slices and derive its blob.
pub fn encode_chunk(
    epoch: EpochNumber,
    group: GroupIndex,
    chunk: ChunkNumber,
    symbol: &[u8],
) -> Result<BuiltChunk, SnapshotError> {
    let payload = SnapshotChunkPayload {
        chunk,
        data: symbol.to_vec(),
    };
    let packed = payload.pack();

    let mut slicer = Slicer::clay_default();
    let slices = slicer.encode(&packed).map_err(|e| {
        SnapshotError::ClayEncode(format!("epoch={} group={group} chunk={chunk}: {e}", epoch.0))
    })?;

    let slices: [Vec<u8>; GROUP_SIZE] =
        slices.try_into().map_err(|v: Vec<Vec<u8>>| SnapshotError::ClayEncodeArity {
            got: v.len(),
            expected: GROUP_SIZE,
        })?;

    let leaves: [Hash; GROUP_SIZE] = core::array::from_fn(|i| hash_leaf(&slices[i]));
    let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

    let stripe_size = slicer.stripe_size();
    let stripe_count = num_stripes(symbol.len(), stripe_size);

    let blob = BlobInfo {
        size: StorageUnits::from_bytes(symbol.len() as u64),
        commitment,
        profile: slicer.profile(),
        stripe_size: StorageUnits::from_bytes(stripe_size as u64),
        stripe_count: StripeCount(stripe_count as u64),
        leaves,
    };

    Ok(BuiltChunk {
        group,
        chunk,
        blob,
        slices,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent, SnapshotEntry};
    use tape_core::system::NodePreferences;
    use tape_core::types::coin::TAPE;
    use tape_core::types::SlotNumber;
    use tape_crypto::tx::Txid;

    use super::*;
    use crate::{assemble_snapshot_log, decode_chunk_payload, K_INNER};

    const TEST_GROUP_COUNT: usize = 20;

    fn entry(slot: u64, event: ReplayableEvent) -> SnapshotEntry {
        SnapshotEntry {
            slot: SlotNumber(slot),
            block_time: None,
            records: vec![ReplayRecord {
                tx_id: Txid::default(),
                actor: None,
                event,
            }],
        }
    }

    fn sample_log(epoch: EpochNumber) -> SnapshotLog {
        let entries = vec![
            entry(
                100,
                ReplayableEvent::AdvanceEpoch {
                    old_epoch: epoch.prev(),
                    new_epoch: epoch,
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                },
            ),
            entry(
                150,
                ReplayableEvent::JoinCommittee {
                    node: [9u8; 32].into(),
                    stake: TAPE(0),
                    key: BlsPubkey::zeroed(),
                    preferences: NodePreferences::zeroed(),
                    activation_epoch: EpochNumber(0),
                },
            ),
        ];
        SnapshotLog {
            epoch,
            start_slot: SlotNumber(100),
            end_slot: SlotNumber(150),
            entries,
        }
    }

    /// encode -> Clay/outer decode -> reconstruct the same log.
    #[test]
    fn round_trips_through_encode_decode() {
        let epoch = EpochNumber(11);
        let tape = Address::from([1u8; 32]);
        let log = sample_log(epoch);

        let chunks = encode_snapshot(tape, epoch, &log, TEST_GROUP_COUNT).unwrap();
        assert!(!chunks.is_empty());

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for c in &chunks {
            let inner: Vec<(usize, &[u8])> = c
                .slices
                .iter()
                .enumerate()
                .take(K_INNER)
                .map(|(i, s)| (i, s.as_slice()))
                .collect();
            let (chunk, symbol) = decode_chunk_payload(&inner).unwrap();
            assert_eq!(chunk, c.chunk);
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((c.group.0 as usize, symbol));
        }

        let decoded = assemble_snapshot_log(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap();
        assert_eq!(decoded.epoch, epoch);
        assert_eq!(decoded.entries.len(), 2);
        assert!(matches!(
            decoded.entries[0].records[0].event,
            ReplayableEvent::AdvanceEpoch { .. }
        ));
        assert!(matches!(
            decoded.entries[1].records[0].event,
            ReplayableEvent::JoinCommittee { .. }
        ));
    }

    /// Track numbers are contiguous from zero and match `snapshot_chunk_key`.
    #[test]
    fn chunk_tracks_are_contiguous() {
        let epoch = EpochNumber(7);
        let tape = Address::from([1u8; 32]);
        let chunks = encode_snapshot(tape, epoch, &sample_log(epoch), TEST_GROUP_COUNT).unwrap();

        for (index, c) in chunks.iter().enumerate() {
            assert_eq!(c.track.track_number, TrackNumber(index as u64));
            assert_eq!(c.track.tape, tape);
            assert_eq!(c.track.key, snapshot_chunk_key(epoch, c.group, c.chunk));
            assert_eq!(c.track.value_hash, c.blob.get_hash());
        }
    }

    fn inner_slices(chunk: &SnapshotChunk, k: usize) -> Vec<(usize, &[u8])> {
        chunk
            .slices
            .iter()
            .enumerate()
            .take(k)
            .map(|(i, s)| (i, s.as_slice()))
            .collect()
    }

    #[test]
    fn decode_fails_below_k_inner() {
        let epoch = EpochNumber(6);
        let chunks =
            encode_snapshot(Address::from([2u8; 32]), epoch, &sample_log(epoch), TEST_GROUP_COUNT)
                .unwrap();
        assert!(decode_chunk_payload(&inner_slices(&chunks[0], K_INNER - 1)).is_err());
    }

    #[test]
    fn one_group_round_trip() {
        let epoch = EpochNumber(22);
        let chunks =
            encode_snapshot(Address::from([3u8; 32]), epoch, &sample_log(epoch), 1).unwrap();
        assert!(!chunks.is_empty());

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for c in &chunks {
            let (chunk, symbol) = decode_chunk_payload(&inner_slices(c, K_INNER)).unwrap();
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((c.group.0 as usize, symbol));
        }

        let log = assemble_snapshot_log(&symbols_by_segment, epoch, 1).unwrap();
        assert_eq!(log.epoch, epoch);
        assert_eq!(log.entries.len(), 2);
    }

    #[test]
    fn assemble_rejects_insufficient_groups() {
        let epoch = EpochNumber(30);
        let chunks =
            encode_snapshot(Address::from([4u8; 32]), epoch, &sample_log(epoch), TEST_GROUP_COUNT)
                .unwrap();
        let outer_k = crate::snapshot_outer_k(TEST_GROUP_COUNT);

        // Give segment 0 only outer_k - 1 symbols.
        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for c in chunks
            .iter()
            .filter(|c| c.chunk == ChunkNumber(0))
            .take(outer_k - 1)
        {
            let (chunk, symbol) = decode_chunk_payload(&inner_slices(c, K_INNER)).unwrap();
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((c.group.0 as usize, symbol));
        }

        let err = assemble_snapshot_log(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap_err();
        assert!(format!("{err}").contains("groups decoded"));
    }
}
