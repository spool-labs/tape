//! Encode an epoch's `SnapshotLog` into erasure-coded chunk tracks. Inverse of
//! `decode`. Pure: no transport, no consensus, no store, no on-chain types — the
//! caller supplies the snapshot tape address, the active group count and the
//! network's profile.

use tape_core::encoding::ErasureCode;
use tape_core::erasure::{MAX_GROUP_SIZE, SLICE_TREE_HEIGHT, slice_root};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::VirtualGroup;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
use tape_core::types::{ChunkNumber, EpochNumber, StorageUnits, StripeCount, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_slicer::{num_stripes, ErasureCoder, OuterCoder, Slicer};

use crate::chunk::{
    pack_segment, snapshot_chunk_key, snapshot_max_segment_bytes, snapshot_outer_k,
    SnapshotChunkPayload,
};
use crate::SnapshotError;

/// One inner-encoded outer symbol: its derived blob and one slice per spool of
/// its group.
#[derive(Debug, Clone)]
pub struct BuiltChunk {
    pub group: VirtualGroup,
    pub chunk: ChunkNumber,
    pub blob: BlobEncoding,
    pub slices: Vec<Vec<u8>>,
}

/// One encoded snapshot chunk: the committed track metadata plus its blob and
/// slices. A node persists the slice for the spool it owns; an offline encoder
/// can keep them all.
#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub group: VirtualGroup,
    pub chunk: ChunkNumber,
    pub track: CompressedTrack,
    pub blob: BlobEncoding,
    pub slices: Vec<Vec<u8>>,
}

/// Encode `log` into `epoch`'s chunk-track set and hand each chunk to `visit` in track order, one chunk of slices live at a time
///
/// Every node has to produce the same bytes, so `profile` is the network's.
pub fn encode_snapshot_streaming<Error, Visit>(
    snapshot_tape: Address,
    epoch: EpochNumber,
    log: &SnapshotLog,
    total_groups: usize,
    profile: ErasureCode,
    mut visit: Visit,
) -> Result<(), Error>
where
    Error: From<SnapshotError>,
    Visit: FnMut(SnapshotChunk) -> Result<(), Error>,
{
    let outer_k = snapshot_outer_k(total_groups);
    if outer_k == 0 {
        return Err(SnapshotError::NoGroups { epoch: epoch.0 }.into());
    }

    // The serialized log is dead once it is compressed, and it is tens of MB.
    let compressed = {
        let serialized = log
            .to_bytes()
            .map_err(|e| SnapshotError::Serialize(format!("epoch={}: {e}", epoch.0)))?;
        lz4_flex::compress_prepend_size(&serialized)
    };

    let max_segment_bytes = snapshot_max_segment_bytes(total_groups);
    let chunk_total = compressed.len().div_ceil(max_segment_bytes).max(1);
    let chunk_size = compressed.len().div_ceil(chunk_total).max(1);

    let mut outer = OuterCoder::new(outer_k, total_groups);

    for chunk_index in 0..chunk_total {
        let start = chunk_index * chunk_size;
        let end = start.saturating_add(chunk_size).min(compressed.len());

        let packed = pack_segment(&compressed[start..end]);
        let symbols = outer.encode(&packed).map_err(|e| {
            SnapshotError::OuterEncode(format!("epoch={} segment={chunk_index}: {e}", epoch.0))
        })?;

        let chunk = ChunkNumber(chunk_index as u64);

        for (group_index, symbol) in symbols.iter().enumerate() {
            let group = VirtualGroup(group_index as u64);
            let built = encode_chunk(epoch, group, chunk, symbol, profile)?;
            let track_number = TrackNumber((chunk_index as u64) * (total_groups as u64) + group.0);

            let track = CompressedTrack {
                tape: snapshot_tape,
                track_number,
                key: snapshot_chunk_key(epoch, group, chunk),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                flags: TrackFlags::NONE,
                size: built.blob.size,
                group,
                value_hash: built.blob.get_hash(),
            };

            visit(SnapshotChunk {
                group,
                chunk,
                track,
                blob: built.blob,
                slices: built.slices,
            })?;
        }
    }

    Ok(())
}

/// Encode `log` into the full chunk-track set for `epoch`'s snapshot, slices included; deterministic from its inputs and equal to the set the on-chain snapshot tape commits
pub fn encode_snapshot(
    snapshot_tape: Address,
    epoch: EpochNumber,
    log: &SnapshotLog,
    total_groups: usize,
    profile: ErasureCode,
) -> Result<Vec<SnapshotChunk>, SnapshotError> {
    let mut chunks = Vec::new();

    encode_snapshot_streaming(
        snapshot_tape,
        epoch,
        log,
        total_groups,
        profile,
        |chunk| -> Result<(), SnapshotError> {
            chunks.push(chunk);
            Ok(())
        },
    )?;

    Ok(chunks)
}

/// Inner-encode one outer symbol into one slice per spool and derive its blob.
pub fn encode_chunk(
    epoch: EpochNumber,
    group: VirtualGroup,
    chunk: ChunkNumber,
    symbol: &[u8],
    profile: ErasureCode,
) -> Result<BuiltChunk, SnapshotError> {
    let payload = SnapshotChunkPayload {
        chunk,
        data: symbol.to_vec(),
    };
    let packed = payload.pack();

    let context = format!("epoch={} group={group} chunk={chunk}", epoch.0);

    let mut slicer = Slicer::try_from_profile(profile)
        .map_err(|e| SnapshotError::InnerEncode(format!("{context}: {e}")))?;
    let slices = slicer
        .encode(&packed)
        .map_err(|e| SnapshotError::InnerEncode(format!("{context}: {e}")))?;

    // The commitment covers one leaf per slice; the leaves past them stay zero.
    let mut leaves = [Hash::default(); MAX_GROUP_SIZE];
    for (leaf, slice) in leaves.iter_mut().zip(slices.iter()) {
        *leaf = slice_root(slice).ok_or_else(|| {
            SnapshotError::InnerEncode(format!("{context}: slice exceeds sub-leaf tree capacity"))
        })?;
    }
    let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves[..slices.len()]);

    let stripe_size = slicer.stripe_size();
    let stripe_count = num_stripes(symbol.len(), stripe_size);

    let blob = BlobEncoding {
        size: StorageUnits::from_bytes(symbol.len() as u64),
        commitment,
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
    use tape_core::track::archive::TrackArchive;
    use tape_core::types::coin::TAPE;
    use tape_core::types::SlotNumber;
    use tape_crypto::tx::Txid;

    use tape_core::encoding::{ClayParams, RSParams, RepParams, TAPENET_PROFILE};

    use super::*;
    use crate::{assemble_snapshot_log, decode_chunk_payload};

    const TEST_GROUP_COUNT: usize = 20;

    /// Inner slices tapenet's profile needs to decode a chunk.
    const K_INNER: usize = TAPENET_PROFILE.k() as usize;

    /// Slice bytes the visitor holds, counted as the encoder hands each chunk over
    #[derive(Debug, Default)]
    struct LiveSlices {
        live: usize,
        peak: usize,
        total: usize,
        visits: usize,
    }

    impl LiveSlices {
        fn visit(&mut self, chunk: SnapshotChunk) {
            let bytes = slice_bytes(&chunk);
            self.live += bytes;
            self.peak = self.peak.max(self.live);
            self.total += bytes;
            self.visits += 1;

            drop(chunk);
            self.live -= bytes;
        }
    }

    fn slice_bytes(chunk: &SnapshotChunk) -> usize {
        chunk.slices.iter().map(Vec::len).sum()
    }

    fn track_root(chunks: &[SnapshotChunk]) -> Hash {
        let mut archive = TrackArchive::zeroed();
        for chunk in chunks {
            archive.append(&chunk.track).expect("append chunk track");
        }
        archive.tree.root()
    }

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

        let chunks = encode_snapshot(tape, epoch, &log, TEST_GROUP_COUNT, TAPENET_PROFILE).unwrap();
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
            let (chunk, symbol) = decode_chunk_payload(&inner, TAPENET_PROFILE).unwrap();
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
        let chunks =
            encode_snapshot(tape, epoch, &sample_log(epoch), TEST_GROUP_COUNT, TAPENET_PROFILE).unwrap();

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
        let tape = Address::from([2u8; 32]);
        let chunks =
            encode_snapshot(tape, epoch, &sample_log(epoch), TEST_GROUP_COUNT, TAPENET_PROFILE).unwrap();
        assert!(decode_chunk_payload(&inner_slices(&chunks[0], K_INNER - 1), TAPENET_PROFILE).is_err());
    }

    #[test]
    fn one_group_round_trip() {
        let epoch = EpochNumber(22);
        let tape = Address::from([3u8; 32]);
        let chunks = encode_snapshot(tape, epoch, &sample_log(epoch), 1, TAPENET_PROFILE).unwrap();
        assert!(!chunks.is_empty());

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for c in &chunks {
            let (chunk, symbol) = decode_chunk_payload(&inner_slices(c, K_INNER), TAPENET_PROFILE).unwrap();
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
        let tape = Address::from([4u8; 32]);
        let chunks =
            encode_snapshot(tape, epoch, &sample_log(epoch), TEST_GROUP_COUNT, TAPENET_PROFILE).unwrap();
        let outer_k = crate::snapshot_outer_k(TEST_GROUP_COUNT);

        // Give segment 0 only outer_k - 1 symbols.
        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for c in chunks
            .iter()
            .filter(|c| c.chunk == ChunkNumber(0))
            .take(outer_k - 1)
        {
            let (chunk, symbol) = decode_chunk_payload(&inner_slices(c, K_INNER), TAPENET_PROFILE).unwrap();
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((c.group.0 as usize, symbol));
        }

        let err = assemble_snapshot_log(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap_err();
        assert!(format!("{err}").contains("groups decoded"));
    }

    // a custom cluster's snapshot round-trips from k inner slices of each
    // group, and commits each chunk over its own n leaves
    #[test]
    fn custom_profiles() {
        let profiles = [
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::Replication(RepParams::new(2)),
        ];
        let epoch = EpochNumber(31);
        let tape = Address::from([7u8; 32]);
        let total_groups = 3;

        for profile in profiles {
            let n = profile.n() as usize;
            let k = profile.k() as usize;
            let chunks =
                encode_snapshot(tape, epoch, &sample_log(epoch), total_groups, profile).unwrap();

            let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> =
                BTreeMap::new();
            for c in &chunks {
                assert_eq!(c.slices.len(), n, "{profile:?}");
                assert_eq!(c.blob.commitment_root(profile), Some(c.blob.commitment), "{profile:?}");

                let (chunk, symbol) =
                    decode_chunk_payload(&inner_slices(c, k), profile).unwrap();
                symbols_by_segment
                    .entry(chunk)
                    .or_default()
                    .push((c.group.0 as usize, symbol));
            }

            let log = assemble_snapshot_log(&symbols_by_segment, epoch, total_groups).unwrap();
            assert_eq!(log.entries.len(), 2, "{profile:?}");
        }
    }

    // the streamed encode gives the same track root, rows, blobs, and slices
    #[test]
    fn streamed_matches_full() {
        let epoch = EpochNumber(13);
        let tape = Address::from([5u8; 32]);
        let log = sample_log(epoch);
        let full = encode_snapshot(tape, epoch, &log, TEST_GROUP_COUNT, TAPENET_PROFILE).expect("full encode");

        let mut streamed: Vec<SnapshotChunk> = Vec::new();
        encode_snapshot_streaming(
            tape,
            epoch,
            &log,
            TEST_GROUP_COUNT,
            TAPENET_PROFILE,
            |chunk| -> Result<(), SnapshotError> {
                streamed.push(chunk);
                Ok(())
            },
        )
        .expect("streamed encode");

        assert_eq!(track_root(&streamed), track_root(&full));
        assert_eq!(streamed.len(), full.len());
        for (left, right) in streamed.iter().zip(full.iter()) {
            assert_eq!(left.group, right.group);
            assert_eq!(left.chunk, right.chunk);
            assert_eq!(left.track, right.track);
            assert_eq!(left.blob, right.blob);
            assert_eq!(left.slices, right.slices);
        }
    }

    // the streamed encode hands over one chunk's slice bytes at a time and stops where the visitor stops
    #[test]
    fn one_chunk_live() {
        let epoch = EpochNumber(14);
        let tape = Address::from([6u8; 32]);
        let log = sample_log(epoch);

        let mut counter = LiveSlices::default();
        encode_snapshot_streaming(
            tape,
            epoch,
            &log,
            TEST_GROUP_COUNT,
            TAPENET_PROFILE,
            |chunk| -> Result<(), SnapshotError> {
                counter.visit(chunk);
                Ok(())
            },
        )
        .expect("streamed encode");

        let full = encode_snapshot(tape, epoch, &log, TEST_GROUP_COUNT, TAPENET_PROFILE).expect("full encode");
        let full_bytes: usize = full.iter().map(slice_bytes).sum();
        let largest_chunk = full.iter().map(slice_bytes).max().expect("a chunk");

        assert!(counter.visits > 1);
        assert_eq!(counter.visits, full.len());
        assert_eq!(counter.total, full_bytes);
        // The set the full encode keeps is the whole point of the visitor, so a
        // peak of one chunk's bytes is the thing to hold it to.
        assert_eq!(counter.peak, largest_chunk);
        assert!(full_bytes > counter.peak, "{full_bytes} bytes vs {}", counter.peak);
        assert_eq!(counter.live, 0);

        // A visitor that stops ends the run where it stopped, so nothing after
        // that chunk is encoded.
        let mut stopped_at = 0usize;
        let stopped = encode_snapshot_streaming(
            tape,
            epoch,
            &log,
            TEST_GROUP_COUNT,
            TAPENET_PROFILE,
            |_| -> Result<(), SnapshotError> {
                stopped_at += 1;
                Err(SnapshotError::NoGroups { epoch: epoch.0 })
            },
        );

        assert!(stopped.is_err());
        assert_eq!(stopped_at, 1);
    }
}
