///! Fetch and decode snapshot logs from the network during bootstrap.
///! Inverse of `build.rs`'s encode path.

use std::collections::BTreeMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_core::snapshot::chunk::{unpack_segment, SnapshotChunkPayload};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::api::{GetSliceReq, GetTrackDataReq, ListTracksByTapeReq};
use tape_protocol::Api;
use tape_slicer::{snapshot_outer_k, ErasureCoder, OuterCoder, Slicer};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn, Instrument};

use crate::context::NodeContext;
use crate::core::error::NodeError;

const K_INNER: usize = 7;
const LIST_TRACKS_LIMIT: u32 = 256;

/// Fetch every chunk for an epoch's snapshot tape from peers, decode, and
/// return the reconstructed `SnapshotLog`.
pub async fn fetch_and_decode_epoch<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<SnapshotLog, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let tape = Address::from(snapshot_tape_pda(epoch).0);
    let candidates = list_snapshot_track_candidates(context, tape).await?;

    let mut last_error = None;
    for (peer, tracks) in candidates {
        match decode_snapshot_tracks(context, epoch, tracks, cancel).await {
            Ok(log) => return Ok(log),
            Err(error) => {
                warn!(node = %peer, ?error, "bootstrap: candidate snapshot track list failed");
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        NodeError::Store(format!(
            "bootstrap: no usable snapshot track list for epoch {epoch}"
        ))
    }))
}

async fn decode_snapshot_tracks<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    tracks: Vec<CompressedTrack>,
    cancel: &CancellationToken,
) -> Result<SnapshotLog, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let tape = Address::from(snapshot_tape_pda(epoch).0);
    validate_snapshot_track_list(epoch, tape, &tracks)?;
    let total_groups = snapshot_track_group_count(epoch, &tracks)?;

    debug!(
        epoch = epoch.0,
        ?tape,
        tracks = tracks.len(),
        "bootstrap: fetched snapshot track list"
    );

    // Fan out: fetch + Clay-decode every track in parallel. Each task returns
    // the `(group, chunk, outer-symbol)` triple recovered from the Clay
    // payload. Tasks that fail are logged and skipped; outer RS recovers as
    // long as enough groups succeed per segment for this snapshot's group count.
    let mut join = JoinSet::new();
    for track in tracks {
        let context = context.clone();
        let cancel = cancel.clone();
        join.spawn(
            async move { fetch_and_decode_track(&context, epoch, track, &cancel).await }
                .in_current_span(),
        );
    }

    let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
    while let Some(result) = join.join_next().await {
        if cancel.is_cancelled() {
            join.abort_all();
            return Err(NodeError::Store("bootstrap: cancelled".into()));
        }
        match result.map_err(|e| NodeError::Store(format!("bootstrap: decode task join: {e}")))? {
            Ok(Decoded { group, chunk, symbol }) => {
                symbols_by_segment
                    .entry(chunk)
                    .or_default()
                    .push((group.0 as usize, symbol));
            }
            Err(error) => {
                warn!(?error, "bootstrap: track decode failed");
            }
        }
    }

    let segments = outer_decode_segments(&symbols_by_segment, epoch, total_groups)?;
    decode_snapshot_log(segments, epoch)
}

fn validate_snapshot_track_list(
    epoch: EpochNumber,
    tape: Address,
    tracks: &[CompressedTrack],
) -> Result<(), NodeError> {
    if tracks.is_empty() {
        return Err(NodeError::Store(format!(
            "snapshot tape {tape} for epoch {epoch} has no tracks"
        )));
    }

    for track in tracks {
        if track.tape != tape {
            return Err(NodeError::Store(format!(
                "bootstrap: peer returned track from wrong tape for epoch {epoch}"
            )));
        }

        if !track.is_blob() {
            return Err(NodeError::Store(format!(
                "bootstrap: peer returned non-blob snapshot track for epoch {epoch}"
            )));
        }
    }

    Ok(())
}

fn snapshot_track_group_count(
    epoch: EpochNumber,
    tracks: &[CompressedTrack],
) -> Result<usize, NodeError> {
    let total_groups = tracks
        .iter()
        .map(|track| track.group.0 as usize + 1)
        .max()
        .unwrap_or(0);

    if total_groups == 0 {
        return Err(NodeError::Store(format!(
            "bootstrap: snapshot for epoch {epoch} has no groups"
        )));
    }

    Ok(total_groups)
}

struct Decoded {
    group: GroupIndex,
    chunk: ChunkNumber,
    symbol: Vec<u8>,
}

/// Fetch K_INNER verified slices for one track and Clay-decode them.
async fn fetch_and_decode_track<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    track: CompressedTrack,
    cancel: &CancellationToken,
) -> Result<Decoded, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let group = track.group;
    let track_address = Address::from(track_pda(track.tape, track.track_number).0);

    let peers = context.state().group_peers(group);
    if peers.is_empty() {
        return Err(NodeError::Store(format!(
            "bootstrap: no peers for group {} in current committee",
            group.0
        )));
    }

    let blob = fetch_blob(context.api.as_ref(), &peers, track_address).await?;
    if blob.get_hash() != track.value_hash {
        return Err(NodeError::Store(format!(
            "bootstrap: blob metadata for epoch={} group={} track={} does not match on-chain value_hash",
            epoch.0, group.0, track_address
        )));
    }

    let slices = fetch_verified_slices(
        context.api.as_ref(),
        &peers,
        group,
        track_address,
        &blob,
        cancel,
    )
    .await?;

    let plaintext = clay_decode(&slices).map_err(|e| {
        NodeError::Store(format!(
            "bootstrap: clay decode epoch={} group={} track={}: {e}",
            epoch.0, group.0, track_address
        ))
    })?;

    let payload = SnapshotChunkPayload::unpack(&plaintext).map_err(|e| {
        NodeError::Store(format!(
            "bootstrap: snapshot chunk payload epoch={} group={} track={}: {e}",
            epoch.0, group.0, track_address
        ))
    })?;

    Ok(Decoded {
        group,
        chunk: payload.chunk,
        symbol: payload.data,
    })
}

async fn fetch_verified_slices<Cluster: Api>(
    api: &Cluster,
    peers: &[(SpoolIndex, Address)],
    group: GroupIndex,
    track: Address,
    blob: &BlobInfo,
    cancel: &CancellationToken,
) -> Result<Vec<(usize, Vec<u8>)>, NodeError> {
    let mut out: Vec<(usize, Vec<u8>)> = Vec::with_capacity(K_INNER);
    for (spool, peer) in peers {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap: cancelled".into()));
        }
        let Some(leaf_idx) = group.position_of(*spool) else {
            continue;
        };
        match api
            .get_slice(*peer, &GetSliceReq { track, spool: *spool })
            .await
        {
            Ok(res) => {
                if !blob.verify_slice(SpoolIndex::from(leaf_idx as u64), &res.data) {
                    warn!(
                        node = %peer,
                        spool = %spool,
                        "bootstrap: slice failed leaf verification"
                    );
                    continue;
                }
                out.push((leaf_idx, res.data));
                if out.len() >= K_INNER {
                    return Ok(out);
                }
            }
            Err(error) => {
                trace!(
                    node = %peer,
                    spool = %spool,
                    ?error,
                    "bootstrap: get_slice failed"
                );
            }
        }
    }
    Err(NodeError::Store(format!(
        "bootstrap: only {}/{} slices for group {}",
        out.len(),
        K_INNER,
        group.0
    )))
}

async fn fetch_blob<Cluster: Api>(
    api: &Cluster,
    peers: &[(SpoolIndex, Address)],
    track: Address,
) -> Result<BlobInfo, NodeError> {
    let mut last_error: Option<NodeError> = None;
    for (_, peer) in peers {
        match api.get_track_data(*peer, &GetTrackDataReq { track }).await {
            Ok(res) => match res.data {
                TrackData::Blob(blob) => return Ok(blob),
                other => {
                    return Err(NodeError::Store(format!(
                        "bootstrap: expected blob track data, got {other:?}"
                    )));
                }
            },
            Err(error) => {
                last_error = Some(NodeError::Store(format!("get_track_data: {error}")));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        NodeError::Store("bootstrap: no peer returned blob metadata".into())
    }))
}

async fn list_snapshot_track_candidates<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    tape: Address,
) -> Result<Vec<(Address, Vec<CompressedTrack>)>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let peers: Vec<Address> = context
        .state()
        .current
        .committee
        .iter()
        .map(|m| m.node)
        .collect();
    if peers.is_empty() {
        return Err(NodeError::Store(
            "bootstrap: no committee peers available for snapshot track listing".into(),
        ));
    }

    let mut candidates = Vec::new();
    let mut last_error: Option<NodeError> = None;
    for peer in &peers {
        match list_tracks_from_peer(context.api.as_ref(), *peer, tape).await {
            Ok(tracks) if tracks.is_empty() => {
                debug!(
                    node = %peer,
                    ?tape,
                    "bootstrap: peer returned empty snapshot track list"
                );
            }
            Ok(tracks) => candidates.push((*peer, tracks)),
            Err(error) => {
                warn!(node = %peer, ?error, "bootstrap: list_tracks_by_tape failed");
                last_error = Some(error);
            }
        }
    }

    if candidates.is_empty() {
        Err(last_error.unwrap_or_else(|| {
            NodeError::Store(format!(
                "bootstrap: no peer returned snapshot tracks for tape {tape}"
            ))
        }))
    } else {
        Ok(candidates)
    }
}

async fn list_tracks_from_peer<Cluster: Api>(
    api: &Cluster,
    peer: Address,
    tape: Address,
) -> Result<Vec<CompressedTrack>, NodeError> {
    let mut out = Vec::new();
    let mut cursor: Option<TrackNumber> = None;
    loop {
        let res = api
            .list_tracks_by_tape(
                peer,
                &ListTracksByTapeReq {
                    tape,
                    cursor,
                    limit: LIST_TRACKS_LIMIT,
                },
            )
            .await
            .map_err(|error| NodeError::Store(format!("list_tracks_by_tape: {error}")))?;

        out.extend(res.tracks);
        match res.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(out),
        }
    }
}


/// Clay-decode a chunk's K_INNER or more verified slices to its plaintext
/// payload (which is a packed [`SnapshotChunkPayload`]).
fn clay_decode(slices: &[(usize, Vec<u8>)]) -> Result<Vec<u8>, tape_slicer::DecodeError> {
    let mut slicer = Slicer::clay_default();
    let refs: Vec<(usize, &[u8])> = slices.iter().map(|(i, d)| (*i, d.as_slice())).collect();
    slicer.decode(&refs)
}

/// Outer RS decode each segment into packed (length-prefixed) compressed
/// bytes, ordered by `chunk`.
fn outer_decode_segments(
    symbols_by_segment: &BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>>,
    epoch: EpochNumber,
    total_groups: usize,
) -> Result<Vec<Vec<u8>>, NodeError> {
    if symbols_by_segment.is_empty() {
        return Err(NodeError::Store(format!(
            "bootstrap: no decoded chunks for epoch {}",
            epoch.0
        )));
    }

    let outer_k = snapshot_outer_k(total_groups);
    if outer_k == 0 {
        return Err(NodeError::Store(format!(
            "bootstrap: snapshot for epoch {} has no groups",
            epoch.0
        )));
    }

    // Chunks must be a contiguous 0..segment_count range — gaps mean not
    // enough groups decoded for some segment, which is a hard failure.
    let segment_count = symbols_by_segment
        .keys()
        .last()
        .map(|c| c.0 as usize + 1)
        .unwrap_or(0);
    for i in 0..segment_count {
        if !symbols_by_segment.contains_key(&ChunkNumber(i as u64)) {
            return Err(NodeError::Store(format!(
                "bootstrap: missing decoded symbols for epoch={} chunk={}",
                epoch.0, i
            )));
        }
    }

    let mut segments = Vec::with_capacity(segment_count);
    for (chunk, symbols) in symbols_by_segment {
        if symbols.len() < outer_k {
            return Err(NodeError::Store(format!(
                "bootstrap: only {}/{} groups decoded for epoch={} chunk={}",
                symbols.len(),
                outer_k,
                epoch.0,
                chunk.0
            )));
        }
        let mut coder = OuterCoder::new(outer_k, total_groups);
        let refs: Vec<(usize, &[u8])> = symbols.iter().map(|(i, d)| (*i, d.as_slice())).collect();
        let packed = coder.decode(&refs).map_err(|e| {
            NodeError::Store(format!(
                "outer rs decode epoch={} chunk={}: {e}",
                epoch.0, chunk.0
            ))
        })?;
        segments.push(packed);
    }

    Ok(segments)
}

/// Concatenate decoded segments (stripping each segment's length prefix),
/// decompress via lz4, and deserialize.
fn decode_snapshot_log(
    segments: Vec<Vec<u8>>,
    epoch: EpochNumber,
) -> Result<SnapshotLog, NodeError> {
    let mut compressed = Vec::new();
    for packed in &segments {
        let segment = unpack_segment(packed).map_err(|e| {
            NodeError::Store(format!("bootstrap: unpack segment for epoch={}: {e}", epoch.0))
        })?;
        compressed.extend_from_slice(segment);
    }

    let decompressed = lz4_flex::decompress_size_prepended(&compressed)
        .map_err(|e| NodeError::Store(format!("lz4 decompress: {e}")))?;

    let log = SnapshotLog::from_bytes(&decompressed)
        .map_err(|e| NodeError::Store(format!("snapshot log deserialize: {e}")))?;

    if log.epoch != epoch {
        return Err(NodeError::Store(format!(
            "bootstrap: decoded snapshot epoch mismatch: expected {}, got {}",
            epoch.0, log.epoch.0
        )));
    }
    Ok(log)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytemuck::Zeroable;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsPubkey;
    use tape_core::snapshot::chunk::pack_segment;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent, SnapshotLog};
    use tape_core::system::NodePreferences;
    use tape_core::types::coin::TAPE;
    use tape_core::types::SlotNumber;
    use tape_crypto::hash::Hash;
    use tape_slicer::{snapshot_max_segment_bytes, OuterCoder};
    use tape_store::ops::EventLogOps;
    use tape_store::TapeStore;
    use tape_crypto::tx::Txid;

    use super::*;
    use crate::features::snapshot::build::{encode_chunk, BuiltChunk};

    const TEST_GROUP_COUNT: usize = 20;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
        }
    }

    /// Build every snapshot chunk for `epoch` across all test groups.
    /// Mirrors production encoding without the per-node slice/sig
    /// bookkeeping — tests only need the raw chunks for decode round-trips.
    fn build_all_chunks_with_group_count(
        store: &TapeStore<MemoryStore>,
        epoch: EpochNumber,
        group_count: usize,
    ) -> Vec<BuiltChunk> {
        let entries = store.get_epoch_events(epoch).unwrap();
        let start_slot = entries.first().map(|e| e.slot).unwrap_or(SlotNumber(0));
        let end_slot = entries.last().map(|e| e.slot).unwrap_or(SlotNumber(0));
        let log = SnapshotLog { epoch, start_slot, end_slot, entries };
        let compressed = lz4_flex::compress_prepend_size(&log.to_bytes().unwrap());
        let segment_count = compressed.len().div_ceil(snapshot_max_segment_bytes(group_count)).max(1);
        let segment_size = compressed.len().div_ceil(segment_count).max(1);

        let mut outer = OuterCoder::new(snapshot_outer_k(group_count), group_count);
        let mut chunks = Vec::with_capacity(segment_count * group_count);
        for segment_idx in 0..segment_count {
            let start = segment_idx * segment_size;
            let end = start.saturating_add(segment_size).min(compressed.len());
            let symbols = outer.encode(&pack_segment(&compressed[start..end])).unwrap();
            let chunk = ChunkNumber(segment_idx as u64);
            for (group_index, symbol) in symbols.into_iter().enumerate() {
                let group = GroupIndex(group_index as u64);
                chunks.push(encode_chunk(epoch, group, chunk, &symbol).unwrap());
            }
        }
        chunks
    }

    fn build_all_chunks(store: &TapeStore<MemoryStore>, epoch: EpochNumber) -> Vec<BuiltChunk> {
        build_all_chunks_with_group_count(store, epoch, TEST_GROUP_COUNT)
    }

    fn append_advance(store: &TapeStore<MemoryStore>, epoch: EpochNumber, slot: u64) {
        store
            .append_record(
                epoch,
                SlotNumber(slot),
                None,
                &record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: epoch.prev(),
                    new_epoch: epoch,
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    nonce: Hash::default(),
                }),
            )
            .unwrap();
    }

    fn take_k_inner(chunk: &BuiltChunk, k: usize) -> Vec<(usize, Vec<u8>)> {
        chunk
            .slices
            .iter()
            .enumerate()
            .take(k)
            .map(|(i, s)| (i, s.clone()))
            .collect()
    }

    /// Clay-decode + unpack one BuiltChunk.
    fn decode_built_chunk(chunk: &BuiltChunk) -> (ChunkNumber, Vec<u8>) {
        let slices = take_k_inner(chunk, K_INNER);
        let plaintext = clay_decode(&slices).expect("clay decode");
        let payload = SnapshotChunkPayload::unpack(&plaintext).expect("unpack payload");
        assert_eq!(payload.chunk, chunk.chunk);
        (payload.chunk, payload.data)
    }

    #[test]
    fn chunk_payload_recovers_chunk_index_from_slices() {
        let store = test_store();
        let epoch = EpochNumber(5);
        append_advance(&store, epoch, 100);

        let chunks = build_all_chunks(&store, epoch);
        assert!(!chunks.is_empty());

        for built in &chunks {
            let (chunk, data) = decode_built_chunk(built);
            assert_eq!(chunk, built.chunk);
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn clay_decode_fails_below_k_inner() {
        let store = test_store();
        let epoch = EpochNumber(6);
        append_advance(&store, epoch, 100);
        let chunks = build_all_chunks(&store, epoch);
        let chunk = &chunks[0];
        let slices = take_k_inner(chunk, K_INNER - 1);
        assert!(clay_decode(&slices).is_err());
    }

    #[test]
    fn full_round_trip_reconstructs_events() {
        let store = test_store();
        let epoch = EpochNumber(11);
        append_advance(&store, epoch, 100);
        store
            .append_record(
                epoch,
                SlotNumber(150),
                None,
                &record(ReplayableEvent::JoinCommittee {
                    node: [9u8; 32].into(),
                    stake: TAPE(0),
                    key: BlsPubkey::zeroed(),
                    preferences: NodePreferences::zeroed(),
                    activation_epoch: EpochNumber(0),
                }),
            )
            .unwrap();

        let chunks = build_all_chunks(&store, epoch);

        // Clay-decode + unpack every chunk, grouped by segment.
        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for built in &chunks {
            let (chunk, data) = decode_built_chunk(built);
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((built.group.0 as usize, data));
        }

        let outer_k = snapshot_outer_k(TEST_GROUP_COUNT);
        // Keep only the derived outer threshold per segment.
        for symbols in symbols_by_segment.values_mut() {
            symbols.sort_by_key(|(i, _)| *i);
            symbols.truncate(outer_k);
        }

        let segments = outer_decode_segments(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap();
        let log = decode_snapshot_log(segments, epoch).unwrap();

        assert_eq!(log.epoch, epoch);
        assert_eq!(log.entries.len(), 2);
        assert!(matches!(
            &log.entries[0].records[0].event,
            ReplayableEvent::AdvanceEpoch { .. }
        ));
        assert!(matches!(
            &log.entries[1].records[0].event,
            ReplayableEvent::JoinCommittee { .. }
        ));
    }

    #[test]
    fn single_segment_round_trip() {
        // Small epoch → exactly one outer RS segment. Confirms the normal
        // path through outer_decode_segments + decode_snapshot_log. Multiple
        // segments are covered structurally by `build.rs`'s split test;
        // triggering the multi-segment case here would need tens of MiB of
        // uncompressible input.
        let store = test_store();
        let epoch = EpochNumber(21);
        append_advance(&store, epoch, 100);

        let chunks = build_all_chunks(&store, epoch);
        let segment_count = chunks
            .iter()
            .map(|c| c.chunk.0)
            .max()
            .unwrap_or(0) as usize
            + 1;

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for built in &chunks {
            let (chunk, data) = decode_built_chunk(built);
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((built.group.0 as usize, data));
        }
        assert_eq!(symbols_by_segment.len(), segment_count);

        let segments = outer_decode_segments(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap();
        assert_eq!(segments.len(), segment_count);
        let log = decode_snapshot_log(segments, epoch).unwrap();
        assert_eq!(log.epoch, epoch);
    }

    #[test]
    fn one_group_round_trip() {
        let store = test_store();
        let epoch = EpochNumber(22);
        append_advance(&store, epoch, 100);

        let chunks = build_all_chunks_with_group_count(&store, epoch, 1);
        assert!(!chunks.is_empty());

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        for built in &chunks {
            let (chunk, data) = decode_built_chunk(built);
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((built.group.0 as usize, data));
        }

        let segments = outer_decode_segments(&symbols_by_segment, epoch, 1).unwrap();
        let log = decode_snapshot_log(segments, epoch).unwrap();
        assert_eq!(log.epoch, epoch);
        assert_eq!(log.entries.len(), 1);
    }

    #[test]
    fn outer_decode_rejects_insufficient_groups() {
        let store = test_store();
        let epoch = EpochNumber(30);
        append_advance(&store, epoch, 100);
        let chunks = build_all_chunks(&store, epoch);

        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
        let outer_k = snapshot_outer_k(TEST_GROUP_COUNT);
        // Give segment 0 only outer_k - 1 symbols.
        for built in chunks.iter().take(outer_k - 1) {
            let (chunk, data) = decode_built_chunk(built);
            symbols_by_segment
                .entry(chunk)
                .or_default()
                .push((built.group.0 as usize, data));
        }

        let err = outer_decode_segments(&symbols_by_segment, epoch, TEST_GROUP_COUNT).unwrap_err();
        assert!(format!("{err}").contains("groups decoded"));
    }
}
