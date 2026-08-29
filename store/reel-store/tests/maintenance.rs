//! The maintenance plane reclaims only when something drives it
//!
//! The engine starts no threads of its own, so a volume nobody ticks never
//! compacts and grows without bound. This holds the passthrough that lets a
//! caller drive it through the store trait: disconnect `Store::maintain` from
//! the engine and the trait's no-op default takes over, nothing is retired, and
//! the footprint assertion below fails.

use reel::{ByteCount, CompactRate, IoBackend, Preallocate, ReelConfig, SyncPolicy, ThreadBudget};
use reel_store::{ReelStore, TAPE_COLUMNS};
use store::{Column, Store};
use tape_store::columns::SliceCol;
use tempfile::TempDir;

/// Segment size, small enough that a few mebibytes seal a handful of them
const SEGMENT_BYTES: u64 = 1024 * 1024;

/// Bytes reserved ahead of the write head, so the footprint tracks the data
const ALLOC_CHUNK: u64 = 256 * 1024;

/// Bytes per record
const VALUE_BYTES: usize = 8 * 1024;

/// Records written, spanning several segments
const RECORDS: usize = 1024;

/// Records deleted, leaving whole segments dead for the pass to retire
const DELETED: usize = 960;

/// Ticks the loop may spend draining before it gives up
const MAX_TICKS: usize = 64;

/// Ticks with an unchanged footprint that count as quiescent
const SETTLED_TICKS: usize = 3;

/// Bytes a slice key occupies: the spool big endian, then the track address
const SLICE_KEY_LEN: usize = 34;

/// A volume that seals often, syncs never, and compacts at device speed
fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        // Durability is not what this measures, and a sync per put would make a
        // thousand records slow enough to matter.
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Auto,
        active_tails: ThreadBudget::threads(1),
        // Portable rather than fastest: the pass under test is the same either way.
        io_backend: IoBackend::Posix,
        ..ReelConfig::default()
    }
}

fn slice_key(at: usize) -> Vec<u8> {
    let mut key = vec![0u8; SLICE_KEY_LEN];
    key[SLICE_KEY_LEN - 8..].copy_from_slice(&(at as u64).to_be_bytes());
    key
}

/// A payload lz4 cannot shrink, so the segments hold the bytes they were given
///
/// Every column declares a codec, and a run of one byte would compress to
/// nothing, leaving a footprint that says more about the codec than the plane.
fn payload(seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut value = Vec::with_capacity(VALUE_BYTES);
    while value.len() < VALUE_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(VALUE_BYTES);
    value
}

/// What the volume occupies on disk, as the store trait reports it
fn footprint(store: &ReelStore) -> u64 {
    store
        .disk_volumes()
        .expect("disk volumes")
        .iter()
        .map(|volume| volume.used_bytes)
        .sum()
}

// a driven volume gives back the space its dead records held
#[test]
fn ticking_reclaims_dead_space() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().join("volume"), config(), TAPE_COLUMNS)
        .expect("open the volume");
    let cf = SliceCol::CF_NAME;

    for at in 0..RECORDS {
        store
            .put(cf, &slice_key(at), &payload(at as u64))
            .expect("put");
    }
    store.flush().expect("flush the writes");

    let filled = footprint(&store);
    assert!(
        filled > SEGMENT_BYTES,
        "the fill should span several segments, got {filled} bytes"
    );

    for at in 0..DELETED {
        store.delete(cf, &slice_key(at)).expect("delete");
    }
    store.flush().expect("flush the deletes");

    // Bounded and unslept: each pass is self-pacing, so the loop asks until the
    // footprint stops moving rather than waiting on a clock.
    let mut settled = 0;
    let mut last = footprint(&store);
    for _ in 0..MAX_TICKS {
        Store::maintain(&store).expect("a maintenance pass");
        let now = footprint(&store);
        settled = if now == last { settled + 1 } else { 0 };
        last = now;
        if settled == SETTLED_TICKS {
            break;
        }
    }
    assert_eq!(settled, SETTLED_TICKS, "the volume never went quiescent");

    let drained = last;
    let live = RECORDS - DELETED;
    // The live share plus a segment of slack: the tail the volume is still
    // writing into is reserved whole and nothing retires it.
    let ceiling = filled / (RECORDS / live) as u64 + SEGMENT_BYTES;
    assert!(
        drained < ceiling,
        "ticking reclaimed nothing: {filled} bytes filled, {drained} after draining, \
         expected under {ceiling}"
    );
}

// a volume nobody ticks keeps every dead byte, which is what the driver exists to stop
#[test]
fn untouched_volume_keeps_dead_space() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().join("volume"), config(), TAPE_COLUMNS)
        .expect("open the volume");
    let cf = SliceCol::CF_NAME;

    for at in 0..RECORDS {
        store
            .put(cf, &slice_key(at), &payload(at as u64))
            .expect("put");
    }
    store.flush().expect("flush the writes");
    let filled = footprint(&store);

    for at in 0..DELETED {
        store.delete(cf, &slice_key(at)).expect("delete");
    }
    store.flush().expect("flush the deletes");

    assert!(
        footprint(&store) >= filled,
        "deleting alone gave space back, so the reclaim test proves nothing"
    );
}
