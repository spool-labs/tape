//! The reel's wider reads, held against the naive defaults they replace
//!
//! Every method the bridge overrides has a default on the store trait that says
//! the same thing one key at a time. A backend is free to be faster; it is not
//! free to answer differently, so each case asks both and compares.

use reel::sync::tension::block_on;
use reel_bridge::{bench_config, MetaBulkStore, ReelBridge, TAPE_COLUMNS};
use store::{Store, Value};

/// Values as plain vectors, so an expectation can be written as bytes
fn owned(values: Vec<Option<Value>>) -> Vec<Option<Vec<u8>>> {
    values
        .into_iter()
        .map(|held| held.map(Value::into_vec))
        .collect()
}
use tempfile::TempDir;

/// A bulk family, whose keys the reel is told are 34 bytes wide
const BULK_CF: &str = "slice";

/// Bytes a key of that family occupies
const BULK_KEY_LEN: usize = 34;

/// A metadata family, which declares no key width
const META_CF: &str = "tape";

/// Records written before anything is asked, enough that a batch is a batch
const RECORDS: usize = 24;

/// The segment size a bench arm opens with, which the shipped allocation chunk fits in
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

fn bulk_key(index: usize) -> Vec<u8> {
    let mut key = vec![0u8; BULK_KEY_LEN];
    key[..2].copy_from_slice(&7u16.to_be_bytes());
    key[2..10].copy_from_slice(&(index as u64).to_be_bytes());
    key
}

fn meta_key(index: usize) -> Vec<u8> {
    format!("tape-{index:04}").into_bytes()
}

fn payload(index: usize) -> Vec<u8> {
    vec![index as u8; 512 + index]
}

/// Fill one family and answer the keys written, in the order written
fn fill(store: &impl Store, cf: &str, key_of: fn(usize) -> Vec<u8>) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(RECORDS);
    for index in 0..RECORDS {
        let key = key_of(index);
        store.put(cf, &key, &payload(index)).expect("put");
        keys.push(key);
    }
    keys
}

/// Ask every way there is and hold the answers against each other
fn agrees(store: &impl Store, cf: &str, keys: &[Vec<u8>], prefix: &[u8]) {
    let asked: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();

    let one_at_a_time: Vec<Option<Vec<u8>>> = asked
        .iter()
        .map(|key| store.get(cf, key).expect("get").map(Value::into_vec))
        .collect();
    assert_eq!(owned(store.get_many(cf, &asked).expect("get_many")), one_at_a_time);
    assert_eq!(
        owned(block_on(store.get_many_wait(cf, &asked)).expect("get_many_wait")),
        one_at_a_time,
    );
    for (at, key) in asked.iter().enumerate() {
        assert_eq!(
            block_on(store.get_wait(cf, key)).expect("get_wait").map(Value::into_vec),
            one_at_a_time[at],
        );
    }

    // A window inside the value, one running past its end, and one starting
    // past it, since a ranged read clamps rather than refusing.
    let whole = one_at_a_time[0].clone().expect("a value");
    for (offset, len) in [(0u64, whole.len()), (8, 16), (4, whole.len() * 2), (0, 0)] {
        let window = whole[offset as usize..(offset as usize + len).min(whole.len())].to_vec();
        assert_eq!(
            store.get_range(cf, asked[0], offset, len).expect("range").map(Value::into_vec),
            Some(window.clone()),
            "range at {offset} for {len}",
        );
        assert_eq!(
            block_on(store.get_range_wait(cf, asked[0], offset, len))
                .expect("range_wait")
                .map(Value::into_vec),
            Some(window),
        );
    }
    assert_eq!(
        store
            .get_range(cf, asked[0], whole.len() as u64 + 1, 4)
            .expect("range past the end")
            .map(Value::into_vec),
        Some(Vec::new()),
    );

    assert_eq!(
        store.count_prefix(cf, prefix).expect("count_prefix"),
        store.iter_keys_prefix(cf, prefix).expect("keys").len() as u64,
    );

    // A key nothing wrote answers nothing, however it is asked.
    let missing = key_of_missing(cf);
    assert!(store.get(cf, &missing).expect("get").is_none());
    assert_eq!(owned(store.get_many(cf, &[&missing]).expect("get_many")), vec![None]);
    assert_eq!(
        store.get_range(cf, &missing, 0, 4).expect("range").map(Value::into_vec),
        None,
        "a missing key answers nothing rather than no bytes",
    );
}

fn key_of_missing(cf: &str) -> Vec<u8> {
    match cf {
        BULK_CF => bulk_key(RECORDS + 1),
        _ => meta_key(RECORDS + 1),
    }
}

// the bridge answers its wider reads the same as one get at a time
#[test]
fn bridge_agrees() {
    let dir = TempDir::new().expect("dir");
    let store = ReelBridge::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

    let keys = fill(&store, BULK_CF, bulk_key);
    agrees(&store, BULK_CF, &keys, &7u16.to_be_bytes());
}

// the split store routes its wider reads to the half that holds the family
#[test]
fn split_routes() {
    let dir = TempDir::new().expect("dir");
    let store = MetaBulkStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

    let bulk = fill(&store, BULK_CF, bulk_key);
    agrees(&store, BULK_CF, &bulk, &7u16.to_be_bytes());

    let meta = fill(&store, META_CF, meta_key);
    agrees(&store, META_CF, &meta, b"tape-");
}

// an awaited write lands where the blocking one does
#[test]
fn awaited_writes() {
    let dir = TempDir::new().expect("dir");
    let store = MetaBulkStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

    let key = bulk_key(1);
    block_on(store.put_wait(BULK_CF, &key, &payload(1))).expect("put_wait");
    assert_eq!(store.get(BULK_CF, &key).expect("get").map(Value::into_vec), Some(payload(1)));

    let mut batch = store::WriteBatch::new();
    let second = bulk_key(2);
    batch.put(BULK_CF, &second, &payload(2));
    batch.put(META_CF, &meta_key(2), &payload(2));
    block_on(store.write_batch_wait(batch)).expect("write_batch_wait");
    assert_eq!(
        store.get(BULK_CF, &second).expect("get").map(Value::into_vec),
        Some(payload(2)),
    );
    assert_eq!(
        store.get(META_CF, &meta_key(2)).expect("get").map(Value::into_vec),
        Some(payload(2)),
    );
}

/// Every key a sweep hands out, paged until the mark comes back empty
fn swept(store: &impl Store, cf: &str, page: usize) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    let mut mark: Option<Vec<u8>> = None;
    loop {
        let (rows, next) = store.sweep(cf, mark.as_deref(), page).expect("sweep");
        for (key, _) in rows {
            seen.push(key);
        }
        match next {
            Some(next) => mark = Some(next),
            None => return seen,
        }
    }
}

// a sweep covers the family whatever page it is asked for, on either backend
#[test]
fn sweep_covers() {
    let dir = TempDir::new().expect("dir");
    let store = ReelBridge::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");
    let keys = fill(&store, BULK_CF, bulk_key);

    for page in [1usize, 7, RECORDS * 2] {
        let mut seen = swept(&store, BULK_CF, page);
        seen.sort();
        let mut wrote = keys.clone();
        wrote.sort();
        assert_eq!(seen, wrote, "page {page} lost or repeated keys");
    }
}

// a mark from nowhere starts the sweep over rather than answering nonsense
#[test]
fn sweep_refuses_foreign() {
    let dir = TempDir::new().expect("dir");
    let store = MetaBulkStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");
    let keys = fill(&store, BULK_CF, bulk_key);

    let (rows, _) = store
        .sweep(BULK_CF, Some(b"not a mark this store minted"), RECORDS * 2)
        .expect("sweep");
    assert_eq!(rows.len(), keys.len(), "a foreign mark should start over");
}
