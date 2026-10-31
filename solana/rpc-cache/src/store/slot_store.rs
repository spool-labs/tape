//! The slot store: the block file, with the most recently read blocks in memory

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use moka::future::Cache as MokaCache;
use moka::policy::EvictionPolicy;
use tracing::{info, warn};

use crate::store::block::{CachedBlock, claim, delete_below, read_block, stored_slots, write_block, write_skipped};
use crate::store::database::Database;
use crate::store::error::StoreResult;

/// A tombstone's weight in memory: small next to a block, and large enough
/// that a run of skips can't fill the layer
const SKIPPED_WEIGHT: u32 = 64;

/// One prune transaction deletes this many blocks, so fills wait on the writer only briefly
const PRUNE_BATCH: u64 = 10_000;

/// Filtered blocks by slot, in a SQLite file with an LRU layer in memory
pub struct SlotStore {
    memory: MokaCache<u64, CachedBlock>,
    database: Database,
}

/// What the memory layer holds
pub struct MemoryUsage {
    pub entries: u64,
    pub bytes: u64,
}

impl SlotStore {
    /// Open the block file at `path` and keep up to `max_bytes` of blocks in
    /// memory. A file filled under another fingerprint is emptied before any
    /// request can read it.
    pub async fn try_open(path: &Path, max_bytes: u64, fingerprint: String) -> StoreResult<Self> {
        let database = Database::try_open(path)?;
        let deleted = database.write(move |connection| claim(connection, &fingerprint)).await?;
        if deleted > 0 {
            info!(deleted, "slot store blocks were filtered under another fingerprint; emptied the file");
        }
        Ok(Self::with_database(database, max_bytes))
    }

    /// An empty store in memory, for an instance that serves no blocks
    pub fn try_in_memory(max_bytes: u64) -> StoreResult<Self> {
        Ok(Self::with_database(Database::try_in_memory()?, max_bytes))
    }

    fn with_database(database: Database, max_bytes: u64) -> Self {
        let memory = MokaCache::builder()
            .weigher(block_weight)
            .max_capacity(max_bytes)
            // A replay reads each block once, and TinyLFU, moka's default, turns such a block away
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self { memory, database }
    }

    /// The block at `slot`, from memory or else the file
    pub async fn get(&self, slot: u64) -> StoreResult<Option<CachedBlock>> {
        if let Some(block) = self.memory.get(&slot).await {
            return Ok(Some(block));
        }
        let stored = self.database.read(move |connection| read_block(connection, slot)).await?;
        if let Some(block) = &stored {
            self.memory.insert(slot, block.clone()).await;
        }
        Ok(stored)
    }

    /// Whether memory holds the slot. Every block stored since the file was
    /// last listed lands there, until newer blocks push it out.
    pub fn is_in_memory(&self, slot: u64) -> bool {
        self.memory.contains_key(&slot)
    }

    /// Keep a block in memory and write it to the file
    pub async fn insert(&self, slot: u64, block: CachedBlock) -> StoreResult<()> {
        self.memory.insert(slot, block.clone()).await;
        self.database.write(move |connection| write_block(connection, slot, &block)).await
    }

    /// Store these slots as skipped. A slot that already holds a block keeps it.
    pub async fn insert_skipped(&self, slots: Vec<u64>) -> StoreResult<()> {
        for &slot in &slots {
            self.memory.entry(slot).or_insert(CachedBlock::Skipped).await;
        }
        self.database.write(move |connection| write_skipped(connection, &slots)).await
    }

    /// Fill a slot `get` missed, with one fetch shared by every caller asking
    /// for it. A block the file can't take is still served from memory.
    pub async fn fill<Fetch>(&self, slot: u64, fetch: Fetch) -> Result<CachedBlock, Arc<String>>
    where
        Fetch: Future<Output = Result<CachedBlock, String>>,
    {
        let fill = async {
            let block = fetch.await?;
            let stored = block.clone();
            let written = self.database.write(move |connection| write_block(connection, slot, &stored)).await;
            if let Err(error) = written {
                warn!(slot, %error, "slot store write failed; the block stays in memory only");
            }
            Ok(block)
        };
        self.memory.try_get_with(slot, fill).await
    }

    /// List the file's slots in `from..=to`
    pub async fn stored_in(&self, from: u64, to: u64) -> StoreResult<HashSet<u64>> {
        self.database.read(move |connection| stored_slots(connection, from, to)).await
    }

    /// Delete the file's blocks below `floor`, a batch per transaction, and
    /// return how many went. Memory keeps its copies until newer blocks push them out.
    pub async fn prune_below(&self, floor: u64) -> StoreResult<u64> {
        let mut total = 0;
        loop {
            let deleted = self
                .database
                .write(move |connection| delete_below(connection, floor, PRUNE_BATCH))
                .await?;
            total += deleted;
            if deleted < PRUNE_BATCH {
                return Ok(total);
            }
        }
    }

    /// Count the blocks and bytes in memory, once its pending work has run
    pub async fn memory_usage(&self) -> MemoryUsage {
        self.memory.run_pending_tasks().await;
        MemoryUsage {
            entries: self.memory.entry_count(),
            bytes: self.memory.weighted_size(),
        }
    }
}

fn block_weight(_slot: &u64, block: &CachedBlock) -> u32 {
    match block {
        CachedBlock::Present { base64, json } => u32::try_from(base64.len() + json.len()).unwrap_or(u32::MAX),
        CachedBlock::Skipped => SKIPPED_WEIGHT,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;

    use super::*;

    const MAX_BYTES: u64 = 1 << 20;

    fn present(tag: &str) -> CachedBlock {
        CachedBlock::Present {
            base64: Bytes::from(format!("base64-{tag}")),
            json: Bytes::from(format!("json-{tag}")),
        }
    }

    /// An empty directory of its own for one test's block file
    fn temp_directory(test: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!("rpc-cache-{test}-{}", std::process::id()));
        std::fs::remove_dir_all(&directory).ok();
        std::fs::create_dir_all(&directory).expect("temp directory");
        directory
    }

    // a reopened file serves the blocks and skips stored before the restart
    #[tokio::test]
    async fn survives_restart() {
        let directory = temp_directory("restart");
        let path = directory.join("blocks.db");
        let store = SlotStore::try_open(&path, MAX_BYTES, "devnet".to_string()).await.expect("open");
        store.insert(10, present("10")).await.expect("insert");
        store.insert_skipped(vec![11]).await.expect("insert skip");
        drop(store);

        let reopened = SlotStore::try_open(&path, MAX_BYTES, "devnet".to_string()).await.expect("reopen");

        assert_eq!(reopened.get(10).await.expect("get"), Some(present("10")));
        assert_eq!(reopened.get(11).await.expect("get"), Some(CachedBlock::Skipped));
        assert_eq!(reopened.get(12).await.expect("get"), None);
        assert_eq!(reopened.stored_in(0, 100).await.expect("list"), HashSet::from([10, 11]));
        std::fs::remove_dir_all(&directory).ok();
    }

    // a file reopened under another fingerprint starts empty
    #[tokio::test]
    async fn other_fingerprint() {
        let directory = temp_directory("fingerprint");
        let path = directory.join("blocks.db");
        let store = SlotStore::try_open(&path, MAX_BYTES, "devnet".to_string()).await.expect("open");
        store.insert(1, present("1")).await.expect("insert");
        drop(store);

        let reopened = SlotStore::try_open(&path, MAX_BYTES, "localnet".to_string()).await.expect("reopen");

        assert_eq!(reopened.get(1).await.expect("get"), None);
        std::fs::remove_dir_all(&directory).ok();
    }

    // a fill runs once, and the file keeps its block
    #[tokio::test]
    async fn fill_once() {
        let store = SlotStore::try_in_memory(MAX_BYTES).expect("open");

        let filled = store.fill(7, async { Ok(present("7")) }).await;
        let refilled = store.fill(7, async { Err("filled twice".to_string()) }).await;

        assert_eq!(filled.ok(), Some(present("7")));
        assert_eq!(refilled.ok(), Some(present("7")));
        assert_eq!(store.stored_in(0, 10).await.expect("list"), HashSet::from([7]));
    }

    // a full memory layer keeps each new block a replay reads once
    #[tokio::test]
    async fn new_blocks() {
        let block = CachedBlock::Present {
            base64: Bytes::from(vec![b'b'; 256]),
            json: Bytes::from(vec![b'j'; 256]),
        };
        let store = SlotStore::try_in_memory(8 * 512).expect("open");
        for slot in 0..8 {
            store.insert(slot, block.clone()).await.expect("insert");
            for _ in 0..3 {
                store.get(slot).await.expect("get");
            }
        }

        for slot in 8..64 {
            store.insert(slot, block.clone()).await.expect("insert");
            store.memory.run_pending_tasks().await;
            assert!(store.is_in_memory(slot), "slot {slot} turned away");
        }
    }

    // a skip leaves a block already in memory alone
    #[tokio::test]
    async fn skip_keeps_block() {
        let store = SlotStore::try_in_memory(MAX_BYTES).expect("open");
        store.insert(5, present("5")).await.expect("insert");

        store.insert_skipped(vec![5, 6]).await.expect("insert skips");

        assert_eq!(store.get(5).await.expect("get"), Some(present("5")));
        assert_eq!(store.get(6).await.expect("get"), Some(CachedBlock::Skipped));
    }

    // a prune larger than one batch deletes every row below the floor
    #[tokio::test]
    async fn prune_batches() {
        let store = SlotStore::try_in_memory(MAX_BYTES).expect("open");
        let rows = 2 * PRUNE_BATCH + 500;
        let slots: Vec<u64> = (0..rows).collect();
        store.insert_skipped(slots).await.expect("insert skips");

        let pruned = store.prune_below(rows - 100).await.expect("prune");

        assert_eq!(pruned, rows - 100);
        assert_eq!(store.stored_in(0, rows).await.expect("list").len(), 100);
    }
}
