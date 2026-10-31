//! The block file: one writer connection for the fills and the prune, and a
//! pool of read-only connections for the requests. Both run their work on the
//! blocking pool, so SQLite never holds a runtime thread.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use tokio::task::spawn_blocking;

use crate::store::SCHEMA;
use crate::store::error::{StoreError, StoreResult};

/// The requests share this many read connections
const READERS: usize = 8;

/// A connection waits this long on another's lock before it fails
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// SQLite cuts the WAL back to this size once a checkpoint has emptied it. A
/// bootstrap fill and a prune both grow the WAL past it.
const WAL_SIZE_LIMIT: i64 = 64 * 1024 * 1024;

/// The writer and the read pool, cheap to clone
#[derive(Clone)]
pub struct Database {
    connections: Arc<Connections>,
}

/// Fields drop in order, so the writer closes last. The last connection to
/// close checkpoints the WAL and removes it, and a read-only one can't.
struct Connections {
    readers: Vec<Mutex<Connection>>,
    writer: Mutex<Connection>,
    next_reader: AtomicUsize,
}

impl Database {
    /// Open the database at `path`, creating every table it lacks
    pub fn try_open(path: &Path) -> StoreResult<Self> {
        let writer = open(path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut readers = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let reader = Connection::open_with_flags(path, flags)?;
            reader.busy_timeout(BUSY_TIMEOUT)?;
            readers.push(Mutex::new(reader));
        }
        Ok(Self::with_connections(writer, readers))
    }

    /// An empty database in memory. It has no file to open again, so reads share the writer.
    pub fn try_in_memory() -> StoreResult<Self> {
        let writer = Connection::open_in_memory()?;
        writer.execute_batch(SCHEMA)?;
        Ok(Self::with_connections(writer, Vec::new()))
    }

    fn with_connections(writer: Connection, readers: Vec<Mutex<Connection>>) -> Self {
        Self {
            connections: Arc::new(Connections {
                readers,
                writer: Mutex::new(writer),
                next_reader: AtomicUsize::new(0),
            }),
        }
    }

    /// Run `work` on the writer, on the blocking pool
    pub async fn write<Value, Work>(&self, work: Work) -> StoreResult<Value>
    where
        Value: Send + 'static,
        Work: FnOnce(&mut Connection) -> StoreResult<Value> + Send + 'static,
    {
        let connections = self.connections.clone();
        spawn_blocking(move || work(&mut *connections.writer()?)).await?
    }

    /// Run `work` on a read connection, on the blocking pool
    pub async fn read<Value, Work>(&self, work: Work) -> StoreResult<Value>
    where
        Value: Send + 'static,
        Work: FnOnce(&Connection) -> StoreResult<Value> + Send + 'static,
    {
        let connections = self.connections.clone();
        spawn_blocking(move || work(&*connections.reader()?)).await?
    }
}

impl Connections {
    fn writer(&self) -> StoreResult<MutexGuard<'_, Connection>> {
        self.writer.lock().map_err(|_| StoreError::Poisoned)
    }

    /// A read connection, the first free one from a rotating start
    fn reader(&self) -> StoreResult<MutexGuard<'_, Connection>> {
        if self.readers.is_empty() {
            return self.writer();
        }
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed);
        for step in 0..self.readers.len() {
            if let Ok(reader) = self.readers[(start + step) % self.readers.len()].try_lock() {
                return Ok(reader);
            }
        }
        self.readers[start % self.readers.len()]
            .lock()
            .map_err(|_| StoreError::Poisoned)
    }
}

/// Open the database at `path` for writing, creating every table it lacks.
/// SQLite checkpoints the WAL on its own as commits grow it.
fn open(path: &Path) -> StoreResult<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;
    // WAL lets the read connections run while the writer commits
    connection.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
    connection.pragma_update(None, "journal_size_limit", WAL_SIZE_LIMIT)?;
    connection.execute_batch(SCHEMA)?;
    Ok(connection)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use tokio::time::timeout;

    use super::*;

    /// Longer than any read beside a held writer should take
    const PROMPT: Duration = Duration::from_secs(2);

    fn block_count(connection: &Connection) -> StoreResult<i64> {
        Ok(connection.query_row("SELECT count(*) FROM blocks", [], |row| row.get(0))?)
    }

    // a read goes on while the writer holds an open transaction
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_split() {
        let directory = std::env::temp_dir().join(format!("rpc-cache-readers-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temp directory");
        let database = Database::try_open(&directory.join("blocks.db")).expect("open");
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let writer = database.clone();
        let holder = tokio::spawn(async move {
            writer
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    transaction.execute("INSERT INTO blocks (slot) VALUES (1)", [])?;
                    held_tx.send(()).ok();
                    release_rx.recv().ok();
                    transaction.commit()?;
                    Ok(())
                })
                .await
        });
        held_rx.recv().expect("writer held");

        let read = timeout(PROMPT, database.read(block_count)).await;
        release_tx.send(()).expect("release writer");
        holder.await.expect("join holder").expect("holder");

        assert_eq!(read.expect("read waited on the writer").expect("count"), 0);
        assert_eq!(database.read(block_count).await.expect("count"), 1);
        std::fs::remove_dir_all(&directory).ok();
    }
}
