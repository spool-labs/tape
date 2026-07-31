//! Reading and writing the bootstrap cache file.
//!
//! A file that cannot be read is treated as absent rather than as an error.
//! The worst a missing cache costs is the slow path, so refusing to run
//! because of a damaged file would trade a small saving for a hard failure.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use super::{BootstrapState, NetworkKey};

/// Rejects foreign or truncated files before the decoder ever sees them.
const MAGIC: [u8; 4] = *b"TPBS";

/// Bumping this makes older files unreadable instead of misread.
const VERSION: u8 = 1;

/// Set to any value to bypass both load and store.
pub const BYPASS_ENV: &str = "TAPE_NO_BOOTSTRAP_CACHE";

/// Distinguishes temp files written by one process at the same moment. The
/// process id alone is not enough, since two threads share it and would
/// otherwise rename each other's file away mid-write.
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("bootstrap cache io: {0}")]
    Io(#[from] std::io::Error),

    #[error("bootstrap cache encode: {0}")]
    Encode(String),

    #[error("bootstrap cache path has no parent directory")]
    NoParent,
}

/// A handle to the cache file. A store with no path is a working no-op, which
/// is how the bypass and any caller without a home directory are served.
pub struct BootstrapStore {
    path: Option<PathBuf>,
}

impl BootstrapStore {
    /// Open the cache at a path, honouring the bypass environment variable.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        match std::env::var_os(BYPASS_ENV) {
            Some(_) => Self::disabled(),
            None => Self {
                path: Some(path.into()),
            },
        }
    }

    /// A store that never reads or writes anything.
    pub fn disabled() -> Self {
        Self { path: None }
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn is_enabled(&self) -> bool {
        self.path.is_some()
    }

    /// Read the cache, or nothing at all if it is missing, damaged, written by
    /// another version, or belongs to a different chain.
    pub fn load(&self, expected: &NetworkKey) -> Option<BootstrapState> {
        let state = self.load_any()?;
        match &state.network == expected {
            true => Some(state),
            false => None,
        }
    }

    /// Read the cache without checking which chain it belongs to.
    ///
    /// Only for showing an operator what is on disk. The normal path must go
    /// through the checked load.
    pub fn load_any(&self) -> Option<BootstrapState> {
        let bytes = fs::read(self.path.as_ref()?).ok()?;
        let body = bytes.strip_prefix(&MAGIC)?;
        let (version, body) = body.split_first()?;
        if *version != VERSION {
            return None;
        }
        wincode::deserialize(body).ok()
    }

    /// Replace the cache wholesale.
    ///
    /// The temp file is created beside the target so the rename stays within
    /// one filesystem and therefore stays atomic. Concurrent writers race and
    /// the last one wins, which is acceptable for hints and reputation.
    pub fn save(&self, state: &BootstrapState) -> Result<(), StoreError> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        let parent = path.parent().ok_or(StoreError::NoParent)?;
        fs::create_dir_all(parent)?;

        let encoded =
            wincode::serialize(state).map_err(|error| StoreError::Encode(format!("{error:?}")))?;
        let mut bytes = Vec::with_capacity(MAGIC.len() + 1 + encoded.len());
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&encoded);

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = path.with_extension(format!("tmp.{}.{}", std::process::id(), sequence));
        fs::write(&temp, &bytes)?;
        match fs::rename(&temp, path) {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = fs::remove_file(&temp);
                Err(error.into())
            }
        }
    }

    /// Delete the cache. Missing is success, since the caller wanted it gone.
    pub fn clear(&self) -> Result<(), StoreError> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tape_crypto::Address;
    use tape_crypto::hash::Hash;

    use super::*;
    use crate::bootstrap::{PeerHealth, Prediction};
    use tape_core::types::EpochNumber;

    fn key() -> NetworkKey {
        NetworkKey {
            program_id: Address::new_unique(),
            genesis: Hash([3u8; 32]),
        }
    }

    fn populated(network: NetworkKey) -> BootstrapState {
        let mut state = BootstrapState::new(network);
        state.fetched_at = 1_700_000_000;
        state.prediction = Some(Prediction {
            epoch: EpochNumber(9),
            total_groups: 50,
            epoch_start: 1_699_999_000,
            epoch_duration: 3_600,
        });
        state.peer_mut(Address::new_unique()).record_success(5, 21.0);
        let flaky = Address::new_unique();
        for tick in 0..4 {
            state.peer_mut(flaky).record_failure(tick);
        }
        state
    }

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("tape-bootstrap-{}-{}", std::process::id(), name));
        path
    }

    // what goes in comes back out unchanged
    #[test]
    fn round_trip() {
        let path = temp_path("round-trip");
        let store = BootstrapStore::new(&path);
        let network = key();
        let state = populated(network);

        store.save(&state).expect("save");
        let loaded = store.load(&network).expect("load");
        assert_eq!(loaded, state);

        let flaky = loaded
            .peers
            .iter()
            .find(|record| record.consecutive_failures == 4)
            .expect("flaky peer");
        assert!(matches!(flaky.health, PeerHealth::Quarantined { .. }));

        store.clear().expect("clear");
        assert!(store.load(&network).is_none());
    }

    // a cache from another chain must not be adopted
    #[test]
    fn wrong_network_is_ignored() {
        let path = temp_path("wrong-network");
        let store = BootstrapStore::new(&path);
        let network = key();
        store.save(&populated(network)).expect("save");

        assert!(store.load(&key()).is_none());
        assert!(store.load_any().is_some());

        store.clear().expect("clear");
    }

    // a file we did not write is not parsed
    #[test]
    fn foreign_file_is_ignored() {
        let path = temp_path("foreign");
        let mut file = fs::File::create(&path).expect("create");
        file.write_all(b"not a tape cache at all").expect("write");
        drop(file);

        let store = BootstrapStore::new(&path);
        assert!(store.load_any().is_none());
        store.clear().expect("clear");
    }

    // a future format is rejected rather than misread
    #[test]
    fn wrong_version_is_ignored() {
        let path = temp_path("version");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION + 1);
        bytes.extend_from_slice(b"whatever follows");
        fs::write(&path, &bytes).expect("write");

        let store = BootstrapStore::new(&path);
        assert!(store.load_any().is_none());
        store.clear().expect("clear");
    }

    // a half written file is absent, not fatal
    #[test]
    fn truncated_file_is_ignored() {
        let path = temp_path("truncated");
        let store = BootstrapStore::new(&path);
        let network = key();
        store.save(&populated(network)).expect("save");

        let bytes = fs::read(&path).expect("read");
        fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");

        assert!(store.load(&network).is_none());
        store.clear().expect("clear");
    }

    // a store with no path silently does nothing
    #[test]
    fn disabled_store_is_a_noop() {
        let store = BootstrapStore::disabled();
        let network = key();
        assert!(!store.is_enabled());
        assert!(store.path().is_none());
        store.save(&populated(network)).expect("save");
        assert!(store.load(&network).is_none());
        store.clear().expect("clear");
    }

    // the last writer wins and the file always stays parseable
    #[test]
    fn concurrent_writes_leave_a_readable_file() {
        let path = temp_path("concurrent");
        let network = key();
        let first = populated(network);
        let mut second = populated(network);
        second.fetched_at = 1_800_000_000;

        std::thread::scope(|scope| {
            for state in [&first, &second] {
                scope.spawn(|| {
                    let store = BootstrapStore::new(&path);
                    for _ in 0..20 {
                        store.save(state).expect("save");
                    }
                });
            }
        });

        let store = BootstrapStore::new(&path);
        let loaded = store.load(&network).expect("load");
        assert!(loaded.fetched_at == first.fetched_at || loaded.fetched_at == second.fetched_at);
        store.clear().expect("clear");
    }
}
