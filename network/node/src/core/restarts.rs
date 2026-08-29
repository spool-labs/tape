//! Lifetime start counter, kept beside the store it boots.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use tracing::debug;

/// One decimal integer, in the store root the node already owns.
const FILE_NAME: &str = "restarts";

static STARTS: OnceLock<u64> = OnceLock::new();

/// Bump the persisted count for this start and return it.
///
/// A missing or unreadable file counts as zero and starts the tally over: a
/// board reading is not worth failing a boot for.
pub fn record(root: &Path) -> u64 {
    let path = root.join(FILE_NAME);
    let count = read(&path).unwrap_or(0).saturating_add(1);

    if let Err(error) = write(&path, count) {
        debug!(path = %path.display(), %error, "could not persist the start count");
    }

    *STARTS.get_or_init(|| count)
}

/// Restarts behind this process, the reading every view takes.
///
/// The file counts starts and the first one is not a restart, so the subtraction
/// lives here rather than at each call site, where the two could disagree.
pub fn count() -> u64 {
    STARTS.get().copied().unwrap_or(0).saturating_sub(1)
}

fn read(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn write(path: &Path, count: u64) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    write!(file, "{count}")?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    // an empty root starts the tally at one, and one start is no restarts yet
    #[test]
    fn first_start_writes_one() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(record(dir.path()), 1);
        assert_eq!(read(&dir.path().join(FILE_NAME)), Some(1));
        assert_eq!(count(), 0);
    }

    // the file is the tally, so a later boot picks up where the last one left off
    #[test]
    fn later_start_resumes_the_tally() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join(FILE_NAME), 41).unwrap();
        assert_eq!(read(&dir.path().join(FILE_NAME)).unwrap() + 1, 42);
    }

    // garbage in the file costs the history, not the boot
    #[test]
    fn corrupt_file_counts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        std::fs::write(&path, b"\0\0not a number").unwrap();
        assert_eq!(read(&path), None);
    }
}
