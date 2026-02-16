use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use tracing_subscriber::EnvFilter;

static LOG_FILE: OnceLock<PathBuf> = OnceLock::new();
static LOG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct FileOut {
    path: PathBuf,
}

impl Write for FileOut {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn make_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("sim-e2e")
        .join("sim.log")
}

pub fn init_log() {
    let path = LOG_FILE.get_or_init(|| {
        let path = make_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path);
        path
    });

    let writer_path = path.clone();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("sim_e2e=info,tape_node=debug")),
        )
        .with_ansi(false)
        .with_writer(move || FileOut {
            path: writer_path.clone(),
        })
        .try_init();

    append_log(&format!("sim log init pid={}", std::process::id()));
}

pub fn log_path() -> Option<&'static Path> {
    LOG_FILE.get().map(PathBuf::as_path)
}

pub fn read_log() -> Option<String> {
    let path = log_path()?;
    fs::read_to_string(path).ok()
}

pub fn append_log(msg: &str) {
    let Some(path) = log_path() else {
        return;
    };
    let lock = LOG_LOCK.get_or_init(|| Mutex::new(()));
    let Ok(_guard) = lock.lock() else {
        return;
    };
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "[{ts}] {msg}");
    }
}
