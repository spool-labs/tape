use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use tracing_subscriber::EnvFilter;

static LOG: OnceLock<(PathBuf, Arc<File>)> = OnceLock::new();

/// One log per test binary, so binaries running side by side keep their own
fn make_path() -> PathBuf {
    let binary = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.file_stem().map(|stem| stem.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "sim".to_string());
    let name = binary.rsplit_once('-').map_or(binary.as_str(), |(name, _)| name);
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("sim-e2e")
        .join(format!("{name}.log"))
}

pub fn init_log() {
    let (_, file) = LOG.get_or_init(|| {
        let path = make_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        // threads share the handle, so each line lands whole at the end
        let _ = fs::write(&path, b"");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open sim log");
        (path, Arc::new(file))
    });

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("tape_e2e_simnet=trace,tape_node=trace")),
        )
        .with_ansi(false)
        .with_writer(file.clone())
        .try_init();

    append_log(&format!("sim log init pid={}", std::process::id()));
}

pub fn log_path() -> Option<&'static Path> {
    LOG.get().map(|(path, _)| path.as_path())
}

pub fn read_log() -> Option<String> {
    let path = log_path()?;
    fs::read_to_string(path).ok()
}

pub fn append_log(msg: &str) {
    let Some((_, file)) = LOG.get() else {
        return;
    };
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let _ = (&**file).write_all(format!("[{ts}] {msg}\n").as_bytes());
}
