use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Result};

/// Ports this process already handed out but has not necessarily bound yet.
static ASSIGNED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

/// Ports reserved per harness process.
const RANGE_SIZE: u64 = 400;

/// Pick a loopback bind address that stays stable until the caller binds it.
///
/// Asking the OS for `:0` and dropping the listener races concurrent
/// harnesses: freed ephemeral ports are reused most-recent-first, so a second
/// simnet on the same host steals them before the node binds. Instead each
/// process probes its own PID-keyed range in 30000-39999, clear of the OS
/// ephemeral range (49152+) and remembers what it handed out, so no two picks
/// in one process ever share a port.
pub fn pick_bind() -> Result<std::net::SocketAddr> {
    let pid = std::process::id() as u64;
    let base = 30_000 + (pid % 25) * RANGE_SIZE;

    let mut assigned = ASSIGNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .expect("port set lock");

    for attempt in 0..RANGE_SIZE {
        let port = (base + attempt) as u16;
        if assigned.contains(&port) {
            continue;
        }
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            let addr = listener.local_addr()?;
            drop(listener);
            assigned.insert(port);
            return Ok(addr);
        }
    }

    bail!("no free port in the harness range {base}..{}", base + RANGE_SIZE)
}
