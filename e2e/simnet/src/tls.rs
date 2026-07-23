use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::Mutex;

use anyhow::{bail, Result};

/// Ports this process already handed out but has not necessarily bound yet.
static ASSIGNED: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// Candidate ports probed per pick before giving up.
const PICK_ATTEMPTS: u64 = 200;

/// Ports reserved per harness process.
const RANGE_SIZE: u64 = 400;

/// Pick a loopback bind address that stays stable until the caller binds it.
///
/// Asking the OS for `:0` and dropping the listener races concurrent
/// harnesses: freed ephemeral ports are reused most-recent-first, so a second
/// simnet on the same host steals them before the node binds. Instead each
/// process probes its own PID-keyed range below the OS ephemeral range
/// (49152+), and remembers what it handed out so two fixtures in one process
/// never receive the same port.
pub fn pick_bind(off: u64) -> Result<std::net::SocketAddr> {
    let pid = std::process::id() as u64;
    let base = 20_000 + (pid % 70) * RANGE_SIZE;

    let mut assigned = ASSIGNED.lock().expect("port set lock");
    let assigned = assigned.get_or_insert_with(HashSet::new);

    for attempt in 0..PICK_ATTEMPTS {
        let port = (base + (off.wrapping_mul(7) + attempt) % RANGE_SIZE) as u16;
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
