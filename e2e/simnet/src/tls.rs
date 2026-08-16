use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Result};

/// Ports this process already handed out but has not necessarily bound yet.
static ASSIGNED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

/// Ports reserved per harness process.
const RANGE_SIZE: u64 = 400;

/// First port of the harness range, below the ephemeral range of the platform.
///
/// A pick binds a port, drops the listener and hands the number back for the
/// caller to bind a moment later. A range that overlaps the ephemeral one loses
/// that window to an outgoing connection of this very process.
///
/// Linux hands out from 32768 (`net.ipv4.ip_local_port_range`), so 30000 sits
/// inside it and a node lost 39206 to a peer connection between the pick and
/// the bind. 20000 is clear of that, and of the fixed `base_port` block at
/// 19000 a fixture uses when it does not ask for a pick.
#[cfg(target_os = "linux")]
const RANGE_BASE: u64 = 20_000;

/// macOS hands out from 49152, so the original range is already clear there.
#[cfg(not(target_os = "linux"))]
const RANGE_BASE: u64 = 30_000;

/// Pick a loopback bind address that stays stable until the caller binds it.
///
/// Asking the OS for `:0` and dropping the listener races concurrent
/// harnesses: freed ephemeral ports are reused most-recent-first, so a second
/// simnet on the same host steals them before the node binds. Instead each
/// process probes its own PID-keyed range and remembers what it handed out, so
/// no two picks in one process ever share a port.
pub fn pick_bind() -> Result<std::net::SocketAddr> {
    let pid = std::process::id() as u64;
    let base = RANGE_BASE + (pid % 25) * RANGE_SIZE;

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
