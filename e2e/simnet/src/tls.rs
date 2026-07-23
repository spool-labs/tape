use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

pub fn pick_bind(off: u64) -> Result<std::net::SocketAddr> {
    if let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") {
        let addr = listener.local_addr().context("read local addr")?;
        drop(listener);
        return Ok(addr);
    }

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("clock drift")?
        .as_nanos();
    let port = 20_000 + ((stamp + off as u128 * 9_973) % 20_000) as u16;
    Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
}
