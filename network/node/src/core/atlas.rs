//! Recent-traffic rings behind the atlas observe endpoint: peer transfers,
//! anonymous caller addresses, and freshly stored objects. Collection is off
//! unless at least one observer key is configured, so nodes that never serve
//! the display pay nothing.

use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::Address;
use tape_observe_api::{AtlasIp, AtlasObject, AtlasRecent, AtlasTransfer};

/// Entries kept per ring; a once-a-second poller sees far fewer between visits.
const RING_CAP: usize = 256;

pub struct AtlasBuffer {
    observers: Vec<NetworkTlsPubkey>,
    seq: AtomicU64,
    transfers: Mutex<VecDeque<(u64, AtlasTransfer)>>,
    ips: Mutex<VecDeque<(u64, AtlasIp)>>,
    objects: Mutex<VecDeque<(u64, AtlasObject)>>,
}

/// Parse configured observer keys (base58 Ed25519 public keys)
pub fn parse_observers(keys: &[String]) -> Result<Vec<NetworkTlsPubkey>, String> {
    let mut observers = Vec::with_capacity(keys.len());
    for key in keys {
        let address: Address = key
            .parse()
            .map_err(|error| format!("bad observer key {key}: {error}"))?;
        observers.push(NetworkTlsPubkey::new(address.into()));
    }
    Ok(observers)
}

impl AtlasBuffer {
    pub fn new(observers: Vec<NetworkTlsPubkey>) -> Self {
        Self {
            observers,
            seq: AtomicU64::new(0),
            transfers: Mutex::new(VecDeque::new()),
            ips: Mutex::new(VecDeque::new()),
            objects: Mutex::new(VecDeque::new()),
        }
    }

    /// Whether any observer is configured; every push is a no-op otherwise
    pub fn enabled(&self) -> bool {
        !self.observers.is_empty()
    }

    /// Whether this identity may read the atlas endpoint
    pub fn is_observer(&self, key: NetworkTlsPubkey) -> bool {
        self.observers.contains(&key)
    }

    /// Record one byte-moving peer call
    pub fn push_transfer(&self, peer: Address, op: &str, sent: bool, bytes: u64) {
        if !self.enabled() {
            return;
        }
        let entry = AtlasTransfer { peer: peer.to_string(), op: op.to_string(), sent, bytes };
        push(&self.transfers, self.next_seq(), entry);
    }

    /// Record one anonymous caller
    pub fn push_ip(&self, ip: IpAddr, write: bool) {
        if !self.enabled() || ip.is_loopback() || ip.is_unspecified() {
            return;
        }
        let entry = AtlasIp { ip: ip.to_string(), write };
        push(&self.ips, self.next_seq(), entry);
    }

    /// Record one freshly stored object
    pub fn push_object(&self, label: String, size: u64, kind: &str) {
        if !self.enabled() {
            return;
        }
        let entry = AtlasObject { label, size, kind: kind.to_string() };
        push(&self.objects, self.next_seq(), entry);
    }

    /// Everything newer than the caller's cursor, plus the cursor to resume
    /// from. The cursor is the newest sequence actually returned, so entries
    /// pushed mid-read are picked up by the next poll rather than skipped.
    pub fn recent(&self, after: u64) -> AtlasRecent {
        let (transfers, newest_transfer) = drain_after(&self.transfers, after);
        let (ips, newest_ip) = drain_after(&self.ips, after);
        let (objects, newest_object) = drain_after(&self.objects, after);
        AtlasRecent {
            seq: after.max(newest_transfer).max(newest_ip).max(newest_object),
            transfers,
            ips,
            objects,
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }
}

fn push<Entry>(ring: &Mutex<VecDeque<(u64, Entry)>>, seq: u64, entry: Entry) {
    let Ok(mut ring) = ring.lock() else {
        return;
    };
    ring.push_back((seq, entry));
    while ring.len() > RING_CAP {
        ring.pop_front();
    }
}

fn drain_after<Entry: Clone>(
    ring: &Mutex<VecDeque<(u64, Entry)>>,
    after: u64,
) -> (Vec<Entry>, u64) {
    let Ok(ring) = ring.lock() else {
        return (Vec::new(), after);
    };
    let mut entries = Vec::new();
    let mut newest = after;
    for (seq, entry) in ring.iter() {
        if *seq > after {
            newest = newest.max(*seq);
            entries.push(entry.clone());
        }
    }
    (entries, newest)
}

/// Shorten an address-like label for the display ticker
pub fn short_label(value: &str) -> String {
    if value.len() > 15 {
        format!("{}…{}", &value[..6], &value[value.len() - 6..])
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observer() -> NetworkTlsPubkey {
        NetworkTlsPubkey::new([7; 32])
    }

    // a buffer with no observers configured records nothing
    #[test]
    fn disabled_buffer() {
        let atlas = AtlasBuffer::new(Vec::new());

        atlas.push_ip("1.2.3.4".parse().expect("ip"), true);
        atlas.push_object("x".into(), 1, "track");

        let recent = atlas.recent(0);
        assert_eq!(recent.seq, 0);
        assert!(recent.ips.is_empty() && recent.objects.is_empty());
    }

    // the returned cursor resumes exactly where the last poll ended
    #[test]
    fn cursor_resume() {
        let atlas = AtlasBuffer::new(vec![observer()]);
        atlas.push_ip("1.2.3.4".parse().expect("ip"), true);
        atlas.push_ip("5.6.7.8".parse().expect("ip"), false);

        let first = atlas.recent(0);
        atlas.push_ip("9.9.9.9".parse().expect("ip"), true);
        let second = atlas.recent(first.seq);

        assert_eq!(first.ips.len(), 2);
        assert_eq!(second.ips.len(), 1);
        assert_eq!(second.ips[0].ip, "9.9.9.9");
        assert!(atlas.recent(second.seq).ips.is_empty());
    }

    // rings stay bounded at the cap
    #[test]
    fn ring_cap() {
        let atlas = AtlasBuffer::new(vec![observer()]);

        for _ in 0..(RING_CAP + 50) {
            atlas.push_object("obj".into(), 1, "track");
        }

        assert_eq!(atlas.recent(0).objects.len(), RING_CAP);
    }

    // loopback and unspecified addresses never enter the ring
    #[test]
    fn local_addresses() {
        let atlas = AtlasBuffer::new(vec![observer()]);

        atlas.push_ip("127.0.0.1".parse().expect("ip"), true);
        atlas.push_ip("0.0.0.0".parse().expect("ip"), true);

        assert!(atlas.recent(0).ips.is_empty());
    }
}
