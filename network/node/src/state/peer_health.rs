use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

struct PeerState {
    failures: u32,
    last_failure: Option<Instant>,
}

pub struct PeerHealth {
    peers: HashMap<SocketAddr, PeerState>,
}

impl PeerHealth {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn is_cooling_down(&self, addr: &SocketAddr) -> bool {
        let Some(state) = self.peers.get(addr) else {
            return false;
        };
        let Some(last) = state.last_failure else {
            return false;
        };
        let cooldown_secs = 1u64 << state.failures.min(6);
        last.elapsed().as_secs() < cooldown_secs
    }

    pub fn record_success(&mut self, addr: &SocketAddr) {
        self.peers.remove(addr);
    }

    pub fn record_failure(&mut self, addr: &SocketAddr) {
        let state = self.peers.entry(*addr).or_insert(PeerState {
            failures: 0,
            last_failure: None,
        });
        state.failures = state.failures.saturating_add(1);
        state.last_failure = Some(Instant::now());
    }

    pub fn reset(&mut self) {
        self.peers.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn healthy_peer() {
        let health = PeerHealth::new();
        assert!(!health.is_cooling_down(&addr(8000)));
    }

    #[test]
    fn cooldown_after_failure() {
        let mut health = PeerHealth::new();
        let a = addr(8000);
        health.record_failure(&a);
        // First failure → 2^1 = 2s cooldown, should be cooling
        assert!(health.is_cooling_down(&a));
    }

    #[test]
    fn success_clears() {
        let mut health = PeerHealth::new();
        let a = addr(8000);
        health.record_failure(&a);
        health.record_success(&a);
        assert!(!health.is_cooling_down(&a));
    }

    #[test]
    fn reset_clears_all() {
        let mut health = PeerHealth::new();
        health.record_failure(&addr(8000));
        health.record_failure(&addr(8001));
        health.reset();
        assert!(!health.is_cooling_down(&addr(8000)));
        assert!(!health.is_cooling_down(&addr(8001)));
    }

    #[test]
    fn max_backoff() {
        let mut health = PeerHealth::new();
        let a = addr(8000);
        // 10 failures → capped at 2^6 = 64s
        for _ in 0..10 {
            health.record_failure(&a);
        }
        assert!(health.is_cooling_down(&a));
        // Verify failures capped at display level
        let state = health.peers.get(&a).unwrap();
        assert_eq!(1u64 << state.failures.min(6), 64);
    }
}
