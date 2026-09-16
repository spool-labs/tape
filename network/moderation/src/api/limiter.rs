//! Per-caller report limits over fixed windows, bounded in memory

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::config::settings::CallerLimitConfig;
use crate::types::ids::ReportCount;

/// IPv6 callers share a limit per /64, the block one host is usually given
const IPV6_PREFIX_MASK: u128 = u128::MAX << 64;

/// Counts reports per caller address
pub struct CallerLimiter {
    state: Mutex<LimiterState>,
    reports: ReportCount,
    window: Duration,
    max_callers: usize,
}

#[derive(Default)]
struct LimiterState {
    windows: HashMap<IpAddr, CallerWindow>,
    /// Window starts, oldest first, for expiry and eviction
    starts: VecDeque<(IpAddr, Instant)>,
}

#[derive(Clone, Copy)]
struct CallerWindow {
    started: Instant,
    count: ReportCount,
}

impl CallerLimiter {
    pub fn new(config: &CallerLimitConfig) -> Self {
        Self {
            state: Mutex::new(LimiterState::default()),
            reports: config.reports,
            window: config.window,
            max_callers: config.max_callers.max(1),
        }
    }

    /// Count a request from the caller; false once it is over its limit for the window
    pub fn try_acquire(&self, caller: IpAddr, now: Instant) -> bool {
        let key = caller_key(caller);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.expire(now, self.window);

        if let Some(window) = state.windows.get_mut(&key) {
            let is_allowed = window.count < self.reports;
            if is_allowed {
                window.count = ReportCount(window.count.0 + 1);
            }
            return is_allowed;
        }

        state.make_room(self.max_callers);
        let window = CallerWindow {
            started: now,
            count: ReportCount(1),
        };
        state.windows.insert(key, window);
        state.starts.push_back((key, now));
        self.reports.0 > 0
    }
}

impl LimiterState {
    fn expire(&mut self, now: Instant, window: Duration) {
        while let Some((key, started)) = self.starts.front().copied() {
            if now.saturating_duration_since(started) < window {
                break;
            }
            self.starts.pop_front();
            self.remove_started(key, started);
        }
    }

    /// Drop the oldest windows until one more caller fits
    fn make_room(&mut self, max_callers: usize) {
        while self.windows.len() >= max_callers {
            let Some((key, started)) = self.starts.pop_front() else {
                break;
            };
            self.remove_started(key, started);
        }
    }

    fn remove_started(&mut self, key: IpAddr, started: Instant) {
        if self.windows.get(&key).is_some_and(|window| window.started == started) {
            self.windows.remove(&key);
        }
    }
}

fn caller_key(caller: IpAddr) -> IpAddr {
    match caller {
        IpAddr::V4(address) => IpAddr::V4(address),
        IpAddr::V6(address) => match address.to_ipv4_mapped() {
            Some(mapped) => IpAddr::V4(mapped),
            None => IpAddr::V6(Ipv6Addr::from(u128::from(address) & IPV6_PREFIX_MASK)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(reports: u64, max_callers: usize) -> CallerLimiter {
        CallerLimiter::new(&CallerLimitConfig {
            reports: ReportCount(reports),
            window: Duration::from_secs(60),
            max_callers,
        })
    }

    fn address(value: &str) -> IpAddr {
        value.parse().expect("address")
    }

    // a caller is limited within its window and allowed again after it
    #[test]
    fn window() {
        let limiter = limiter(2, 10);
        let start = Instant::now();
        let caller = address("198.51.100.9");

        let first = limiter.try_acquire(caller, start);
        let second = limiter.try_acquire(caller, start);
        let third = limiter.try_acquire(caller, start + Duration::from_secs(59));
        let next_window = limiter.try_acquire(caller, start + Duration::from_secs(60));

        assert_eq!([first, second, third, next_window], [true, true, false, true]);
    }

    // callers are counted apart, and one IPv6 /64 counts as one caller
    #[test]
    fn callers() {
        let limiter = limiter(1, 10);
        let now = Instant::now();

        assert!(limiter.try_acquire(address("198.51.100.9"), now));
        assert!(limiter.try_acquire(address("198.51.100.10"), now));
        assert!(limiter.try_acquire(address("2001:db8::1"), now));
        assert!(!limiter.try_acquire(address("2001:db8::2"), now));
        assert!(limiter.try_acquire(address("2001:db8:0:1::1"), now));
    }

    // the map never holds more callers than configured
    #[test]
    fn bounded() {
        let limiter = limiter(1, 3);
        let now = Instant::now();

        for last in 1..=10 {
            limiter.try_acquire(address(&format!("198.51.100.{last}")), now);
        }

        let state = limiter.state.lock().expect("lock");
        assert_eq!(state.windows.len(), 3);
        assert!(state.starts.len() <= 3);
    }
}
