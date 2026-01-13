//! Data caching layer for the Tapedrive Network Monitor.
//!
//! The [`DataCache`] stores fetched data and tracks when it needs to be refreshed.
//! This reduces RPC load and provides consistent data for UI rendering.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tape_api::state::{Archive, Epoch, System};

use super::{NetworkEvent, NodeState};

/// Maximum number of events to keep in the cache.
const MAX_EVENTS: usize = 1000;

/// Default refresh interval (2 seconds).
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

/// Cached data for the network monitor.
///
/// Stores the latest fetched data and tracks when it was last refreshed.
/// The cache automatically determines when data needs to be refreshed
/// based on a configurable interval.
#[derive(Debug)]
pub struct DataCache {
    // Cached on-chain state
    system: Option<System>,
    epoch: Option<Epoch>,
    archive: Option<Archive>,
    nodes: Vec<NodeState>,

    // Event log (newest first)
    events: VecDeque<NetworkEvent>,

    // Timing
    refresh_interval: Duration,
    last_refresh: Instant,
    last_slot: Option<u64>,

    // Connection state
    rpc_connected: bool,
}

impl Default for DataCache {
    fn default() -> Self {
        Self::new(DEFAULT_REFRESH_INTERVAL)
    }
}

impl DataCache {
    /// Create a new DataCache with the specified refresh interval.
    pub fn new(refresh_interval: Duration) -> Self {
        Self {
            system: None,
            epoch: None,
            archive: None,
            nodes: Vec::new(),
            events: VecDeque::with_capacity(MAX_EVENTS),
            refresh_interval,
            last_refresh: Instant::now() - refresh_interval, // Force immediate refresh
            last_slot: None,
            rpc_connected: false,
        }
    }

    /// Create a new DataCache with default refresh interval.
    pub fn with_defaults() -> Self {
        Self::default()
    }

    // ========================================================================
    // Refresh Tracking
    // ========================================================================

    /// Check if the cache needs to be refreshed.
    pub fn needs_refresh(&self) -> bool {
        self.last_refresh.elapsed() >= self.refresh_interval
    }

    /// Get the time since the last refresh.
    pub fn time_since_refresh(&self) -> Duration {
        self.last_refresh.elapsed()
    }

    /// Get the configured refresh interval.
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    /// Set the refresh interval.
    pub fn set_refresh_interval(&mut self, interval: Duration) {
        self.refresh_interval = interval;
    }

    /// Mark the cache as just refreshed.
    pub fn mark_refreshed(&mut self) {
        self.last_refresh = Instant::now();
    }

    /// Force a refresh on the next check by resetting the last refresh time.
    pub fn invalidate(&mut self) {
        self.last_refresh = Instant::now() - self.refresh_interval;
    }

    // ========================================================================
    // Connection State
    // ========================================================================

    /// Check if the RPC connection is established.
    pub fn is_rpc_connected(&self) -> bool {
        self.rpc_connected
    }

    /// Set the RPC connection status.
    pub fn set_rpc_connected(&mut self, connected: bool) {
        self.rpc_connected = connected;
    }

    /// Get the last known slot number.
    pub fn last_slot(&self) -> Option<u64> {
        self.last_slot
    }

    /// Update the last known slot number.
    pub fn set_last_slot(&mut self, slot: u64) {
        self.last_slot = Some(slot);
    }

    // ========================================================================
    // System State
    // ========================================================================

    /// Get the cached System account data.
    pub fn get_system(&self) -> Option<&System> {
        self.system.as_ref()
    }

    /// Update the cached System account data.
    pub fn update_system(&mut self, system: System) {
        self.system = Some(system);
    }

    /// Check if System data is available.
    pub fn has_system(&self) -> bool {
        self.system.is_some()
    }

    // ========================================================================
    // Epoch State
    // ========================================================================

    /// Get the cached Epoch account data.
    pub fn get_epoch(&self) -> Option<&Epoch> {
        self.epoch.as_ref()
    }

    /// Update the cached Epoch account data.
    pub fn update_epoch(&mut self, epoch: Epoch) {
        self.epoch = Some(epoch);
    }

    /// Check if Epoch data is available.
    pub fn has_epoch(&self) -> bool {
        self.epoch.is_some()
    }

    // ========================================================================
    // Archive State
    // ========================================================================

    /// Get the cached Archive account data.
    pub fn get_archive(&self) -> Option<&Archive> {
        self.archive.as_ref()
    }

    /// Update the cached Archive account data.
    pub fn update_archive(&mut self, archive: Archive) {
        self.archive = Some(archive);
    }

    /// Check if Archive data is available.
    pub fn has_archive(&self) -> bool {
        self.archive.is_some()
    }

    // ========================================================================
    // Node State
    // ========================================================================

    /// Get the cached node states.
    pub fn get_nodes(&self) -> &[NodeState] {
        &self.nodes
    }

    /// Get a mutable reference to the cached node states.
    pub fn get_nodes_mut(&mut self) -> &mut Vec<NodeState> {
        &mut self.nodes
    }

    /// Update the cached node states.
    pub fn update_nodes(&mut self, nodes: Vec<NodeState>) {
        self.nodes = nodes;
    }

    /// Get the number of cached nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get a node by index.
    pub fn get_node(&self, index: usize) -> Option<&NodeState> {
        self.nodes.get(index)
    }

    /// Count nodes by health status.
    pub fn count_nodes_by_status(&self) -> (usize, usize, usize, usize) {
        let mut online = 0;
        let mut offline = 0;
        let mut syncing = 0;
        let mut unknown = 0;

        for node in &self.nodes {
            match node.health {
                super::HealthStatus::Online => online += 1,
                super::HealthStatus::Offline => offline += 1,
                super::HealthStatus::Syncing => syncing += 1,
                super::HealthStatus::Unknown => unknown += 1,
            }
        }

        (online, offline, syncing, unknown)
    }

    // ========================================================================
    // Events
    // ========================================================================

    /// Get the event log (newest first).
    pub fn get_events(&self) -> &VecDeque<NetworkEvent> {
        &self.events
    }

    /// Add a new event to the log.
    ///
    /// Events are prepended (newest first) and the log is capped at [`MAX_EVENTS`].
    pub fn add_event(&mut self, event: NetworkEvent) {
        self.events.push_front(event);

        // Trim to max size
        while self.events.len() > MAX_EVENTS {
            self.events.pop_back();
        }
    }

    /// Add multiple events to the log.
    ///
    /// Events are added in order (first event becomes newest).
    pub fn add_events(&mut self, events: impl IntoIterator<Item = NetworkEvent>) {
        for event in events {
            self.add_event(event);
        }
    }

    /// Clear all events.
    pub fn clear_events(&mut self) {
        self.events.clear();
    }

    /// Get the number of events in the log.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get events as a slice for iteration.
    pub fn events_iter(&self) -> impl Iterator<Item = &NetworkEvent> {
        self.events.iter()
    }

    // ========================================================================
    // Bulk Updates
    // ========================================================================

    /// Update all on-chain state at once.
    pub fn update_all_state(&mut self, system: System, epoch: Epoch, archive: Archive) {
        self.system = Some(system);
        self.epoch = Some(epoch);
        self.archive = Some(archive);
        self.rpc_connected = true;
        self.mark_refreshed();
    }

    /// Update all data (state + nodes) at once.
    pub fn update_all(
        &mut self,
        system: System,
        epoch: Epoch,
        archive: Archive,
        nodes: Vec<NodeState>,
    ) {
        self.system = Some(system);
        self.epoch = Some(epoch);
        self.archive = Some(archive);
        self.nodes = nodes;
        self.rpc_connected = true;
        self.mark_refreshed();
    }

    /// Clear all cached data (used on connection loss).
    pub fn clear_all(&mut self) {
        self.system = None;
        self.epoch = None;
        self.archive = None;
        self.nodes.clear();
        self.rpc_connected = false;
        self.last_slot = None;
    }

    // ========================================================================
    // Derived Data
    // ========================================================================

    /// Check if all required data is available for dashboard rendering.
    pub fn is_ready(&self) -> bool {
        self.system.is_some() && self.epoch.is_some() && self.archive.is_some()
    }

    /// Get the current committee size from cached System data.
    pub fn committee_size(&self) -> Option<usize> {
        self.system.as_ref().map(|s| s.committee.size())
    }

    /// Get the next committee size from cached System data.
    pub fn committee_next_size(&self) -> Option<usize> {
        self.system.as_ref().map(|s| s.committee_next.size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_needs_refresh() {
        let mut cache = DataCache::new(Duration::from_millis(100));

        // Should need refresh immediately (we set last_refresh in the past)
        assert!(cache.needs_refresh());

        cache.mark_refreshed();
        assert!(!cache.needs_refresh());

        // Wait for interval to pass
        std::thread::sleep(Duration::from_millis(150));
        assert!(cache.needs_refresh());
    }

    #[test]
    fn test_event_log_capacity() {
        let mut cache = DataCache::default();

        // Add more than MAX_EVENTS
        for i in 0..1100 {
            cache.add_event(NetworkEvent::info(format!("Event {}", i)));
        }

        // Should be capped at MAX_EVENTS
        assert_eq!(cache.event_count(), MAX_EVENTS);

        // Newest event should be last one added
        let first_event = cache.get_events().front().unwrap();
        assert_eq!(first_event.description, "Event 1099");
    }

    #[test]
    fn test_invalidate() {
        let mut cache = DataCache::new(Duration::from_secs(60));
        cache.mark_refreshed();

        assert!(!cache.needs_refresh());

        cache.invalidate();
        assert!(cache.needs_refresh());
    }
}
