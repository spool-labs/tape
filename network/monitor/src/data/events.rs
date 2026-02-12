//! Event stream processing for the Tapedrive Network Monitor.
//!
//! The [`EventWatcher`] generates events based on state changes:
//! - Node health transitions (online/offline/syncing)
//! - Epoch phase transitions
//! - Track/tape count changes
//!
//! This provides real-time visibility into network activity without
//! requiring WebSocket subscriptions to Solana program logs.

use std::collections::HashMap;

use tape_api::state::{Epoch, SnapshotState};
use tape_core::types::{EpochNumber, NodeId};

use super::{EventType, NetworkEvent, NodeState};

/// State tracker for detecting changes and generating events.
#[derive(Debug, Default)]
pub struct EventWatcher {
    /// Previous health status for each node (by NodeId).
    previous_node_health: HashMap<NodeId, super::HealthStatus>,
    /// Previous epoch number.
    previous_epoch: Option<EpochNumber>,
    /// Previous epoch phase (syncing, settling, active).
    previous_phase: Option<String>,
    /// Previous track count.
    previous_track_count: u64,
    /// Previous tape count.
    previous_tape_count: u64,
    /// Previous snapshot latest_epoch (fully certified).
    previous_snapshot_latest_epoch: Option<EpochNumber>,
    /// Previous snapshot certified_count (for certifying_epoch).
    previous_snapshot_certified_count: u64,
    /// Whether this is the first update (skip initial state as "events").
    first_update: bool,
}

impl EventWatcher {
    /// Create a new EventWatcher.
    pub fn new() -> Self {
        Self {
            previous_node_health: HashMap::new(),
            previous_epoch: None,
            previous_phase: None,
            previous_track_count: 0,
            previous_tape_count: 0,
            previous_snapshot_latest_epoch: None,
            previous_snapshot_certified_count: 0,
            first_update: true,
        }
    }

    /// Update state and generate events based on changes.
    ///
    /// Call this after each data fetch to detect state changes and generate
    /// appropriate network events.
    ///
    /// # Arguments
    /// * `nodes` - Current node states with health information
    /// * `epoch` - Current epoch state (optional if not yet available)
    /// * `track_count` - Current total track count
    /// * `tape_count` - Current active tape count
    ///
    /// # Returns
    /// A vector of events that occurred since the last update.
    pub fn update_state(
        &mut self,
        nodes: &[NodeState],
        epoch: Option<&Epoch>,
        track_count: u64,
        tape_count: u64,
        snapshot: Option<&SnapshotState>,
    ) -> Vec<NetworkEvent> {
        let mut events = Vec::new();

        // Skip generating events on first update - we're just capturing initial state
        if self.first_update {
            self.capture_initial_state(nodes, epoch, track_count, tape_count, snapshot);
            self.first_update = false;
            return events;
        }

        // Check for node health changes
        events.extend(self.check_node_health_changes(nodes));

        // Check for epoch transitions
        if let Some(epoch) = epoch {
            events.extend(self.check_epoch_changes(epoch));
        }

        // Check for track count changes (tracks certified)
        if track_count > self.previous_track_count {
            let new_tracks = track_count - self.previous_track_count;
            if new_tracks == 1 {
                events.push(NetworkEvent::new(
                    EventType::TrackCertified,
                    format!("Track #{} certified", track_count),
                ));
            } else {
                events.push(NetworkEvent::new(
                    EventType::TrackCertified,
                    format!("{} new tracks certified (total: {})", new_tracks, track_count),
                ));
            }
            self.previous_track_count = track_count;
        }

        // Check for tape count changes
        if tape_count > self.previous_tape_count {
            let new_tapes = tape_count - self.previous_tape_count;
            if new_tapes == 1 {
                events.push(NetworkEvent::new(
                    EventType::TapeReserved,
                    format!("New tape reserved (total active: {})", tape_count),
                ));
            } else {
                events.push(NetworkEvent::new(
                    EventType::TapeReserved,
                    format!("{} new tapes reserved (total active: {})", new_tapes, tape_count),
                ));
            }
            self.previous_tape_count = tape_count;
        }

        // Check for snapshot certification changes
        if let Some(snap) = snapshot {
            events.extend(self.check_snapshot_changes(snap));
        }

        events
    }

    /// Capture initial state without generating events.
    fn capture_initial_state(
        &mut self,
        nodes: &[NodeState],
        epoch: Option<&Epoch>,
        track_count: u64,
        tape_count: u64,
        snapshot: Option<&SnapshotState>,
    ) {
        // Store initial node health states
        for node in nodes {
            self.previous_node_health.insert(node.id(), node.health);
        }

        // Store initial epoch state
        if let Some(epoch) = epoch {
            self.previous_epoch = Some(epoch.id);
            self.previous_phase = Some(self.epoch_phase_string(epoch));
        }

        // Store initial counts
        self.previous_track_count = track_count;
        self.previous_tape_count = tape_count;

        // Store initial snapshot state
        if let Some(snap) = snapshot {
            self.previous_snapshot_latest_epoch = Some(snap.latest_epoch);
            self.previous_snapshot_certified_count = snap.certified_count;
        }
    }

    /// Check for node health state transitions.
    fn check_node_health_changes(&mut self, nodes: &[NodeState]) -> Vec<NetworkEvent> {
        let mut events = Vec::new();

        for node in nodes {
            let node_id = node.id();
            let current_health = node.health;
            let node_name = node.display_name();

            if let Some(&previous_health) = self.previous_node_health.get(&node_id) {
                // Check for transitions
                match (previous_health, current_health) {
                    // Came online
                    (super::HealthStatus::Offline | super::HealthStatus::Unknown, super::HealthStatus::Online) => {
                        events.push(NetworkEvent::node_online(node_id, &node_name));
                    }
                    // Went offline
                    (super::HealthStatus::Online | super::HealthStatus::Syncing, super::HealthStatus::Offline) => {
                        events.push(NetworkEvent::node_offline(node_id, &node_name));
                    }
                    // Started syncing
                    (super::HealthStatus::Offline | super::HealthStatus::Unknown, super::HealthStatus::Syncing) => {
                        events.push(NetworkEvent::with_details(
                            EventType::NodeOnline,
                            format!("Node #{} started syncing", node_id),
                            &node_name,
                        ));
                    }
                    // Finished syncing
                    (super::HealthStatus::Syncing, super::HealthStatus::Online) => {
                        events.push(NetworkEvent::with_details(
                            EventType::NodeOnline,
                            format!("Node #{} finished syncing", node_id),
                            &node_name,
                        ));
                    }
                    // No change or unknown -> unknown
                    _ => {}
                }
            }

            // Update stored state
            self.previous_node_health.insert(node_id, current_health);
        }

        events
    }

    /// Check for epoch phase transitions.
    fn check_epoch_changes(&mut self, epoch: &Epoch) -> Vec<NetworkEvent> {
        let mut events = Vec::new();
        let current_phase = self.epoch_phase_string(epoch);

        // Check for epoch number change
        if let Some(prev_epoch) = self.previous_epoch {
            if epoch.id != prev_epoch {
                events.push(NetworkEvent::epoch_transition(epoch.id.0, &current_phase));
            }
        }

        // Check for phase change within same epoch
        if let Some(ref prev_phase) = self.previous_phase {
            if *prev_phase != current_phase && self.previous_epoch == Some(epoch.id) {
                events.push(NetworkEvent::new(
                    EventType::EpochTransition,
                    format!("Epoch E{} entered {} phase", epoch.id.0, current_phase),
                ));
            }
        }

        // Update stored state
        self.previous_epoch = Some(epoch.id);
        self.previous_phase = Some(current_phase);

        events
    }

    /// Get the epoch phase as a display string.
    fn epoch_phase_string(&self, epoch: &Epoch) -> String {
        if epoch.state.is_syncing() {
            "Syncing".to_string()
        } else if epoch.state.is_settling() {
            "Settling".to_string()
        } else if epoch.state.is_active() {
            "Active".to_string()
        } else {
            "Unknown".to_string()
        }
    }

    /// Check for snapshot certification state changes.
    fn check_snapshot_changes(&mut self, snapshot: &SnapshotState) -> Vec<NetworkEvent> {
        let mut events = Vec::new();

        // Check if a new epoch was fully certified
        if let Some(prev_epoch) = self.previous_snapshot_latest_epoch {
            if snapshot.latest_epoch > prev_epoch {
                events.push(NetworkEvent::new(
                    EventType::SnapshotCertified,
                    format!("Snapshot E{} fully certified", snapshot.latest_epoch.0),
                ));
            }
        }

        // Update stored state
        self.previous_snapshot_latest_epoch = Some(snapshot.latest_epoch);
        self.previous_snapshot_certified_count = snapshot.certified_count;

        events
    }

    /// Reset the watcher state (useful when reconnecting).
    pub fn reset(&mut self) {
        self.previous_node_health.clear();
        self.previous_epoch = None;
        self.previous_phase = None;
        self.previous_track_count = 0;
        self.previous_tape_count = 0;
        self.previous_snapshot_latest_epoch = None;
        self.previous_snapshot_certified_count = 0;
        self.first_update = true;
    }

    /// Generate an info event.
    pub fn info_event(message: impl Into<String>) -> NetworkEvent {
        NetworkEvent::info(message)
    }

    /// Generate a warning event.
    pub fn warning_event(message: impl Into<String>) -> NetworkEvent {
        NetworkEvent::warning(message)
    }

    /// Generate an error event.
    pub fn error_event(message: impl Into<String>) -> NetworkEvent {
        NetworkEvent::error(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::HealthStatus;
    use solana_sdk::pubkey::Pubkey;
    use tape_api::state::Node;
    use tape_core::types::NodeId;

    fn make_test_node(id: u64, health: HealthStatus) -> NodeState {
        // Create a minimal Node for testing
        let node = unsafe { std::mem::zeroed::<Node>() };
        let mut state = NodeState::new(Pubkey::new_unique(), node);
        state.node.id = NodeId(id);
        state.health = health;
        state
    }

    #[test]
    fn test_first_update_no_events() {
        let mut watcher = EventWatcher::new();
        let nodes = vec![make_test_node(1, HealthStatus::Online)];

        let events = watcher.update_state(&nodes, None, 100, 10, None);

        // First update should not generate events
        assert!(events.is_empty());
    }

    #[test]
    fn test_node_goes_offline() {
        let mut watcher = EventWatcher::new();

        // Initial state: node online
        let nodes = vec![make_test_node(1, HealthStatus::Online)];
        watcher.update_state(&nodes, None, 0, 0, None);

        // Node goes offline
        let nodes = vec![make_test_node(1, HealthStatus::Offline)];
        let events = watcher.update_state(&nodes, None, 0, 0, None);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::NodeOffline);
    }

    #[test]
    fn test_node_comes_online() {
        let mut watcher = EventWatcher::new();

        // Initial state: node offline
        let nodes = vec![make_test_node(1, HealthStatus::Offline)];
        watcher.update_state(&nodes, None, 0, 0, None);

        // Node comes online
        let nodes = vec![make_test_node(1, HealthStatus::Online)];
        let events = watcher.update_state(&nodes, None, 0, 0, None);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::NodeOnline);
    }

    #[test]
    fn test_track_count_increase() {
        let mut watcher = EventWatcher::new();

        // Initial state
        watcher.update_state(&[], None, 100, 10, None);

        // Track count increases
        let events = watcher.update_state(&[], None, 105, 10, None);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::TrackCertified);
        assert!(events[0].description.contains("5 new tracks"));
    }

    #[test]
    fn test_no_events_on_no_change() {
        let mut watcher = EventWatcher::new();

        let nodes = vec![make_test_node(1, HealthStatus::Online)];
        watcher.update_state(&nodes, None, 100, 10, None);

        // Same state, no changes
        let events = watcher.update_state(&nodes, None, 100, 10, None);

        assert!(events.is_empty());
    }
}
