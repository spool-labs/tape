//! The serde JSON event log that feeds the group animation.
//!
//! Round-level and boundary-level events are emitted by default. Per-observer
//! score events sit behind a verbose flag to bound the file size. Every type
//! serializes natively, so the log is a single array of tagged events written in
//! one pass, enough to render the group on a globe at real cities, animate each
//! round's proof fan-out and its per-peer attestation arrivals, draw the
//! certificate the moment the quorum lands, and show nodes dropping out and fresh
//! cities lighting up at each boundary.

use serde::Serialize;
use tape_core::types::{EpochNumber, NodeId};

use crate::sim::node::Behavior;
use crate::sim::scoreboard::EvictionReason;
use crate::types::{GroupPosition, RoundNumber, SlotNumber};

/// A committee member as placed at a real city for one epoch.
#[derive(Clone, Serialize)]
pub struct NodeRef {
    pub id: NodeId,
    pub behavior: Behavior,
    pub city: String,
    pub country: String,
    pub latitude: f64,
    pub longitude: f64,
}

/// One peer's attestation timing and verdict within a single challenge round.
#[derive(Clone, Serialize)]
pub struct Attestation {
    pub observer: NodeId,
    pub arrival_ms: f64,
    pub witnessed: bool,
    pub signed: bool,
}

/// Everything one challenge round produced for the animation.
#[derive(Clone, Serialize)]
pub struct ChallengeRecord {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub target: NodeId,
    pub target_position: GroupPosition,
    pub seed: String,
    pub entropy_slot: SlotNumber,
    pub deadline_ms: f64,
    pub ready_ms: f64,
    pub witnesses: usize,
    pub signer_count: usize,
    pub certified: bool,
    pub certificate_ms: Option<f64>,
    pub attestations: Vec<Attestation>,
}

/// One entry in the animation event log.
#[derive(Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    EpochStarted {
        epoch: EpochNumber,
        committee: Vec<NodeRef>,
    },
    Challenge(ChallengeRecord),
    Certificate {
        epoch: EpochNumber,
        round: RoundNumber,
        target: NodeId,
        signer_count: usize,
        formed_ms: f64,
    },
    EvictionProposed {
        epoch: EpochNumber,
        observer: NodeId,
        target: NodeId,
        reason: EvictionReason,
    },
    Eviction {
        epoch: EpochNumber,
        effective_epoch: EpochNumber,
        target: NodeId,
        behavior: Behavior,
        reason: EvictionReason,
    },
    Join {
        epoch: EpochNumber,
        node: NodeId,
        city: String,
        country: String,
        latitude: f64,
        longitude: f64,
    },
    Score {
        epoch: EpochNumber,
        observer: NodeId,
        target: NodeId,
        opportunities: u64,
        successes: u64,
    },
}

/// The ordered event log for one simulation run.
#[derive(Default)]
pub struct Timeline {
    events: Vec<Event>,
    verbose: bool,
}

impl Timeline {
    pub fn new(verbose: bool) -> Self {
        Self {
            events: Vec::new(),
            verbose,
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Read-only view of the recorded events, used by the export and by tests.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn push_epoch_started(&mut self, epoch: EpochNumber, committee: Vec<NodeRef>) {
        self.events.push(Event::EpochStarted { epoch, committee });
    }

    pub fn push_challenge(&mut self, record: ChallengeRecord) {
        self.events.push(Event::Challenge(record));
    }

    pub fn push_certificate(
        &mut self,
        epoch: EpochNumber,
        round: RoundNumber,
        target: NodeId,
        signer_count: usize,
        formed_ms: f64,
    ) {
        self.events.push(Event::Certificate {
            epoch,
            round,
            target,
            signer_count,
            formed_ms,
        });
    }

    pub fn push_eviction_proposed(
        &mut self,
        epoch: EpochNumber,
        observer: NodeId,
        target: NodeId,
        reason: EvictionReason,
    ) {
        self.events.push(Event::EvictionProposed {
            epoch,
            observer,
            target,
            reason,
        });
    }

    pub fn push_eviction(
        &mut self,
        epoch: EpochNumber,
        effective_epoch: EpochNumber,
        target: NodeId,
        behavior: Behavior,
        reason: EvictionReason,
    ) {
        self.events.push(Event::Eviction {
            epoch,
            effective_epoch,
            target,
            behavior,
            reason,
        });
    }

    pub fn push_join(
        &mut self,
        epoch: EpochNumber,
        node: NodeId,
        city: String,
        country: String,
        latitude: f64,
        longitude: f64,
    ) {
        self.events.push(Event::Join {
            epoch,
            node,
            city,
            country,
            latitude,
            longitude,
        });
    }

    /// Record one observer's current record about one target, only when verbose.
    pub fn push_score(
        &mut self,
        epoch: EpochNumber,
        observer: NodeId,
        target: NodeId,
        opportunities: u64,
        successes: u64,
    ) {
        if !self.verbose {
            return;
        }
        self.events.push(Event::Score {
            epoch,
            observer,
            target,
            opportunities,
            successes,
        });
    }
}
