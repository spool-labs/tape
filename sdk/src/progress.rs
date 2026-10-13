//! Live progress of a write or a read, for a caller that draws it.
//!
//! `Metrics` times a phase once it has ended, which is too late to draw from.
//! A `Progress` hears each step as it happens. A client with none installed
//! reports nothing and moves its slices exactly as it did before.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_peer_http::SliceWatch;

/// The position of a transfer's only unit, in the steps a lone track reports
pub(crate) const LONE_UNIT: usize = 0;

/// Hears every step of the transfers a client runs
///
/// `report` is called from the transfer's own path, on whichever task the step
/// happens on, so it must return at once.
pub trait Progress: Send + Sync {
    fn report(&self, step: Step);
}

/// What one unit of a transfer is
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum UnitKind {
    /// A coded track
    Track,
    /// A chunk of a stream
    Chunk,
}

/// One thing that happened in a transfer
///
/// A unit is known by its position in the transfer until its track is known,
/// and by the track's address from then on. `Storing` ties the two on a write
/// and `Fetching` on a read.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Step {
    /// The units of a transfer and their payload sizes are known
    Planned { kind: UnitKind, sizes: Vec<StorageUnits> },
    /// A unit is already on the tape, and none of it moves
    Skipped { unit: usize },
    /// A unit has begun encoding
    Encoding { unit: usize },
    /// A unit's register is on its way to the chain
    Registering { unit: usize },
    /// A unit's track is registered at this address, and its slices can upload
    Storing { unit: usize, track: Address },
    /// A unit's track is found at this address, and its slices can download
    Fetching { unit: usize, track: Address },
    /// This much of a slice has crossed the wire in its current attempt
    Moved { track: Address, slice: SpoolIndex, moved: StorageUnits, size: StorageUnits },
    /// A node holds a slice
    Stored { track: Address, slice: SpoolIndex },
    /// A slice has arrived whole
    Fetched { track: Address, slice: SpoolIndex },
    /// A slice's push failed and starts over
    Retried { track: Address, slice: SpoolIndex },
    /// A slice's push has run out of attempts
    Failed { track: Address, slice: SpoolIndex },
    /// Enough of a track's slices are held, and its certification is on its way
    Certifying { track: Address },
    /// A track is certified, or read back whole
    Done { track: Address },
}

/// Tells a client's progress what one slice of a track is doing
#[derive(Clone)]
pub(crate) struct SliceReport {
    steps: Arc<dyn Progress>,
    track: Address,
    slice: SpoolIndex,
}

impl SliceReport {
    pub(crate) fn new(steps: Arc<dyn Progress>, track: Address, slice: SpoolIndex) -> Self {
        Self { steps, track, slice }
    }

    /// A watch that reports how far the slice has got across the wire
    pub(crate) fn watch(&self) -> SliceWatch {
        let report = self.clone();
        Arc::new(move |moved, size| report.moved(moved, size))
    }

    fn moved(&self, moved: StorageUnits, size: StorageUnits) {
        self.steps.report(Step::Moved { track: self.track, slice: self.slice, moved, size });
    }

    pub(crate) fn stored(&self) {
        self.steps.report(Step::Stored { track: self.track, slice: self.slice });
    }

    pub(crate) fn fetched(&self) {
        self.steps.report(Step::Fetched { track: self.track, slice: self.slice });
    }

    pub(crate) fn retried(&self) {
        self.steps.report(Step::Retried { track: self.track, slice: self.slice });
    }

    pub(crate) fn failed(&self) {
        self.steps.report(Step::Failed { track: self.track, slice: self.slice });
    }
}
