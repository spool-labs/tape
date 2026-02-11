//! Epoch snapshot feature for fast node bootstrap.
//!
//! This module provides:
//! - **event_capture**: Convert ParsedInstruction → ReplayableEvent during block processing
//! - **builder**: Serialize SnapshotLog, two-level encode, store slices
//! - **certifier**: BLS sign, register, certify 50 snapshot tracks
//! - **bootstrap**: Download, outer-decode, replay for fast bootstrap

pub mod event_capture;
pub mod builder;
pub mod certifier;
pub mod bootstrap;
