//! Snapshot tasks — stubs for snapshot build, certify, bootstrap, register, certify.

use crate::supervisor::{TaskKey, TaskOutcome};

pub fn run_stub(key: &TaskKey) -> TaskOutcome {
    tracing::warn!(task = ?key, "snapshot task not yet implemented");
    TaskOutcome::Permanent("not yet implemented".into())
}
