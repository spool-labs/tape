//! Reconciler — diffs desired vs running tasks based on FSM state changes.
//!
//! The reconciler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the supervisor. It maintains a view of what tasks *should*
//! be running and tells the supervisor to schedule or cancel tasks accordingly.

use std::sync::Arc;

use store::Store;

use crate::core::NodeContext;
use crate::fsm::StateChange;
use crate::supervisor::TaskKey;

/// A directive from the reconciler to the supervisor.
#[derive(Debug, Clone)]
pub enum Directive {
    /// Schedule a new task.
    Schedule(TaskKey),
    /// Cancel a running task.
    Cancel(TaskKey),
}

/// Diffs desired state against running tasks to produce scheduling directives.
pub struct Reconciler<S: Store> {
    context: Arc<NodeContext<S>>,
}

impl<S: Store> Reconciler<S> {
    pub fn new(context: Arc<NodeContext<S>>) -> Self {
        Self { context }
    }

    /// Process state changes and return directives for the supervisor.
    pub fn reconcile(&self, _changes: &[StateChange]) -> Vec<Directive> {
        // Stub — will be implemented in a future phase.
        Vec::new()
    }
}
