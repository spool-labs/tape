//! What the last key press did, held for the status line.
//!
//! The node keys act on the last running node, which is the bottom row of a
//! table that shows only what fits, so on a short terminal they act on a node
//! the operator cannot see. Worse, a command that fails logs through tracing,
//! which the orchestrator sends to a sink. Both cases look identical to a key
//! that did nothing. This carries the outcome back to the screen.

use std::sync::Arc;

use arc_swap::ArcSwap;

/// The outcome of one command, phrased for the status line.
#[derive(Clone, Default)]
pub struct ActionLog {
    /// What happened, empty before the first key press.
    pub text: String,
    /// The node it acted on, so the table can scroll it into view.
    pub node: Option<usize>,
    /// Whether the command failed rather than found nothing to do.
    pub failed: bool,
}

/// Shared handle: the command loop writes, the TUI reads.
pub type ActionHandle = Arc<ArcSwap<ActionLog>>;

impl ActionLog {
    /// A command that did what it was asked, naming the node it touched.
    pub fn done(text: String, node: usize) -> Self {
        Self {
            text,
            node: Some(node),
            failed: false,
        }
    }

    /// A command that ran but had nothing to act on.
    pub fn idle(text: String) -> Self {
        Self {
            text,
            node: None,
            failed: false,
        }
    }

    /// A command that failed, carrying the error the sink would have swallowed.
    pub fn failed(text: String) -> Self {
        Self {
            text,
            node: None,
            failed: true,
        }
    }
}
