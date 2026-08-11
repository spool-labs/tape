use std::sync::Arc;

use arc_swap::ArcSwap;

#[derive(Clone, Default)]
pub struct ActionLog {
    pub text: String,
    pub node: Option<usize>,
    pub failed: bool,
}

pub type ActionHandle = Arc<ArcSwap<ActionLog>>;

impl ActionLog {
    pub fn done(text: String, node: usize) -> Self {
        Self {
            text,
            node: Some(node),
            failed: false,
        }
    }

    pub fn idle(text: String) -> Self {
        Self {
            text,
            node: None,
            failed: false,
        }
    }

    pub fn failed(text: String) -> Self {
        Self {
            text,
            node: None,
            failed: true,
        }
    }
}
