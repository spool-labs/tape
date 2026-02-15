//! Shared application state for HTTP handlers.

use std::sync::Arc;

use store::Store;

use crate::core::NodeContext;

/// Shared state wrapper for axum handlers.
///
/// Clone is implemented manually to avoid requiring `S: Clone` — the
/// `Arc<NodeContext<S>>` is always cheaply cloneable regardless of `S`.
pub struct AppState<S: Store> {
    pub context: Arc<NodeContext<S>>,
}

impl<S: Store> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
