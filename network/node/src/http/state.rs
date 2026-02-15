//! Shared application state for HTTP handlers.

use std::sync::Arc;

use store::Store;
use tokio::sync::mpsc;

use crate::core::NodeContext;
use crate::fsm::UserEvent;

/// Shared state wrapper for axum handlers.
///
/// Clone is implemented manually to avoid requiring `S: Clone` — the
/// `Arc<NodeContext<S>>` is always cheaply cloneable regardless of `S`.
pub struct AppState<S: Store> {
    pub context: Arc<NodeContext<S>>,
    pub user_event_tx: Option<mpsc::Sender<UserEvent>>,
}

impl<S: Store> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            user_event_tx: self.user_event_tx.clone(),
        }
    }
}
