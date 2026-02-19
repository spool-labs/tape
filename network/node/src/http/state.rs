//! Shared application state for HTTP handlers.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio::sync::mpsc;

use crate::runtime::NodeContext;
use crate::fsm::UserEvent;

/// Shared state wrapper for axum handlers.
///
/// Clone is implemented manually to avoid requiring `S: Clone` — the
/// `Arc<NodeContext<S, R>>` is always cheaply cloneable regardless of `S`.
pub struct AppState<S: Store, R: Rpc> {
    pub context: Arc<NodeContext<S, R>>,
    pub user_event_tx: Option<mpsc::Sender<UserEvent>>,
}

impl<S: Store, R: Rpc> Clone for AppState<S, R> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            user_event_tx: self.user_event_tx.clone(),
        }
    }
}
