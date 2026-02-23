//! Shared application state for HTTP handlers.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio::sync::mpsc;

use tape_core::types::EpochNumber;

use crate::core::NodeContext;
use crate::http::error::ApiError;
use crate::fsm::UserEvent;

/// Shared state wrapper for axum handlers.
///
/// Clone is implemented manually to avoid requiring `S: Clone` — the
/// `Arc<NodeContext<S, R>>` is always cheaply cloneable regardless of `S`.
pub struct AppState<S: Store, R: Rpc> {
    pub context: Arc<NodeContext<S, R>>,
    pub user_event_tx: Option<mpsc::Sender<UserEvent>>,
}

/// Load current chain epoch, rejecting requests if not yet initialized.
pub fn require_chain_epoch<S: Store, R: Rpc>(state: &AppState<S, R>) -> Result<EpochNumber, ApiError> {
    let epoch = state.context.chain_state.load().epoch;
    if epoch.is_zero() {
        return Err(ApiError::BadRequest("chain epoch missing".into()));
    }
    Ok(epoch)
}

impl<S: Store, R: Rpc> Clone for AppState<S, R> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            user_event_tx: self.user_event_tx.clone(),
        }
    }
}
