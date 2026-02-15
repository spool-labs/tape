//! HTTP server — axum-based API for node-to-node and public endpoints.
//!
//! Serves slice data, metadata, BLS signing, repair, sync, and health routes.
//! Uses tower middleware for body limits, concurrency throttling, and load shedding.

use std::sync::Arc;

use store::Store;

use crate::core::NodeContext;

/// The HTTP server serving the node API.
pub struct HttpServer<S: Store> {
    context: Arc<NodeContext<S>>,
}

impl<S: Store> HttpServer<S> {
    pub fn new(context: Arc<NodeContext<S>>) -> Self {
        Self { context }
    }
}
