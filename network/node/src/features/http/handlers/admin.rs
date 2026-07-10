//! Admin trigger endpoints.
//!
//! The eviction driver is manually seeded for v1: an operator posts a node
//! address here and the local eviction manager begins proposing and voting to
//! evict it. The eviction itself still requires a supermajority of every
//! group, so a seed only enrolls this node as a voter.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_crypto::Address;
use tape_protocol::Api;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

/// Route path for seeding an eviction target.
pub const ADMIN_EVICT_PATH: &str = "/admin/evictions";

pub async fn evict_node<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let text = std::str::from_utf8(&body)
        .map_err(|_| RouteError::BadRequest("node address is not valid utf8".into()))?;
    let node = text
        .trim()
        .parse::<Address>()
        .map_err(|_| RouteError::BadRequest("invalid node address".into()))?;

    if node == Address::default() {
        return Err(RouteError::BadRequest("node address is empty".into()));
    }

    state.context.eviction_queue.insert(node);
    Ok(StatusCode::ACCEPTED)
}
