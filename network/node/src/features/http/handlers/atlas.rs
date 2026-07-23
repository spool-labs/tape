//! Recent-traffic snapshot for the atlas display, gated to configured
//! observer identities on the mTLS listener.

use axum::extract::{Query, State};
use axum::Json;
use rpc::Rpc;
use serde::Deserialize;
use store::Store;
use tape_observe_api::AtlasRecent;
use tape_protocol::Api;

use crate::features::http::auth::ObserverPeer;
use crate::features::http::state::AppState;

#[derive(Deserialize)]
pub struct AtlasQuery {
    /// Resume cursor, from the previous response's seq field.
    #[serde(default)]
    pub after: u64,
}

pub async fn recent<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _observer: ObserverPeer,
    Query(query): Query<AtlasQuery>,
) -> Json<AtlasRecent> {
    Json(state.context.atlas.recent(query.after))
}
