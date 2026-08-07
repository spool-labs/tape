use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::orchestrator::Orchestrator;
use crate::process::RemoveNodeError;
use crate::upload::UploadManager;
use crate::view::LocalnetView;

#[derive(Clone)]
pub struct AppState {
    pub orchestrator: Arc<Mutex<Orchestrator>>,
    pub upload_manager: Arc<UploadManager>,
    pub snapshot: Arc<ArcSwap<LocalnetView>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/nodes", get(list_nodes).post(add_node))
        .route("/api/nodes/{id}", delete(remove_node))
        .route("/api/uploads", get(list_uploads).post(start_upload))
        .route("/api/snapshot", get(snapshot))
        .route(tape_observe_api::NETWORK_PATH, get(observe_network))
        .route(tape_observe_api::PEER_BOARD_PATH, get(observe_peer))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// Serve the shared observe contract so the dashboard talks to the orchestrator
// the same way it talks to a standalone node.
fn to_network(view: &LocalnetView) -> tape_observe_api::Network {
    use tape_observe_api as obs;
    let c = &view.cluster;
    let committee = view
        .nodes
        .iter()
        .map(|n| {
            let stats = n.stats.as_ref().map(obs::NodeStats::from);
            obs::NetworkNode {
                index: n.local_id,
                address: n.node_address.clone(),
                name: String::new(),
                spools: n.stats.as_ref().map(|s| s.owned_spools).unwrap_or(0),
                status: if n.healthy { obs::LinkStatus::Up } else { obs::LinkStatus::Down },
                source: if n.stats.is_some() { obs::StatsSource::Public } else { obs::StatsSource::None },
                non_committee: false,
                endpoint: n.address.clone(),
                stake: n.pool_stake,
                stats,
            }
        })
        .collect();
    let spools = view
        .spools
        .iter()
        .map(|s| obs::NetworkSpool {
            spool: s.spool,
            owner: s.owner_node.clone(),
            owner_index: s.owner_local_id,
        })
        .collect();
    obs::Network {
        epoch: c.epoch,
        phase: c.phase.clone(),
        phase_index: c.phase_index,
        slot: c.slot,
        groups: c.live_group_count,
        prev_committee_size: c.committee_prev_size as u64,
        committee_size: c.committee_size as u64,
        next_committee_size: c.committee_next_size as u64,
        peers: view.nodes.len() as u64,
        committee,
        spools,
    }
}

async fn observe_network(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.snapshot.load_full();
    Json(to_network(snap.as_ref())).into_response()
}

// Resolve the pubkey to a running node and forward its board snapshot.
async fn observe_peer(
    State(state): State<AppState>,
    Path(addr): Path<String>,
) -> impl IntoResponse {
    let snap = state.snapshot.load_full();
    let local_id = match snap.nodes.iter().find(|n| n.node_address == addr) {
        Some(n) => n.local_id,
        None => return error_response(StatusCode::NOT_FOUND, "node not found"),
    };
    let port = {
        let orch = state.orchestrator.lock().await;
        orch.node_refs().iter().find(|r| r.id == local_id).map(|r| r.plaintext_port)
    };
    let port = match port {
        Some(p) => p,
        None => return error_response(StatusCode::NOT_FOUND, "node not running"),
    };
    let url = format!(
        "http://{}:{port}{}",
        crate::process::LOCAL_HOST,
        tape_observe_api::BOARD_PATH
    );
    match async { reqwest::get(&url).await?.bytes().await }.await {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(e) => error_response(StatusCode::BAD_GATEWAY, &e.to_string()),
    }
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn list_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.snapshot.load_full();
    Json(json!(snap.nodes.clone())).into_response()
}

async fn add_node(State(state): State<AppState>) -> impl IntoResponse {
    let mut orch = state.orchestrator.lock().await;
    match orch.add_node().await {
        Ok(id) => {
            let port = orch.node_refs().iter().find(|n| n.id == id).map(|n| n.port);
            Json(json!({"id": id, "port": port})).into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}")),
    }
}

async fn remove_node(
    State(state): State<AppState>,
    Path(id): Path<usize>,
) -> impl IntoResponse {
    let mut orch = state.orchestrator.lock().await;
    match orch.remove_node(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(RemoveNodeError::NotFound) => {
            error_response(StatusCode::NOT_FOUND, "node not found")
        }
        Err(RemoveNodeError::AlreadyStopped) => {
            error_response(StatusCode::CONFLICT, "node already stopped")
        }
        Err(RemoveNodeError::StopFailed(e)) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}"))
        }
    }
}

async fn list_uploads(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.upload_manager.snapshot()).into_response()
}

async fn start_upload(State(state): State<AppState>) -> impl IntoResponse {
    match state.upload_manager.start_random_upload() {
        Ok(upload) => (StatusCode::ACCEPTED, Json(upload)).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}")),
    }
}

async fn snapshot(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.snapshot.load_full();
    Json(snap.as_ref().clone()).into_response()
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn error_response(status: StatusCode, message: &str) -> axum::response::Response {
    (
        status,
        Json(ErrorBody {
            error: message.to_string(),
        }),
    )
        .into_response()
}
