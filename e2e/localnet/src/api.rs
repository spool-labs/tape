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
        .layer(CorsLayer::permissive())
        .with_state(state)
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
