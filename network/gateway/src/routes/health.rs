use axum::{http::StatusCode, response::IntoResponse, Json};

/// GET /status
///
/// Gateway health check.
pub async fn get_status() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}
