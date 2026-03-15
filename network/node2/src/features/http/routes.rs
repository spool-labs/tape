use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: HealthStatus::Ready,
    })
}
