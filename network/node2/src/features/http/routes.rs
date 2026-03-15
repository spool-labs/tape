// <todo>

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: HealthStatus::Ready,
    })
}

