
pub struct HttpServer {
    config: HttpConfig,
    cancel: CancellationToken,
}

impl HttpServer {
    pub fn new(config: HttpConfig, cancel: CancellationToken) -> Self {
        Self { config, cancel }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let app = Router::new().route("/health", get(routes::health)).layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_http_error))
                .layer(TraceLayer::new_for_http())
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(self.config.concurrency_limit))
                .layer(TimeoutLayer::new(self.config.request_timeout)),
        );

        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(address = %self.config.bind_addr, "http server listening");

        let cancel = self.cancel.clone();

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel.cancelled().await;
            })
            .await
            .map_err(NodeError::Io)
    }
}

async fn handle_http_error(error: BoxError) -> StatusCode {
    if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::REQUEST_TIMEOUT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
