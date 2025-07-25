use std::{
    convert::Infallible, fmt::{self}, net::{Ipv4Addr, SocketAddr, SocketAddrV4}, sync::Once, time::Instant
};

use hyper::{Request, Response};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use http_body_util::Full;

use tokio::net::TcpListener;

use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec,
    Opts, Registry, TextEncoder,
};

use log::{error, info};

use crate::web::{RpcError, RpcMethod};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    static ref TAPE_RPC_REQUESTS_TOTAL:IntCounterVec = IntCounterVec::new(
        Opts::new("TAPE_RPC_requests_total", "Total number of Tape RPC calls, labelled by method and status"),
        &["method", "status"]
    ).unwrap();

    pub static ref TAPE_RPC_REQUEST_DURATION_SECONDS:HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "TAPE_RPC_request_duration_seconds",
            "RPC request latency in seconds, labeled by method."
        )
        .buckets(vec![
            0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
            1.000, 2.500, 5.000, 10.000, 30.000, 60.000
        ]),
        &["method"],
    )
    .unwrap();
}

fn metrics_handler() -> Result<Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });
        
    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics)))
        .unwrap())
}


async fn handle_metrics_request(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => metrics_handler(),
        _ => Ok(not_found_handler()),
    }
}

fn not_found_handler() -> Response<Full<Bytes>> {
    Response::builder()
        .status(404)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}


pub fn run_metrics_server() -> anyhow::Result<()> {
    // Register once
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }
        register!(TAPE_RPC_REQUESTS_TOTAL);
        register!(TAPE_RPC_REQUEST_DURATION_SECONDS);
    });
    // endpoint = http://0.0.0.0:8875
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 8875));

    tokio::spawn(async move {
        let listener = match TcpListener::bind(address).await {
            Ok(l) => {
                info!("Prometheus server started at http://{address}/metrics");
                l
            },
            Err(e) => {
                error!("Failed to bind Prometheus server: {e:?}");
                return;
            }
        };

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("Prometheus accept failed: {e:?}");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let service = service_fn(
                move |req: Request<Incoming>| 
                    handle_metrics_request(req)
                );

            tokio::spawn(async move {
                let builder = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
                let conn = builder.serve_connection(io, service);
                if let Err(e) = conn.await {
                    error!("Prometheus connection failed: {e:?}");
                }
            });
        }
    });

    Ok(())
}


pub enum RpcRequestStatus<'a> {
    Ok,
    Error(&'a RpcError),
}

impl fmt::Display for RpcRequestStatus<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcRequestStatus::Ok => write!(f, "OK"),
            RpcRequestStatus::Error(e) => write!(f, "{}", e.err_code()),
        }
    }
}

pub fn inc_td_api_status_total(method: &RpcMethod, status: RpcRequestStatus) {
    TAPE_RPC_REQUESTS_TOTAL
        .with_label_values(&[method.as_str() , &status.to_string()])
        .inc();
}

pub fn record_td_api_latency(method: &RpcMethod, time_elapsed: f64) {
    TAPE_RPC_REQUEST_DURATION_SECONDS
        .with_label_values(&[method.as_str()])
        .observe(time_elapsed);
}

pub fn record_metrics<T, F>(method: &RpcMethod, f: F) -> Result<T, RpcError>
where
    F: FnOnce() -> Result<T, RpcError>,
{
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed().as_secs_f64();
    record_td_api_latency(method, elapsed);
    match &result {
        Ok(_) => inc_td_api_status_total(method, RpcRequestStatus::Ok),
        Err(err) => {
            inc_td_api_status_total(method, RpcRequestStatus::Error(err));
        }
    };

    result
}

