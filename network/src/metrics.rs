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
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, 
    IntGauge, Opts, Registry, TextEncoder
};

use log::{error, info};

use crate::web::{RpcError, RpcMethod};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    // Total number of rpc requests
    static ref TAPE_RPC_REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "tape_rpc_requests_total",
            "Total number of Tape RPC calls, labelled by method and status"
        ),
        &["method", "status"]
    ).unwrap();

    // RPC request duration on seconds
    pub static ref TAPE_RPC_REQUEST_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "tape_rpc_request_duration_seconds",
            "RPC request latency in seconds, labeled by method."
        )
        .buckets(vec![
            0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
            1.000, 2.500, 5.000, 10.000, 30.000, 60.000
        ]),
        &["method"],
    )
    .unwrap();

    // Total number of challenges solved
    pub static ref TAPE_MINING_CHALLENGES_SOLVED_TOTAL: IntCounter = IntCounter::new(
        "tape_mining_challenges_solved_total",
        "Total number of mining challenges solved successfully"
    ).unwrap();

    // Total number of mining attempts
    pub static ref TAPE_MINING_ATTEMPTS_TOTAL: IntCounter = IntCounter::new(
        "tape_mining_attempts_total",
        "Total number of mining attempts"
    ).unwrap();

    // Time taken to successfully mine a tape
    pub static ref TAPE_MINING_DURATION_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "tape_mining_iteration_duration_seconds",
            "Time taken per mining iteration in seconds"
        ).buckets(vec![
            0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
            1.000, 2.500, 5.000, 10.000, 30.000, 60.000
        ]),
    ).unwrap();

    // Current Mining Iteration
    pub static ref TAPE_CURRENT_MINING_ITERTION: IntGauge = IntGauge::new(
        "tape_current_mining_iteration",
        "Current mining iteration"
    ).unwrap();

    // Total Tapes Written
    pub static ref TAPE_TOTAL_TAPES_WRITTEN: IntCounter = IntCounter::new(
        "tape_total_tapes_written",
        "Tape total tapes written"
    ).unwrap();

    // Total Segments Written
    pub static ref TAPE_TOTAL_SEGMENTS_WRITTEN: IntCounter = IntCounter::new(
        "tape_total_segments_written",
        "Tape total segments written"
    ).unwrap();

    
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



pub enum Process {
    Mine,
    Archive,
    Web
}

impl Process {
    fn metrics_port(&self) -> u16 {
        match self {
            Process::Archive => 8875,
            Process::Mine => 8874,
            Process::Web => 8873
        }
    }
}


pub fn run_metrics_server(process: Process) -> anyhow::Result<()> {
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

        match process { 
            Process::Archive => {
                register!(TAPE_TOTAL_TAPES_WRITTEN);
                register!(TAPE_TOTAL_SEGMENTS_WRITTEN);
            },
            Process::Mine => {
                register!(TAPE_MINING_ATTEMPTS_TOTAL);
                register!(TAPE_MINING_CHALLENGES_SOLVED_TOTAL);
                register!(TAPE_MINING_DURATION_SECONDS);
                register!(TAPE_CURRENT_MINING_ITERTION);
            },
            Process::Web => {
                register!(TAPE_RPC_REQUESTS_TOTAL);
                register!(TAPE_RPC_REQUEST_DURATION_SECONDS);
            }
        }
    });

    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), process.metrics_port()));

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

pub fn inc_tape_mining_challenges_solved_total() {
    TAPE_MINING_CHALLENGES_SOLVED_TOTAL.inc();
}

pub fn inc_tape_mining_attempts_total() {
    TAPE_MINING_ATTEMPTS_TOTAL.inc();
}

pub fn observe_tape_mining_duration(duration_secs: f64) {
    TAPE_MINING_DURATION_SECONDS
        .observe(duration_secs);
}

pub fn set_current_mining_iteration(current_iteration: u64) {
    TAPE_CURRENT_MINING_ITERTION.set(current_iteration as i64);
}

pub fn inc_total_tapes_written() {
    TAPE_TOTAL_TAPES_WRITTEN.inc();
}

pub fn inc_total_tapes_written_batch(n: u64) {
    TAPE_TOTAL_TAPES_WRITTEN.inc_by(n);
}

pub fn inc_total_segments_written() {
    TAPE_TOTAL_SEGMENTS_WRITTEN.inc();
}

pub fn inc_total_segments_written_batch(n: u64) {
    TAPE_TOTAL_SEGMENTS_WRITTEN.inc_by(n);
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

