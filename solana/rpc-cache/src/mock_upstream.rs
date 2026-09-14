//! A scripted JSON-RPC upstream on a local socket, for tests

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

/// One scripted answer
pub struct Reply {
    pub status: &'static str,
    pub body: String,
    /// Held before answering, to keep the request in flight
    pub delay: Duration,
}

impl Reply {
    /// A 200 carrying this JSON-RPC result
    pub fn result(result: Value) -> Self {
        let body = json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string();
        Self { status: "200 OK", body, delay: Duration::ZERO }
    }

    /// A 200 carrying this JSON-RPC error
    pub fn error(code: i64, message: &str) -> Self {
        let error = json!({"code": code, "message": message});
        let body = json!({"jsonrpc": "2.0", "id": 1, "error": error}).to_string();
        Self { status: "200 OK", body, delay: Duration::ZERO }
    }

    /// A bare HTTP status with no JSON-RPC body
    pub fn status(status: &'static str) -> Self {
        Self { status, body: String::new(), delay: Duration::ZERO }
    }
}

/// A request as the mock received it
pub struct Request {
    pub arrived: Instant,
    pub body: Value,
}

/// State the connection tasks share
struct Shared<Respond> {
    respond: Respond,
    requests: Arc<Mutex<Vec<Request>>>,
    in_flight: AtomicUsize,
    max_in_flight: Arc<AtomicUsize>,
}

/// A running mock upstream; the server stops when this drops
pub struct MockUpstream {
    pub url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    max_in_flight: Arc<AtomicUsize>,
    server: JoinHandle<()>,
}

impl MockUpstream {
    /// Serve each request with the reply `respond` picks from its arrival index and body
    pub async fn start<Respond>(respond: Respond) -> Self
    where
        Respond: Fn(usize, &Value) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock upstream");
        let address = listener.local_addr().expect("mock upstream address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let shared = Arc::new(Shared {
            respond,
            requests: requests.clone(),
            in_flight: AtomicUsize::new(0),
            max_in_flight: max_in_flight.clone(),
        });

        Self {
            url: format!("http://{address}"),
            requests,
            max_in_flight,
            server: tokio::spawn(accept(listener, shared)),
        }
    }

    /// Arrival times of the requests received so far
    pub fn arrivals(&self) -> Vec<Instant> {
        let requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
        requests.iter().map(|request| request.arrived).collect()
    }

    /// JSON-RPC bodies of the requests received so far, in arrival order
    pub fn bodies(&self) -> Vec<Value> {
        let requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
        requests.iter().map(|request| request.body.clone()).collect()
    }

    /// Most requests the mock held at once
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn accept<Respond>(listener: TcpListener, shared: Arc<Shared<Respond>>)
where
    Respond: Fn(usize, &Value) -> Reply + Send + Sync + 'static,
{
    let mut connections = JoinSet::new();
    while let Ok((socket, _)) = listener.accept().await {
        while connections.try_join_next().is_some() {}
        connections.spawn(answer(socket, shared.clone()));
    }
}

async fn answer<Respond>(mut socket: TcpStream, shared: Arc<Shared<Respond>>)
where
    Respond: Fn(usize, &Value) -> Reply + Send + Sync + 'static,
{
    let Some(body) = read_body(&mut socket).await else {
        return;
    };
    let in_flight = shared.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    shared.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);

    let index = {
        let mut requests = shared.requests.lock().unwrap_or_else(PoisonError::into_inner);
        requests.push(Request { arrived: Instant::now(), body: body.clone() });
        requests.len() - 1
    };
    let reply = (shared.respond)(index, &body);
    tokio::time::sleep(reply.delay).await;

    let response = format!(
        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        reply.status,
        reply.body.len(),
        reply.body,
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
    shared.in_flight.fetch_sub(1, Ordering::SeqCst);
}

/// Read one HTTP request and parse its body as JSON
async fn read_body(socket: &mut TcpStream) -> Option<Value> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);

        let Some(header_end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
        let length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let body_start = header_end + 4;
        if buf.len() >= body_start + length {
            return serde_json::from_slice(&buf[body_start..body_start + length]).ok();
        }
    }
}
