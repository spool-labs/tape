//! End-to-end endpoint selection: a 502 on the primary endpoint must hand off
//! to the next one rather than exhausting the retry budget against a dead
//! server, and each strategy must pick the endpoint its config promises.
//!
//! These tests drive the client through real sockets to hold that behaviour
//! down.

use std::time::Duration;

use rpc::Rpc;
use rpc_solana::{EndpointStrategy, RpcConfig, RpcRetryConfig, SolanaRpc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SLOT: u64 = 475_097_571;

/// Serve one canned HTTP response to every request, forever.
async fn serve(status: &'static str, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    format!("http://{addr}")
}

async fn bad_endpoint() -> String {
    serve("502 Bad Gateway", "Bad Gateway").await
}

async fn good_endpoint() -> String {
    serve("200 OK", r#"{"jsonrpc":"2.0","result":475097571,"id":1}"#).await
}

/// Retry quickly so a failing endpoint does not stall the test.
fn config(endpoints: Vec<String>) -> RpcConfig {
    RpcConfig {
        endpoints,
        retry: RpcRetryConfig {
            max_retries: 3,
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            jitter: false,
            max_endpoint_attempts: 3,
            endpoint_cooldown: Duration::from_millis(200),
        },
        ..RpcConfig::default()
    }
}

// a 502 on the primary falls over to the healthy fallback
#[tokio::test]
async fn falls_over_from_bad_gateway() {
    let bad = bad_endpoint().await;
    let good = good_endpoint().await;

    let rpc = SolanaRpc::new(config(vec![bad, good])).expect("build client");
    let slot = rpc.get_slot().await.expect("slot through fallback");

    assert_eq!(slot, SLOT);
}

// with only the failing endpoint there is nowhere to go, so the call fails
#[tokio::test]
async fn single_endpoint_has_no_fallback() {
    let bad = bad_endpoint().await;

    let rpc = SolanaRpc::new(config(vec![bad])).expect("build client");

    assert!(rpc.get_slot().await.is_err());
}

// the healthy endpoint is used directly when it comes first
#[tokio::test]
async fn primary_serves_without_failover() {
    let good = good_endpoint().await;
    let bad = bad_endpoint().await;

    let rpc = SolanaRpc::new(config(vec![good, bad])).expect("build client");
    let slot = rpc.get_slot().await.expect("slot from primary");

    assert_eq!(slot, SLOT);
}

const PRIMARY_SLOT: u64 = 111;
const FALLBACK_SLOT: u64 = 222;

/// Serve 502 until `heals_after`, then answer with `PRIMARY_SLOT`.
async fn recovering_endpoint(heals_after: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let started = std::time::Instant::now();

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let healthy = started.elapsed() >= heals_after;
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let (status, body) = if healthy {
                    ("200 OK", format!(r#"{{"jsonrpc":"2.0","result":{PRIMARY_SLOT},"id":1}}"#))
                } else {
                    ("502 Bad Gateway", "Bad Gateway".to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    format!("http://{addr}")
}

fn config_with_strategy(endpoints: Vec<String>, strategy: EndpointStrategy) -> RpcConfig {
    RpcConfig {
        strategy,
        ..config(endpoints)
    }
}

// round-robin spreads consecutive operations across healthy endpoints
#[tokio::test]
async fn round_robin_rotates() {
    let first = serve("200 OK", r#"{"jsonrpc":"2.0","result":111,"id":1}"#).await;
    let second = serve("200 OK", r#"{"jsonrpc":"2.0","result":222,"id":1}"#).await;

    let config = config_with_strategy(vec![first, second], EndpointStrategy::RoundRobin);
    let rpc = SolanaRpc::new(config).expect("build client");

    // Rotation starts after the first endpoint, then wraps.
    assert_eq!(rpc.get_slot().await.expect("first pick"), 222);
    assert_eq!(rpc.get_slot().await.expect("second pick"), 111);
    assert_eq!(rpc.get_slot().await.expect("third pick"), 222);
}

// failover-sticky stays on the fallback even after the primary recovers
#[tokio::test]
async fn sticky_stays() {
    let primary = recovering_endpoint(Duration::from_millis(400)).await;
    let fallback = serve("200 OK", r#"{"jsonrpc":"2.0","result":222,"id":1}"#).await;

    let config = config_with_strategy(vec![primary, fallback], EndpointStrategy::FailoverSticky);
    let rpc = SolanaRpc::new(config).expect("build client");

    // The primary is down, so this is served by the fallback.
    assert_eq!(rpc.get_slot().await.expect("fallback slot"), FALLBACK_SLOT);

    // Let the primary heal and its cooldown lapse.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Sticky never walks back on its own.
    assert_eq!(rpc.get_slot().await.expect("still fallback"), FALLBACK_SLOT);
}

// a recovered primary is picked back up once its cooldown lapses
#[tokio::test]
async fn returns_to_primary_after_cooldown() {
    let primary = recovering_endpoint(Duration::from_millis(400)).await;
    let fallback = serve("200 OK", r#"{"jsonrpc":"2.0","result":222,"id":1}"#).await;

    let rpc = SolanaRpc::new(config(vec![primary, fallback])).expect("build client");

    // The primary is down, so this is served by the fallback.
    assert_eq!(rpc.get_slot().await.expect("fallback slot"), FALLBACK_SLOT);

    // Let the primary heal and its cooldown lapse.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Traffic must come home rather than stay on the fallback forever.
    assert_eq!(rpc.get_slot().await.expect("primary slot"), PRIMARY_SLOT);
}
