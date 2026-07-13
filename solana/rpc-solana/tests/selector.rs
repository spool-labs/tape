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
const PRIMARY_SLOT: u64 = 111;
const FALLBACK_SLOT: u64 = 222;

/// Serve every request with the response the picker returns, forever.
async fn serve_with<Pick>(pick: Pick) -> String
where
    Pick: Fn() -> (&'static str, String) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let (status, body) = pick();
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

fn slot_body(slot: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","result":{slot},"id":1}}"#)
}

/// A healthy endpoint answering every getSlot with this slot.
async fn slot_endpoint(slot: u64) -> String {
    serve_with(move || ("200 OK", slot_body(slot))).await
}

async fn bad_endpoint() -> String {
    serve_with(|| ("502 Bad Gateway", "Bad Gateway".to_string())).await
}

/// Serve 502 until the heal delay passes, then answer with PRIMARY_SLOT.
async fn recovering_endpoint(heals_after: Duration) -> String {
    let started = std::time::Instant::now();

    serve_with(move || {
        if started.elapsed() >= heals_after {
            ("200 OK", slot_body(PRIMARY_SLOT))
        } else {
            ("502 Bad Gateway", "Bad Gateway".to_string())
        }
    })
    .await
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

fn config_with_strategy(endpoints: Vec<String>, strategy: EndpointStrategy) -> RpcConfig {
    RpcConfig {
        strategy,
        ..config(endpoints)
    }
}

// a 502 on the primary falls over to the healthy fallback
#[tokio::test]
async fn falls_over_from_bad_gateway() {
    let bad = bad_endpoint().await;
    let good = slot_endpoint(SLOT).await;

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
    let good = slot_endpoint(SLOT).await;
    let bad = bad_endpoint().await;

    let rpc = SolanaRpc::new(config(vec![good, bad])).expect("build client");
    let slot = rpc.get_slot().await.expect("slot from primary");

    assert_eq!(slot, SLOT);
}

// round-robin spreads consecutive operations across healthy endpoints
#[tokio::test]
async fn round_robin_rotates() {
    let first = slot_endpoint(PRIMARY_SLOT).await;
    let second = slot_endpoint(FALLBACK_SLOT).await;

    let config = config_with_strategy(vec![first, second], EndpointStrategy::RoundRobin);
    let rpc = SolanaRpc::new(config).expect("build client");

    // Rotation starts after the first endpoint, then wraps.
    assert_eq!(rpc.get_slot().await.expect("first pick"), FALLBACK_SLOT);
    assert_eq!(rpc.get_slot().await.expect("second pick"), PRIMARY_SLOT);
    assert_eq!(rpc.get_slot().await.expect("third pick"), FALLBACK_SLOT);
}

// failover-sticky stays on the fallback even after the primary recovers
#[tokio::test]
async fn sticky_stays() {
    let primary = recovering_endpoint(Duration::from_millis(400)).await;
    let fallback = slot_endpoint(FALLBACK_SLOT).await;

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
    let fallback = slot_endpoint(FALLBACK_SLOT).await;

    let rpc = SolanaRpc::new(config(vec![primary, fallback])).expect("build client");

    // The primary is down, so this is served by the fallback.
    assert_eq!(rpc.get_slot().await.expect("fallback slot"), FALLBACK_SLOT);

    // Let the primary heal and its cooldown lapse.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Traffic must come home rather than stay on the fallback forever.
    assert_eq!(rpc.get_slot().await.expect("primary slot"), PRIMARY_SLOT);
}
