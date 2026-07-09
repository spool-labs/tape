//! End-to-end failover: a 502 on the primary endpoint must hand off to the next
//! one rather than exhausting the retry budget against a dead server.
//!
//! Callers used to build the client with a single endpoint, so the rotation in
//! `failover.rs` could never fire in production. These tests drive the client
//! through real sockets to hold that behaviour down.

use std::time::Duration;

use rpc::Rpc;
use rpc_solana::{RpcConfig, RpcRetryConfig, SolanaRpc};
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
