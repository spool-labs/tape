//! End-to-end handshake tests: tokio-rustls server (self-signed Ed25519 cert)
//! and rustls client (pinned-pubkey verifier) negotiate TLS 1.3 over loopback.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use peer_tls::{TlsVerifier, build_server_config, install_default_provider};
use rand::thread_rng;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as EdKeypair;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn init() {
    install_default_provider();
}

async fn run_server(listener: TcpListener, acceptor: TlsAcceptor, payload: &'static [u8]) {
    let (tcp, _) = listener.accept().await.expect("accept");
    let mut stream = acceptor.accept(tcp).await.expect("tls accept");
    stream.write_all(payload).await.expect("write");
    stream.shutdown().await.expect("shutdown");
}

async fn connect_pinned(
    addr: std::net::SocketAddr,
    pin: Address,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let verifier = Arc::new(TlsVerifier::pinned(pin));
    let config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .dangerous()
    .with_custom_certificate_verifier(verifier)
    .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await?;
    let sni = ServerName::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST).into());
    let mut stream = connector.connect(sni, tcp).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

#[tokio::test]
async fn end_to_end_handshake_with_matching_pin() {
    init();
    let mut rng = thread_rng();
    let server_kp = EdKeypair::new(&mut rng);
    let pin = server_kp.address();

    let server_config =
        build_server_config(&server_kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("server cfg");
    let acceptor = TlsAcceptor::from(server_config);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let handle = tokio::spawn(run_server(listener, acceptor, b"hello"));
    let out = connect_pinned(addr, pin).await.expect("client");
    assert_eq!(out, b"hello");
    handle.await.expect("server join");
}

#[tokio::test]
async fn end_to_end_handshake_rejects_wrong_pin() {
    init();
    let mut rng = thread_rng();
    let server_kp = EdKeypair::new(&mut rng);
    let wrong = EdKeypair::new(&mut rng).address();

    let server_config =
        build_server_config(&server_kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("server cfg");
    let acceptor = TlsAcceptor::from(server_config);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let _handle = tokio::spawn(async move {
        if let Ok((tcp, _)) = listener.accept().await {
            let _ = acceptor.accept(tcp).await;
        }
    });

    let result = connect_pinned(addr, wrong).await;
    assert!(result.is_err(), "expected pin-mismatch rejection, got {result:?}");
}

#[tokio::test]
async fn pinned_verifier_exposes_expected_key_via_public_api() {
    init();
    let addr = Address::new_unique();
    match TlsVerifier::pinned(addr) {
        TlsVerifier::PinnedPublicKey(p) => assert_eq!(p.expected_key(), addr),
        _ => panic!("expected PinnedPublicKey"),
    }
}
