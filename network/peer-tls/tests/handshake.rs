//! End-to-end handshake tests: tokio-rustls server (self-signed P-256 cert)
//! and rustls client (pinned-pubkey verifier) negotiate TLS 1.3 over loopback.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use peer_tls::{
    TlsVerifier, apply_pinned_tls_with_identity, build_server_config,
    build_server_config_with_peer_auth, install_default_provider,
};
use rand::thread_rng;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::p256::Keypair as P256Keypair;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn init() {
    install_default_provider();
}

fn pubkey_of(kp: &P256Keypair) -> NetworkTlsPubkey {
    NetworkTlsPubkey::new(kp.public_key_bytes())
}

async fn run_server(listener: TcpListener, acceptor: TlsAcceptor, payload: &'static [u8]) {
    let (tcp, _) = listener.accept().await.expect("accept");
    let mut stream = acceptor.accept(tcp).await.expect("tls accept");
    stream.write_all(payload).await.expect("write");
    stream.shutdown().await.expect("shutdown");
}

async fn connect_pinned(
    addr: std::net::SocketAddr,
    pin: NetworkTlsPubkey,
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
    let server_kp = P256Keypair::generate(&mut rng);
    let pin = pubkey_of(&server_kp);

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
    let server_kp = P256Keypair::generate(&mut rng);
    let wrong = pubkey_of(&P256Keypair::generate(&mut rng));

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
    let pubkey = NetworkTlsPubkey::new_unique();
    match TlsVerifier::pinned(pubkey) {
        TlsVerifier::PinnedPublicKey(p) => assert_eq!(p.expected_key(), pubkey),
        _ => panic!("expected PinnedPublicKey"),
    }
}

#[tokio::test]
async fn mtls_handshake_captures_client_cert() {
    init();
    let mut rng = thread_rng();
    let server_kp = P256Keypair::generate(&mut rng);
    let client_kp = P256Keypair::generate(&mut rng);
    let expected_client_spki = pubkey_of(&client_kp);

    let server_config = build_server_config_with_peer_auth(
        &server_kp,
        &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
    )
    .expect("server cfg");
    let acceptor = TlsAcceptor::from(server_config);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let captured: Arc<tokio::sync::Mutex<Option<NetworkTlsPubkey>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let captured_clone = captured.clone();

    let server_task = tokio::spawn(async move {
        use x509_parser::prelude::FromDer;
        let (tcp, _) = listener.accept().await.expect("accept");
        let mut stream = acceptor.accept(tcp).await.expect("tls accept");
        {
            let (_io, conn) = stream.get_ref();
            let certs = conn.peer_certificates().expect("client cert present");
            let (_, parsed) =
                x509_parser::certificate::X509Certificate::from_der(certs[0].as_ref())
                    .expect("parse");
            let spki =
                peer_tls::decode_p256_spki(parsed.public_key().raw).expect("p256 spki");
            *captured_clone.lock().await = Some(spki);
        }
        stream.write_all(b"ack").await.expect("write");
        stream.shutdown().await.expect("shutdown");
    });

    let builder = reqwest::Client::builder();
    let builder = apply_pinned_tls_with_identity(builder, pubkey_of(&server_kp), &client_kp)
        .expect("client tls");
    let client = builder.build().expect("build");
    let url = format!("https://{addr}/");
    // We expect the request itself to fail because there's no HTTP server on
    // the other end — but the TLS handshake must complete and populate
    // peer_certificates before the task drops. Reqwest gives us a connection
    // error after send, which is fine.
    let _ = client.get(&url).send().await;

    server_task.await.expect("server join");

    let locked = captured.lock().await;
    assert_eq!(*locked, Some(expected_client_spki));
}
