//! Custom axum-server acceptor that captures the mTLS client certificate
//! pubkey from each connection and injects it as a request extension.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum_server::accept::Accept;
use axum_server::tls_rustls::RustlsAcceptor;
use peer_tls::decode_ed25519_spki;
use tape_crypto::address::Address;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tower::Service;
use x509_parser::prelude::FromDer;
use x509_parser::certificate::X509Certificate;

/// The mTLS-derived identity of an inbound connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerIdentity(pub Option<Address>);

impl PeerIdentity {
    pub fn anonymous() -> Self {
        Self(None)
    }

    pub fn authenticated(address: Address) -> Self {
        Self(Some(address))
    }

    pub fn pubkey(self) -> Option<Address> {
        self.0
    }
}

fn identity_from_certs(
    certs: Option<&[rustls::pki_types::CertificateDer<'_>]>,
) -> PeerIdentity {
    let Some(certs) = certs else {
        return PeerIdentity::anonymous();
    };
    let Some(leaf) = certs.first() else {
        return PeerIdentity::anonymous();
    };
    let Ok((_, parsed)) = X509Certificate::from_der(leaf.as_ref()) else {
        return PeerIdentity::anonymous();
    };
    match decode_ed25519_spki(parsed.public_key().raw) {
        Some(addr) => PeerIdentity::authenticated(addr),
        None => PeerIdentity::anonymous(),
    }
}

/// Wraps [`RustlsAcceptor`]; after the handshake, reads the client cert and
/// attaches a [`PeerIdentity`] to every request on that connection.
#[derive(Clone)]
pub struct PeerIdentityAcceptor {
    inner: RustlsAcceptor,
}

impl PeerIdentityAcceptor {
    pub fn new(inner: RustlsAcceptor) -> Self {
        Self { inner }
    }
}

impl<I, S> Accept<I, S> for PeerIdentityAcceptor
where
    RustlsAcceptor: Accept<I, S>,
    <RustlsAcceptor as Accept<I, S>>::Stream: AsAny,
    <RustlsAcceptor as Accept<I, S>>::Future: Send,
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Stream = <RustlsAcceptor as Accept<I, S>>::Stream;
    type Service = InjectPeerIdentity<<RustlsAcceptor as Accept<I, S>>::Service>;
    type Future = Pin<
        Box<
            dyn Future<
                    Output = io::Result<(Self::Stream, Self::Service)>,
                > + Send,
        >,
    >;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner_future = self.inner.accept(stream, service);
        Box::pin(async move {
            let (tls_stream, service) = inner_future.await?;
            let identity = tls_stream.peer_identity();
            Ok((
                tls_stream,
                InjectPeerIdentity {
                    inner: service,
                    identity,
                },
            ))
        })
    }
}

/// Helper trait letting us extract peer certs from any TLS stream type the
/// inner acceptor might return.
pub trait AsAny {
    fn peer_identity(&self) -> PeerIdentity;
}

impl<I> AsAny for TlsStream<I> {
    fn peer_identity(&self) -> PeerIdentity {
        let (_io, conn) = self.get_ref();
        identity_from_certs(conn.peer_certificates())
    }
}

/// Tower service wrapper that inserts the captured [`PeerIdentity`] into
/// every request's extensions before delegating to the inner service.
#[derive(Clone)]
pub struct InjectPeerIdentity<S> {
    inner: S,
    identity: PeerIdentity,
}

impl<S, B> Service<hyper::Request<B>> for InjectPeerIdentity<S>
where
    S: Service<hyper::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: hyper::Request<B>) -> Self::Future {
        req.extensions_mut().insert(self.identity);
        self.inner.call(req)
    }
}
