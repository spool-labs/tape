//! TLS pinning, self-signed cert generation, and rustls provider helpers for
//! tapedrive peer-to-peer HTTPS.
//!
//! # Trust model
//!
//! Each node publishes its Ed25519 TLS public key on-chain in
//! `Node.metadata.network_tls`. Peer clients pin that exact key per
//! destination when building their HTTPS clients. A client has no CA trust
//! store for peer traffic — pinning is the sole authentication.
//!
//! For browser-trusted access via a real domain, operators front the node
//! with a reverse proxy (Caddy, nginx) that terminates a CA-issued cert and
//! forwards to the node's plaintext HTTP port. The peer HTTPS listener stays
//! self-signed + pinned; the reverse proxy handles the browser audience.
//!
//! For non-peer HTTPS (SDK calls to public RPC endpoints, external services),
//! use the WebPKI builders, which validate against the Mozilla root store.
//!
//! # Usage sketch
//!
//! ```no_run
//! use peer_tls::{install_default_provider, apply_pinned_tls, build_server_config};
//! use tape_core::types::tls::NetworkTlsPubkey;
//!
//! // Once per process, before any rustls config is built:
//! install_default_provider();
//!
//! // Client, peer-pinned (per destination):
//! let peer_pubkey = NetworkTlsPubkey::new_unique();
//! let client = apply_pinned_tls(reqwest::Client::builder(), peer_pubkey)
//!     .expect("pin")
//!     .build()
//!     .expect("build");
//!
//! // Server (self-signed):
//! # let keypair: tape_crypto::ed25519::Keypair = unimplemented!();
//! let san = vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)];
//! let server_cfg = build_server_config(&keypair, &san).expect("server cfg");
//! ```

pub mod cert;
pub mod client;
pub mod error;
pub mod provider;
pub mod server;
pub mod spki;
pub mod verifier;

pub use cert::{SelfSignedCert, self_signed_cert};
pub use client::{apply_pinned_tls, apply_pinned_tls_with_identity, apply_webpki_tls, pinned_client};
pub use error::TlsError;
pub use provider::install_default as install_default_provider;
pub use server::{build_server_config, build_server_config_with_peer_auth};
pub use spki::{ED25519_SPKI_LEN, decode_ed25519_spki, encode_ed25519_spki};
pub use verifier::{PeerClientVerifier, PinnedVerifier, TlsVerifier};
