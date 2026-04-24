//! TLS pinning, self-signed cert generation, and rustls provider helpers for
//! tapedrive peer-to-peer HTTPS.
//!
//! # Trust model
//!
//! Each node publishes its P-256 (secp256r1) TLS public key on-chain in
//! `Node.metadata.network_tls`. Peer clients pin that exact key per
//! destination when building their HTTPS clients. A client has no CA trust
//! store for peer traffic — pinning is the sole authentication.
//!
//! Operators who own a domain can serve a CA-issued cert for that domain
//! (e.g. via Let's Encrypt) by pointing the node at a PEM cert file; the cert
//! must be issued to the same P-256 keypair whose pubkey is on-chain. The
//! server validates that invariant at startup. Browser/curl trust is then
//! satisfied by the CA chain, and SDK/peer pinning is satisfied by the key
//! match.
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
//! # let keypair: tape_crypto::p256::Keypair = unimplemented!();
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
pub use server::{build_server_config, build_server_config_from_pem, build_server_config_with_peer_auth};
pub use spki::{P256_SPKI_LEN, decode_p256_spki, encode_p256_spki};
pub use verifier::{PeerClientVerifier, PinnedVerifier, TlsVerifier};
