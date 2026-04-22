//! rustls crypto-provider installation.
//!
//! rustls 0.23 requires a `CryptoProvider` to be installed as the process-wide
//! default before the first `ClientConfig` or `ServerConfig` is built. Call
//! [`install_default`] once at process start (node binaries, test harnesses).

use std::sync::Arc;

use rustls::crypto::{CryptoProvider, ring};

/// Install the ring crypto provider as the rustls process-wide default.
///
/// Idempotent: if the default is already set (to ring or anything else), this
/// is a no-op. Returns the currently-installed provider so callers can inspect
/// it if needed.
pub fn install_default() -> Arc<CryptoProvider> {
    let provider = ring::default_provider();
    let _ = provider.clone().install_default();
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(provider))
}

/// The ring crypto provider used by this crate.
pub fn ring_provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}
