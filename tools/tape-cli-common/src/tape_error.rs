//! Typed extraction of tapedrive program errors from an `rpc::RpcError`.
//!
//! `rpc::RpcError` only exposes its transaction failure as a stringified
//! message, so the one unavoidable string parse lives here. Callers match
//! on typed [`TapeError`] variants instead of hex-code substrings.

use tape_api::program::prelude::TapeError;

/// Extract a typed `TapeError` from an `RpcError` if the underlying cause
/// is a custom program error from the tapedrive program. Returns `None`
/// for all other shapes (transport, deserialization, non-tape program,
/// etc.).
pub fn as_tape_error(error: &rpc::RpcError) -> Option<TapeError> {
    let message = format!("{error:?}");
    let marker = "custom program error: 0x";
    let idx = message.find(marker)?;
    let rest = &message[idx + marker.len()..];
    let hex: String = rest.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    if hex.is_empty() {
        return None;
    }
    let code = u32::from_str_radix(&hex, 16).ok()?;
    TapeError::try_from(code).ok()
}

/// `AccountAlreadyInitialized` comes from the Solana runtime (not
/// tapedrive), so it's not a `TapeError`. This substring check is still
/// needed for idempotent "already exists" paths like register/stake.
pub fn is_already_initialized_runtime(error: &rpc::RpcError) -> bool {
    let s = format!("{error:?}");
    s.contains("AccountAlreadyInitialized")
        || s.contains("already initialized")
        || s.contains("Account already initialized")
        || s.contains("requires an uninitialized account")
}
