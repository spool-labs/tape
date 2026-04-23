//! Solana RPC caching proxy.
//!
//! One process, HTTP server + in-process cache + upstream forwarder.
//! Purpose: collapse duplicate `get_*` calls from a tape-node fleet into
//! a single upstream call per cache window. See [`docs/rpc-cache.md`].
//!
//! Caller-facing protocol is standard JSON-RPC over HTTP. Batch requests
//! are passed through uncached (v1 simplification).

pub mod cache;
pub mod config;
pub mod key;
pub mod server;
pub mod submit_log;
pub mod upstream;
