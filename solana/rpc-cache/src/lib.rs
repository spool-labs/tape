//! Solana RPC caching proxy.
//!
//! One process, HTTP server + in-process cache + upstream forwarder.
//! Purpose: collapse duplicate `get_*` calls from a tape-node fleet into
//! a single upstream call per cache window. See [`docs/rpc-cache.md`].
//!
//! Caller-facing protocol is standard JSON-RPC over HTTP. Batch requests
//! are passed through uncached (v1 simplification).
//!
//! `getBlock` takes a separate fast path: a slot-keyed in-memory store
//! pre-warmed at boot with confirmed blocks from the current tape epoch
//! start to the live edge, filtered down to tape-relevant data only.
//! See [`runtime`] for the bootstrap/live-tail tasks and [`filter`] for
//! the per-block reduction.

pub mod cache;
pub mod config;
pub mod filter;
pub mod key;
pub mod runtime;
pub mod server;
pub mod submit_log;
pub mod upstream;
