//! The block store behind the `getBlock` fast path: a SQLite file that keeps
//! the filtered blocks across restarts, with a memory layer in front of it

pub mod block;
pub mod database;
pub mod error;
pub mod meta;
pub mod slot_store;
pub mod sql;

/// Every table, safe to run again on open
pub const SCHEMA: &str = include_str!("schema.sql");
