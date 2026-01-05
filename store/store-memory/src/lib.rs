//! store-memory: In-memory backend for the Store trait
//!
//! This crate provides a simple HashMap-based implementation of the `Store` trait.
//! It's useful for:
//! - Unit testing without filesystem dependencies
//! - Prototyping and development
//! - As a template for implementing custom storage backends
//!
//! # Creating Your Own Backend
//!
//! To create a custom storage backend, copy this crate and implement the `Store` trait
//! from the `store` crate. See `memory.rs` for a complete reference implementation.
//!
//! The `Store` trait requires implementing:
//! - `get(cf, key)` - Read a value
//! - `put(cf, key, value)` - Write a value
//! - `delete(cf, key)` - Delete a value
//! - `contains(cf, key)` - Check if key exists
//! - `write_batch(batch)` - Atomic batch operations
//! - `iter(cf)` - Iterate all entries
//! - `iter_prefix(cf, prefix)` - Iterate entries with key prefix
//! - `iter_from(cf, start, direction)` - Iterate from a starting key
//! - `iter_range(cf, start, end)` - Iterate a key range
//!
//! # Example
//!
//! ```
//! use store_memory::MemoryStore;
//! use store::Store;
//!
//! let store = MemoryStore::new();
//!
//! store.put("users", b"alice", b"admin").unwrap();
//! let value = store.get("users", b"alice").unwrap();
//! assert_eq!(value, Some(b"admin".to_vec()));
//! ```

mod memory;

pub use memory::MemoryStore;
