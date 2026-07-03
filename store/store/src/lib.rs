//! store: A thin typed key-value store abstraction
//!
//! This crate provides a storage abstraction layer with the `Store` trait.
//! The trait is designed to be implemented by various backends:
//!
//! # Available Backends
//!
//! - `store-memory`: In-memory HashMap-based storage (for testing)
//! - `store-rocks`: RocksDB-based persistent storage (for production)
//!
//! The crate offers both low-level byte-oriented access via the `Store` trait
//! and high-level typed access via the `TypedStore` wrapper with `Column` definitions.
//!
//! # Low-level Example
//!
//! ```ignore
//! use store::{Store, WriteBatch};
//! use store_memory::MemoryStore;
//!
//! let store = MemoryStore::new();
//!
//! // Basic operations
//! store.put("users", b"alice", b"admin").unwrap();
//! let value = store.get("users", b"alice").unwrap();
//! assert_eq!(value, Some(b"admin".to_vec()));
//!
//! // Batch operations
//! let mut batch = WriteBatch::new();
//! batch.put("users", b"bob", b"user");
//! batch.put("users", b"charlie", b"user");
//! batch.delete("users", b"alice");
//! store.write_batch(batch).unwrap();
//! ```
//!
//! # Typed Column Example
//!
//! ```ignore
//! use store::{Column, TypedStore};
//! use store_memory::MemoryStore;
//!
//! // Define a column with primitive types (u64 key, String value)
//! struct Users;
//! impl Column for Users {
//!     const CF_NAME: &'static str = "users";
//!     type Key = u64;
//!     type Value = String;
//! }
//!
//! let store = TypedStore::new(MemoryStore::new());
//! store.put::<Users>(&1, &"Alice:30".to_string()).unwrap();
//! let user = store.get::<Users>(&1).unwrap();
//! assert_eq!(user, Some("Alice:30".to_string()));
//! ```

pub mod batch;
mod column;
mod error;
pub mod store;
mod typed;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "metrics")]
pub use metrics::{get_metrics, init_metrics, OperationTimer, StoreMetrics};

pub use batch::{BatchOp, WriteBatch};
pub use column::Column;
pub use error::Error;
pub use store::{DiskVolume, Direction, KeyValue, Store, StoreIter, StoreVolume};
pub use typed::TypedStore;

pub type Result<T> = std::result::Result<T, Error>;
