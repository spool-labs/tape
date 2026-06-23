//! High-level SDK for tapedrive blob upload/download operations.

pub mod codec;
pub mod error;
pub mod gateway;
pub mod keys;
pub mod metrics;
pub mod object;
pub mod staking;
pub mod stream;
pub mod tape;
pub mod tapedrive;
pub mod track;
pub mod transfer;

pub use gateway::Gateway;
pub use tapedrive::Tapedrive;
