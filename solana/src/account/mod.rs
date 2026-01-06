//! Account handling module for Solana programs.
//!
//! Provides validation, deserialization, CPI helpers, and account management.

mod close;
mod cpi;
mod deserialize;
mod lamports;
mod validation;

pub use close::*;
pub use cpi::*;
pub use deserialize::*;
pub use lamports::*;
pub use validation::*;
