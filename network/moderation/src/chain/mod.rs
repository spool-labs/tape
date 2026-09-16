//! The chain: tape upkeep, reservations, and inline writes and deletes through `tape-sdk`

pub mod error;
#[cfg(test)]
pub mod fake;
pub mod keys;
pub mod sdk;
pub mod tape_chain;
