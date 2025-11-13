//! Spool allocation and reassignment.
//!
//! Defines the `Spooler` trait and shared types to allocate and migrate spools
//! among committee members.
//!
//! Implementations provided:
//! - `DhondtSpooler`: D'Hondt method with tie-breaking and caps.
//! - `SainteLagueSpooler`: Sainte-Lague method with tie-breaking and caps.
//!
//! Low-level types:
//! - `SpoolAssignment`: compact, zero-copy assignment table with const-generic size.
//!
//! Typical flow:
//! 1) Use a `Spooler` (e.g., `DhondtSpooler`) to compute per-member spool counts.
//! 2) Call `migrate_spools` to minimally move spools from current -> next layout.

mod assignment;
mod priority;
mod migrate;
mod dhondt;
mod sainte_lague;

pub use assignment::*;
pub use priority::*;
pub use migrate::*;
pub use dhondt::*;
pub use sainte_lague::*;

use crate::types::{Coin, TAPE};

/// No committee member may have more than 2% of the spools 
/// (for sufficiently large committees).
const MAX_SPOOL_ALLOCATION: u64 = 20;

/// Minimum committee size needed to enforce max per-node share.
const MIN_MEMBER_COUNT: u64 = 32;

pub type SpoolIndex = u16;
pub type SpoolCount = u16;
pub type SpoolMapping = u8;

/// Errors used across spool allocation and migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    CountMismatch,
    MemberLimit,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    BadIndex,
    NotNext,
}

/// Trait for spool allocation policies.
///
/// Implementations compute per-member spool counts given stake weights and
/// a target spool total.
pub trait Spooler: Default {
    fn allocate(
        &mut self,
        stake_weight: &[Coin<TAPE>],
        total_spools: u16,
    ) -> Result<Vec<SpoolCount>, SpoolerError>;
}


/// Limit the maximum number of spools per node based on the total number of nodes.
pub fn cap_spools(node_count: u64, spool_count: u64) -> u64 {
    if spool_count == 0 || node_count == 0 {
        return 0;
    }
    if node_count >= MIN_MEMBER_COUNT {
        spool_count / MAX_SPOOL_ALLOCATION
    } else {
        let num = spool_count.saturating_mul(MIN_MEMBER_COUNT);
        let den = node_count.saturating_mul(MAX_SPOOL_ALLOCATION);
        num.saturating_add(den - 1) / den
    }
}

