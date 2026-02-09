//! Group-aware spool allocation and migration.
//!
//! Provides allocation policies (D'Hondt, Sainte-Lague) and a group-constrained
//! migration algorithm that ensures no node holds more than one spool per group.
//!
//! Typical flow:
//! 1) Use `DhondtSpooler` or `SainteLagueSpooler` to compute per-member spool counts.
//! 2) Call `migrate_spools` to minimally reassign spools from current -> next layout,
//!    enforcing group constraints (1 spool per group per node).

use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::{SpoolCount, SpoolerError};
use tape_core::system::Committee;
use tape_core::types::EpochNumber;

mod heap;
mod priority;
pub mod dhondt;
pub mod sainte_lague;
pub mod migrate;

pub use dhondt::{DhondtSpooler, dhondt_allocate};
pub use sainte_lague::{SainteLagueSpooler, sainte_lague_allocate};
pub use migrate::{migrate_spools, initial_assignment};

/// Stake weight used by allocation algorithms.
/// Internally u64 to match `Coin<TAPE>::as_u64()`.
pub type Stake = u64;

/// Maximum spools any single node can hold (one per group = 50).
pub const MAX_SPOOLS_PER_NODE: SpoolCount = SPOOL_GROUP_COUNT as SpoolCount;

/// Divisor for cap calculation (1000 / 20 = 50 = MAX_SPOOLS_PER_NODE at full quorum).
const MAX_SPOOL_ALLOCATION: u64 = 20;

/// Minimum committee size for the linear cap formula.
const MIN_COMMITTEE_SIZE: u64 = SPOOL_GROUP_SIZE as u64;

/// Compute the per-node spool cap.
///
/// With `node_count >= MIN_COMMITTEE_SIZE`, cap = `spool_count / MAX_SPOOL_ALLOCATION`.
/// For smaller committees the cap is scaled up so that all spools can be distributed.
pub fn cap_spools(node_count: u64, spool_count: u64) -> u64 {
    if spool_count == 0 || node_count == 0 {
        return 0;
    }
    if node_count >= MIN_COMMITTEE_SIZE {
        spool_count / MAX_SPOOL_ALLOCATION
    } else {
        let num = spool_count.saturating_mul(MIN_COMMITTEE_SIZE);
        let den = node_count.saturating_mul(MAX_SPOOL_ALLOCATION);
        num.saturating_add(den - 1) / den
    }
}

/// Allocate + migrate in one call using D'Hondt.
pub fn migrate_dhondt<const SPOOLS: usize, const N: usize>(
    assignment: &mut tape_core::spooler::SpoolAssignment<SPOOLS>,
    current: &Committee<N>,
    next: &Committee<N>,
    epoch: EpochNumber,
) -> Result<(), SpoolerError> {
    let members_current = current.active_members();
    let members_next = next.active_members();
    let stakes_next: Vec<Stake> = next.active_stakes().iter().map(|c| c.as_u64()).collect();

    let mut dh = DhondtSpooler::default();
    let spool_counts = dh.allocate(&stakes_next, SPOOLS as u16)?;

    let spools = migrate_spools(
        &assignment.0,
        &members_current,
        &members_next,
        &spool_counts,
        epoch,
    )?;
    for i in 0..SPOOLS {
        assignment.0[i] = spools[i];
    }
    Ok(())
}

/// Allocate + migrate in one call using Sainte-Lague.
pub fn migrate_sainte_lague<const SPOOLS: usize, const N: usize>(
    assignment: &mut tape_core::spooler::SpoolAssignment<SPOOLS>,
    current: &Committee<N>,
    next: &Committee<N>,
    epoch: EpochNumber,
) -> Result<(), SpoolerError> {
    let members_current = current.active_members();
    let members_next = next.active_members();
    let stakes_next: Vec<Stake> = next.active_stakes().iter().map(|c| c.as_u64()).collect();

    let mut sl = SainteLagueSpooler::default();
    let spool_counts = sl.allocate(&stakes_next, SPOOLS as u16)?;

    let spools = migrate_spools(
        &assignment.0,
        &members_current,
        &members_next,
        &spool_counts,
        epoch,
    )?;
    for i in 0..SPOOLS {
        assignment.0[i] = spools[i];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_with_standard_committee() {
        assert_eq!(cap_spools(20, 1000), 50);
        assert_eq!(cap_spools(128, 1000), 50);
        assert_eq!(cap_spools(50, 1000), 50);
    }

    #[test]
    fn cap_with_small_committee() {
        assert_eq!(cap_spools(10, 1000), 100);
        assert_eq!(cap_spools(5, 1000), 200);
        assert_eq!(cap_spools(1, 1000), 1000);
    }

    #[test]
    fn cap_edge_cases() {
        assert_eq!(cap_spools(0, 1000), 0);
        assert_eq!(cap_spools(50, 0), 0);
        assert_eq!(cap_spools(0, 0), 0);
    }
}
