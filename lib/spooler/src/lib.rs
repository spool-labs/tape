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
use tape_core::spooler::{SpoolAssignment, SpoolCount, SpoolerError};
use tape_core::system::Committee;
use tape_crypto::hash::Hash;

mod heap;
mod priority;
pub mod dhondt;
pub mod sainte_lague;
pub mod migrate;

pub use dhondt::{DhondtSpooler, dhondt_allocate};
pub use sainte_lague::{SainteLagueSpooler, sainte_lague_allocate};
pub use migrate::{migrate_spools, initial_assignment};

/// Maximum spools any single node can hold (one per group = 50).
pub const MAX_SPOOLS_PER_NODE: SpoolCount = SPOOL_GROUP_COUNT as SpoolCount;

/// Compute the per-node spool cap.
///
/// With `node_count >= SPOOL_GROUP_SIZE` (20), cap = `spool_count / SPOOL_GROUP_SIZE`.
/// For smaller committees the cap is scaled up so that all spools can be distributed.
pub fn cap_spools(node_count: u64, spool_count: u64) -> u64 {
    let group_size = SPOOL_GROUP_SIZE as u64;
    if spool_count == 0 || node_count == 0 {
        return 0;
    }
    if node_count >= group_size {
        spool_count / group_size
    } else {
        let num = spool_count.saturating_mul(group_size);
        let den = node_count.saturating_mul(group_size);
        num.saturating_add(den - 1) / den
    }
}

/// Allocate + migrate in one call using D'Hondt.
pub fn migrate_dhondt<const SPOOLS: usize, const N: usize>(
    assignment: &mut SpoolAssignment<SPOOLS>,
    current: &Committee<N>,
    next: &Committee<N>,
    seed: &Hash,
) -> Result<(), SpoolerError> {
    let members_current = current.active_members();
    let members_next = next.active_members();
    let stakes_next = next.active_stakes();

    let dh = DhondtSpooler::default();
    let spool_counts = dh.allocate(&stakes_next, SPOOLS as u16)?;

    let spools = migrate_spools(
        &assignment.0,
        &members_current,
        &members_next,
        &spool_counts,
        seed,
    )?;
    assignment.0.copy_from_slice(&spools);
    Ok(())
}

/// Allocate + migrate in one call using Sainte-Lague.
pub fn migrate_sainte_lague<const SPOOLS: usize, const N: usize>(
    assignment: &mut SpoolAssignment<SPOOLS>,
    current: &Committee<N>,
    next: &Committee<N>,
    seed: &Hash,
) -> Result<(), SpoolerError> {
    let members_current = current.active_members();
    let members_next = next.active_members();
    let stakes_next = next.active_stakes();

    let sl = SainteLagueSpooler::default();
    let spool_counts = sl.allocate(&stakes_next, SPOOLS as u16)?;

    let spools = migrate_spools(
        &assignment.0,
        &members_current,
        &members_next,
        &spool_counts,
        seed,
    )?;
    assignment.0.copy_from_slice(&spools);
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
