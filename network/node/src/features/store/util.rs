use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::TrackGroup;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;

use crate::core::error::NodeError;

/// Whether this node holds any of the track group's n spools.
pub fn is_responsible_for_group<Db: Store>(
    store: &TapeStore<Db>,
    group: TrackGroup,
    profile: ErasureCode,
) -> Result<bool, NodeError> {
    for slice in 0..usize::from(profile.n()) {
        let spool = group.spool_at(slice);
        if store
            .get_spool_state(spool)
            .map_err(store_error)?
            .is_some()
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
