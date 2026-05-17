use store::Store;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;

use crate::core::error::NodeError;

pub fn is_responsible_for_group<Db: Store>(
    store: &TapeStore<Db>,
    group: GroupIndex,
) -> Result<bool, NodeError> {
    for slice in 0..GROUP_SIZE {
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
