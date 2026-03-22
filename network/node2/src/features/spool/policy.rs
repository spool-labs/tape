use store::Store;
use tape_store::ops::ObjectInfoOps;
use tape_store::types::ObjectInfo;
use tape_store::TapeStore;

use crate::core::error::NodeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackRequirement {
    /// Track is certified — repair/recovery work is legitimate.
    Required,
    /// Track is uncertified, invalid, or blacklisted — pending work can be removed.
    NotRequired,
    /// ObjectInfo is missing for a track that exists — local state is inconsistent.
    /// Callers should treat this as retryable, not as a clean skip.
    Inconsistent,
}

pub fn track_requirement<Db: Store>(
    store: &TapeStore<Db>,
    track: tape_store::types::Pubkey,
) -> Result<TrackRequirement, NodeError> {
    let info = store
        .get_object_info(track)
        .map_err(|e| NodeError::Store(format!("get_object_info: {e}")))?;

    match info {
        Some(ObjectInfo::Valid { certified_epoch: Some(_), .. }) => Ok(TrackRequirement::Required),
        Some(_) => Ok(TrackRequirement::NotRequired),
        None => Ok(TrackRequirement::Inconsistent),
    }
}
