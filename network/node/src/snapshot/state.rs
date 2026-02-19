use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_store::ops::MetaOps;
use tape_store::ops::CommitteeOps;
use tape_store::types::{ChunkIndex, NodeInfo};

use crate::runtime::NodeContext;
use crate::runtime::committee::{our_member, our_member_index, our_snapshot_groups};
use crate::snapshot::epoch::{SnapshotNeed, snapshot_epochs};
use crate::supervisor::TaskOutcome;

pub struct SnapshotContext {
    pub current: EpochNumber,
    pub target: EpochNumber,
    pub committee: Vec<NodeInfo>,
    pub groups: HashSet<SpoolGroup>,
    pub member_index: Option<usize>,
    pub owned_spools: Option<usize>,
}

pub fn load_snapshot_context<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    need: SnapshotNeed,
    with_member: bool,
) -> Result<SnapshotContext, TaskOutcome> {
    let (current, target) = match snapshot_epochs(context, need) {
        Ok(value) => value,
        Err(outcome) => return Err(outcome),
    };

    let committee = match context.store.get_committee(current) {
        Ok(Some(committee)) => committee,
        Ok(None) => return Err(TaskOutcome::Retryable("no committee".into())),
        Err(e) => return Err(TaskOutcome::Retryable(format!("read committee: {e}"))),
    };

    let groups = match our_snapshot_groups(&committee, context.keypair.pubkey()) {
        Ok(groups) => groups,
        Err(e) => return Err(TaskOutcome::Retryable(e.into())),
    };

    let member_index = if with_member {
        match our_member_index(&committee, context.keypair.pubkey()) {
            Ok(index) => Some(index),
            Err(e) => return Err(TaskOutcome::Retryable(e.into())),
        }
    } else {
        None
    };

    let owned_spools = if with_member {
        match our_member(&committee, context.keypair.pubkey()) {
            Ok(member) => Some(member.spools.len()),
            Err(e) => return Err(TaskOutcome::Retryable(e.into())),
        }
    } else {
        None
    };

    Ok(SnapshotContext {
        current,
        target,
        committee,
        groups,
        member_index,
        owned_spools,
    })
}

/// Returns true when a snapshot build has been fully materialized for an epoch.
///
/// A complete build requires every chunk commitment and encoding metadata entry to
/// exist for the target epoch.
pub fn is_snapshot_build_complete<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    target: EpochNumber,
) -> Result<bool, String> {
    for group in 0..SPOOL_GROUP_COUNT {
        let chunk_index = ChunkIndex(group as u64);
        if context
            .store
            .get_snapshot_commitment(target, chunk_index)
            .map_err(|e| format!("read snapshot commitment: {e}"))?
            .is_none()
        {
            return Ok(false);
        }

        if context
            .store
            .get_snapshot_metadata(target, chunk_index)
            .map_err(|e| format!("read snapshot metadata: {e}"))?
            .is_none()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Returns true when a single snapshot chunk has commitment and metadata.
pub fn is_snapshot_chunk_ready<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    target: EpochNumber,
    group: u64,
) -> Result<bool, String> {
    let chunk_index = ChunkIndex(group);

    if context
        .store
        .get_snapshot_commitment(target, chunk_index)
        .map_err(|e| format!("read snapshot commitment: {e}"))?
        .is_none()
    {
        return Ok(false);
    }

    if context
        .store
        .get_snapshot_metadata(target, chunk_index)
        .map_err(|e| format!("read snapshot metadata: {e}"))?
        .is_none()
    {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_store::types::SnapshotChunkMeta;

    use crate::runtime::test_utils::test_context;

    #[test]
    fn snapshot_complete_all() {
        let ctx = test_context();
        let target = EpochNumber(2);
        let _ = ctx.store.set_chain_epoch(EpochNumber(3));

        for i in 0..SPOOL_GROUP_COUNT {
            ctx.store
                .set_snapshot_commitment(target, ChunkIndex(i as u64), Hash::new_unique())
                .unwrap();
        }

        let missing = is_snapshot_build_complete(&ctx, target).unwrap();
        assert!(!missing);

        for i in 0..SPOOL_GROUP_COUNT {
            ctx.store
                .set_snapshot_metadata(
                    target,
                    ChunkIndex(i as u64),
                    SnapshotChunkMeta {
                        leaves: Vec::new(),
                        stripe_size: 0,
                        stripe_count: 0,
                        encoding_type: 0,
                        encoding_params: 0,
                    },
                )
                .unwrap();
        }

        assert!(is_snapshot_build_complete(&ctx, target).unwrap());
    }

    #[test]
    fn chunk_ready() {
        let ctx = test_context();
        let target = EpochNumber(2);

        assert!(!is_snapshot_chunk_ready(&ctx, target, 3).unwrap());

        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(3), Hash::new_unique())
            .unwrap();
        assert!(!is_snapshot_chunk_ready(&ctx, target, 3).unwrap());

        ctx.store
            .set_snapshot_metadata(
                target,
                ChunkIndex(3),
                SnapshotChunkMeta {
                    leaves: Vec::new(),
                    stripe_size: 0,
                    stripe_count: 0,
                    encoding_type: 0,
                    encoding_params: 0,
                },
            )
            .unwrap();

        assert!(is_snapshot_chunk_ready(&ctx, target, 3).unwrap());
    }
}
