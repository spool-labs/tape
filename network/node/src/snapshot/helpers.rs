use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use tape_protocol::Api;
use rpc_client::RpcError;
use solana_sdk::signature::Signer;
use store::Store;
use tape_api::errors::is_account_state_pending_error;
use tape_core::encoding::ClayParams;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_store::ops::MetaOps;
use tape_crypto::hash::Hash;
use tape_slicer::{ClayCoder, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER};
use tape_store::types::{NodeInfo, SnapshotCertResult, SnapshotChunkMeta};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::TaskOutcome;
use crate::core::committee::{our_member, our_member_index, our_snapshot_groups};
use rpc_client::parse_tape_error;

/// Shared retry delay for snapshot collect and submit polling loops.
pub const SNAPSHOT_PENDING_DELAY: Duration = Duration::from_secs(2);

/// Result class for tx submissions that hit an on-chain endpoint.
#[derive(Debug, PartialEq, Eq)]
pub enum SubmitClass {
    /// Transaction already completed on-chain.
    Done,
    /// Transaction in progress or waiting for chain state.
    Pending,
    /// Transaction failed and may be retried.
    Retryable,
}

/// How a snapshot task should resolve its local snapshot epoch.
pub enum SnapshotNeed {
    /// No strict dependency on local snapshot data; skip if unavailable.
    AllowMissing,
    /// Require local snapshot readiness for build.
    RequireBuild,
    /// Require local snapshot readiness for certification collection.
    RequireCertify,
    /// Require local snapshot readiness for registration.
    RequireRegister,
}

/// Shared snapshot task context passed to build, collect, register, and submit handlers.
pub struct SnapshotTaskContext {
    /// Current chain epoch as read from local store.
    pub current_chain_epoch: EpochNumber,
    /// Snapshot epoch for this local pipeline cycle.
    pub local_epoch: EpochNumber,
    /// Committee members for the current chain epoch.
    pub committee: Vec<NodeInfo>,
    /// Spool groups owned by this node.
    pub owned_groups: HashSet<SpoolGroup>,
    /// Optional committee member index for the local node.
    pub member_index: Option<usize>,
    /// Optional number of spools owned by the local node.
    pub owned_spools: Option<usize>,
}

/// Snapshot artifacts persisted per group for certification and submission.
pub struct SnapshotGroupArtifacts {
    /// Optional commitment root for the group.
    pub commitment: Option<Hash>,
    /// Optional metadata for the built chunk.
    pub metadata: Option<SnapshotChunkMeta>,
    /// Optional certification result for the group.
    pub cert: Option<SnapshotCertResult>,
}


/// Build a typed snapshot task context from on-chain and local committee state.
pub fn load_snapshot_task_context<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    need: SnapshotNeed,
    with_member: bool,
) -> Result<SnapshotTaskContext, TaskOutcome> {
    let current_chain_epoch = {
        let cs = context.chain_state.load();
        if cs.epoch.is_zero() {
            return Err(TaskOutcome::Retryable("no current epoch".into()));
        }
        cs.epoch
    };
    let local_epoch = match load_snapshot_local_epoch(current_chain_epoch, need) {
        Ok(epoch) => epoch,
        Err(outcome) => return Err(outcome),
    };

    let cs = context.chain_state.load();
    if cs.epoch != current_chain_epoch {
        return Err(TaskOutcome::Retryable("no committee".into()));
    }
    if cs.committee.is_empty() {
        return Err(TaskOutcome::Retryable("no committee".into()));
    }
    let committee = cs.committee.clone();

    let owned_groups = match our_snapshot_groups(&committee, context.keypair.pubkey()) {
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

    Ok(SnapshotTaskContext {
        current_chain_epoch,
        local_epoch,
        committee,
        owned_groups,
        member_index,
        owned_spools,
    })
}

/// Check if all snapshot build artifacts exist for all groups.
pub fn is_snapshot_build_complete<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    local_epoch: EpochNumber,
) -> Result<bool, String> {
    for group in 0..SPOOL_GROUP_COUNT {
        let chunk_index = ChunkIndex(group as u64);
        if context
            .store
            .get_snapshot_commitment(local_epoch, chunk_index)
            .map_err(|e| format!("read snapshot commitment: {e}"))?
            .is_none()
        {
            return Ok(false);
        }

        if context
            .store
            .get_snapshot_metadata(local_epoch, chunk_index)
            .map_err(|e| format!("read snapshot metadata: {e}"))?
            .is_none()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Check if a single snapshot chunk has both commitment and metadata.
pub fn is_snapshot_chunk_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    local_epoch: EpochNumber,
    group: SpoolGroup,
) -> Result<bool, String> {
    let chunk_index = ChunkIndex(group.0);

    if context
        .store
        .get_snapshot_commitment(local_epoch, chunk_index)
        .map_err(|e| format!("read snapshot commitment: {e}"))?
        .is_none()
    {
        return Ok(false);
    }

    if context
        .store
        .get_snapshot_metadata(local_epoch, chunk_index)
        .map_err(|e| format!("read snapshot metadata: {e}"))?
        .is_none()
    {
        return Ok(false);
    }

    Ok(true)
}

/// Load commitment, metadata, and cert for a single snapshot group.
pub fn load_group_artifacts<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    local_epoch: EpochNumber,
    group: SpoolGroup,
) -> Result<SnapshotGroupArtifacts, String> {
    let chunk_index = ChunkIndex(group.0);

    let commitment = context
        .store
        .get_snapshot_commitment(local_epoch, chunk_index)
        .map_err(|e| format!("read snapshot commitment: {e}"))?;
    let metadata = context
        .store
        .get_snapshot_metadata(local_epoch, chunk_index)
        .map_err(|e| format!("read snapshot metadata: {e}"))?;
    let cert = context
        .store
        .get_snapshot_cert(local_epoch, chunk_index)
        .map_err(|e| format!("read snapshot cert: {e}"))?;

    Ok(SnapshotGroupArtifacts {
        commitment,
        metadata,
        cert,
    })
}

/// Select local snapshot epoch for snapshot tasks.
pub fn load_snapshot_local_epoch(
    current_chain_epoch: EpochNumber,
    need: SnapshotNeed,
) -> Result<EpochNumber, TaskOutcome> {
    let local_epoch = match derive_snapshot_local_epoch(current_chain_epoch) {
        Some(local_epoch) => local_epoch,
        None => {
            return Err(match need {
                SnapshotNeed::AllowMissing => TaskOutcome::Success,
                SnapshotNeed::RequireBuild => {
                    TaskOutcome::Retryable("build local snapshot not ready".into())
                }
                SnapshotNeed::RequireCertify => {
                    TaskOutcome::Retryable("certify local snapshot not ready".into())
                }
                SnapshotNeed::RequireRegister => {
                    TaskOutcome::Retryable("register local snapshot not ready".into())
                }
            });
        }
    };

    Ok(local_epoch)
}

/// Map a Solana RPC error to a submission class.
pub fn classify_submit_error(err: &RpcError) -> SubmitClass {
    let tape_error = parse_tape_error(err);

    if tape_error
        .map(|error| error.is_already_done())
        .unwrap_or(false)
    {
        return SubmitClass::Done;
    }

    if tape_error.map(|error| error.is_retriable()).unwrap_or(false) {
        return SubmitClass::Pending;
    }

    if is_account_state_pending_error(&err.to_string()) {
        return SubmitClass::Pending;
    }

    SubmitClass::Retryable
}

/// Return whether the chain is far enough to produce a local snapshot.
pub fn snapshot_ready(epoch: EpochNumber) -> bool {
    epoch >= EpochNumber(2)
}

/// Compute the local snapshot epoch for the given chain epoch.
pub fn derive_snapshot_local_epoch(epoch: EpochNumber) -> Option<EpochNumber> {
    if epoch >= EpochNumber(2) {
        Some(epoch - EpochNumber(1))
    } else {
        None
    }
}

/// Standardize retryable outcome when required local state is not yet available.
pub fn missing_state(message: impl Into<String>) -> TaskOutcome {
    TaskOutcome::Retryable(format!("missing state: {}", message.into()))
}

/// Return `Success` when cancellation has been requested, else continue.
pub fn skip_if_cancelled(cancel: &CancellationToken) -> Option<TaskOutcome> {
    cancel.is_cancelled().then_some(TaskOutcome::Success)
}

/// Decode one inner encoded snapshot chunk from indexed slice payloads.
pub fn decode_group(group: usize, slices: &[(usize, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let refs: Vec<(usize, &[u8])> = slices
        .iter()
        .map(|(index, data)| (*index, data.as_slice()))
        .collect();
    let mut slicer = Slicer::new(ClayCoder::from_params(ClayParams::default()));
    slicer.set_chunk_index(group as u64);
    slicer
        .decode(&refs)
        .map_err(|e| format!("inner decode group {group}: {e}"))
}

/// Decode the outer snapshot payload from recovered chunk payloads.
pub fn decode_outer(decoded_chunks: Vec<Option<(usize, Vec<u8>)>>) -> Result<Vec<u8>, String> {
    let refs: Vec<(usize, &[u8])> = decoded_chunks
        .iter()
        .filter_map(|chunk| chunk.as_ref().map(|(index, data)| (*index, data.as_slice())))
        .collect();

    if refs.len() < DEFAULT_K_OUTER {
        return Err(format!(
            "not enough decoded chunks: {}/{}",
            refs.len(),
            DEFAULT_K_OUTER
        ));
    }

    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    outer.decode(&refs).map_err(|e| format!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::test_utils;
    use tape_api::errors::TapeError;
    use tape_crypto::Hash;
    use tape_store::types::SnapshotChunkMeta;

    /// Ready check transitions at epoch boundary.
    #[test]
    fn epoch_ready() {
        assert!(!snapshot_ready(EpochNumber(0)));
        assert!(snapshot_ready(EpochNumber(2)));
    }

    /// No local snapshot before protocol window.
    #[test]
    fn local_epoch_missing() {
        assert_eq!(derive_snapshot_local_epoch(EpochNumber(1)), None);
    }

    /// Local snapshot is previous epoch when ready.
    #[test]
    fn local_epoch_present() {
        assert_eq!(derive_snapshot_local_epoch(EpochNumber(3)), Some(EpochNumber(2)));
    }

    /// Build completeness is false until all metadata is persisted.
    #[test]
    fn build_complete() {
        let ctx = test_utils::test_context();
        let local_epoch = EpochNumber(2);

        for i in 0..SPOOL_GROUP_COUNT {
            ctx.store
                .set_snapshot_commitment(local_epoch, ChunkIndex(i as u64), Hash::new_unique())
                .unwrap();
        }

        let ready = is_snapshot_build_complete(&ctx, local_epoch).unwrap();
        assert!(!ready);

        for i in 0..SPOOL_GROUP_COUNT {
            ctx.store
                .set_snapshot_metadata(
                    local_epoch,
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

        assert!(is_snapshot_build_complete(&ctx, local_epoch).unwrap());
    }

    /// Chunk readiness requires both commitment and metadata.
    #[test]
    fn chunk_ready() {
        let ctx = test_utils::test_context();
        let local_epoch = EpochNumber(2);

        assert!(!is_snapshot_chunk_ready(&ctx, local_epoch, SpoolGroup(3)).unwrap());

        ctx.store
            .set_snapshot_commitment(local_epoch, ChunkIndex(3), Hash::new_unique())
            .unwrap();
        assert!(!is_snapshot_chunk_ready(&ctx, local_epoch, SpoolGroup(3)).unwrap());

        ctx.store
            .set_snapshot_metadata(
                local_epoch,
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

        assert!(is_snapshot_chunk_ready(&ctx, local_epoch, SpoolGroup(3)).unwrap());
    }

    #[test]
    fn submit_error_retriable_is_pending() {
        let err = RpcError::Transaction(format!(
            "custom program error: 0x{:x}",
            TapeError::TooSoon as u32
        ));
        assert_eq!(classify_submit_error(&err), SubmitClass::Pending);
    }

    #[test]
    fn submit_error_already_done_is_done() {
        let err = RpcError::Transaction(format!(
            "custom program error: 0x{:x}",
            TapeError::AlreadyCertified as u32
        ));
        assert_eq!(classify_submit_error(&err), SubmitClass::Done);
    }
}
