use std::sync::Arc;

use bytemuck::Zeroable;
use rpc::Rpc;
use store::Store;
use tape_api::event::{SnapshotCertified, SnapshotFinalized, SnapshotInit};
use tape_core::bls::BlsSignature;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::snapshot::chunk::SnapshotChunkMeta;
use tape_core::snapshot::info::{
    SnapshotEpochInfo, SnapshotEpochStatus, SnapshotGroupInfo, SnapshotGroupStatus,
};
use tape_core::types::{CommitteeBitmap, EpochNumber, SnapshotGroupBitmap};
use tape_crypto::hash::Hash;
use tape_protocol::Api;
use tape_protocol::ProtocolState;
use tape_store::ops::SnapshotOps;
use tracing::debug;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;

pub async fn observe_state<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: Arc<ProtocolState>,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let Some(epoch) = locally_pending_snapshot_epoch(context.node_id(), state.as_ref()) else {
        return Ok(());
    };

    let existing = context
        .store
        .get_epoch_info(epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?;

    if existing.is_some() {
        return Ok(());
    }

    let info = SnapshotEpochInfo {
        parent_epoch: inferred_parent_epoch(epoch),
        status: SnapshotEpochStatus::Pending,
        certified_groups: SnapshotGroupBitmap::zeroed(),
    };

    context
        .store
        .put_epoch_info(epoch, info)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        signing_epoch = state.epoch.0,
        snapshot_epoch = epoch.0,
        "snapshot epoch became locally pending"
    );

    Ok(())
}

pub async fn observe_block<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    block: Arc<ParsedBlock>,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    use tape_blocks::ParsedInstruction;

    for instruction in &block.instructions {
        match instruction {
            ParsedInstruction::InitSnapshotEpoch { event } => {
                handle_snapshot_init(context, event)?;
            }
            ParsedInstruction::CertifySnapshotGroup { event } => {
                handle_snapshot_certified(context, event)?;
            }
            ParsedInstruction::FinalizeSnapshotEpoch { event } => {
                handle_snapshot_finalized(context, event)?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn handle_snapshot_init<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    event: &SnapshotInit,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let existing = context
        .store
        .get_epoch_info(event.current)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?;

    if let Some(info) = &existing {
        match info.status {
            SnapshotEpochStatus::Initialized
            | SnapshotEpochStatus::PartiallyCertified
            | SnapshotEpochStatus::Finalized => return Ok(()),
            SnapshotEpochStatus::Pending | SnapshotEpochStatus::Built => {}
        }
    }

    let info = SnapshotEpochInfo {
        parent_epoch: event.parent,
        status: SnapshotEpochStatus::Initialized,
        certified_groups: existing
            .map(|i| i.certified_groups)
            .unwrap_or_else(SnapshotGroupBitmap::zeroed),
    };

    context
        .store
        .put_epoch_info(event.current, info)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = event.current.0,
        parent = event.parent.0,
        "snapshot epoch initialized"
    );

    Ok(())
}

fn handle_snapshot_certified<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    event: &SnapshotCertified,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut epoch_info = context
        .store
        .get_epoch_info(event.epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?
        .unwrap_or(SnapshotEpochInfo {
            parent_epoch: EpochNumber(0),
            status: SnapshotEpochStatus::PartiallyCertified,
            certified_groups: SnapshotGroupBitmap::zeroed(),
        });

    epoch_info.certified_groups.set(event.group.0 as usize);
    if epoch_info.status != SnapshotEpochStatus::Finalized {
        epoch_info.status = SnapshotEpochStatus::PartiallyCertified;
    }

    context
        .store
        .put_epoch_info(event.epoch, epoch_info)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    let mut group_info = context
        .store
        .get_group_info(event.epoch, event.group)
        .map_err(|e| NodeError::Store(format!("get_group_info: {e}")))?
        .unwrap_or(SnapshotGroupInfo {
            status: SnapshotGroupStatus::Missing,
            meta: SnapshotChunkMeta::zeroed(),
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
            bitmap: CommitteeBitmap::zeroed(),
            signature: BlsSignature::zeroed(),
            track_number: None,
        });

    group_info.status = SnapshotGroupStatus::CertifiedOnChain;
    group_info.track_number = Some(event.track);
    group_info.meta.commitment = event.commitment;

    context
        .store
        .put_group_info(event.epoch, event.group, group_info)
        .map_err(|e| NodeError::Store(format!("put_group_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = event.epoch.0,
        group = event.group.0,
        track = event.track.0,
        "snapshot group certified on-chain"
    );

    Ok(())
}

fn handle_snapshot_finalized<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    event: &SnapshotFinalized,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut epoch_info = context
        .store
        .get_epoch_info(event.current)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?
        .unwrap_or(SnapshotEpochInfo {
            parent_epoch: event.parent,
            status: SnapshotEpochStatus::Finalized,
            certified_groups: SnapshotGroupBitmap::zeroed(),
        });

    epoch_info.status = SnapshotEpochStatus::Finalized;

    context
        .store
        .put_epoch_info(event.current, epoch_info)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = event.current.0,
        parent = event.parent.0,
        "snapshot epoch finalized"
    );

    Ok(())
}

fn locally_pending_snapshot_epoch(
    node_id: tape_core::types::NodeId,
    state: &ProtocolState,
) -> Option<EpochNumber> {
    if state.epoch < EpochNumber(2) {
        return None;
    }

    if state.find_member(node_id).is_none() {
        return None;
    }

    Some(state.epoch.saturating_sub(EpochNumber(1)))
}

fn inferred_parent_epoch(snapshot_epoch: EpochNumber) -> EpochNumber {
    snapshot_epoch.saturating_sub(EpochNumber(1))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tape_api::event::{SnapshotCertified, SnapshotFinalized, SnapshotInit};
    use tape_core::snapshot::info::{SnapshotEpochInfo, SnapshotEpochStatus, SnapshotGroupStatus};
    use tape_core::system::CommitteeMember;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{EpochNumber, SnapshotGroupBitmap, SlotNumber, TrackNumber};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::hash::Hash;
    use tape_protocol::ProtocolState;
    use tape_store::ops::SnapshotOps;

    use super::{observe_block, observe_state};
    use crate::context::test_utils::test_context;
    use crate::features::block::ingestor::ParsedBlock;

    fn state(epoch: EpochNumber, local_member: bool) -> Arc<ProtocolState> {
        let mut state = ProtocolState {
            epoch,
            ..ProtocolState::default()
        };

        if local_member {
            state.committee.push(CommitteeMember::new(
                tape_core::types::NodeId(0),
                Coin::<TAPE>::new(1_000),
            ));
        } else {
            state.committee.push(CommitteeMember::new(
                tape_core::types::NodeId(7),
                Coin::<TAPE>::new(1_000),
            ));
        }

        Arc::new(state)
    }

    fn init_block(parent: EpochNumber, current: EpochNumber) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(current.0 * 10),
            instructions: vec![tape_blocks::ParsedInstruction::InitSnapshotEpoch {
                event: SnapshotInit { parent, current },
            }],
        })
    }

    fn certify_block(
        epoch: EpochNumber,
        group: SpoolGroup,
        track: TrackNumber,
        commitment: Hash,
    ) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(epoch.0 * 10 + 1),
            instructions: vec![tape_blocks::ParsedInstruction::CertifySnapshotGroup {
                event: SnapshotCertified {
                    epoch,
                    group,
                    track,
                    commitment,
                    signer_count: 80u64.to_le_bytes(),
                    signer_weight: 100u64.to_le_bytes(),
                },
            }],
        })
    }

    fn finalize_block(parent: EpochNumber, current: EpochNumber) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(current.0 * 10 + 2),
            instructions: vec![tape_blocks::ParsedInstruction::FinalizeSnapshotEpoch {
                event: SnapshotFinalized { parent, current },
            }],
        })
    }

    #[tokio::test]
    async fn state_marks_pending_snapshot_epoch() {
        let ctx = test_context();

        observe_state(&ctx, state(EpochNumber(2), true)).await.unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(1)).unwrap().unwrap();
        assert_eq!(info.parent_epoch, EpochNumber(0));
        assert_eq!(info.status, SnapshotEpochStatus::Pending);
    }

    #[tokio::test]
    async fn state_ignores_bootstrap_epochs() {
        let ctx = test_context();

        observe_state(&ctx, state(EpochNumber(1), true)).await.unwrap();

        assert!(ctx.store.get_epoch_info(EpochNumber(0)).unwrap().is_none());
    }

    #[tokio::test]
    async fn state_ignores_non_committee_nodes() {
        let ctx = test_context();

        observe_state(&ctx, state(EpochNumber(4), false)).await.unwrap();

        assert!(ctx.store.get_epoch_info(EpochNumber(3)).unwrap().is_none());
    }

    #[tokio::test]
    async fn state_does_not_regress_existing_epoch_info() {
        let ctx = test_context();
        let epoch = EpochNumber(3);

        ctx.store
            .put_epoch_info(
                epoch,
                SnapshotEpochInfo {
                    parent_epoch: EpochNumber(1),
                    status: SnapshotEpochStatus::Initialized,
                    certified_groups: SnapshotGroupBitmap::zeroed(),
                },
            )
            .unwrap();

        observe_state(&ctx, state(EpochNumber(4), true)).await.unwrap();

        let info = ctx.store.get_epoch_info(epoch).unwrap().unwrap();
        assert_eq!(info.parent_epoch, EpochNumber(1));
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
    }

    // InitSnapshotEpoch creates epoch info with Initialized status.
    #[tokio::test]
    async fn init() {
        let ctx = test_context();
        let block = init_block(EpochNumber(4), EpochNumber(5));

        observe_block(&ctx, block).await.unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.parent_epoch, EpochNumber(4));
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
    }

    // Repeated init doesn't regress status or overwrite bitmap.
    #[tokio::test]
    async fn init_idempotent() {
        let ctx = test_context();

        // Init then certify a group.
        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();
        observe_block(
            &ctx,
            certify_block(
                EpochNumber(5),
                SpoolGroup(2),
                TrackNumber(1),
                Hash::new_unique(),
            ),
        )
        .await
        .unwrap();

        // Second init should not regress.
        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::PartiallyCertified);
        assert!(info.certified_groups.is_set(2));
    }

    // Init upgrades Pending/Built to Initialized.
    #[tokio::test]
    async fn init_promotes_pending() {
        let ctx = test_context();

        // Seed a Pending epoch info directly.
        ctx.store
            .put_epoch_info(
                EpochNumber(5),
                SnapshotEpochInfo {
                    parent_epoch: EpochNumber(0),
                    status: SnapshotEpochStatus::Pending,
                    certified_groups: SnapshotGroupBitmap::zeroed(),
                },
            )
            .unwrap();

        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
        assert_eq!(info.parent_epoch, EpochNumber(4));
    }

    // CertifySnapshotGroup updates epoch bitmap and group status.
    #[tokio::test]
    async fn certify_group() {
        let ctx = test_context();
        let commitment = Hash::new_unique();

        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();
        observe_block(
            &ctx,
            certify_block(EpochNumber(5), SpoolGroup(3), TrackNumber(7), commitment),
        )
        .await
        .unwrap();

        let epoch = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(epoch.status, SnapshotEpochStatus::PartiallyCertified);
        assert!(epoch.certified_groups.is_set(3));

        let group = ctx
            .store
            .get_group_info(EpochNumber(5), SpoolGroup(3))
            .unwrap()
            .unwrap();
        assert_eq!(group.status, SnapshotGroupStatus::CertifiedOnChain);
        assert_eq!(group.track_number, Some(TrackNumber(7)));
        assert_eq!(group.meta.commitment, commitment);
    }

    // Certify works when epoch/group info doesn't exist yet.
    #[tokio::test]
    async fn certify_creates_missing() {
        let ctx = test_context();
        let commitment = Hash::new_unique();

        observe_block(
            &ctx,
            certify_block(EpochNumber(5), SpoolGroup(1), TrackNumber(3), commitment),
        )
        .await
        .unwrap();

        let epoch = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(epoch.status, SnapshotEpochStatus::PartiallyCertified);
        assert!(epoch.certified_groups.is_set(1));

        let group = ctx
            .store
            .get_group_info(EpochNumber(5), SpoolGroup(1))
            .unwrap()
            .unwrap();
        assert_eq!(group.status, SnapshotGroupStatus::CertifiedOnChain);
        assert_eq!(group.track_number, Some(TrackNumber(3)));
    }

    // Certify keeps locally-built fields when group was Built.
    #[tokio::test]
    async fn certify_preserves_local() {
        use bytemuck::Zeroable;
        use tape_core::bls::BlsSignature;
        use tape_core::encoding::EncodingProfile;
        use tape_core::erasure::SPOOL_GROUP_SIZE;
        use tape_core::snapshot::chunk::SnapshotChunkMeta;
        use tape_core::snapshot::info::SnapshotGroupInfo;
        use tape_core::types::{CommitteeBitmap, StorageUnits, StripeCount};

        let ctx = test_context();
        let local_commitment = Hash::new_unique();
        let local_leaves = [Hash::new_unique(); SPOOL_GROUP_SIZE];

        // Seed a Built group with local data.
        let built = SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            meta: SnapshotChunkMeta {
                commitment: local_commitment,
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(1024),
                stripe_count: StripeCount(4),
            },
            leaves: local_leaves,
            bitmap: CommitteeBitmap::from_indices(&[0, 1, 2], 128),
            signature: BlsSignature::zeroed(),
            track_number: None,
        };
        ctx.store
            .put_group_info(EpochNumber(5), SpoolGroup(2), built)
            .unwrap();

        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();
        observe_block(
            &ctx,
            certify_block(
                EpochNumber(5),
                SpoolGroup(2),
                TrackNumber(9),
                local_commitment,
            ),
        )
        .await
        .unwrap();

        let group = ctx
            .store
            .get_group_info(EpochNumber(5), SpoolGroup(2))
            .unwrap()
            .unwrap();
        assert_eq!(group.status, SnapshotGroupStatus::CertifiedOnChain);
        assert_eq!(group.track_number, Some(TrackNumber(9)));
        // Locally built fields preserved.
        assert_eq!(group.leaves, local_leaves);
        assert_eq!(group.meta.profile, EncodingProfile::basic_default());
        assert_eq!(group.meta.stripe_size, StorageUnits::from_bytes(1024));
    }

    // FinalizeSnapshotEpoch sets status to Finalized.
    #[tokio::test]
    async fn finalize() {
        let ctx = test_context();

        observe_block(&ctx, init_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();
        observe_block(&ctx, finalize_block(EpochNumber(4), EpochNumber(5)))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Finalized);
        assert_eq!(info.parent_epoch, EpochNumber(4));
    }

    // Init then certify groups then finalize, verify final state.
    #[tokio::test]
    async fn full_lifecycle() {
        let ctx = test_context();
        let epoch = EpochNumber(5);
        let parent = EpochNumber(4);

        observe_block(&ctx, init_block(parent, epoch)).await.unwrap();

        for g in 0..3u64 {
            observe_block(
                &ctx,
                certify_block(
                    epoch,
                    SpoolGroup(g),
                    TrackNumber(g + 100),
                    Hash::new_unique(),
                ),
            )
            .await
            .unwrap();
        }

        observe_block(&ctx, finalize_block(parent, epoch))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(epoch).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Finalized);
        assert_eq!(info.parent_epoch, parent);
        assert!(info.certified_groups.is_set(0));
        assert!(info.certified_groups.is_set(1));
        assert!(info.certified_groups.is_set(2));
        assert!(!info.certified_groups.is_set(3));

        for g in 0..3u64 {
            let group = ctx
                .store
                .get_group_info(epoch, SpoolGroup(g))
                .unwrap()
                .unwrap();
            assert_eq!(group.status, SnapshotGroupStatus::CertifiedOnChain);
            assert_eq!(group.track_number, Some(TrackNumber(g + 100)));
        }
    }
}
