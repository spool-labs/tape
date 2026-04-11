use std::sync::Arc;

use bytemuck::Zeroable;
use rpc::Rpc;
use store::Store;
use tape_api::event::{SnapshotCertified, SnapshotFinalized, SnapshotInit};
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_core::snapshot::chunk::snapshot_chunk_key;
use tape_core::snapshot::info::{
    SnapshotEpochInfo, SnapshotEpochStatus, SnapshotGroupInfo, SnapshotGroupStatus,
};
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{EpochNumber, NodeId, SlotNumber, SnapshotGroupBitmap, TrackNumber};
use tape_protocol::Api;
use tape_protocol::ProtocolState;
use tape_store::ops::{
    ObjectInfoOps, SliceOps, SnapshotOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::types::{ObjectInfo, TapeInfo};
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
                handle_snapshot_certified(context, block.slot, event)?;
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
        .get_epoch_info(event.epoch)
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
        status: SnapshotEpochStatus::Initialized,
        certified_groups: existing
            .map(|i| i.certified_groups)
            .unwrap_or_else(SnapshotGroupBitmap::zeroed),
    };

    context
        .store
        .put_epoch_info(event.epoch, info)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = event.epoch.0,
        "snapshot epoch initialized"
    );

    Ok(())
}

fn handle_snapshot_certified<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    slot: SlotNumber,
    event: &SnapshotCertified,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut snapshot_epoch = context
        .store
        .get_epoch_info(event.epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?
        .unwrap_or(SnapshotEpochInfo {
            status: SnapshotEpochStatus::PartiallyCertified,
            certified_groups: SnapshotGroupBitmap::zeroed(),
        });

    snapshot_epoch.certified_groups.set(event.group.0 as usize);
    if snapshot_epoch.status != SnapshotEpochStatus::Finalized {
        snapshot_epoch.status = SnapshotEpochStatus::PartiallyCertified;
    }

    context
        .store
        .put_epoch_info(event.epoch, snapshot_epoch)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    let mut snapshot_group = context
        .store
        .get_group_info(event.epoch, event.group)
        .map_err(|e| NodeError::Store(format!("get_group_info: {e}")))?
        .unwrap_or_else(missing_snapshot_group);

    let has_local_artifacts = matches!(snapshot_group.status, SnapshotGroupStatus::Built);
    if has_local_artifacts && snapshot_group.blob.commitment != event.commitment {
        return Err(NodeError::Store(format!(
            "snapshot group commitment mismatch for epoch {} group {}",
            event.epoch.0, event.group.0
        )));
    }

    snapshot_group.status = SnapshotGroupStatus::CertifiedOnChain;
    snapshot_group.track_number = Some(event.track);
    if !has_local_artifacts {
        snapshot_group.blob.commitment = event.commitment;
    }

    context
        .store
        .put_group_info(event.epoch, event.group, snapshot_group)
        .map_err(|e| NodeError::Store(format!("put_group_info: {e}")))?;

    if has_local_artifacts {
        materialize_snapshot_track(context, slot, event, snapshot_group)?;
    }

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
    let mut snapshot_epoch = context
        .store
        .get_epoch_info(event.epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_info: {e}")))?
        .unwrap_or(SnapshotEpochInfo {
            status: SnapshotEpochStatus::Finalized,
            certified_groups: SnapshotGroupBitmap::zeroed(),
        });

    snapshot_epoch.status = SnapshotEpochStatus::Finalized;

    context
        .store
        .put_epoch_info(event.epoch, snapshot_epoch)
        .map_err(|e| NodeError::Store(format!("put_epoch_info: {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = event.epoch.0,
        "snapshot epoch finalized"
    );

    Ok(())
}

fn locally_pending_snapshot_epoch(
    node_id: NodeId,
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

fn missing_snapshot_group() -> SnapshotGroupInfo {
    SnapshotGroupInfo {
        status: SnapshotGroupStatus::Missing,
        blob: BlobInfo::zeroed(),
        track_number: None,
    }
}

fn materialize_snapshot_track<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    slot: SlotNumber,
    event: &SnapshotCertified,
    snapshot_group: SnapshotGroupInfo,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut owned_spools = context
        .my_spools()
        .into_iter()
        .filter(|spool| event.group.contains(*spool))
        .collect::<Vec<_>>();
    if owned_spools.is_empty() {
        return Ok(());
    }
    owned_spools.sort_unstable();

    let (snapshot_tape, _) = snapshot_tape_pda(event.epoch);
    let track_address = track_pda(snapshot_tape, event.track).0;
    let next_track_number = TrackNumber(event.track.0.saturating_add(1));

    let mut snapshot_tape_record = context
        .store
        .get_tape(snapshot_tape)
        .map_err(|e| NodeError::Store(format!("get_tape: {e}")))?
        .unwrap_or(TapeInfo {
            end_epoch: EpochNumber(u64::MAX),
            next_track_number,
        });
    if snapshot_tape_record.next_track_number.0 < next_track_number.0 {
        snapshot_tape_record.next_track_number = next_track_number;
    }
    snapshot_tape_record.end_epoch = EpochNumber(u64::MAX);
    context
        .store
        .put_tape(snapshot_tape, snapshot_tape_record)
        .map_err(|e| NodeError::Store(format!("put_tape: {e}")))?;

    let track = CompressedTrack {
        tape: snapshot_tape,
        key: snapshot_chunk_key(event.epoch, event.group),
        track_number: event.track,
        kind: TrackKind::Blob as u64,
        state: TrackState::Certified as u64,
        size: snapshot_group.blob.size,
        spool_group: event.group,
        value_hash: snapshot_group.blob.get_hash(),
    };
    context
        .store
        .put_track(track_address, track)
        .map_err(|e| NodeError::Store(format!("put_track: {e}")))?;
    context
        .store
        .put_track_data(track_address, TrackData::Blob(snapshot_group.blob))
        .map_err(|e| NodeError::Store(format!("put_track_data: {e}")))?;
    context
        .store
        .put_object_info(
            track_address,
            ObjectInfo::Valid {
                track_address,
                registered_epoch: event.epoch,
                certified_epoch: Some(event.epoch),
                slot,
            },
        )
        .map_err(|e| NodeError::Store(format!("put_object_info: {e}")))?;

    for spool in owned_spools {
        let Some(slice_position) = event.group.slice_of(spool) else {
            continue;
        };
        let slice = context
            .store
            .get_group_slice(event.epoch, event.group, slice_position)
            .map_err(|e| NodeError::Store(format!("get_group_slice: {e}")))?
            .ok_or_else(|| {
                NodeError::Store(format!(
                    "missing snapshot slice for epoch {} group {} slice {}",
                    event.epoch.0, event.group.0, slice_position
                ))
            })?;
        context
            .store
            .put_slice(spool, track_address, slice)
            .map_err(|e| NodeError::Store(format!("put_slice: {e}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytemuck::Zeroable;
    use tape_api::event::{SnapshotCertified, SnapshotFinalized, SnapshotInit};
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::info::{
        SnapshotEpochInfo, SnapshotEpochStatus, SnapshotGroupInfo, SnapshotGroupStatus,
    };
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::types::{
        EpochNumber, NodeId, SnapshotGroupBitmap, SlotNumber, StorageUnits, StripeCount,
        TrackNumber,
    };
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::hash::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::ProtocolState;
    use tape_store::ops::{
        ObjectInfoOps, SliceOps, SnapshotOps, TapeOps, TrackDataOps, TrackOps,
    };
    use tape_store::types::ObjectInfo;

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
                NodeId(0),
                Coin::<TAPE>::new(1_000),
            ));
        } else {
            state.committee.push(CommitteeMember::new(
                NodeId(7),
                Coin::<TAPE>::new(1_000),
            ));
        }

        Arc::new(state)
    }

    fn state_with_owned_spool(epoch: EpochNumber, spool: u16) -> ProtocolState {
        let mut state = ProtocolState {
            epoch,
            ..ProtocolState::default()
        };
        state.committee = vec![
            CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1_000)),
            CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1_000)),
        ];

        let mut spools = [1u8; SPOOL_COUNT];
        spools[spool as usize] = 0;
        state.spools = SpoolAssignment::new(spools);

        state
    }

    fn local_blob() -> BlobInfo {
        let leaves = [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE];
        BlobInfo {
            size: StorageUnits::from_bytes(1_537),
            commitment: root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        }
    }

    fn init_block(epoch: EpochNumber) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(epoch.0 * 10),
            instructions: vec![ParsedInstruction::InitSnapshotEpoch {
                event: SnapshotInit { epoch },
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
            instructions: vec![ParsedInstruction::CertifySnapshotGroup {
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

    fn finalize_block(epoch: EpochNumber) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(epoch.0 * 10 + 2),
            instructions: vec![ParsedInstruction::FinalizeSnapshotEpoch {
                event: SnapshotFinalized { epoch },
            }],
        })
    }

    #[tokio::test]
    async fn state_marks_pending_snapshot_epoch() {
        let ctx = test_context();

        observe_state(&ctx, state(EpochNumber(2), true)).await.unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(1)).unwrap().unwrap();
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
                    status: SnapshotEpochStatus::Initialized,
                    certified_groups: SnapshotGroupBitmap::zeroed(),
                },
            )
            .unwrap();

        observe_state(&ctx, state(EpochNumber(4), true)).await.unwrap();

        let info = ctx.store.get_epoch_info(epoch).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
    }

    // InitSnapshotEpoch creates epoch info with Initialized status.
    #[tokio::test]
    async fn init() {
        let ctx = test_context();
        let block = init_block(EpochNumber(5));

        observe_block(&ctx, block).await.unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
    }

    // Repeated init doesn't regress status or overwrite bitmap.
    #[tokio::test]
    async fn init_idempotent() {
        let ctx = test_context();

        // Init then certify a group.
        observe_block(&ctx, init_block(EpochNumber(5)))
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
        observe_block(&ctx, init_block(EpochNumber(5)))
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
                    status: SnapshotEpochStatus::Pending,
                    certified_groups: SnapshotGroupBitmap::zeroed(),
                },
            )
            .unwrap();

        observe_block(&ctx, init_block(EpochNumber(5)))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Initialized);
    }

    // CertifySnapshotGroup updates epoch bitmap and group status.
    #[tokio::test]
    async fn certify_group() {
        let ctx = test_context();
        let commitment = Hash::new_unique();

        observe_block(&ctx, init_block(EpochNumber(5)))
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
        assert_eq!(group.blob.commitment, commitment);
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
        assert_eq!(group.blob.commitment, commitment);
    }

    // Certify materializes a local blob track into the generic store.
    #[tokio::test]
    async fn certify_materializes_local_track() {
        let ctx = test_context();
        let group = SpoolGroup(2);
        let owned_spool = group.spool_at(5);
        let blob = local_blob();
        let slice_bytes = vec![0xAB; 96];
        let epoch = EpochNumber(5);
        let track_number = TrackNumber(9);

        ctx.set_state(state_with_owned_spool(EpochNumber(6), owned_spool))
            .unwrap();

        ctx.store
            .put_epoch_info(
                epoch,
                SnapshotEpochInfo {
                    status: SnapshotEpochStatus::Built,
                    certified_groups: SnapshotGroupBitmap::zeroed(),
                },
            )
            .unwrap();
        let built = SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            blob,
            track_number: None,
        };
        ctx.store
            .put_group_info(epoch, group, built)
            .unwrap();
        ctx.store
            .put_group_slice(epoch, group, group.slice_of(owned_spool).unwrap(), slice_bytes.clone())
            .unwrap();

        observe_block(&ctx, init_block(epoch)).await.unwrap();
        observe_block(&ctx, certify_block(epoch, group, track_number, blob.commitment))
        .await
        .unwrap();

        let group = ctx
            .store
            .get_group_info(epoch, group)
            .unwrap()
            .unwrap();
        assert_eq!(group.status, SnapshotGroupStatus::CertifiedOnChain);
        assert_eq!(group.track_number, Some(track_number));
        assert_eq!(group.blob, blob);

        let (snapshot_tape, _) = snapshot_tape_pda(epoch);
        let track_address = track_pda(snapshot_tape, track_number).0;
        let tape = ctx.store.get_tape(snapshot_tape).unwrap().unwrap();
        assert_eq!(tape.next_track_number, TrackNumber(10));

        let track = ctx.store.get_track(track_address).unwrap().unwrap();
        assert_eq!(track.tape, snapshot_tape);
        assert_eq!(track.track_number, track_number);
        assert_eq!(track.size, blob.size);
        assert_eq!(track.value_hash, blob.get_hash());

        let track_data = ctx.store.get_track_data(track_address).unwrap().unwrap();
        assert_eq!(track_data, TrackData::Blob(blob));

        let object = ctx.store.get_object_info(track_address).unwrap().unwrap();
        assert_eq!(
            object,
            ObjectInfo::Valid {
                track_address,
                registered_epoch: epoch,
                certified_epoch: Some(epoch),
                slot: SlotNumber(epoch.0 * 10 + 1),
            }
        );

        let copied_slice = ctx.store.get_slice(owned_spool, track_address).unwrap().unwrap();
        assert_eq!(copied_slice, slice_bytes);
    }

    // FinalizeSnapshotEpoch sets status to Finalized.
    #[tokio::test]
    async fn finalize() {
        let ctx = test_context();

        observe_block(&ctx, init_block(EpochNumber(5)))
            .await
            .unwrap();
        observe_block(&ctx, finalize_block(EpochNumber(5)))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Finalized);
    }

    // Init then certify groups then finalize, verify final state.
    #[tokio::test]
    async fn full_lifecycle() {
        let ctx = test_context();
        let epoch = EpochNumber(5);

        observe_block(&ctx, init_block(epoch)).await.unwrap();

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

        observe_block(&ctx, finalize_block(epoch))
            .await
            .unwrap();

        let info = ctx.store.get_epoch_info(epoch).unwrap().unwrap();
        assert_eq!(info.status, SnapshotEpochStatus::Finalized);
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
