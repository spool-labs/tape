pub use crate::snapshot::{
    run_bootstrap, run_build, run_collect, run_register, run_submit,
};

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use solana_sdk::signature::Signer;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
    use tape_core::cert::snapshot::SnapshotMessage;
    use tape_core::erasure::{group_for_spool, SPOOL_GROUP_COUNT};
    use tape_core::types::ChunkIndex;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_crypto::Hash;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_store::types::{
        NodeInfo,
        Pubkey as StorePubkey,
        SnapshotCertResult,
        SnapshotChunkMeta,
        SnapshotPartialSignature,
        SPOOL_GROUP_SIZE,
    };
    use tokio_util::sync::CancellationToken;

    use crate::runtime::PeerService;
    use crate::runtime::test_utils::test_context;
    use crate::supervisor::TaskOutcome;

    fn mark_snapshot_build_complete<S: crate::store::Store, R: crate::rpc::Rpc>(
        ctx: &std::sync::Arc<crate::runtime::NodeContext<S, R>>,
        local_epoch: EpochNumber,
    ) {
        for group in 0..SPOOL_GROUP_COUNT {
            let chunk_index = ChunkIndex(group as u64);
            ctx.store
                .set_snapshot_commitment(local_epoch, chunk_index, Hash::new_unique())
                .unwrap();
            ctx.store
                .set_snapshot_metadata(
                    local_epoch,
                    chunk_index,
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
    }

    fn set_group_ready<S: crate::store::Store, R: crate::rpc::Rpc>(
        ctx: &std::sync::Arc<crate::runtime::NodeContext<S, R>>,
        local_epoch: EpochNumber,
        group: u64,
    ) {
        let chunk_index = ChunkIndex(group);
        ctx.store
            .set_snapshot_commitment(local_epoch, chunk_index, Hash::new_unique())
            .unwrap();
        ctx.store
            .set_snapshot_metadata(
                local_epoch,
                chunk_index,
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

    fn set_group_commitment_only<S: crate::store::Store, R: crate::rpc::Rpc>(
        ctx: &std::sync::Arc<crate::runtime::NodeContext<S, R>>,
        local_epoch: EpochNumber,
        group: u64,
    ) {
        let chunk_index = ChunkIndex(group);
        ctx.store
            .set_snapshot_commitment(local_epoch, chunk_index, Hash::new_unique())
            .unwrap();
    }

    #[tokio::test]
    async fn build_waits_epoch2() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn build_empty_epoch() {
        let ctx = test_context();
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(crate::snapshot::is_snapshot_build_complete(&ctx, local_epoch).unwrap());
    }

    #[tokio::test]
    async fn build_stores_commitments() {
        let ctx = test_context();
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        // Populate event log
        ctx.store
            .append_event(
                local_epoch,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // All 50 commitments stored
        for i in 0..SPOOL_GROUP_COUNT {
            assert!(
                ctx.store
                    .get_snapshot_commitment(local_epoch, ChunkIndex(i as u64))
                    .unwrap()
                    .is_some(),
                "commitment missing for chunk {i}"
            );
        }

        // All 50 metadata entries stored
        for i in 0..SPOOL_GROUP_COUNT {
            let meta = ctx
                .store
                .get_snapshot_metadata(local_epoch, ChunkIndex(i as u64))
                .unwrap();
            assert!(meta.is_some(), "metadata missing for chunk {i}");
            let meta = meta.unwrap();
            assert_eq!(meta.leaves.len(), SPOOL_GROUP_SIZE);
        }

        // Event log cleaned up
        assert!(!ctx.store.has_epoch_events(local_epoch).unwrap());
    }

    #[tokio::test]
    async fn bootstrap_early_epoch() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn bootstrap_idempotent_with_matching_marker() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        ctx.store.set_sync_cursor(SlotNumber(500)).unwrap();
        ctx.store.set_bootstrap_target_epoch(EpochNumber(2)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn bootstrap_marker_epoch_mismatch_retries() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        // Simulate already-synced cursor with wrong marker epoch.
        ctx.store.set_sync_cursor(SlotNumber(500)).unwrap();
        ctx.store.set_bootstrap_target_epoch(EpochNumber(4)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn build_idempotent() {
        let ctx = test_context();
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        mark_snapshot_build_complete(&ctx, local_epoch);

        // Add events (shouldn't be processed)
        ctx.store
            .append_event(
                local_epoch,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Build was skipped entirely
        for i in 0..SPOOL_GROUP_COUNT {
            assert!(
                ctx
                    .store
                    .get_snapshot_commitment(local_epoch, ChunkIndex(i as u64))
                    .unwrap()
                    .is_some(),
                "commitment missing for chunk {i}"
            );
        }

        // Event log should NOT have been deleted (build was skipped)
        assert!(ctx.store.has_epoch_events(local_epoch).unwrap());
    }

    #[tokio::test]
    async fn collect_resume() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey: BlsPubkey::zeroed(),
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();
        mark_snapshot_build_complete(&ctx, local_epoch);

        let group = group_for_spool(5);
        let chunk = ChunkIndex(group);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                chunk,
                SnapshotCertResult {
                    member_indices: vec![0],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_collect(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_collect(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn single_owner_collect() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        set_group_ready(&ctx, local_epoch, group);

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        let bls_pubkey = ctx.bls_keypair.public_key().unwrap();
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey,
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();

        let commitment = ctx
            .store
            .get_snapshot_commitment(local_epoch, ChunkIndex(group))
            .unwrap()
            .unwrap();
        let message = SnapshotMessage::new(local_epoch, commitment.into()).to_bytes();
        let signature = ctx.bls_keypair.sign(&message).unwrap();
        ctx.store
            .set_snapshot_partial_signature(
                local_epoch,
                group,
                SnapshotPartialSignature {
                    member_index: 0,
                    signature,
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_collect(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let cert = ctx
            .store
            .get_snapshot_cert(local_epoch, ChunkIndex(group))
            .unwrap();
        assert!(cert.is_some());
        let cert = cert.unwrap();
        assert_eq!(cert.member_indices, vec![0]);
    }

    #[tokio::test]
    async fn register_missing_metadata_is_hard_fail() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        set_group_commitment_only(&ctx, local_epoch, group);

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey: ctx.bls_keypair.public_key().unwrap(),
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_register(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Permanent(_)));
    }

    #[tokio::test]
    async fn submit_missing_commitment_is_hard_fail() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                ChunkIndex(group),
                SnapshotCertResult {
                    member_indices: vec![0],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey: ctx.bls_keypair.public_key().unwrap(),
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_submit(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Permanent(_)));
    }

    #[tokio::test]
    async fn build_unreachable_peer_fallback() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let local_epoch = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        let mut dead_addr = NetworkAddress::default();
        dead_addr.set_flags(2);
        let own_addr = NetworkAddress::new_ipv4([127, 0, 0, 1], 8000);

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![
                    NodeInfo {
                        node_address: StorePubkey::new(node_address.to_bytes()),
                        bls_pubkey: ctx.bls_keypair.public_key().unwrap(),
                        tls_pubkey: StorePubkey::new([0u8; 32]),
                        network_address: own_addr,
                        spools: vec![5],
                    },
                    NodeInfo {
                        node_address: StorePubkey::new_unique(),
                        bls_pubkey: BlsPrivateKey::from_random().public_key().unwrap(),
                        tls_pubkey: StorePubkey::new([1u8; 32]),
                        network_address: dead_addr,
                        spools: vec![6],
                    },
                ],
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let signature = ctx
            .store
            .get_snapshot_partial_signature(local_epoch, group, 0)
            .unwrap();
        assert!(signature.is_some());
        assert_eq!(signature.unwrap().member_index, 0);
    }
}
