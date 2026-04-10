use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::CERTIFY_SNAPSHOT_GROUP_CU;
use tape_api::instruction::build_certify_snapshot_group_ix;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;
use crate::features::snapshot::signing::SnapshotGroupCert;

pub async fn submit_certify_snapshot_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    blob: &BlobInfo,
    cert: &SnapshotGroupCert,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let ix = build_certify_snapshot_group_ix(
        fee_payer,
        epoch,
        cert.signing_epoch,
        group,
        blob,
        cert.bitmap,
        cert.signature,
    );

    ctx.rpc
        .send_instructions_with_compute_unit_limit(
            ctx.signer(),
            CERTIFY_SNAPSHOT_GROUP_CU,
            vec![ix],
        )
        .await
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::errors::TapeError;
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::spooler::SpoolGroup;
    use tape_core::system::EpochPhase;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{CommitteeBitmap, EpochNumber, StorageUnits, StripeCount};
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;

    use super::submit_certify_snapshot_group;
    use crate::chain::submit_init_snapshot_epoch;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
    use crate::features::snapshot::signing::SnapshotGroupCert;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const SNAPSHOT_EPOCH: EpochNumber = EpochNumber(2);
    const NODE: usize = 7;

    #[tokio::test]
    async fn rejects_without_quorum() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .no_prev_snapshot_manifest()
            .build()
            .await
            .expect("build harness");

        let ctx = harness.ctx_for(NODE);

        submit_init_snapshot_epoch(&ctx, SNAPSHOT_EPOCH)
            .await
            .expect("init snapshot epoch");

        let leaves = [Hash::default(); SPOOL_GROUP_SIZE];

        let blob = BlobInfo {
            size: StorageUnits::from_bytes(2_048),
            commitment: root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let cert = SnapshotGroupCert {
            signing_epoch: EPOCH,
            bitmap: CommitteeBitmap::zeroed(),
            signature: BlsSignature::zeroed(),
        };

        let outcome = classify_tx(
            submit_certify_snapshot_group(
                &ctx,
                SNAPSHOT_EPOCH,
                SpoolGroup(0),
                &blob,
                &cert,
            )
            .await,
        );

        assert!(matches!(outcome, TxOutcome::Program(TapeError::NoQuorum)));
    }
}
