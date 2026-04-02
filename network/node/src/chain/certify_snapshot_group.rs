use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::CERTIFY_SNAPSHOT_GROUP_CU;
use tape_api::instruction::build_certify_snapshot_group_ix;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::bls::BlsSignature;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{EpochNumber, StorageUnits, StripeCount};
use tape_crypto::Hash;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_certify_snapshot_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
    signing_epoch: EpochNumber,
    group: SpoolGroup,
    commitment: Hash,
    profile: EncodingProfile,
    stripe_size: StorageUnits,
    stripe_count: StripeCount,
    leaves: [Hash; SPOOL_GROUP_SIZE],
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(CERTIFY_SNAPSHOT_GROUP_CU);
    let ix = build_certify_snapshot_group_ix(
        fee_payer,
        snapshot_epoch,
        signing_epoch,
        group,
        commitment,
        profile,
        stripe_size,
        stripe_count,
        leaves,
        bitmap,
        signature,
    );

    ctx.rpc
        .send_instructions(ctx.signer(), vec![cu_ix, ix])
        .await
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::errors::TapeError;
    use tape_api::program::tapedrive::CommitteeBitmap;
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::spooler::SpoolGroup;
    use tape_core::system::EpochPhase;
    use tape_core::types::{EpochNumber, StorageUnits, StripeCount};
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;

    use super::submit_certify_snapshot_group;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
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
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let leaves = [Hash::default(); SPOOL_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        let outcome = classify_tx(
            submit_certify_snapshot_group(
                &ctx,
                SNAPSHOT_EPOCH,
                EPOCH,
                SpoolGroup(0),
                commitment,
                EncodingProfile::basic_default(),
                StorageUnits::from_bytes(512),
                StripeCount(4),
                leaves,
                CommitteeBitmap::zeroed(),
                BlsSignature::zeroed(),
            )
            .await,
        );

        assert!(matches!(outcome, TxOutcome::Program(TapeError::NoQuorum)));
    }
}
